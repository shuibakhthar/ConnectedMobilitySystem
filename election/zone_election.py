import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from dataclasses import dataclass

log = logging.getLogger("cluster.zone")

@dataclass
class Peer:
    server_id: int
    host: str
    tcp_port: int
    ctrl_port: int
    last_seen: datetime

class ZoneCoordinator:
    """Pure logic for per-zone Bully election - transport injected"""
    
    def __init__(self, zone: str, my_id: int, transport, 
                 heartbeat_ttl: int = 10, answer_wait: float = 1.0):
        self.zone = zone
        self.my_id = my_id
        self.transport = transport  # Injected ControlTransport
        self.heartbeat_ttl = timedelta(seconds=heartbeat_ttl)
        self.answer_wait = answer_wait
        
        self.zone_members: Dict[int, Peer] = {}
        self.zone_leader_id: Optional[int] = None
        self.last_leader_hb: Optional[datetime] = None
        
        self._election_in_progress = False
        self._role = "FOLLOWER"
        self._got_answer = False

    def note_member(self, server_id: int, host: str, tcp_port: int, ctrl_port: int):
        """Called by discovery when server beacon received"""
        self.zone_members[server_id] = Peer(server_id, host, tcp_port, ctrl_port, datetime.now())

    def on_message(self, msg: dict, addr: Tuple[str,int]):
        """Route control messages (called by transport)"""
        if msg.get("zone") != self.zone:
            return
        msg_type = msg.get("type")
        if msg_type == "zone_election":
            self._on_election(msg, addr)
        elif msg_type == "zone_answer":
            self._on_answer(msg)
        elif msg_type == "zone_coordinator":
            self._on_coordinator(msg)
        elif msg_type == "zone_heartbeat":
            self._on_heartbeat(msg)

    def is_leader(self) -> bool:
        return self.zone_leader_id == self.my_id

    def get_leader_addr(self) -> Optional[Tuple[str, int]]:
        lid = self.zone_leader_id
        if lid == self.my_id:
            return None  # Would need own TCP addr
        peer = self.zone_members.get(lid)
        return (peer.host, peer.tcp_port) if peer else None

    async def maybe_elect(self):
        """Trigger election if no leader or leader timed out"""
        if self.zone_leader_id is None or self._leader_timed_out():
            if not self._election_in_progress:
                await self._start_election()

    def _leader_timed_out(self) -> bool:
        if self.zone_leader_id == self.my_id:
            return False
        if not self.last_leader_hb:
            return True
        return (datetime.now() - self.last_leader_hb) > self.heartbeat_ttl

    async def _start_election(self):
        self._election_in_progress = True
        self._role = "CANDIDATE"
        self._got_answer = False
        
        higher = [p for sid, p in self.zone_members.items() if sid > self.my_id]
        if not higher:
            self._declare_leader()
            return
        
        # Send election to higher IDs
        for peer in higher:
            self.transport.send_to(peer.host, peer.ctrl_port, {
                "type": "zone_election",
                "zone": self.zone,
                "candidate_id": self.my_id
            })
        
        # Wait for answers
        await asyncio.sleep(self.answer_wait)
        
        if self._got_answer:
            self._role = "FOLLOWER"
            self._election_in_progress = False
        else:
            self._declare_leader()

    def _on_election(self, msg: dict, addr: Tuple[str,int]):
        cand = msg.get("candidate_id")
        if cand and self.my_id > cand:
            self.transport.send_to(addr[0], addr[1], {
                "type": "zone_answer",
                "zone": self.zone,
                "responder_id": self.my_id
            })
            if not self._election_in_progress:
                asyncio.create_task(self._start_election())

    def _on_answer(self, msg: dict):
        self._got_answer = True

    def _on_coordinator(self, msg: dict):
        leader = msg.get("leader_id")
        if leader:
            self.zone_leader_id = leader
            self._role = "FOLLOWER" if leader != self.my_id else "LEADER"
            self._election_in_progress = False
            log.info(f"[{self.zone}] Zone leader = {leader}")

    def _on_heartbeat(self, msg: dict):
        leader = msg.get("leader_id")
        if leader:
            self.zone_leader_id = leader
            self.last_leader_hb = datetime.now()
            if leader != self.my_id:
                self._role = "FOLLOWER"

    def _declare_leader(self):
        self.zone_leader_id = self.my_id
        self._role = "LEADER"
        self._election_in_progress = False
        log.info(f"[{self.zone}] I am zone leader (id={self.my_id})")
        
        # Broadcast coordinator
        peers = [(p.host, p.ctrl_port) for p in self.zone_members.values()]
        self.transport.send_to_many(peers, {
            "type": "zone_coordinator",
            "zone": self.zone,
            "leader_id": self.my_id
        })

    async def send_heartbeat_loop(self):
        """Background task: send heartbeats if leader"""
        while True:
            await asyncio.sleep(2)
            if self.is_leader():
                peers = [(p.host, p.ctrl_port) for p in self.zone_members.values()]
                self.transport.send_to_many(peers, {
                    "type": "zone_heartbeat",
                    "zone": self.zone,
                    "leader_id": self.my_id,
                    "ts": datetime.now().timestamp()
                })