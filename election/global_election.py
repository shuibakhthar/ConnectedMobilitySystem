import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Set

log = logging.getLogger("cluster.global")

class GlobalCoordinator:
    """Pure logic for global (interzone) Bully election - transport injected"""
    
    def __init__(self, my_id: int, transport, 
                 heartbeat_ttl: int = 10, answer_wait: float = 1.0):
        self.my_id = my_id
        self.transport = transport  # Injected ControlTransport
        self.heartbeat_ttl = timedelta(seconds=heartbeat_ttl)
        self.answer_wait = answer_wait
        
        # Zone leaders: {leader_id -> (host, ctrl_port, zone)}
        self.zone_leaders: Dict[int, Tuple[str, int, str]] = {}
        self.global_leader_id: Optional[int] = None
        self.last_global_hb: Optional[datetime] = None
        
        self._election_in_progress = False
        self._role = "FOLLOWER"
        self._got_answer = False

    def note_zone_leader(self, leader_id: int, host: str, ctrl_port: int, zone: str):
        """Register a zone leader (called when zone elections complete)"""
        self.zone_leaders[leader_id] = (host, ctrl_port, zone)

    def remove_zone_leader(self, leader_id: int):
        """Remove zone leader (when server dies or loses leadership)"""
        self.zone_leaders.pop(leader_id, None)
        if self.global_leader_id == leader_id:
            self.global_leader_id = None
            self.last_global_hb = None

    def on_message(self, msg: dict, addr: Tuple[str,int]):
        """Route global election messages"""
        msg_type = msg.get("type")
        if msg_type == "global_election":
            self._on_election(msg, addr)
        elif msg_type == "global_answer":
            self._on_answer(msg)
        elif msg_type == "global_coordinator":
            self._on_coordinator(msg)
        elif msg_type == "global_heartbeat":
            self._on_heartbeat(msg)

    def is_global_leader(self) -> bool:
        return self.global_leader_id == self.my_id

    def get_global_leader_info(self) -> Optional[Tuple[int, str]]:
        """Returns (leader_id, zone) or None"""
        if self.global_leader_id is None:
            return None
        leader_info = self.zone_leaders.get(self.global_leader_id)
        if leader_info:
            return (self.global_leader_id, leader_info[2])  # id, zone
        return None

    async def maybe_elect(self, i_am_zone_leader: bool):
        """Trigger global election if I'm a zone leader and no global leader exists"""
        if not i_am_zone_leader:
            # Only zone leaders participate in global election
            return
        
        if self.global_leader_id is None or self._global_leader_timed_out():
            if not self._election_in_progress:
                await self._start_election()

    def _global_leader_timed_out(self) -> bool:
        if self.global_leader_id == self.my_id:
            return False
        if not self.last_global_hb:
            return True
        return (datetime.now() - self.last_global_hb) > self.heartbeat_ttl

    async def _start_election(self):
        self._election_in_progress = True
        self._role = "CANDIDATE"
        self._got_answer = False
        
        # Find zone leaders with higher IDs
        higher = [(host, port) for lid, (host, port, zone) in self.zone_leaders.items() 
                  if lid > self.my_id]
        
        if not higher:
            self._declare_global_leader()
            return
        
        # Send election to higher-ID zone leaders
        for host, port in higher:
            self.transport.send_to(host, port, {
                "type": "global_election",
                "candidate_id": self.my_id
            })
        
        # Wait for answers
        await asyncio.sleep(self.answer_wait)
        
        if self._got_answer:
            self._role = "FOLLOWER"
            self._election_in_progress = False
        else:
            self._declare_global_leader()

    def _on_election(self, msg: dict, addr: Tuple[str,int]):
        cand = msg.get("candidate_id")
        if cand and self.my_id > cand:
            # I have higher ID, send answer
            self.transport.send_to(addr[0], addr[1], {
                "type": "global_answer",
                "responder_id": self.my_id
            })
            # Start my own election if not already running
            if not self._election_in_progress:
                asyncio.create_task(self._start_election())

    def _on_answer(self, msg: dict):
        self._got_answer = True

    def _on_coordinator(self, msg: dict):
        leader = msg.get("leader_id")
        if leader:
            self.global_leader_id = leader
            self._role = "FOLLOWER" if leader != self.my_id else "LEADER"
            self._election_in_progress = False
            log.info(f"[GLOBAL] Global leader = {leader}")

    def _on_heartbeat(self, msg: dict):
        leader = msg.get("leader_id")
        if leader:
            self.global_leader_id = leader
            self.last_global_hb = datetime.now()
            if leader != self.my_id:
                self._role = "FOLLOWER"

    def _declare_global_leader(self):
        self.global_leader_id = self.my_id
        self._role = "LEADER"
        self._election_in_progress = False
        log.info(f"[GLOBAL] I am global leader (id={self.my_id})")
        
        # Broadcast coordinator to all zone leaders
        peers = [(host, port) for host, port, zone in self.zone_leaders.values()]
        self.transport.send_to_many(peers, {
            "type": "global_coordinator",
            "leader_id": self.my_id
        })

    async def send_heartbeat_loop(self, i_am_zone_leader_func):
        """Background task: send global heartbeats if I'm the global leader"""
        while True:
            await asyncio.sleep(3)
            if i_am_zone_leader_func() and self.is_global_leader():
                peers = [(host, port) for host, port, zone in self.zone_leaders.values()]
                self.transport.send_to_many(peers, {
                    "type": "global_heartbeat",
                    "leader_id": self.my_id,
                    "ts": datetime.now().timestamp()
                })