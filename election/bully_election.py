import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple, Callable

log = logging.getLogger("cluster.bully")

class BullyElection:
    """Reusable Bully algorithm implementation - works for zone OR global elections"""
    
    def __init__(self, 
                 scope: str,           # "zone_A", "global", etc (for logging/message type)
                 my_id: int,
                 transport,
                 heartbeat_ttl: int = 10,
                 answer_wait: float = 1.0):
        self.scope = scope
        self.my_id = my_id
        self.transport = transport
        self.heartbeat_ttl = timedelta(seconds=heartbeat_ttl)
        self.answer_wait = answer_wait
        
        # Peers: {peer_id -> (host, ctrl_port, metadata)}
        self.peers: Dict[int, Tuple[str, int, dict]] = {}
        self.leader_id: Optional[int] = None
        self.last_leader_hb: Optional[datetime] = None
        
        self._election_in_progress = False
        self._role = "FOLLOWER"
        self._got_answer = False
        
        # Message type prefix (zone/global)
        self._msg_prefix = scope.split('_')[0]  # "zone" or "global"

    def add_peer(self, peer_id: int, host: str, ctrl_port: int, metadata: dict = None):
        """Add/update a peer in this election scope"""
        self.peers[peer_id] = (host, ctrl_port, metadata or {})
        log.debug(f"[{self.scope}] Added peer {peer_id}")

    def remove_peer(self, peer_id: int):
        """Remove peer (server died or lost eligibility)"""
        self.peers.pop(peer_id, None)
        if self.leader_id == peer_id:
            self.leader_id = None
            self.last_leader_hb = None

    def on_message(self, msg: dict, addr: Tuple[str, int]) -> bool:
        """
        Handle election messages. Returns True if message was handled.
        Caller should filter by scope before calling.
        """
        msg_type = msg.get("type")
        
        if msg_type == f"{self._msg_prefix}_election":
            self._on_election(msg, addr)
            return True
        elif msg_type == f"{self._msg_prefix}_answer":
            self._on_answer(msg)
            return True
        elif msg_type == f"{self._msg_prefix}_coordinator":
            self._on_coordinator(msg)
            return True
        elif msg_type == f"{self._msg_prefix}_heartbeat":
            self._on_heartbeat(msg)
            return True
        return False

    def is_leader(self) -> bool:
        return self.leader_id == self.my_id

    def get_leader_id(self) -> Optional[int]:
        return self.leader_id

    def get_leader_addr(self) -> Optional[Tuple[str, int]]:
        """Returns (host, tcp_port) of current leader, or None"""
        if self.leader_id == self.my_id:
            return None  # Self
        peer = self.peers.get(self.leader_id)
        return (peer[0], peer[1]) if peer else None

    async def maybe_elect(self):
        """Check if election needed (no leader or leader timed out)"""
        if self.leader_id is None or self._leader_timed_out():
            if not self._election_in_progress:
                await self.start_election()

    def _leader_timed_out(self) -> bool:
        if self.leader_id == self.my_id:
            return False
        if not self.last_leader_hb:
            return True
        return (datetime.now() - self.last_leader_hb) > self.heartbeat_ttl

    async def start_election(self):
        """Initiate Bully election"""
        if self._election_in_progress:
            return
            
        self._election_in_progress = True
        self._role = "CANDIDATE"
        self._got_answer = False
        
        # Find peers with higher IDs
        higher_peers = [(host, port) for pid, (host, port, _) in self.peers.items() 
                        if pid > self.my_id]
        
        if not higher_peers:
            # I have highest ID - become leader immediately
            self._declare_leader()
            return
        
        # Send election message to all higher-ID peers
        for host, port in higher_peers:
            self.transport.send_to(host, port, {
                "type": f"{self._msg_prefix}_election",
                "scope": self.scope,
                "candidate_id": self.my_id
            })
        
        # Wait for answers
        await asyncio.sleep(self.answer_wait)
        
        if self._got_answer:
            # Higher peer responded - wait for them to become coordinator
            self._role = "FOLLOWER"
            self._election_in_progress = False
            log.debug(f"[{self.scope}] Higher peer answered, waiting for coordinator")
        else:
            # No higher peer responded - I become leader
            self._declare_leader()

    def _on_election(self, msg: dict, addr: Tuple[str, int]):
        """Handle incoming election message"""
        cand = msg.get("candidate_id")
        if cand is None or cand >= self.my_id:
            return
        
        # I have higher ID - send answer
        self.transport.send_to(addr[0], addr[1], {
            "type": f"{self._msg_prefix}_answer",
            "scope": self.scope,
            "responder_id": self.my_id
        })
        
        # Start my own election if not already running
        if not self._election_in_progress:
            asyncio.create_task(self.start_election())

    def _on_answer(self, msg: dict):
        """Handle answer from higher-ID peer"""
        self._got_answer = True

    def _on_coordinator(self, msg: dict):
        """Handle coordinator announcement"""
        leader = msg.get("leader_id")
        if leader is None:
            return
        
        self.leader_id = leader
        self._role = "FOLLOWER" if leader != self.my_id else "LEADER"
        self._election_in_progress = False
        log.info(f"[{self.scope}] Leader elected: {leader}")

    def _on_heartbeat(self, msg: dict):
        """Handle leader heartbeat"""
        leader = msg.get("leader_id")
        if leader is None:
            return
        
        self.leader_id = leader
        self.last_leader_hb = datetime.now()
        
        if leader != self.my_id:
            self._role = "FOLLOWER"

    def _declare_leader(self):
        """Declare self as leader and broadcast"""
        self.leader_id = self.my_id
        self._role = "LEADER"
        self._election_in_progress = False
        log.info(f"[{self.scope}] I am leader (id={self.my_id})")
        
        # Broadcast coordinator message to all peers
        peers = [(host, port) for host, port, _ in self.peers.values()]
        self.transport.send_to_many(peers, {
            "type": f"{self._msg_prefix}_coordinator",
            "scope": self.scope,
            "leader_id": self.my_id
        })

    async def send_heartbeat_loop(self):
        """Background task: send heartbeats when leader"""
        while True:
            await asyncio.sleep(2)
            if self.is_leader():
                peers = [(host, port) for host, port, _ in self.peers.values()]
                self.transport.send_to_many(peers, {
                    "type": f"{self._msg_prefix}_heartbeat",
                    "scope": self.scope,
                    "leader_id": self.my_id,
                    "ts": datetime.now().timestamp()
                })