import asyncio
import logging
from config.settings import ZONE, MY_SERVER_ID, MY_HOST, TCP_PORT, CTRL_PORT
from election.transport import create_control_transport
from election.bully_election import BullyElection
from discovery.discovery_protocol import listen_for_beacons

log = logging.getLogger("cluster")

class Coordinator:
    """Main coordinator - manages zone and global elections using BullyElection"""
    
    def __init__(self):
        self.zone = ZONE
        self.my_id = MY_SERVER_ID
        self.transport = None
        
        # Two instances of same algorithm: zone and global
        self.zone_election = None
        self.global_election = None

    async def start(self):
        # Create single shared transport
        self.transport = await create_control_transport(
            MY_HOST, CTRL_PORT,
            on_message=self._route_message
        )
        
        # Create zone election (scoped to this zone)
        self.zone_election = BullyElection(
            scope=f"zone_{self.zone}",
            my_id=self.my_id,
            transport=self.transport
        )
        
        # Create global election (among zone leaders)
        self.global_election = BullyElection(
            scope="global",
            my_id=self.my_id,
            transport=self.transport
        )
        
        # Listen for server beacons
        protocol, _ = await listen_for_beacons()
        protocol.on_server_discovered = self._on_server_discovered
        
        # Start background tasks
        asyncio.create_task(self._zone_election_loop())
        asyncio.create_task(self._global_election_loop())
        asyncio.create_task(self.zone_election.send_heartbeat_loop())
        asyncio.create_task(self.global_election.send_heartbeat_loop())
        
        log.info(f"Coordinator started: zone={self.zone}, id={self.my_id}")

    def _route_message(self, msg: dict, addr):
        """Route messages to appropriate election instance"""
        msg_type = msg.get("type", "")
        
        if msg_type.startswith("zone_"):
            # Check if it's for our zone
            if msg.get("scope") == f"zone_{self.zone}":
                self.zone_election.on_message(msg, addr)
        
        elif msg_type.startswith("global_"):
            self.global_election.on_message(msg, addr)

    def _on_server_discovered(self, msg: dict, addr):
        """Handle server beacon - add to zone election"""
        zone = msg.get("zone")
        if zone == self.zone:
            self.zone_election.add_peer(
                peer_id=msg["server_id"],
                host=msg["host"],
                ctrl_port=msg["ctrl_port"],
                metadata={"tcp_port": msg["tcp_port"]}
            )

    async def _zone_election_loop(self):
        """Periodically check zone election + update global eligibility"""
        prev_leader = None
        while True:
            await asyncio.sleep(1)
            await self.zone_election.maybe_elect()
            
            curr_leader = self.zone_election.get_leader_id()
            
            # If I became zone leader, register for global election
            if curr_leader == self.my_id and prev_leader != self.my_id:
                log.info(f"I became zone leader - joining global election")
                self.global_election.add_peer(
                    peer_id=self.my_id,
                    host=MY_HOST,
                    ctrl_port=CTRL_PORT,
                    metadata={"zone": self.zone}
                )
            
            # If I lost zone leadership, remove from global
            elif prev_leader == self.my_id and curr_leader != self.my_id:
                log.info(f"I lost zone leadership - leaving global election")
                self.global_election.remove_peer(self.my_id)
            
            prev_leader = curr_leader

    async def _global_election_loop(self):
        """Periodically check global election (only if zone leader)"""
        while True:
            await asyncio.sleep(1)
            if self.zone_election.is_leader():
                await self.global_election.maybe_elect()

    # Public API for TCPServer
    def is_zone_leader(self) -> bool:
        return self.zone_election.is_leader()

    def zone_leader_addr(self):
        return self.zone_election.get_leader_addr()

    def is_global_leader(self) -> bool:
        return self.global_election.is_leader()

    def get_roles(self) -> dict:
        """Debugging helper"""
        return {
            "zone": self.zone,
            "server_id": self.my_id,
            "zone_leader": self.zone_election.is_leader(),
            "zone_leader_id": self.zone_election.get_leader_id(),
            "global_leader": self.global_election.is_leader(),
            "global_leader_id": self.global_election.get_leader_id(),
        }