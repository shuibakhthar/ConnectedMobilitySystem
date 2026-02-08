import sys
sys.path.append('.')
import asyncio
import socket
import json
import time
from config.settings import (
    BEACON_INTERVAL,
    BEACON_PORT,
    DISCOVERY_LOGGER,
    LEADER_HEARTBEAT_TIMEOUT,
    REGISTRY_CLEANUP_INTERVAL,
)
from components.tcp_server import ServerInfo


class ServerRegistry:
    def __init__(self, ttl_seconds=REGISTRY_CLEANUP_INTERVAL, is_client=False):
        self.servers = {}
        self.history = {}
        self.leader_id = None
        self.leader_info = None
        self.ttl_seconds = ttl_seconds
        self.global_seq_counter = 0
        self.last_election_time = 0
        self.is_client = is_client

    def register_server(self, beacon_msg, addr):
        server_info = ServerInfo.from_beacon(beacon_msg, last_seen=time.time())
        self.servers[server_info.server_id] = server_info
        self.history[server_info.server_id] = server_info
        
        # Update leader from beacon
        beacon_leader_id = beacon_msg.get("leader_id")
        beacon_leader_info = beacon_msg.get("leader_info")
        if beacon_leader_id:
            time_since_last_election = time.time() - self.last_election_time
            if not self.leader_id or time_since_last_election > LEADER_HEARTBEAT_TIMEOUT:
                if self.leader_id != beacon_leader_id:
                    if not self.is_client:
                        DISCOVERY_LOGGER.info(f"Updating leader from beacon: {beacon_leader_id} previous leader: {self.leader_id if self.leader_id else 'None'} ")
                    self.last_election_time = time.time()
                        
                self.leader_id = beacon_leader_id
                if beacon_leader_info:
                    self.leader_info = ServerInfo.from_dict(beacon_leader_info)
            else:
                if not self.is_client:
                    DISCOVERY_LOGGER.info(f"Received leader {beacon_leader_id} from beacon but ignoring due to recent election ({time_since_last_election:.1f}s ago)")

    def get_server(self, server_id):
        return self.servers.get(server_id)

    def get_all_servers(self):
        return list(self.servers.values())

    def get_history(self):
        return list(self.history.values())

    def get_leader_id(self):
        return self.leader_id

    def get_leader_info(self):
        return self.leader_info

    def set_leader(self, server_id, server_info=None):
        self.leader_id = server_id
        self.leader_info = server_info if server_info else self.servers.get(server_id)
        self.last_election_time = time.time()
        DISCOVERY_LOGGER.info(f"New leader elected: {self.leader_id}")
        for s in self.servers.values():
            s.leaderId = self.leader_id
            s.leaderInfo = (
                self.leader_info.to_dict(shallow_leader=True)
                if self.leader_info
                else None
            )

    def update_global_seq_counter_from_eventlog(self, event_log):
        if event_log:
            max_seq = max(event['seq'] for event in event_log if 'seq' in event)
            self.global_seq_counter = max(self.global_seq_counter, max_seq)
        else:
            self.global_seq_counter = max(self.global_seq_counter, 0)
            
    def cleanup_stale_servers(self):
        now = time.time()
        stale_ids = [
            sid
            for sid, sinfo in self.servers.items()
            if (now - sinfo.last_seen) > self.ttl_seconds
        ]
        for sid in stale_ids:
            del self.servers[sid]
            DISCOVERY_LOGGER.debug(f"Pruned stale server {sid}")

        # Clear stale leader
        if self.leader_id and self.leader_id not in self.servers:
            DISCOVERY_LOGGER.info(f"Cleared stale leader {self.leader_id}")
            self.leader_id = None
            self.leader_info = None

    def __repr__(self):
        return f"ServerRegistry(servers={self.servers})"


class BeaconServer:
    def __init__(self, server_info, registry):
        self.server_info = server_info
        self.registry = registry

    async def beacon_task(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)

        broadcast_addr = ("255.255.255.255", BEACON_PORT)

        while True:
            try:
                # Build message with current leader info
                msg = {
                    "type": "server_beacon",
                    "server_id": self.server_info.server_id,
                    "host": self.server_info.host,
                    "tcp_port": self.server_info.port,
                    "ctrl_port": self.server_info.ctrl_port,
                    "leader_id": self.registry.get_leader_id(),
                    "leader_info": self.registry.get_leader_info().to_dict(shallow_leader=True) if self.registry.get_leader_info() else None,
                    "active_clients": self.server_info.active_clients,
                    "resources": self.server_info.get_resources(),
                }
                sock.sendto(json.dumps(msg).encode("utf-8"), broadcast_addr)
                DISCOVERY_LOGGER.debug(
                    f"Beacon sent: server_id={self.server_info.server_id}, leader={self.registry.get_leader_id()}"
                )
            except Exception as e:
                DISCOVERY_LOGGER.error(f"Beacon send error: {e}")
            await asyncio.sleep(BEACON_INTERVAL)

    async def start(self):
        await self.beacon_task()


class BeaconListener(asyncio.DatagramProtocol):
    def __init__(self, registry, on_server_discovered=None):
        self.registry = registry
        self.discovered_servers = set()
        self.on_server_discovered = on_server_discovered

    def datagram_received(self, data, addr):
        raw = data.decode("utf-8")
        try:
            msg = json.loads(raw)
            if msg.get("type") == "server_beacon":
                DISCOVERY_LOGGER.debug(f"Server beacon from {addr}: {msg}")
                self.registry.register_server(msg, addr)
                if self.on_server_discovered:
                    self.on_server_discovered(msg, addr)
        except Exception as e:
            DISCOVERY_LOGGER.error(f"Error parsing beacon data: {e}")
            return

        if raw not in self.discovered_servers:
            self.discovered_servers.add(raw)


async def start_beacon_listener(registry, on_server_discovered=None):
    loop = asyncio.get_event_loop()
    
    # Create socket with SO_REUSEADDR for Windows multi-process support
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    # Try SO_REUSEPORT if available
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    except (AttributeError, OSError):
        pass
    
    sock.bind(("0.0.0.0", BEACON_PORT))
    sock.setblocking(False)
    
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: BeaconListener(registry, on_server_discovered=on_server_discovered),
        sock=sock,
    )
    return protocol, transport


async def cleanup_loop(registry):
    while True:
        await asyncio.sleep(REGISTRY_CLEANUP_INTERVAL)
        registry.cleanup_stale_servers()
        DISCOVERY_LOGGER.debug("Cleanup completed")