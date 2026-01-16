import sys
sys.path.append('.')
import asyncio
import socket
from config.settings import BEACON_INTERVAL, BEACON_PORT, DISCOVERY_LOGGER, LEADER_HEARTBEAT_TIMEOUT, REGISTRY_CLEANUP_INTERVAL
import json
import datetime
from components.tcp_server import ServerInfo
import time

'''
python discovery/discovery_protocol.py
'''


class ServerRegistry:
    def __init__(self, ttl_seconds=REGISTRY_CLEANUP_INTERVAL):
        self.servers = {}
        self.history = {}
        self.leader_id = None
        self.leader_info = None
        self.ttl_seconds = ttl_seconds  # server_id -> ServerInfo

    def register_server(self, beacon_msg, addr):
        server_info = ServerInfo.from_beacon(beacon_msg, last_seen=time.time())
        self.servers[server_info.server_id] = server_info
        self.history[server_info.server_id] = server_info
        if server_info.server_id == self.leader_id and time.time() - server_info.last_seen < LEADER_HEARTBEAT_TIMEOUT:
            self.leader_info = server_info

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
        for s in self.servers.values():
            s.leaderId = self.leader_id
            s.leaderInfo = self.leader_info.to_dict(shallow_leader=True) if self.leader_info else None
    
    '''def cleanup_stale_servers(self):
        now = time.time()
        stale_ids = [sid for sid, sinfo in self.servers.items() if (now - sinfo.last_seen) > self.ttl_seconds]
        for sid in stale_ids:
            del self.servers[sid]'''
    # revert

    def cleanup_stale_servers(self):
        now = time.time()
        stale_ids = [sid for sid, sinfo in self.servers.items() 
                    if (now - sinfo.last_seen) > self.ttl_seconds]  # 20s
        for sid in stale_ids:
            del self.servers[sid]
            DISCOVERY_LOGGER.debug(f"Pruned stale server {sid}")
        
        # FIXED: Clear stale leader
        if self.leader_id and self.leader_id not in self.servers:
            DISCOVERY_LOGGER.info(f"Cleared stale leader {self.leader_id}")
            self.leader_id = None
            self.leader_info = None

    def __repr__(self):
        return f"ServerRegistry(servers={self.servers})"
    
class BeaconServer:
    def __init__(self, server_host, server_port, zone=None, server_id=None, ctrl_port=None, serverInfo=None):
        self.server_host = server_host  # IP addr of TCP server to advertise
        self.server_port = server_port

        self.zone = zone
        self.server_id = server_id
        self.ctrl_port = ctrl_port
        self.serverInfo = serverInfo

    async def beacon_task(self):
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)

        # Build message: include server metadata if provided
        # if self.zone and self.server_id and self.ctrl_port:
        message = json.dumps({
                "type": "server_beacon",
                "client_addr": f"{self.server_host}:{self.server_port}",  # for clients
                # "zone": self.zone,
                "server_id": self.server_id,
                "host": self.server_host,
                "tcp_port": self.server_port,
                "ctrl_port": self.server_port + 1,
            }).encode('utf-8')
        # else:
        #     # Legacy client-only beacon
        #     message = f"{self.server_host}:{self.server_port}".encode('utf-8')

        # message = f"{self.server_host}:{self.server_port}".encode('utf-8')
        broadcast_addr = ('255.255.255.255', BEACON_PORT)  # Global broadcast

        while True:
            try:
                sock.sendto(message, broadcast_addr)
                DISCOVERY_LOGGER.debug(f"Beacon sent: {message.decode()} to {broadcast_addr}")
                # print(f"Beacon sent: {message.decode()} to {broadcast_addr}")
            except Exception as e:
                DISCOVERY_LOGGER.error(f"Beacon send error: {e}")
                # print(f"Beacon send error: {e}")
            await asyncio.sleep(BEACON_INTERVAL)

    async def start(self):
        await self.beacon_task()

class BeaconListener(asyncio.DatagramProtocol):
    def __init__(self, registry):
        self.registry = registry
        self.discovered_servers = set()
        self.on_server_discovered = None  # callback function
        self.on_client_discovered = None  # callback function

    def datagram_received(self, data, addr):
        server_info = data.decode('utf-8')
        try:
            msg = json.loads(server_info)
            if msg.get("type") == "server_beacon":
                DISCOVERY_LOGGER.debug(f"Server beacon from {addr}: {msg}")
                self.registry.register_server(msg, addr)
                # self.registry.cleanup_stale_servers()
                DISCOVERY_LOGGER.info(f"Updated registry: {server_info}")
        except Exception as e:
            DISCOVERY_LOGGER.error(f"Error parsing beacon data: {e}")
            return
        

        # Debug print to verify packet reception
        DISCOVERY_LOGGER.debug(f"Beacon received from {addr}: {server_info}")
        # print(f"Beacon received from {addr}: {server_info}")
        if server_info not in self.discovered_servers:
            self.discovered_servers.add(server_info)
            if self.on_server_discovered:
                self.on_server_discovered(server_info, addr)

    # def datagram_received(self, data, addr):
    #     try:
    #         # Try parsing as JSON (new format)
    #         msg = json.loads(data.decode('utf-8'))
    #         if msg.get("type") == "server_beacon":
    #             DISCOVERY_LOGGER.debug(f"Server beacon from {addr}: {msg}")
    #             if self.on_server_discovered:
    #                 self.on_server_discovered(msg, addr)
    #             # Also notify client callback with legacy format
    #             if self.on_client_discovered:
    #                 client_addr = msg.get("client_addr")
    #                 if client_addr:
    #                     self.on_client_discovered(client_addr)
    #     except json.JSONDecodeError:
    #         # Legacy format: "host:port"
    #         server_info = data.decode('utf-8')
    #         DISCOVERY_LOGGER.debug(f"Beacon received from {addr}: {server_info}")
    #         if server_info not in self.discovered_servers:
    #             self.discovered_servers.add(server_info)
    #             if self.on_client_discovered:
    #                 self.on_client_discovered(server_info)

async def listen_for_beacons():
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: BeaconListener(),
        local_addr=('0.0.0.0', BEACON_PORT)
    )
    return protocol, transport

async def cleanup_loop(registry):
    while True:
        await asyncio.sleep(REGISTRY_CLEANUP_INTERVAL)  # Every 10s
        registry.cleanup_stale_servers()
        DISCOVERY_LOGGER.debug("Cleanup completed")

async def handle_server_list_request(reader, writer, registry):
    '''# Send the current server list as JSON
    data = json.dumps([s.to_dict() for s in registry.get_all_servers()]).encode('utf-8')
    writer.write(data)
    await writer.drain()
    writer.close()
    await writer.wait_closed()'''

    # revert

    try:
        request_data = await reader.read(4096)
        DISCOVERY_LOGGER.debug(f"Received server list request: {request_data.decode()}")
        request_json = json.loads(request_data.decode()) if request_data else {}
        request_type = request_json.get("request_type", "get_registry")
    except Exception as e:
        request_type = "full_list"
        DISCOVERY_LOGGER.error(f"Error parsing server list request: {e}")
        writer.close()
        await writer.wait_closed()

    # Send the current server list as JSON
    leader_info = registry.get_leader_info()
    leader_id = registry.get_leader_id()
    server_list = [s.to_dict() for s in registry.get_all_servers()]

    if request_type == "leader_info":
        response = {
            "leader_id": leader_id,
            "leader_info": leader_info.to_dict(shallow_leader=True) if leader_info else None
        }
    elif request_type == "full_list":
        response = {
            "server_list": server_list
        }
    elif request_type == "update_leader_info":
        new_leader_id = request_json.get("leader_id")
        new_leader_info = request_json.get("leader_info")
        if new_leader_id and new_leader_info:
            registry.set_leader(new_leader_id, ServerInfo.from_dict(new_leader_info))
            DISCOVERY_LOGGER.info(f"Leader updated to {new_leader_id} via request")
            response = {"status": "success", "message": f"Leader updated to {new_leader_id}"}
        else:
            response = {"status": "error", "message": "Invalid leader info"}
    else:
        response = {
            "leader_id": leader_id,
            "leader_info": leader_info.to_dict(shallow_leader=True) if leader_info else None,
            "server_list": server_list
        }
    data = json.dumps(response).encode('utf-8')
    writer.write(data)
    await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():

    registry = ServerRegistry()

    asyncio.create_task(cleanup_loop(registry))

    loop = asyncio.get_event_loop()


    protocol, transport = await loop.create_datagram_endpoint(
        lambda: BeaconListener(registry),
        local_addr=('0.0.0.0', BEACON_PORT)
    )

    
    DISCOVERY_LOGGER.debug(f"Listening for beacons on {BEACON_PORT}...")

    server = await asyncio.start_server(
        lambda r, w: handle_server_list_request(r, w, registry), '127.0.0.1', BEACON_PORT - 1
    )
    DISCOVERY_LOGGER.debug(f"TCP server listening on {BEACON_PORT - 1}...")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())