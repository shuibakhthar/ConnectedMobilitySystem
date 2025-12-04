import asyncio
import socket
from config.settings import BEACON_INTERVAL, BEACON_PORT, DISCOVERY_LOGGER
import json

class BeaconServer:
    def __init__(self, server_host, server_port, beacon_zone=None, server_id=None, ctrl_port=None):
        self.server_host = server_host  # IP addr of TCP server to advertise
        self.server_port = server_port

        self.beacon_zone = beacon_zone
        self.server_id = server_id
        self.ctrl_port = ctrl_port

    async def beacon_task(self):
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)

        # Build message with zone info
        message = json.dumps({
            "type": "server_beacon",
            "client_addr": f"{self.server_host}:{self.server_port}",
            "host": self.server_host,
            "tcp_port": self.server_port,
            "zone": self.beacon_zone,
            "server_id": self.server_id,
            "ctrl_port": self.ctrl_port
        }).encode('utf-8')

        broadcast_addr = ('255.255.255.255', BEACON_PORT)

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
    def __init__(self):
        self.discovered_servers = set()
        self.on_server_discovered = None  # callback function
        self.on_client_discovered = None  # callback function

    def datagram_received(self, data, addr):
        try:
            raw = data.decode('utf-8')
            # Debug print to verify packet reception
            DISCOVERY_LOGGER.debug(f"Beacon received from {addr}: {raw}")
            # Try parsing as JSON (new format with zone)
            try:
                msg = json.loads(raw)
                if msg.get("type") == "server_beacon":
                    server_info = raw  # Pass the full JSON string
                    if server_info not in self.discovered_servers:
                        self.discovered_servers.add(server_info)
                        if self.on_server_discovered:
                            self.on_server_discovered(server_info, msg)  # Pass JSON string and parsed msg
                        return
            except json.JSONDecodeError:
                pass
            
            # Legacy format: "host:port"
            server_info = raw
            if server_info not in self.discovered_servers:
                self.discovered_servers.add(server_info)
                if self.on_server_discovered:
                    self.on_server_discovered(server_info, None)  # No zone info for legacy
        except Exception as e:
            DISCOVERY_LOGGER.error(f"Error processing beacon: {e}")

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
