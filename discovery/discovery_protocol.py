import asyncio
import socket

BEACON_INTERVAL = 5  # seconds
BEACON_PORT = 9999  # UDP port for beacon

class BeaconServer:
    def __init__(self, server_host, server_port):
        self.server_host = server_host  # IP addr of TCP server to advertise
        self.server_port = server_port

    async def beacon_task(self):
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)

        message = f"{self.server_host}:{self.server_port}".encode('utf-8')
        broadcast_addr = ('255.255.255.255', BEACON_PORT)  # Global broadcast

        while True:
            try:
                sock.sendto(message, broadcast_addr)
                print(f"Beacon sent: {message.decode()} to {broadcast_addr}")
            except Exception as e:
                print(f"Beacon send error: {e}")
            await asyncio.sleep(BEACON_INTERVAL)

    async def start(self):
        await self.beacon_task()

class BeaconListener(asyncio.DatagramProtocol):
    def __init__(self):
        self.discovered_servers = set()
        self.on_server_discovered = None  # callback function

    def datagram_received(self, data, addr):
        server_info = data.decode('utf-8')
        # Debug print to verify packet reception
        print(f"Beacon received from {addr}: {server_info}")
        if server_info not in self.discovered_servers:
            self.discovered_servers.add(server_info)
            if self.on_server_discovered:
                self.on_server_discovered(server_info)

async def listen_for_beacons():
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: BeaconListener(),
        local_addr=('0.0.0.0', BEACON_PORT)
    )
    return protocol, transport
