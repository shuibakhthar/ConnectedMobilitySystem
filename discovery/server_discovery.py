# # discovery/server_discovery.py
# import asyncio
# import json
# import socket
# import logging
# from typing import Callable, Optional, Tuple
# from config.settings import SERVER_DISCOVERY_PORT, BEACON_INTERVAL

# log = logging.getLogger("discovery.server")

# class ServerBeacon:
#     def __init__(self, zone: str, server_id: int, host: str, tcp_port: int, ctrl_port: int):
#         self.zone = zone
#         self.server_id = server_id
#         self.host = host
#         self.tcp_port = tcp_port
#         self.ctrl_port = ctrl_port

#     async def start(self):
#         loop = asyncio.get_running_loop()
#         sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
#         sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
#         sock.setblocking(False)
#         broadcast_addr = ('255.255.255.255', SERVER_DISCOVERY_PORT)
#         try:
#             while True:
#                 msg = {
#                     "type": "discover",
#                     "zone": self.zone,
#                     "server_id": self.server_id,
#                     "host": self.host,
#                     "tcp_port": self.tcp_port,
#                     "ctrl_port": self.ctrl_port,
#                 }
#                 data = json.dumps(msg).encode("utf-8")
#                 try:
#                     sock.sendto(data, broadcast_addr)
#                 except Exception as e:
#                     log.debug(f"Beacon send error: {e}")
#                 await asyncio.sleep(BEACON_INTERVAL)
#         finally:
#             sock.close()

# class ServerDiscovery(asyncio.DatagramProtocol):
#     def __init__(self, on_discovered: Optional[Callable[[dict, Tuple[str,int]], None]] = None):
#         self.on_discovered = on_discovered

#     def datagram_received(self, data: bytes, addr: Tuple[str,int]):
#         try:
#             msg = json.loads(data.decode("utf-8"))
#             if msg.get("type") == "discover":
#                 if self.on_discovered:
#                     self.on_discovered(msg, addr)
#         except Exception as e:
#             log.debug(f"Bad discovery packet from {addr}: {e}")

# async def start_server_discovery(on_discovered: Callable[[dict, Tuple[str,int]], None]):
#     loop = asyncio.get_event_loop()
#     transport, protocol = await loop.create_datagram_endpoint(
#         lambda: ServerDiscovery(on_discovered),
#         local_addr=('0.0.0.0', SERVER_DISCOVERY_PORT)
#     )
#     return transport, protocol