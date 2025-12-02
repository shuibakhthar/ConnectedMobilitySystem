import asyncio
import json
import logging
from typing import Callable, Tuple

log = logging.getLogger("cluster.transport")

class ControlTransport(asyncio.DatagramProtocol):
    """Reusable UDP transport for inter-server control messages"""
    
    def __init__(self, on_message: Callable[[dict, Tuple[str,int]], None]):
        self.on_message = on_message
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        log.debug("Control transport ready")

    def datagram_received(self, data: bytes, addr: Tuple[str,int]):
        try:
            msg = json.loads(data.decode("utf-8"))
            self.on_message(msg, addr)
        except Exception as e:
            log.debug(f"Bad control msg from {addr}: {e}")

    def send_to(self, host: str, port: int, msg: dict):
        """Send message to specific peer"""
        if not self.transport:
            return
        try:
            data = json.dumps(msg).encode("utf-8")
            self.transport.sendto(data, (host, port))
        except Exception as e:
            log.debug(f"Send error to {host}:{port}: {e}")

    def send_to_many(self, peers: list, msg: dict):
        """Broadcast to multiple peers"""
        for host, port in peers:
            self.send_to(host, port, msg)

    def close(self):
        if self.transport:
            self.transport.close()

async def create_control_transport(host: str, port: int, on_message: Callable) -> ControlTransport:
    """Factory to create control transport bound to host:port"""
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ControlTransport(on_message),
        local_addr=(host, port)
    )
    return protocol