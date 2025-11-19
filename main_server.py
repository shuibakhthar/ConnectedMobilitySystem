import asyncio
from components.tcp_server import TCPServer
from discovery.discovery_protocol import BeaconServer

async def main():
    host = '127.0.0.1'
    tcp_port = 8888
    beacon_port = 9999  # Predefined constant for beacon

    server = TCPServer(host, tcp_port)
    beacon = BeaconServer(host, tcp_port)

    # Run TCP server and beacon broadcaster concurrently
    await asyncio.gather(
        server.start(),
        beacon.start()
    )

if __name__ == "__main__":
    asyncio.run(main())
