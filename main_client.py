import asyncio
from components.tcp_client import TCPClient  # Your previously written client TCP code
from discovery.discovery_protocol import listen_for_beacons

async def discover_server(timeout=15):
    protocol, transport = await listen_for_beacons()
    servers = []

    def on_discovered(server_info):
        print(f"Discovered server: {server_info}")
        servers.append(server_info)

    protocol.on_server_discovered = on_discovered

    print(f"Listening for server beacons for {timeout} seconds...")
    await asyncio.sleep(timeout)

    transport.close()

    if servers:
        host, port = servers[0].split(':')
        return host, int(port)
    return None, None

async def main():
    client_id = "ambulance_1"
    server_host, server_port = await discover_server()
    if not server_host:
        print("No server discovered.")
        return

    print(f"Connecting to dynamic server {server_host}:{server_port}")
    client = TCPClient(server_host, server_port, client_id)
    await client.run()

if __name__ == "__main__":
    asyncio.run(main())
