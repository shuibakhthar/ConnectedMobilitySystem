import asyncio
from components.tcp_server import TCPServer, ServerRegistry, ServerInfo
from discovery.discovery_protocol import BeaconServer, listen_for_beacons
from config.settings import MAIN_SERVER_LOGGER
import argparse
import datetime
import json
'''
python main_server.py --host=127.0.0.1 --tcp_port=8888 --beacon_port=9999
'''

def parse_args():
    p = argparse.ArgumentParser(description="Main Server with TCP and Beacon")
    p.add_argument('--host', type=str, default='127.0.0.1')
    p.add_argument('--tcp_port', type=int, default=8888)
    p.add_argument('--beacon_port', type=int, default=9999)
    return p.parse_args()

async def main():
    # host = '127.0.0.1'
    # tcp_port = 8888
    # beacon_port = 9999  # Predefined constant for beacon

    args = parse_args()

    server = TCPServer(args.host, args.tcp_port, args.tcp_port + 1)
    server_registry = ServerRegistry()
    beacon = BeaconServer(args.host, args.tcp_port, server_id=server.uid)

    listener, transport = await listen_for_beacons()

    def on_server_discovered(server_info, addr):
        try:
            server_registry.register_server(server_info, addr)
            server_registry.cleanup_stale_servers()
            MAIN_SERVER_LOGGER.info(f"Updated registry: {server_info}")
        except Exception as e:
            MAIN_SERVER_LOGGER.error(f"Error parsing beacon data: {e}")
            return


    listener.on_server_discovered = on_server_discovered

    MAIN_SERVER_LOGGER.info(f"Starting Main Server on {args.host}:{args.tcp_port} with Beacon on port {args.beacon_port}") 

    # Run TCP server and beacon broadcaster concurrently
    await asyncio.gather(
        server.start(),
        beacon.start()
    )

if __name__ == "__main__":
    asyncio.run(main())


'''

async def main():
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    
    # TCP server for clients
    server = TCPServer('0.0.0.0', TCP_PORT)
    
    # Beacon with server metadata (reuse existing BeaconServer)
    beacon = BeaconServer(
        server_host=MY_HOST,
        server_port=TCP_PORT,
        zone=ZONE,
        server_id=MY_SERVER_ID,
        ctrl_port=CTRL_PORT
    )
    
    # Coordinator (handles discovery + election)
    coord = Coordinator()
    
    await asyncio.gather(
        coord.start(),
        server.start(),
        beacon.start()
    )
'''