import asyncio
import argparse
import logging
from components.tcp_server import TCPServer
from discovery.discovery_protocol import ServerRegistry, BeaconServer, start_beacon_listener, cleanup_loop
from config.settings import MAIN_SERVER_LOGGER, BEACON_PORT, setup_server_file_logging, get_local_ip

''' python main_server.py --tcp_port=8000'''

def parse_args():
    p = argparse.ArgumentParser(description="Main Server with Dynamic Discovery")
    p.add_argument("--host", type=str, default=None, help="Host IP (auto-detected if not provided)")
    p.add_argument("--tcp_port", type=int, default=8888)
    p.add_argument("--logging_level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    return p.parse_args()

'''
Main server with dynamic discovery via beacon protocol.

Arguments:
- host: Host IP address (auto-detected if not provided)
- tcp_port: TCP port for the main server (default: 8888)
- logging_level: Logging level (default: INFO)

Starting:
python3.14 main_server.py --tcp_port=8000

Further information:
Python3.14 is required to run this server.
'''
async def main():
    args = parse_args()

    host = args.host if args.host else get_local_ip()

    # Create shared registry
    registry = ServerRegistry()
    
    # Create TCP server
    server = TCPServer(host, args.tcp_port, args.tcp_port + 1, registry=registry)

    # Setup file logging
    log_path = setup_server_file_logging(host, args.tcp_port, args.logging_level)

    MAIN_SERVER_LOGGER.info(
        f"Starting server {server.serverInfo.server_id[:8]} on {host}:{args.tcp_port}"
    )
    MAIN_SERVER_LOGGER.info(f"Logs saved to: {log_path}")

    # Start beacon listener (receives beacons from other servers)
    await start_beacon_listener(registry)
    
    # Start beacon broadcaster (advertises this server)
    beacon_task = asyncio.create_task(BeaconServer(server.serverInfo, registry).start())
    
    # Start cleanup task
    cleanup_task = asyncio.create_task(cleanup_loop(registry))

    # Start TCP server
    try:
        await server.start()
    finally:
        beacon_task.cancel()
        cleanup_task.cancel()
        await asyncio.gather(beacon_task, cleanup_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())