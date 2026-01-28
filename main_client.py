import asyncio
import argparse
import uuid
from discovery.discovery_protocol import ServerRegistry, start_beacon_listener
from config.settings import MAIN_CLIENT_LOGGER, setup_client_file_logging, CLIENT_SERVER_DISCOVERY_TIMEOUT


'''
python main_client.py --discover_timeout=10 --client_id=ambulance_1  --client_type=Ambulance --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=ambulance_2  --client_type=Ambulance --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=hospital_1  --client_type=Hospital --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=car_1  --client_type=car --heartbeat_interval=15
'''

def parse_args():
    p = argparse.ArgumentParser(description="Main Client with Dynamic Discovery")
    p.add_argument("--discover_timeout", type=int, default=CLIENT_SERVER_DISCOVERY_TIMEOUT, help="Time to listen for beacons")
    p.add_argument("--client_id", type=str, default="ambulance_1")
    p.add_argument("--client_type", type=str, default="Ambulance")
    p.add_argument("--heartbeat_interval", type=int, default=15)
    p.add_argument("--logging_level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO")
    return p.parse_args()

'''
Leader discovery via beacon protocol.
'''
async def discover_leader_via_beacon(timeout=CLIENT_SERVER_DISCOVERY_TIMEOUT):
    """Listen to beacons and extract leader info"""
    registry = ServerRegistry()
    protocol, transport = await start_beacon_listener(registry)
    
    MAIN_CLIENT_LOGGER.info(f"Listening for server beacons for {timeout}s...")
    await asyncio.sleep(timeout)
    transport.close()

    leader_id = registry.get_leader_id()
    leader_info = registry.get_leader_info()
    
    if leader_info:
        MAIN_CLIENT_LOGGER.info(f"Discovered leader {leader_id[:8]} at {leader_info.host}:{leader_info.port}")
        return leader_info.host, leader_info.port
    
    # Fallback: connect to any discovered server
    servers = registry.get_all_servers()
    if servers:
        s = min(servers, key=lambda s:s.active_clients)  # choose server with least active clients
        MAIN_CLIENT_LOGGER.info(f"No leader found, connecting to server {s.server_id[:8]} at {s.host}:{s.port}")
        return s.host, s.port
    
    MAIN_CLIENT_LOGGER.error("No servers discovered")
    return None, None

'''
Client that discovers main server via beacon protocol.

Arguments:
- discover_timeout: Time to listen for beacons (default: 10s)
- client_id: Unique identifier for the client (default: ambulance_1)
- client_type: Type of client (e.g., Ambulance, Hospital) (default: Ambulance)
- heartbeat_interval: Interval for sending heartbeats to server (default: 15s)
- logging_level: Logging level (default: INFO)

Starting:
python3.14 main_client.py

Further information:
Python3.14 is required to run this server.
'''
async def main():
    from components.tcp_client import TCPClient

    args = parse_args()
    
    # Generate client UUID and setup file logging
    client_uuid = str(uuid.uuid7())
    log_path = setup_client_file_logging(args.client_id, args.client_type, args.logging_level)
    
    # Logging startup info
    MAIN_CLIENT_LOGGER.info(f"Starting client {args.client_id} ({args.client_type})")
    MAIN_CLIENT_LOGGER.info(f"Logs saved to: {log_path}")
    
    server_host, server_port = await discover_leader_via_beacon(args.discover_timeout)
    
    # If no server discovered, exit
    if not server_host:
        MAIN_CLIENT_LOGGER.error("No server discovered.")
        return
    
    # Start TCP client
    MAIN_CLIENT_LOGGER.info(f"Connecting to {server_host}:{server_port}")
    client = TCPClient(
        server_host,
        server_port,
        client_id=args.client_id,
        client_type=args.client_type,
        heartbeat_interval=args.heartbeat_interval,
    )
    await client.run()


if __name__ == "__main__":
    asyncio.run(main())