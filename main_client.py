import asyncio
import argparse
import uuid
from discovery.discovery_protocol import ServerRegistry, start_beacon_listener
from config.settings import MAIN_CLIENT_LOGGER, setup_client_file_logging


def parse_args():
    p = argparse.ArgumentParser(description="Main Client with Dynamic Discovery")
    p.add_argument("--discover_timeout", type=int, default=10, help="Time to listen for beacons")
    p.add_argument("--client_id", type=str, default="ambulance_1")
    p.add_argument("--client_type", type=str, default="Ambulance")
    p.add_argument("--heartbeat_interval", type=int, default=15)
    return p.parse_args()


async def discover_leader_via_beacon(timeout=10):
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
        s = servers[0]
        MAIN_CLIENT_LOGGER.info(f"No leader found, connecting to server {s.server_id[:8]} at {s.host}:{s.port}")
        return s.host, s.port
    
    MAIN_CLIENT_LOGGER.error("No servers discovered")
    return None, None


async def main():
    from components.tcp_client import TCPClient

    args = parse_args()
    
    # Generate client UUID and setup file logging
    client_uuid = str(uuid.uuid7())
    log_path = setup_client_file_logging(args.client_id, args.client_type)
    
    MAIN_CLIENT_LOGGER.info(f"Starting client {args.client_id} ({args.client_type})")
    MAIN_CLIENT_LOGGER.info(f"Logs saved to: {log_path}")
    
    server_host, server_port = await discover_leader_via_beacon(args.discover_timeout)
    
    if not server_host:
        MAIN_CLIENT_LOGGER.error("No server discovered.")
        return
    
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