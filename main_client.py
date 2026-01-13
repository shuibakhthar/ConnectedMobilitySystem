import asyncio
from components.tcp_client import TCPClient  # Your previously written client TCP code
from discovery.discovery_protocol import listen_for_beacons
from config.settings import MAIN_CLIENT_LOGGER, REGISTRY_HOST, REGISTRY_PORT
import argparse
import json
'''
python main_client.py --discover_timeout=10 --client_id=ambulance_1  --client_type=Ambulance --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=ambulance_2  --client_type=Ambulance --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=hospital_1  --client_type=Hospital --heartbeat_interval=15
python main_client.py --discover_timeout=10 --client_id=car_1  --client_type=car --heartbeat_interval=15
'''


def parse_args():
    p = argparse.ArgumentParser(description="Main Client with Dynamic Server Discovery")
    p.add_argument('--discover_timeout', type=int, default=15, help="Time to listen for server beacons")
    p.add_argument('--client_id', type=str, default="ambulance_1", help="client ID to run")
    p.add_argument('--client_type', type=str, default="Ambulance", help="Type of the client")
    p.add_argument('--heartbeat_interval', type=int, default=15, help="Interval for sending heartbeats")
    return p.parse_args()

async def discover_leader(timeout=15):
    try: 
        MAIN_CLIENT_LOGGER.info("Discovering leader server...")
        reader, writer = await asyncio.open_connection(REGISTRY_HOST, REGISTRY_PORT)
        request_msg = json.dumps({"request_type": "leader_info"}).encode('utf-8')
        writer.write(request_msg)
        await writer.drain()
        data = await reader.readline()
        writer.close()
        await writer.wait_closed()

    
        response = json.loads(data.decode('utf-8'))
        MAIN_CLIENT_LOGGER.info(f"Leader discovery response: {response}")
        leader_info = response.get("leader_info")
        leader_id = response.get("leader_id")
        if leader_info:
            host = leader_info.get('host')
            port = leader_info.get('port')
            MAIN_CLIENT_LOGGER.info(f"Discovered leader server: {leader_id} at {host}:{port}")
            return host, int(port)
    except Exception as e:
        MAIN_CLIENT_LOGGER.error(f"Error discovering leader server: {e}")
        return None, None
    

async def discover_server(timeout=15):
    protocol, transport = await listen_for_beacons()
    servers = []

    def on_discovered(server_info, addr):
        MAIN_CLIENT_LOGGER.info(f"Discovered server: {server_info}")
        # print(f"Discovered server: {server_info}")
        servers.append(server_info)

    protocol.on_server_discovered = on_discovered

    MAIN_CLIENT_LOGGER.info(f"Listening for server beacons for {timeout} seconds...")
    # print(f"Listening for server beacons for {timeout} seconds...")
    await asyncio.sleep(timeout)

    transport.close()

    if servers:
        try:
            server_data = json.loads(servers[0]) if isinstance(servers[0], str) else servers[0]
            host = server_data.get('host')
            port = server_data.get('tcp_port')
            return host, int(port)
        except (json.JSONDecodeError, AttributeError, ValueError) as e:
            MAIN_CLIENT_LOGGER.error(f"Error parsing server data: {e}")
            return None, None
    return None, None

async def main():
    args = parse_args()
    
    # client_ids = ("ambulance_1","ambulance_2", "hospital_1","car_1")
    # server_host, server_port = await discover_server(args.discover_timeout if hasattr(args, 'discover_timeout') else 15)
    server_host, server_port = await discover_leader(args.discover_timeout if hasattr(args, 'discover_timeout') else 15)

    if not server_host:
        MAIN_CLIENT_LOGGER.info("No server discovered.")
        # print("No server discovered.")
        return
    try:
        MAIN_CLIENT_LOGGER.info(f"Connecting to dynamic server {server_host}:{server_port}")
        # print(f"Connecting to dynamic server {server_host}:{server_port}")
        client = TCPClient(server_host, server_port, client_id = args.client_id, client_type = args.client_type, heartbeat_interval=args.heartbeat_interval)
        await client.run()
    except KeyboardInterrupt:
        MAIN_CLIENT_LOGGER.info("Client shutting down")
        # print("Client shutting down")
    except Exception as e:
        MAIN_CLIENT_LOGGER.error(f"Error in client operation: {e}")
        # print(f"Error in client operation: {e}")  

if __name__ == "__main__":
    asyncio.run(main())
