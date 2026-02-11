import logging
import os
from pathlib import Path
import socket

def get_local_ip():
    """Auto-detect local network IP, filtering out virtual adapters"""
    try:
        # Prefer UDP method (returns the IP used for outbound traffic / default route)
        virtual_prefixes = ('127.', '172.17.', '192.168.56.', '192.168.99.', '10.0.2.')
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            # connect to a public IP (no packets actually sent) to determine outbound interface
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            if not any(ip.startswith(prefix) for prefix in virtual_prefixes):
                return ip
        except Exception:
            # If UDP method fails, fall back to hostname resolution below
            pass

        # Fallback: get all network interfaces via hostname resolution
        hostname = socket.gethostname()
        all_ips = socket.gethostbyname_ex(hostname)[2]

        # Return first non-virtual IP from hostname list
        for ip in all_ips:
            if not any(ip.startswith(prefix) for prefix in virtual_prefixes):
                return ip

        # If still not found, return the UDP-derived ip if we have it, else localhost
        try:
            return ip  # from UDP method if present
        except Exception:
            return "127.0.0.1"
    except Exception as e:
        # Fallback to localhost
        return "127.0.0.1"

# Logging configuration
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'

def setup_server_file_logging(host, port, logging_level, server_id=None):
    """Setup file logging for server"""
    log_filename = f"{host}_{port}_{server_id[:8]}.log" if server_id else f"{host}_{port}.log"
    log_path = LOG_DIR / log_filename
    
    file_handler = logging.FileHandler(log_path, mode='w')
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    
    # Add to all loggers
    for logger_name in ["MainServer", "TCPServer", "DiscoveryProtocol", "BullyElection"]:
        logger = logging.getLogger(logger_name)
        logger.addHandler(file_handler)
        logger.setLevel(logging_level)
    
    return str(log_path)

def setup_client_file_logging(client_id, client_type, logging_level, client_uuid=None):
    """Setup file logging for client - attach to both MainClient and TCPClient loggers"""
    log_filename = f"{client_id}_{client_type}_{client_uuid[:8]}.log" if client_uuid else f"{client_id}_{client_type}.log"
    log_path = LOG_DIR / log_filename
    
    file_handler = logging.FileHandler(log_path, mode='w')
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=LOG_DATE_FORMAT))
    
    # Add to both MainClient and TCPClient loggers
    for logger_name in ["MainClient", "TCPClient", "MainServer", "TCPServer", "DiscoveryProtocol", "BullyElection"]:
        logger = logging.getLogger(logger_name)
        logger.addHandler(file_handler)
        logger.setLevel(logging_level)  # Ensure logger itself passes DEBUG messages
    
    return str(log_path)



SERVER_STATUS = [
    "ack_register", "ack_crash", "ack_location_update",
    "ack_on_duty", "ack_answer_call", "ack_arrived_at_scene", "ack_transporting_patient", "ack_at_hospital", "ack_available",
    "ack_light_green", "ack_light_yellow", "ack_light_red",
    "ack_open", "ack_closed", "ack_occupancy_update",
    "dispose_crash", "client_reconnected",
    "election_start","election_ack_ok", "election_coordinator", 
    "dispatch_ambulance","heartbeat", "assign_patient_to_hospital", 
    "request_server_assignment", "assign_server", "help_coming",
    "request_workflow", "workflow_event","workflow_event_batch", "workflow_completed", "execute_command", "client_reconnected", "status_update_report", "request_buffered_events"]

CLIENT_TYPES = ["Ambulance", "Car", "TrafficLight", "Hospital"]
CLIENT_STATUS = {
    "AllClients": ["register", "location_update", "heartbeat","request_server_assignment", "ack_crash_response"],
    "Ambulance": ["on_duty", "report_crash", "answer_call", "arrived_at_scene", "transporting_patient", "at_hospital", "available"],
    "Car": ["report_crash"],
    "TrafficLight": ["light_green", "light_yellow", "light_red"],
    "Hospital": ["open", "closed", "occupancy_update"]
}

# Consolidated NEEDED_PAYLOADS for all statuses (server and client)
NEEDED_PAYLOADS = {
    # Server statuses
    "election_start": ["from"],
    "election_ack_ok": ["from"],
    "election_coordinator": ["from"],
    "assign_server": ["host", "port", "server_id"],
    "assign_patient_to_hospital": ["hospital_id", "car_id", "crash_location"],
    "dispatch_ambulance": ["ambulance_id", "car_id", "crash_location", "hospital_id"],
    "request_workflow": ["car_id", "crash_location" ],
    "workflow_event" : ["seq", "event_type", "workflow_id"],
    "workflow_event_batch": ["events"],
    "status_update_report": ["events"],
    "workflow_completed": ["workflow_id"],
    "execute_command": ["command"],
    "client_reconnected": ["client_id", "workflow_id"],
    # Client statuses
    "location_update": ["latitude", "longitude"],
    "report_crash": ["latitude", "longitude"],
    "answer_call": ["call_id"],
    "arrived_at_scene": ["call_id"],
    "transporting_patient": ["call_id", "hospital_id"],
    "at_hospital": ["call_id", "hospital_id"],
    "occupancy_update": ["current_occupancy"],
    "dispose_crash": ["crash_id", "car_client_id"]
}

REGISTRY_HOST = '192.168.0.118'
REGISTRY_PORT = 9998


BEACON_INTERVAL = 5  # seconds
BEACON_PORT = 9999  # UDP port for beacon
CONTROL_PORT = 10050  # TCP port for control messages

ZONE_ANSWER_WAIT_TIME = 2  # seconds to wait for zone server answers
ZONE_ELECTION_TIMEOUT = 5  # seconds for zone server election
GLOBAL_ANSWER_WAIT_TIME = 2  # seconds to wait for global server answers   
GLOBAL_ELECTION_TIMEOUT = 5  # seconds for global server election

LEADER_HEARTBEAT_TIMEOUT = 15  # seconds to wait for leader heartbeat before triggering election
LEADER_MONITOR_INTERVAL = 3  # seconds between leader health checks

REGISTRY_CLEANUP_INTERVAL = 7  # seconds between registry cleanup checks
SERVER_STALE_TIME = 15  # seconds before a server is considered stale

RETRY_BUFFERED_EVENTS_INTERVAL = 10  # seconds between retries to send buffered events to leader
CLIENT_SERVER_DISCOVERY_TIMEOUT = 5  # seconds to listen for beacons

MAX_RETRIES = 3  # max retries for certain operations


logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s')
MAIN_CLIENT_LOGGER = logging.getLogger("MainClient")
MAIN_SERVER_LOGGER = logging.getLogger("MainServer")
TCP_SERVER_LOGGER = logging.getLogger("TCPServer")
TCP_CLIENT_LOGGER = logging.getLogger("TCPClient")
DISCOVERY_LOGGER = logging.getLogger("DiscoveryProtocol")
BULLY_ELECTION_LOGGER = logging.getLogger("BullyElection")

def get_tcp_client_logger(client_uid: str):
    """Return a namespaced TCP client logger for a given client UID.

    NOTE: Handlers are attached via setup_client_file_logging() in main_client.
    """
    return logging.getLogger(f"TCPClient[{client_uid}]")