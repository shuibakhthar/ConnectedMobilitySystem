import logging

SERVER_STATUS = [
    "ack_register", "ack_crash", "ack_location_update",
    "ack_answer_call", "ack_arrived_at_scene", "ack_transporting_patient", "ack_at_hospital", "ack_available",
    "ack_light_green", "ack_light_yellow", "ack_light_red",
    "ack_open", "ack_closed", "ack_occupancy_update",
    "dispose_crash", "election_start","election_ack_ok", "election_coordinator", "heartbeat",  "request_server_assignment", "assign_server"]
NEEDED_PAYLOADS = {
   "election_start": ["from"],
   "election_ack_ok": ["from"],
    "election_coordinator": ["from"],
}

CLIENT_TYPES = ["Ambulance", "Car", "TrafficLight", "Hospital"]
CLIENT_STATUS = {
    "AllClients": ["register", "location_update", "heartbeat"],
    "Ambulance": ["report_crash", "answer_call", "arrived_at_scene", "transporting_patient", "at_hospital", "available"],
    "Car": ["report_crash"],
    "TrafficLight": ["light_green", "light_yellow", "light_red"],
    "Hospital": ["open", "closed", "occupancy_update"]
}
NEEDED_PAYLOADS = {
    "location_update": ["latitude", "longitude"],
    "report_crash": ["latitude", "longitude"],
    "answer_call": ["call_id"],
    "arrived_at_scene": ["call_id"],
    "transporting_patient": ["call_id", "hospital_id"],
    "at_hospital": ["call_id", "hospital_id"],
    "occupancy_update": ["current_occupancy", "max_capacity"],
    "dispose_crash": ["crash_id", "car_client_id"]
}

REGISTRY_HOST = '127.0.0.1'
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

REGISTRY_CLEANUP_INTERVAL = 10  # seconds between registry cleanup checks
SERVER_STALE_TIME = 20  # seconds before a server is considered stale


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
MAIN_CLIENT_LOGGER = logging.getLogger("MainClient")
MAIN_SERVER_LOGGER = logging.getLogger("MainServer")
TCP_SERVER_LOGGER = logging.getLogger(f"TCPServer[{BEACON_PORT}:{CONTROL_PORT}]")
DISCOVERY_LOGGER = logging.getLogger("DiscoveryProtocol")
BULLY_ELECTION_LOGGER = logging.getLogger("BullyElection")