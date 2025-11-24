import json

CLIENT_TYPES = ["Ambulance", "Car", "TrafficLight", "Hospital"]
CLIENT_STATUS = {
    "AllClients": ["register", "location_update", "heartbeat"],
    "Ambulance": ["report_crash", "answer_call", "arrived_at_scene", "transporting_patient", "at_hospital", "available"],
    "Car": ["register_crash"],
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
    "occupancy_update": ["current_occupancy", "max_capacity"]
}

class ClientMessage:
    def __init__(self, client_id, client_type, status, payload):
        self.client_id = client_id

        #Client type validation
        if client_type not in CLIENT_TYPES:
            raise ValueError(f"Invalid client type: {client_type}")
        self.client_type = client_type

        #Client status validation
        if status not in CLIENT_STATUS.get(client_type, []) and status not in CLIENT_STATUS["AllClients"]:
            raise ValueError(f"Invalid status '{status}' for client type '{client_type}'")
        self.status = status

        #Client payload validation
        if status in NEEDED_PAYLOADS:
            required_fields = NEEDED_PAYLOADS[status]
            for field in required_fields:
                if field not in payload:
                    raise ValueError(f"Missing required payload field '{field}' for status '{status}'")
        self.payload = payload

    def serialize(self):
        message = {
            "client_id": self.client_id,
            "client_type": self.client_type,
            "status": self.status,
            "payload": self.payload
        }
        return (json.dumps(message) + "\n").encode('utf-8')
    

def deserialize_client_message(data):
    try:
        message = json.loads(data)
        client_id = message.get("client_id")
        client_type = message.get("client_type")
        status = message.get("status")
        payload = message.get("payload", {})
        return ClientMessage(client_id, client_type, status, payload)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Failed to deserialize message: {e}")
        return None
