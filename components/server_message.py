import json
SERVER_STATUS = [
    "ack_register", "ack_crash", "ack_location_update",
    "ack_answer_call", "ack_arrived_at_scene", "ack_transporting_patient", "ack_at_hospital", "ack_available",
    "ack_light_green", "ack_light_yellow", "ack_light_red",
    "ack_open", "ack_closed", "ack_occupancy_update",
    "dispose_crash"]
NEEDED_PAYLOADS = {
    "dispose_crash": ["crash_id", "car_client_id"]
}

class ServerMessage:
    def __init__(self, server_id, status, payload):
        self.server_id = server_id

        #Server status validation
        if status not in SERVER_STATUS:
            raise ValueError(f"Invalid status '{status}'.")
        self.status = status

        #Server payload validation
        if status in NEEDED_PAYLOADS:
            required_fields = NEEDED_PAYLOADS[status]
            for field in required_fields:
                if field not in payload:
                    raise ValueError(f"Missing required payload field '{field}' for status '{status}'")
        self.payload = payload

    def serialize(self):
        message = {
            "server_id": self.server_id,
            "status": self.status,
            "payload": self.payload
        }
        return (json.dumps(message) + "\n").encode('utf-8')
    

def deserialize_server_message(data):
    try:
        message = json.loads(data)
        server_id = message.get("server_id")
        status = message.get("status")
        payload = message.get("payload", {})
        return ServerMessage(server_id, status, payload)
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Failed to deserialize message: {e}")
        return None
