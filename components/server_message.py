import json
from config.settings import SERVER_STATUS, NEEDED_PAYLOADS

'''
ServerMessage class to handle serialization and deserialization of server messages.

ServerMessage Structure:
{
    "server_id": str,
    "status": str,       # Must be one of SERVER_STATUS
    "payload": dict      # Must contain required fields as per NEEDED_PAYLOADS
}
'''
class ServerMessage:
    def __init__(self, server_id, status, payload, server_zone=None):
        self.server_zone = server_zone # server_zone is deprecated
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
    
# Deserialize function to create a ServerMessage object from JSON string
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
