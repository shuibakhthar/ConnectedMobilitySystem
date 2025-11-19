import json

def serialize_message(msg_type, sender_id, payload):
    message = {
        "type": msg_type,
        "sender_id": sender_id,
        "payload": payload
    }
    return (json.dumps(message) + "\n").encode('utf-8')

def deserialize_message(data):
    try:
        print(f"Deserializing data: {data}")
        return json.loads(data)
    except json.JSONDecodeError:
        return None
