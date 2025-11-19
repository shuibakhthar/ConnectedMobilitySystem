import asyncio
from components.message import deserialize_message, serialize_message

class TCPServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.clients = {}  # sender_id -> (reader, writer)

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"Connection from {addr}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    print("No data received, closing connection.")
                    break
                msg = deserialize_message(data.decode())
                if msg:
                    await self.process_message(msg, writer)
        except ConnectionResetError:
            pass
        finally:
            sender_id = None
            for sid, (r, w) in self.clients.items():
                if w == writer:
                    sender_id = sid
                    break
            if sender_id:
                print(f"Client {sender_id} disconnected")
                del self.clients[sender_id]
            writer.close()
            await writer.wait_closed()

    async def process_message(self, msg, writer):
        msg_type = msg.get("type")
        sender_id = msg.get("sender_id")
        payload = msg.get("payload")
        if msg_type == "register":
            self.clients[sender_id] = (None, writer)
            print(f"Registered client {sender_id}")
            ack = serialize_message("ack_register", "server", {"status": "ok"})
            writer.write(ack)
            await writer.drain()
        else:
            print(f"Received message from {sender_id}: {msg}")

    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"TCP Server listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

# For manual test, run this code:
if __name__ == "__main__":
    server = TCPServer('127.0.0.1', 8888)
    asyncio.run(server.start())
