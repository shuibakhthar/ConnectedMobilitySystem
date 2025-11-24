import asyncio
from datetime import datetime
from components.client_message import deserialize_client_message
from components.server_message import ServerMessage

class TCPServer:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.clients = {}  # sender_id -> (reader, writer, client_type, status, last_heartbeat)

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"Connection from {addr}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    print("No data received, closing connection.")
                    break
                msg = deserialize_client_message(data.decode())
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
        client_type = msg.client_type
        client_id = msg.client_id
        status_msg = msg.status
        payload = msg.payload

        #register client if not already registered
        if status_msg == "register":
            await self.handle_register_client(client_id, client_type, writer)
        elif status_msg == "heartbeat":
            await self.handle_heartbeat(client_id)
        else:
            print(f"Received unknown message from {client_id}: {msg}")


    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"TCP Server listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    #Handle client messages
    async def handle_register_client(self, client_id, client_type, writer):
        self.clients[client_id] = (None, writer, client_type, None, datetime.now())
        print(f"Registered client {client_id} as {client_type}")
        ack_msg = ServerMessage(self.host, "ack_register", {"status": "ok"})
        writer.write(ack_msg.serialize())
        await writer.drain()
    async def handle_heartbeat(self, client_id):
        if client_id in self.clients:
            r, w, client_type, status, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, client_type, status, datetime.now())
            print(f"Heartbeat received from {client_id}")
        else:
            print(f"Heartbeat from unregistered client {client_id}")

# For manual test, run this code:
if __name__ == "__main__":
    server = TCPServer('127.0.0.1', 8888)
    asyncio.run(server.start())
