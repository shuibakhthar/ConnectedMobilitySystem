import asyncio
from components.message import serialize_message, deserialize_message

class TCPClient:
    def __init__(self, server_host, server_port, client_id):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.server_host, self.server_port)
        await self.register()

    async def register(self):
        reg_msg = serialize_message("register", self.client_id, {"info": "Client registration"})
        self.writer.write(reg_msg)
        await self.writer.drain()
        response = await self.reader.readline()
        resp_msg = deserialize_message(response.decode())
        print(f"Registration response: {resp_msg}")

    async def send_status(self, status):
        msg = serialize_message("status_update", self.client_id, status)
        self.writer.write(msg)
        await self.writer.drain()

    async def listen(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                msg = deserialize_message(data.decode())
                print(f"Received from server: {msg}")
        except asyncio.CancelledError:
            pass

    async def run(self):
        await self.connect()
        listener_task = asyncio.create_task(self.listen())
        # For demo: send periodic status updates every 5 seconds
        try:
            while True:
                await self.send_status({"heartbeat": "alive"})
                await asyncio.sleep(5)
        except KeyboardInterrupt:
            print("Client shutting down")
        finally:
            listener_task.cancel()
            self.writer.close()
            await self.writer.wait_closed()

# For manual test, run this code:
if __name__ == "__main__":
    client = TCPClient('127.0.0.1', 8888, client_id="ambulance_1")
    asyncio.run(client.run())
