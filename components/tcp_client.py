import asyncio
from components.client_message import ClientMessage
from components.server_message import deserialize_server_message
import logging

class TCPClient:
    def __init__(self, server_host, server_port, client_id, client_type="Car", heartbeat_interval=15):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.client_type = client_type
        self.heartbeat_interval = heartbeat_interval
        self.log = logging.getLogger(f"TCPClient[{client_id}]")

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.server_host, self.server_port)
        await self.register()

    async def register(self):
        reg_msg = ClientMessage(self.client_id, self.client_type, "register", {})
        #reg_msg = serialize_message("register", self.client_id, {"info": "Client registration"})
        self.writer.write(reg_msg.serialize())
        await self.writer.drain()
        response = await self.reader.readline()
        resp_msg = deserialize_server_message(response.decode())
        self.log.info(f"Registration response: {resp_msg.serialize() if resp_msg else 'None'}")
        # print(f"Registration response: {resp_msg.serialize()}")

    async def send_heartbeat(self):
        heartbeat_msg = ClientMessage(self.client_id, self.client_type, "heartbeat", {})
        self.writer.write(heartbeat_msg.serialize())
        await self.writer.drain()
        self.log.debug("Heartbeat sent.")
        # print("Heartbeat sent.")

    #async def send_status(self, status):
    #    msg = serialize_message("status_update", self.client_id, status)
    #    self.writer.write(msg)
    #    await self.writer.drain()

    async def listen(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                msg = deserialize_server_message(data.decode())
                self.log.info(f"Received from server: {msg.serialize() if msg else 'None'}")
                # print(f"Received from server: {msg}")
        except asyncio.CancelledError:
            pass

    async def run(self):
        await self.connect()
        listener_task = asyncio.create_task(self.listen())
        # For demo: send periodic status updates every 5 seconds
        try:
            while True:
                await self.send_heartbeat()
                await asyncio.sleep(15)
        except KeyboardInterrupt:
            self.log.info("Client shutting down")
            # print("Client shutting down")
        finally:
            listener_task.cancel()
            self.writer.close()
            await self.writer.wait_closed()

# For manual test, run this code:
if __name__ == "__main__":
    client = TCPClient('127.0.0.1', 8888, client_id="ambulance_1", client_type="Ambulance")
    asyncio.run(client.run())
