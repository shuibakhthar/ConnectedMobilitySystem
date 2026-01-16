import asyncio
from components.client_message import ClientMessage
from components.server_message import deserialize_server_message
import logging
import uuid

from config.settings import MAX_RETRIES
class TCPClient:
    def __init__(self, server_host, server_port, client_id, client_type="Car", heartbeat_interval=15):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.client_type = client_type
        self.heartbeat_interval = heartbeat_interval
        self.client_uid = uuid.uuid7()
        self.log = logging.getLogger(f"TCPClient[{self.client_uid}]")

    async def connect(self):

        server_host, server_port = await self.request_server_assignment()

        self.reader, self.writer = await asyncio.open_connection(server_host, server_port)
        await self.register()

    async def request_server_assignment(self, max_retries= MAX_RETRIES):
        leader_host = self.server_host
        leader_port = self.server_port

        for attempt in range(max_retries):
            try:
                self.log.info(f"Requesting server assignment from leader at {leader_host}:{leader_port}, attempt {attempt + 1}")
                reader, writer = await asyncio.open_connection(leader_host, leader_port)
                request_msg = ClientMessage(self.client_id, self.client_type, "request_server_assignment",  {})
                writer.write(request_msg.serialize())
                await writer.drain()

                response = await reader.readline()
                if not response:
                    raise ConnectionError("No response received from leader")
                
                response_msg = deserialize_server_message(response.decode())
                if response_msg:
                    if response_msg.status == "assign_server":
                        assigned_info = response_msg.payload
                        assigned_host = assigned_info.get("host")
                        assigned_port = assigned_info.get("port")
                        self.log.info(f"Assigned to server at {assigned_host}:{assigned_port}")
                        writer.close()
                        await writer.wait_closed()
                        return assigned_host, assigned_port
                    elif response_msg.status == "redirect_to_leader":
                        new_leader_info = response_msg.payload
                        self.log.info(f"Redirected to new leader {new_leader_info.get('host')}:{new_leader_info.get('port') }")
                        leader_host = new_leader_info.get("host")
                        leader_port = new_leader_info.get("port")
                        writer.close()
                        await writer.wait_closed()
                        await asyncio.sleep(1)  # brief pause before retrying
                        continue

                else:
                    raise ValueError("Invalid response or status from leader")
                
            except (ConnectionError, OSError) as e:
                self.log.error(f"Assignment attempt {attempt + 1} connection failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                        raise
            except Exception as e:
                self.log.error(f"Assignment attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)
        raise ConnectionError("Failed to get server assignment after all retries")

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
