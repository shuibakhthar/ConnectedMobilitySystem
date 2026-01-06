import asyncio
from datetime import datetime
from components.client_message import deserialize_client_message
from components.server_message import ServerMessage
from config.settings import TCP_SERVER_LOGGER
import logging
from datetime import datetime, timedelta
import uuid
import json

class ServerInfo:
    def __init__(self, server_id, host, port, ctrl_port, zone=None, last_seen=None):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.zone = zone
        self.ctrl_port = ctrl_port
        self.last_seen = last_seen
    
    @classmethod
    def from_beacon(cls, beacon_data, last_seen=datetime.now()):
        TCP_SERVER_LOGGER.debug(f"Parsing beacon data: {beacon_data}")
        return cls(
            server_id=beacon_data.get("server_id"),
            host=beacon_data.get("host"),
            port=beacon_data.get("tcp_port"),
            ctrl_port=beacon_data.get("ctrl_port"),
            zone=beacon_data.get("zone"),
            last_seen=last_seen
        )

    def __repr__(self):
        return f"ServerInfo(id={self.server_id}, host={self.host}, port={self.port}, zone={self.zone}, last_seen={self.last_seen})"
    
class ServerRegistry:
    def __init__(self, ttl_seconds=15):
        self.servers = {}
        self.history = {}
        self.ttl_seconds = ttl_seconds  # server_id -> ServerInfo

    def register_server(self, beacon_msg, addr):
        server_info = ServerInfo.from_beacon(json.loads(beacon_msg), last_seen=datetime.now())
        self.servers[server_info.server_id] = server_info
        self.history[server_info.server_id] = server_info

    def get_server(self, server_id):
        return self.servers.get(server_id)

    def get_all_servers(self):
        return list(self.servers.values())
    
    def get_history(self):
        return list(self.history.values())
    
    def cleanup_stale_servers(self):
        now = datetime.now()
        stale_ids = [sid for sid, sinfo in self.servers.items() if (now - sinfo.last_seen).total_seconds() > self.ttl_seconds]
        for sid in stale_ids:
            del self.servers[sid]

    def __repr__(self):
        return f"ServerRegistry(servers={self.servers})"
class TCPServer:
    def __init__(self, host, port, ctrl_port, heartbeat_timeout=30):
        self.uid = str(uuid.uuid7())
        self.host = host
        self.port = port
        self.ctrl_port = ctrl_port
        self.heartbeat_timeout = timedelta(seconds=heartbeat_timeout)
        self.clients = {}  # sender_id -> (reader, writer, client_type, status, last_heartbeat)
        self.writer_to_client = {}  # writer -> sender_id
        self.log = TCP_SERVER_LOGGER

    async def cleanup_task(self):
        try:
            while True:
                await asyncio.sleep(5)
                now = datetime.now()
                stale = [
                    cid for cid, (_, w, _, _, last) in self.clients.items()
                    if last is None or (now - last) > self.heartbeat_timeout
                ]
                for cid in stale:
                    _, w, _, _, _ = self.clients.pop(cid)
                    self.writer_to_client.pop(w, None)
                    try:
                        w.close()
                        await w.wait_closed()
                    except Exception:
                        pass
                    self.log.info(f"Evicted stale client {cid}")
        except asyncio.CancelledError:
            self.log.info("Cleanup task cancelled")

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.log.info(f"Connection from {addr}")
        # print(f"Connection from {addr}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    self.log.info("No data received, closing connection.")
                    # print("No data received, closing connection.")
                    break
                msg = deserialize_client_message(data.decode())
                if msg:
                    await self.process_message(msg, writer)
                else:
                    self.log.warning("Failed to deserialize client message.")
                    # print("Failed to deserialize client message.")
        except ConnectionResetError:
            self.log.warning("Connection reset by peer.")
            pass
        except Exception as e:
            self.log.error(f"Error handling client: {e}")
            pass    
        finally:
            client_id = self.writer_to_client.get(writer)
            if client_id and client_id in self.clients:
                self.log.info(f"Client {client_id} disconnected")
                del self.clients[client_id]
            self.writer_to_client.pop(writer, None)
            writer.close()
            await writer.wait_closed()

            # sender_id = None
            # for sid, (r, w) in self.clients.items():
            #     if w == writer:
            #         sender_id = sid
            #         break
            # if sender_id:
            #     print(f"Client {sender_id} disconnected")
            #     del self.clients[sender_id]
            # writer.close()
            # await writer.wait_closed()

    async def process_message(self, msg, writer):
        try:
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
                self.log.info(f"Received unknown message from {client_id}: {msg}")
                # print(f"Received unknown message from {client_id}: {msg}")
                #  TODO: handle other message types
        except Exception as e:
            self.log.error(f"Error processing message: {e}")


    async def start(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        # print(f"TCP Server listening on {self.host}:{self.port}")
        self.log.info(f"TCP Server {self.uid} listening on {self.host}:{self.port}")
        cleanup = asyncio.create_task(self.cleanup_task())
        async with server:
            try:
                await server.serve_forever()
            except asyncio.CancelledError:
                self.log.info("Server serve_forever cancelled")
            finally:
                self.log.debug("Cancelling cleanup task")
                cleanup.cancel()

    #Handle client messages
    async def handle_register_client(self, client_id, client_type, writer):
        self.clients[client_id] = (None, writer, client_type, None, datetime.now())
        self.writer_to_client[writer] = client_id
        self.log.info(f"Registered client {client_id} as {client_type}")
        # print(f"Registered client {client_id} as {client_type}")
        ack_msg = ServerMessage(self.host, "ack_register", {"status": "ok"})
        writer.write(ack_msg.serialize())
        await writer.drain()

    async def handle_heartbeat(self, client_id):
        if client_id in self.clients:
            r, w, client_type, status, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, client_type, status, datetime.now())
            self.log.info(f"Heartbeat received from {client_id}")
            # print(f"Heartbeat received from {client_id}")
        else:
            self.log.warning(f"Heartbeat from unregistered client {client_id}")
            # print(f"Heartbeat from unregistered client {client_id}")

# For manual test, run this code:
if __name__ == "__main__":
    server = TCPServer('127.0.0.1', 8888)
    asyncio.run(server.start())
