import asyncio
from datetime import datetime
from components.client_message import deserialize_client_message
from components.server_message import ServerMessage
from config.settings import LEADER_MONITOR_INTERVAL, REGISTRY_HOST, REGISTRY_PORT, TCP_SERVER_LOGGER, LEADER_HEARTBEAT_TIMEOUT
import logging
from datetime import datetime, timedelta
import uuid
import json
import time
from election.bully_election import BullyElection
class ServerInfo:
    def __init__(self, server_id, host, port, ctrl_port, zone=None, last_seen=None, leaderId=None, leaderInfo=None):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.zone = zone
        self.ctrl_port = ctrl_port
        self.last_seen = last_seen

        self.leaderId = leaderId
        self.leaderInfo = leaderInfo
        # revert all
    
    @classmethod
    def from_beacon(cls, beacon_data, last_seen=time.time()):
        TCP_SERVER_LOGGER.debug(f"Parsing beacon data: {beacon_data}")
        return cls(
            server_id=beacon_data.get("server_id"),
            host=beacon_data.get("host"),
            port=beacon_data.get("tcp_port"),
            ctrl_port=beacon_data.get("ctrl_port"),
            zone=beacon_data.get("zone"),
            last_seen=last_seen,
            leaderId=beacon_data.get("leader_id"),
            leaderInfo=beacon_data.get("leader_info")
        )
    
    # 
    def update_Leader_Info(self, leaderId, leaderInfo):
        self.leaderId = leaderId
        self.leaderInfo = leaderInfo
    
    def get_leader_Info(self):
        return {
            "leader_id": self.leaderId,
            "leader_info": self.leaderInfo
        }
    # 

    def to_dict(self, shallow_leader = False):
        d = {
            "server_id": self.server_id,
            "host": self.host,
            "port": self.port,
            "ctrl_port": self.ctrl_port,
            "zone": self.zone,
            "last_seen": self.last_seen,
            "leader_id": self.leaderId,
        }
        if self.leaderInfo and not shallow_leader:
            if isinstance(self.leaderInfo, ServerInfo):
                d["leader_info"] = self.leaderInfo.to_dict(shallow_leader=True)
            elif isinstance(self.leaderInfo, dict):
                shallow = dict(self.leaderInfo)
                shallow["leader_info"] = None
                d["leader_info"] = shallow
            else:
                d["leader_info"] = None
        else:
            d["leader_info"] = None
                
        return d
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            server_id=data.get("server_id"),
            host=data.get("host"),
            port=data.get("port"),
            ctrl_port=data.get("ctrl_port"),
            zone=data.get("zone"),
            last_seen=data.get("last_seen"),
            leaderId=data.get("leader_id"),
            leaderInfo=data.get("leader_info")
        )

    def __repr__(self):
        return f"ServerInfo(id={self.server_id}, host={self.host}, port={self.port}, zone={self.zone}, last_seen={self.last_seen})"
    
class TCPServer:
    def __init__(self, host, port, ctrl_port, heartbeat_timeout=30):
        self.serverInfo = ServerInfo( str(uuid.uuid7()), host, port, ctrl_port , last_seen=time.time(), leaderId=None)

        # self.uid = str(uuid.uuid7())
        # self.host = host
        # self.port = port
        # self.ctrl_port = ctrl_port
        # revert

        self.heartbeat_timeout = timedelta(seconds=heartbeat_timeout)
        self.clients = {}  # sender_id -> (reader, writer, client_type, status, last_heartbeat)
        self.writer_to_client = {}  # writer -> sender_id
        self.log = TCP_SERVER_LOGGER
        self.local_server_registry = [] 
        
        self.leader_Node = None  # List of known servers

        self.election = BullyElection(
            node_id=self.serverInfo.server_id, 
            get_registry_func=lambda: self.fetch_server_registry,
            host=self.serverInfo.host,
            ctrl_port=self.serverInfo.ctrl_port,
            serverInfo=self.serverInfo,
        )

    async def fetch_server_registry(self, registry_host=REGISTRY_HOST, registry_port=REGISTRY_PORT):
        try:
            reader, writer = await asyncio.open_connection(registry_host, registry_port)
            request_msg = json.dumps({"request_type": "get_registry"}).encode('utf-8')
            writer.write(request_msg)
            await writer.drain()
            data = await reader.read()
            writer.close()
            await writer.wait_closed()
            response = json.loads(data.decode('utf-8'))
            return response
        except Exception as e:
            self.log.error(f"Error fetching server registry: {e}")
            return None
        
    async def update_registry_periodically(self, interval=5):
        try:
            while True:
                new_registry = await self.fetch_server_registry()
                if new_registry:
                    self.local_server_registry = new_registry.get("server_list", [])
                    self.serverInfo.update_Leader_Info(
                        new_registry.get("leader_id"),
                        new_registry.get("leader_info")
                    )
                    self.log.debug("Updated server registry periodically")
                    self.log.debug(f"Leader Info: {self.serverInfo.get_leader_Info()}")
                    print("Updated registry:", self.local_server_registry)
                await asyncio.sleep(interval)
        except asyncio.CancelledError:
            self.log.info("Registry updater task cancelled")
        except Exception as e:
            self.log.error(f"Error in registry updater: {e}")

    async def handle_ctrl_message(self, reader, writer):
        try:
            data = await reader.read()
            from components.server_message import deserialize_server_message
            msg = deserialize_server_message(data.decode())
            if not msg:
                return
            if msg.status == "election_start":
                await self.election.handle_election_message(msg.payload["from"])
            elif msg.status == "election_ack_ok":
                await self.election.handle_ok_message(msg.payload["from"])
            elif msg.status == "election_coordinator":
                await self.election.handle_coordinator_message(msg.payload["from"])
        except Exception as e:
            self.log.error(f"Error handling ctrl message: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    def update_Leader_Node(self):
        leader = self.get_leader()
        if leader:
            self.leader_Node = leader
            self.log.info(f"Updated Leader: {self.election.coordinator_id} ({leader['host']}:{leader['port']})")
        else:
            self.log.info(f"Updated Leader: {self.election.coordinator_id} (not found in registry)")

    async def get_leader(self, registry_host=REGISTRY_HOST, registry_port=REGISTRY_PORT):
        '''
        if self.election.coordinator_id is None:
            return None
        leader = next((s for s in self.local_server_registry if s['server_id'] == self.election.coordinator_id), None)
        self.log.debug(f"Leader lookup: {leader}")
        return leader
        '''
        try:
            reader, writer = await asyncio.open_connection(registry_host, registry_port)
            request_msg = json.dumps({"request_type": "leader_info"}).encode('utf-8')
            writer.write(request_msg)
            await writer.drain()
            data = await reader.read()
            writer.close()
            await writer.wait_closed()
            response = json.loads(data.decode('utf-8'))
            return response
        except Exception as e:
            self.log.error(f"Error fetching server registry: {e}")
            return None

    def is_leader_missing_or_dead(self):
        # if self.election.coordinator_id is None:
        #     return True
        # leader = self.get_leader()

        '''
        if not self.leader_Node:
            self.log.debug("Leader node is None")
            return True
        # Check last_seen (should be epoch float)
        if time.time() - self.leader_Node['last_seen'] > LEADER_HEARTBEAT_TIMEOUT:
            self.log.debug(f"Leader last seen: {self.leader_Node['last_seen']}, current time: {time.time()}")
            self.log.debug("Leader node is considered dead due to heartbeat timeout")
            return True
        return False
        '''
        # revert
        
        if not self.serverInfo.leaderId or not self.serverInfo.leaderInfo:
            self.log.debug(f"Leader node is None {self.serverInfo.leaderId} {self.leader_Node}")
            return True
        # Check last_seen (should be epoch float)
        if time.time() - self.serverInfo.leaderInfo['last_seen'] > LEADER_HEARTBEAT_TIMEOUT:
            self.log.debug(f"Leader last seen: {self.serverInfo.leaderInfo['last_seen']}, current time: {time.time()}")
            self.log.debug("Leader node is considered dead due to heartbeat timeout")
            return True
        return False

    async def monitor_leader(self):
        try:
            while True:
                await asyncio.sleep(LEADER_MONITOR_INTERVAL)
                if self.is_leader_missing_or_dead():
                    self.log.warning("Leader missing or dead, starting election")
                    await self.election.start_election()
                    # self.update_Leader_Node()
        except asyncio.CancelledError:
            self.log.info("Leader monitor task cancelled")

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

        registry_response = await self.fetch_server_registry()
        self.log.info(f"Fetched server registry: {registry_response}")

        try:
            if registry_response:
                self.local_server_registry = registry_response.get("server_list", [])
                self.serverInfo.update_Leader_Info(
                    registry_response.get("leader_id"),
                    registry_response.get("leader_info")
                )
                self.log.info("Initialized server registry from registry service")
                self.log.info(f"Leader Info: {self.serverInfo.get_leader_Info()}")
        except Exception as e:
            self.log.error(f"Error updating server registry from response: {e}")

        # print(f"Fetched server registry: {self.local_server_registry}")
        
        '''
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        # print(f"TCP Server listening on {self.host}:{self.port}")
        self.log.info(f"TCP Server {self.uid} listening on {self.host}:{self.port}")
        # CTRL_SERVER is where all the server related functionality happens like election, inter-server comms, etc.
        ctrl_server = await asyncio.start_server(self.handle_ctrl_message, self.host, self.ctrl_port)
        self.log.info(f"Control Server {self.uid} listening on {self.host}:{self.ctrl_port}")
        '''
        # revert
        
        server = await asyncio.start_server(self.handle_client, self.serverInfo.host, self.serverInfo.port)
        # print(f"TCP Server listening on {self.host}:{self.port}")
        self.log.info(f"TCP Server {self.serverInfo.server_id} listening on {self.serverInfo.host}:{self.serverInfo.port}")

        # CTRL_SERVER is where all the server related functionality happens like election, inter-server comms, etc.
        ctrl_server = await asyncio.start_server(self.handle_ctrl_message, self.serverInfo.host, self.serverInfo.ctrl_port)
        self.log.info(f"Control Server {self.serverInfo.server_id} listening on {self.serverInfo.host}:{self.serverInfo.ctrl_port}")
        
        # self.get_leader()
        if self.is_leader_missing_or_dead():
            await self.election.start_election()
        
        # self.update_Leader_Node()
    
        cleanup = asyncio.create_task(self.cleanup_task())
        registry_updater = asyncio.create_task(self.update_registry_periodically())
        leader_monitor = asyncio.create_task(self.monitor_leader())
        async with server, ctrl_server:
            try:
                # await server.serve_forever()
                await asyncio.gather(server.serve_forever(), ctrl_server.serve_forever())
            except asyncio.CancelledError:
                self.log.info("Server serve_forever cancelled")
            finally:
                self.log.debug("Cancelling cleanup task")
                cleanup.cancel()
                registry_updater.cancel()
                leader_monitor.cancel()

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
