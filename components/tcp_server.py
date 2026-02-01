import asyncio
from datetime import datetime, timedelta
import uuid
import time
from components.client_message import deserialize_client_message
from components.server_message import ServerMessage
from config.settings import (
    LEADER_MONITOR_INTERVAL,
    LEADER_HEARTBEAT_TIMEOUT,
    TCP_SERVER_LOGGER,
)
from election.bully_election import BullyElection


class ServerInfo:
    def __init__(self, server_id, host, port, ctrl_port, zone=None, last_seen=None, leaderId=None, leaderInfo=None, active_clients=0):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.zone = zone
        self.ctrl_port = ctrl_port
        self.last_seen = last_seen
        self.leaderId = leaderId
        self.leaderInfo = leaderInfo
        self.active_clients = active_clients

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
            leaderInfo=beacon_data.get("leader_info"),
            active_clients=beacon_data.get("active_clients", 0)
        )

    def update_Leader_Info(self, leaderId, leaderInfo):
        self.leaderId = leaderId
        self.leaderInfo = leaderInfo

    def get_leader_Info(self):
        return {"leader_id": self.leaderId, "leader_info": self.leaderInfo}

    def to_dict(self, shallow_leader=False):
        d = {
            "server_id": self.server_id,
            "host": self.host,
            "port": self.port,
            "ctrl_port": self.ctrl_port,
            "zone": self.zone,
            "last_seen": self.last_seen,
            "leader_id": self.leaderId,
            "active_clients": self.active_clients,
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
            leaderInfo=data.get("leader_info"),
            active_clients=data.get("active_clients", 0)
        )

    def __repr__(self):
        return f"ServerInfo(id={self.server_id}, host={self.host}, port={self.port}, zone={self.zone})"


class TCPServer:
    def __init__(self, host, port, ctrl_port, registry, heartbeat_timeout=30):
        self.registry = registry
        self.serverInfo = ServerInfo(
            str(uuid.uuid7()), host, port, ctrl_port, last_seen=time.time(), leaderId=None, active_clients=0
        )
        self.heartbeat_timeout = timedelta(seconds=heartbeat_timeout)
        self.clients = {}
        self.writer_to_client = {}
        self.log = TCP_SERVER_LOGGER

        self.election = BullyElection(
            serverInfo=self.serverInfo,
            registry=self.registry,
        )

    def get_local_server_list(self):
        """Get fresh server list from in-memory registry"""
        return [s.to_dict() for s in self.registry.get_all_servers()]

    def is_leader_missing_or_dead(self):
        leader_id = self.registry.get_leader_id()
        if not leader_id:
            self.log.debug("No leader ID set")
            return True

        servers = self.get_local_server_list()
        leader = next((s for s in servers if s["server_id"] == leader_id), None)
        
        if not leader:
            self.log.debug(f"Leader {leader_id} not found in registry")
            return True

        time_since_seen = time.time() - leader["last_seen"]
        if time_since_seen > LEADER_HEARTBEAT_TIMEOUT:
            self.log.debug(f"Leader last seen {time_since_seen:.1f}s ago (timeout: {LEADER_HEARTBEAT_TIMEOUT}s)")
            return True
        return False

    async def monitor_leader(self):
        try:
            while True:
                await asyncio.sleep(LEADER_MONITOR_INTERVAL)
                if self.is_leader_missing_or_dead():
                    self.log.warning("Leader missing or dead, starting election")
                    await self.election.start_election()
        except asyncio.CancelledError:
            self.log.info("Leader monitor task cancelled")

    async def cleanup_task(self):
        try:
            while True:
                await asyncio.sleep(5)
                now = datetime.now()
                stale = [
                    cid
                    for cid, (_, w, _, _, last) in self.clients.items()
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
                self.serverInfo.active_clients = len(self.clients)
        except asyncio.CancelledError:
            self.log.info("Cleanup task cancelled")

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info("peername")
        self.log.info(f"Connection from {addr}")
        try:
            while True:
                data = await reader.readline()
                if not data:
                    self.log.info("No data received, closing connection.")
                    break
                msg = deserialize_client_message(data.decode())
                if msg:
                    await self.process_message(msg, writer)
                else:
                    self.log.warning("Failed to deserialize client message.")
        except ConnectionResetError:
            self.log.warning("Connection reset by peer.")
        except Exception as e:
            self.log.error(f"Error handling client: {e}")
        finally:
            client_id = self.writer_to_client.get(writer)
            if client_id and client_id in self.clients:
                self.log.info(f"Client {client_id} disconnected")
                del self.clients[client_id]
                self.serverInfo.active_clients = len(self.clients)
            self.writer_to_client.pop(writer, None)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionResetError:
                self.log.warning("Connection reset by peer.")


    async def handle_ctrl_message(self, reader, writer):
        from components.server_message import deserialize_server_message
        try:
            data = await reader.read()
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

    async def handle_client_assignment_request(self, client_id, client_type, writer):
        try:
            self.log.debug(f"Handling server assignment request from {client_id} ({client_type})")
            
            leader_id = self.registry.get_leader_id()
            leader_info = self.registry.get_leader_info()
            
            if not leader_id:
                self.log.error("No leader available for assignment")
                ack_msg = ServerMessage(
                    self.serverInfo.server_id,
                    "assign_server",
                    {"status": "error", "message": "No leader available"},
                )
                writer.write(ack_msg.serialize())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            if leader_id != self.serverInfo.server_id:
                self.log.error("This server is not the leader, cannot assign servers")
                ack_msg = ServerMessage(
                    self.serverInfo.server_id,
                    "assign_server",
                    {
                        "status": "error",
                        "message": "Not the leader",
                        "redirect_to_leader": True,
                        "host": leader_info.host if leader_info else None,
                        "port": leader_info.port if leader_info else None,
                    },
                )
                writer.write(ack_msg.serialize())
                await writer.drain()
                writer.close()
                await writer.wait_closed()
                return

            self.log.info(f"Assigning server to client {client_id} ({client_type})")
            servers = self.get_local_server_list()

            def server_min_load_criteria(s):
                load = s.get("active_clients", 0)
                last_seen = s.get("last_seen", 0)
                return (load, -last_seen)

            assigned_server = min(servers, key=server_min_load_criteria) if servers else None
            
            if assigned_server:
                ack_msg = ServerMessage(
                    self.serverInfo.server_id,
                    "assign_server",
                    {
                        "status": "ok",
                        "host": assigned_server["host"],
                        "port": assigned_server["port"],
                        "server_id": assigned_server["server_id"],
                    },
                )
                writer.write(ack_msg.serialize())
                await writer.drain()
                self.log.info(f"Assigned server {assigned_server['server_id']} to client {client_id}")
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            self.log.error(f"Error handling assignment request: {e}")
            ack_msg = ServerMessage(
                self.serverInfo.server_id, "assign_server", {"status": "error", "message": str(e)}
            )
            writer.write(ack_msg.serialize())
            await writer.drain()
            writer.close()
            await writer.wait_closed()

    async def process_message(self, msg, writer):
        try:
            client_type = msg.client_type
            client_id = msg.client_id
            status_msg = msg.status

            if status_msg == "register":
                await self.handle_register_client(client_id, client_type, writer)
            elif status_msg == "request_server_assignment":
                await self.handle_client_assignment_request(client_id, client_type, writer)
            elif status_msg == "heartbeat":
                await self.handle_heartbeat(client_id)
            else:
                self.log.info(f"Received unknown message from {client_id}: {msg}")
        except Exception as e:
            self.log.error(f"Error processing message: {e}")

    async def handle_register_client(self, client_id, client_type, writer):
        self.clients[client_id] = (None, writer, client_type, "registered", datetime.now())
        self.writer_to_client[writer] = client_id
        self.serverInfo.active_clients = len(self.clients)
        ack = ServerMessage(self.serverInfo.server_id, "ack_register", {"client_id": client_id})
        writer.write(ack.serialize())
        await writer.drain()
        self.log.info(f"Registered client {client_id} ({client_type})")

    async def handle_heartbeat(self, client_id):
        if client_id in self.clients:
            r, w, ct, st, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, ct, st, datetime.now())
            self.log.debug(f"Heartbeat updated for {client_id}")

    async def start(self):
        # Start monitoring and cleanup tasks
        monitor_task = asyncio.create_task(self.monitor_leader())
        cleanup_task = asyncio.create_task(self.cleanup_task())

        server = await asyncio.start_server(
            self.handle_client, self.serverInfo.host, self.serverInfo.port
        )
        ctrl_server = await asyncio.start_server(
            self.handle_ctrl_message, self.serverInfo.host, self.serverInfo.ctrl_port
        )

        self.log.info(
            f"TCP Server listening on {self.serverInfo.host}:{self.serverInfo.port} "
            f"(ctrl {self.serverInfo.ctrl_port})"
        )

        async with server, ctrl_server:
            try:
                await asyncio.gather(server.serve_forever(), ctrl_server.serve_forever())
            finally:
                monitor_task.cancel()
                cleanup_task.cancel()
                await asyncio.gather(monitor_task, cleanup_task, return_exceptions=True)