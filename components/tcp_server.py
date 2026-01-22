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
        self.local_server_crash_count = 0
        self.connected_cars = {}
        self.connected_ambulances = {}
        self.connected_hospitals = {}

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
                    _, w, ct, _, _ = self.clients.pop(cid)
                    self.writer_to_client.pop(w, None)
                    
                    # Remove from type-specific dictionary
                    if ct == "Car":
                        self.connected_cars.pop(cid, None)
                    elif ct == "Ambulance":
                        self.connected_ambulances.pop(cid, None)
                    elif ct == "Hospital":
                        self.connected_hospitals.pop(cid, None)
                    
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
                _, _, client_type, _, _ = self.clients[client_id]
                self.log.info(f"Client {client_id} disconnected")
                del self.clients[client_id]
                
                # Remove from type-specific dictionary
                if client_type == "Car":
                    self.connected_cars.pop(client_id, None)
                elif client_type == "Ambulance":
                    self.connected_ambulances.pop(client_id, None)
                elif client_type == "Hospital":
                    self.connected_hospitals.pop(client_id, None)
                
                self.serverInfo.active_clients = len(self.clients)
            self.writer_to_client.pop(writer, None)
            writer.close()
            await writer.wait_closed()

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
            elif status_msg == "report_crash":
                await self.handle_crash_report(client_id, client_type, msg.payload, writer)
            elif status_msg == "occupancy_update":
                await self.handle_occupancy_update(client_id, client_type, msg.payload, writer)
            elif status_msg in ["on_duty", "available", "arrived_at_scene", "transporting_patient", "at_hospital", "answer_call"]:
                await self.handle_ambulance_status(client_id, client_type, status_msg, msg.payload, writer)
            elif status_msg == "ack_crash_response":
                await self.handle_crash_response_ack(client_id, client_type, msg.payload, writer)
            else:
                self.log.info(f"Received unknown message from {client_id}: {msg}")
        except Exception as e:
            self.log.error(f"Error processing message: {e}")

    async def handle_register_client(self, client_id, client_type, writer):
        self.clients[client_id] = (None, writer, client_type, "registered", datetime.now())
        self.writer_to_client[writer] = client_id
        
        # Add to type-specific dictionary
        client_info = {
            "writer": writer,
            "status": "registered",
            "client_id": client_id
        }
        if client_type == "Car":
            self.connected_cars[client_id] = client_info
        elif client_type == "Ambulance":
            self.connected_ambulances[client_id] = client_info
        elif client_type == "Hospital":
            self.connected_hospitals[client_id] = client_info
        
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

    async def assign_hospital(self, crash_location, local_crash_count, car_id):
        self.log.debug(f"Available hospitals {self.connected_hospitals}")
        """Pick the first hospital, reserve a bed, and notify it."""
        if not self.connected_hospitals:
            self.log.warning("No hospitals available for assignment")
            return None

        hospital_id, hospital_info = next(iter(self.connected_hospitals.items()))
        current_occupancy = hospital_info.get("current_occupancy")
        if current_occupancy is not None and current_occupancy <= 0:
            self.log.warning(f"Hospital {hospital_id} has no available beds")
            return None

        # Reserve a bed if we track occupancy
        if current_occupancy is not None:
            self.connected_hospitals[hospital_id]["current_occupancy"] = current_occupancy - 1

        writer = hospital_info.get("writer")
        if writer:
            msg = ServerMessage(
                self.serverInfo.server_id,
                "assign_patient_to_hospital",
                {
                    "hospital_id": hospital_id,
                    "car_id": car_id,
                    "crash_location": crash_location,
                },
            )
            writer.write(msg.serialize())
            await writer.drain()
            self.log.info(f"Notified hospital {hospital_id} about crash {local_crash_count}")
        else:
            self.log.warning(f"No writer found to notify hospital {hospital_id}")

        return hospital_id

    async def assign_ambulance(self, crash_location, hospital_id, local_crash_count, car_id):
        self.log.debug(f"Available ambulances {self.connected_ambulances}")
        """Pick the first ambulance and dispatch it to crash location with hospital destination."""
        if not self.connected_ambulances:
            self.log.warning("No ambulances available for dispatch")
            return

        ambulance_id, ambulance_info = next(iter(self.connected_ambulances.items()))
        writer = ambulance_info.get("writer")
        if writer:
            payload = {
                "ambulance_id": ambulance_id,
                "car_id": car_id,
                "crash_location": crash_location,
                "hospital_id": hospital_id,
            }
            msg = ServerMessage(self.serverInfo.server_id, "dispatch_ambulance", payload)
            writer.write(msg.serialize())
            await writer.drain()
            self.connected_ambulances[ambulance_id]["status"] = "dispatched"
            self.log.info(
                f"Dispatched ambulance {ambulance_id} for crash {local_crash_count} toward hospital {hospital_id}"
            )
        else:
            self.log.warning(f"No writer found to notify ambulance {ambulance_id}")

    async def handle_crash_report(self, client_id, client_type, payload, writer):
        latitude = payload.get("latitude")
        longitude = payload.get("longitude")

        self.local_server_crash_count += 1
        local_crash_count = self.local_server_crash_count
        self.log.warning(
            f"CRASH REPORTED from {client_id} ({client_type}) - lat={latitude}, lon={longitude}, LocalServer_CrashCount={local_crash_count}"
        )

        # Update client status to crashed (if registered)
        if client_id in self.clients:
            r, w, ct, _, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, ct, "crashed", datetime.now())
            
            # Update status in type-specific dictionary
            if ct == "Car" and client_id in self.connected_cars:
                self.connected_cars[client_id]["status"] = "crashed"
                self.connected_cars[client_id]["crash_location"] = {"latitude": latitude, "longitude": longitude}
                self.connected_cars[client_id]["LocalServer_CrashCount"] = local_crash_count
            else:
                self.log.warning(
            f"Wrong Client Type is reporting crashed {client_id} ({client_type}) - lat={latitude}, lon={longitude}, LocalServer_CrashCount={local_crash_count}")
                
        if (1):
            self.log.info(f"Leader processing crash {local_crash_count} reported by {client_id}")
            crash_location = {"latitude": latitude, "longitude": longitude}
            hospital_id = await self.assign_hospital(crash_location, local_crash_count, client_id)
            await self.assign_ambulance(crash_location, hospital_id, local_crash_count, client_id)           
            

        ack = ServerMessage(
            self.serverInfo.server_id,
            "ack_crash",
            {
                "client_id": client_id,
                "client_type": client_type,
                "LocalServer_CrashCount": local_crash_count,
                "latitude": latitude,
                "longitude": longitude,
            },
        )
        writer.write(ack.serialize())
        await writer.drain()

    async def handle_occupancy_update(self, client_id, client_type, payload, writer):
        current_occupancy = payload.get("current_occupancy")
        self.log.info(f"OCCUPANCY UPDATE from {client_id} ({client_type}) - {current_occupancy} beds available")

        # Update occupancy in hospital dictionary
        if client_type == "Hospital" and client_id in self.connected_hospitals:
            self.connected_hospitals[client_id]["current_occupancy"] = current_occupancy

        ack = ServerMessage(
            self.serverInfo.server_id,
            "ack_occupancy_update",
            {
                "client_id": client_id,
                "current_occupancy": current_occupancy,
            },
        )
        writer.write(ack.serialize())
        await writer.drain()

    async def handle_ambulance_status(self, client_id, client_type, status, payload, writer):
        """Handle ambulance status updates"""
        status_display = {
            "on_duty": "ON DUTY",
            "available": "AVAILABLE",
            "arrived_at_scene": "REACHED AT SCENE",
            "transporting_patient": "TRANSPORTING PATIENT",
            "at_hospital": "AT HOSPITAL",
            "answer_call": "ANSWERING CALL"
        }
        
        display_status = status_display.get(status, status.upper())
        self.log.info(f"AMBULANCE STATUS UPDATE from {client_id} ({client_type}) - Status: {display_status}")

        # Update client status if registered
        if client_id in self.clients:
            r, w, ct, _, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, ct, status, datetime.now())
            
            # Update status in type-specific dictionary
            if ct == "Ambulance" and client_id in self.connected_ambulances:
                self.connected_ambulances[client_id]["status"] = status
                # Save additional status-specific data from payload if available
                if payload:
                    for key, value in payload.items():
                        self.connected_ambulances[client_id][key] = value

        ack = ServerMessage(
            self.serverInfo.server_id,
            f"ack_{status}",
            {
                "client_id": client_id,
                "status": status,
            },
        )
        writer.write(ack.serialize())
        await writer.drain()

    async def handle_crash_response_ack(self, client_id, client_type, payload, writer):
        """Handle acknowledgement from ambulance/hospital confirming they received dispatch/assignment."""
        car_id = payload.get("car_id")
        ack_type = payload.get("ack_type")
        responder_id = payload.get("responder_id")
        responder_type = payload.get("responder_type")
        
        self.log.info(f"CRASH RESPONSE ACK: {responder_type} {responder_id} acknowledged {ack_type} for car {car_id}")
        
        # Inform the car that help is coming
        if car_id in self.clients:
            _, car_writer, car_type, _, _ = self.clients[car_id]
            if car_type == "Car" and car_writer:
                help_msg = ServerMessage(
                    self.serverInfo.server_id,
                    "help_coming",
                    {
                        "responder_type": responder_type,
                        "responder_id": responder_id,
                        "ack_type": ack_type
                    }
                )
                car_writer.write(help_msg.serialize())
                await car_writer.drain()
                self.log.info(f"Notified car {car_id} that {responder_type} {responder_id} is responding")
        else:
            self.log.warning(f"Car {car_id} not found in clients to notify about help")

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