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
from config.events_logging import EventOrderingLogger
from election.bully_election import BullyElection


class ServerInfo:
    def __init__(self, server_id, host, port, ctrl_port, zone=None, last_seen=None, leaderId=None, leaderInfo=None, active_clients=0, cached_resources={}):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.zone = zone
        self.ctrl_port = ctrl_port
        self.last_seen = last_seen
        self.leaderId = leaderId
        self.leaderInfo = leaderInfo
        self.active_clients = active_clients

        self.server_ref = None
        self.cached_resources = cached_resources

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
            active_clients=beacon_data.get("active_clients", 0),
            cached_resources=beacon_data.get("resources", {})
        )

    def update_Leader_Info(self, leaderId, leaderInfo):
        self.leaderId = leaderId
        self.leaderInfo = leaderInfo

    def get_leader_Info(self):
        return {"leader_id": self.leaderId, "leader_info": self.leaderInfo}

    def set_server_ref(self, server):
        """Link to TCPServer for live resource access"""
        self.server_ref = server

    # NEW METHOD: Get resources (live if local, cached if remote)
    def get_resources(self):
        """Get resource snapshot"""
        if self.server_ref:
            # Local server - get live data
            return {
                "ambulances": {
                    k: {"status": v.get("status")} 
                    for k, v in self.server_ref.connected_ambulances.items()
                },
                "hospitals": {
                    k: {"current_occupancy": v.get("current_occupancy", 0)} 
                    for k, v in self.server_ref.connected_hospitals.items()
                }
            }
        # Remote server - return cached data from beacon
        return self.cached_resources

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

        self.serverInfo.set_server_ref(self)

        self.event_log = []
        self.global_seq_counter = 0
        self.active_workflows = {}

        self.global_resources = {}

        self.event_ordering_logger = EventOrderingLogger(self.serverInfo.server_id)
        self.last_event_seq = {}

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

    async def update_global_resources_from_beacons(self):
        """Leader: Continuously update global resource map from beacon registry"""
        try:
            while True:
                await asyncio.sleep(2)  # Update every 2 seconds
                
                # Only leader maintains global resources
                if self.registry.get_leader_id() != self.serverInfo.server_id:
                    continue
                
                # Collect resources from all servers in registry
                servers = self.registry.get_all_servers()
                for server in servers:
                    self.global_resources[server.server_id] = server.get_resources()
                
                self.log.debug(f"[LEADER] Updated global resources: "
                              f"{sum(len(r.get('ambulances', {})) for r in self.global_resources.values())} ambulances, "
                              f"{sum(len(r.get('hospitals', {})) for r in self.global_resources.values())} hospitals")
                
        except asyncio.CancelledError:
            self.log.info("Global resource update task cancelled")

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
             # NEW: Workflow handlers
            elif msg.status == "request_workflow":
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    await self.leader_start_workflow(msg.payload)
            
            elif msg.status == "workflow_event":
                await self.handle_global_event(msg.payload)
            
            elif msg.status == "workflow_completed":
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    # Leader sequences completion
                    self.global_seq_counter += 1
                    event = {
                        "seq": self.global_seq_counter,
                        "event_type": "workflow_completed",
                        "workflow_id": msg.payload['workflow_id'],
                        "ambulance_id": msg.payload.get('ambulance_id'),
                        "timestamp": time.time()
                    }
                    await self.broadcast_global_event(event)
            
            elif msg.status == "execute_command":
                command = msg.payload.get("command")
                await self.execute_command(command, msg.payload)
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

    async def leader_start_workflow(self, payload):
        """LEADER ONLY: Allocate resources from global state and broadcast workflow"""
        self.log.info(f"[LEADER] Processing workflow request for car {payload['car_id']}")
        
        # 1. Find resources from GLOBAL resource map (built from beacons)
        assigned_amb, assigned_amb_server = None, None
        assigned_hosp, assigned_hosp_server = None, None
        
        # Search for available ambulance
        for server_id, resources in self.global_resources.items():
            for amb_id, amb_info in resources.get("ambulances", {}).items():
                if amb_info.get("status") == "available":
                    assigned_amb = amb_id
                    assigned_amb_server = server_id
                    self.log.info(f"[LEADER] Found ambulance {amb_id} on server {server_id[:8]}")
                    break
            if assigned_amb:
                break
        
        # Search for hospital with beds
        for server_id, resources in self.global_resources.items():
            for hosp_id, hosp_info in resources.get("hospitals", {}).items():
                if hosp_info.get("current_occupancy", 0) > 0:
                    assigned_hosp = hosp_id
                    assigned_hosp_server = server_id
                    self.log.info(f"[LEADER] Found hospital {hosp_id} on server {server_id[:8]}")
                    break
            if assigned_hosp:
                break

        if not assigned_amb:
            self.log.warning("[LEADER] No ambulance available for workflow")
            return
        
        if not assigned_hosp:
            self.log.warning("[LEADER] No hospital beds available")
            # Still dispatch ambulance, hospital can be assigned later
        
        # 2. Update leader's global resource state (reserve resources)
        if assigned_amb_server in self.global_resources:
            if assigned_amb in self.global_resources[assigned_amb_server].get("ambulances", {}):
                self.global_resources[assigned_amb_server]["ambulances"][assigned_amb]["status"] = "allocated"
        
        if assigned_hosp and assigned_hosp_server in self.global_resources:
            if assigned_hosp in self.global_resources[assigned_hosp_server].get("hospitals", {}):
                current_beds = self.global_resources[assigned_hosp_server]["hospitals"][assigned_hosp].get("current_occupancy", 0)
                self.global_resources[assigned_hosp_server]["hospitals"][assigned_hosp]["current_occupancy"] = max(0, current_beds - 1)
        
        # 3. Create workflow event with sequence number
        self.global_seq_counter += 1
        workflow_id = f"wf_{payload['car_id']}_{self.global_seq_counter}"
        
        event = {
            "seq": self.global_seq_counter,
            "event_type": "workflow_started",
            "workflow_id": workflow_id,
            "car_id": payload['car_id'],
            "crash_location": payload['crash_location'],
            "ambulance_id": assigned_amb,
            "ambulance_server": assigned_amb_server,
            "hospital_id": assigned_hosp,
            "hospital_server": assigned_hosp_server,
            "timestamp": time.time()
        }
        
        # 4. Broadcast to ALL servers for state replication
        await self.broadcast_global_event(event)
        
        # 5. Send execution commands to specific servers
        await self.send_execute_command(assigned_amb_server, "dispatch_ambulance", {
            "workflow_id": workflow_id,
            "ambulance_id": assigned_amb,
            "car_id": payload['car_id'],
            "crash_location": payload['crash_location'],
            "hospital_id": assigned_hosp
        })
        
        if assigned_hosp:
            await self.send_execute_command(assigned_hosp_server, "reserve_bed", {
                "workflow_id": workflow_id,
                "hospital_id": assigned_hosp,
                "car_id": payload['car_id']
            })

    
    async def broadcast_global_event(self, event):
        """Broadcast event to ALL servers for state replication"""
        self.log.info(f"[BROADCAST] Event #{event['seq']}: {event['event_type']}")
        
        servers = [s.to_dict() for s in self.registry.get_all_servers()]
        
        for server in servers:
            if server['server_id'] == self.serverInfo.server_id:
                # Apply locally
                await self.handle_global_event(event)
            else:
                # Send to remote server
                try:
                    msg = ServerMessage(self.serverInfo.server_id, "workflow_event", event)
                    _, w = await asyncio.open_connection(server['host'], server['ctrl_port'])
                    w.write(msg.serialize())
                    await w.drain()
                    w.close()
                    await w.wait_closed()
                except Exception as e:
                    self.log.error(f"Failed to broadcast to {server['server_id'][:8]}: {e}")


    async def handle_global_event(self, event):
        """ALL SERVERS: Apply event to local state (no execution, just replication)"""
        self.event_log.append(event)

        self.event_ordering_logger.log_event(
            seq=event['seq'],
            event_type=event['event_type'],
            workflow_id=event['workflow_id'],
            data=event.get('data', {})
        )
        evt_type = event['event_type']
        
        if evt_type == "workflow_started":
            workflow_id = event['workflow_id']
            self.active_workflows[workflow_id] = event
            self.log.info(f"[STATE] Workflow {workflow_id} registered in event log")
            
            # If we just became leader (failover), rebuild resource state
            if self.registry.get_leader_id() == self.serverInfo.server_id:
                self.rebuild_resource_state_from_event(event)
                
        elif evt_type == "workflow_completed":
            workflow_id = event['workflow_id']
            
            if workflow_id in self.active_workflows:
                workflow = self.active_workflows[workflow_id]
                
                # If we are leader, release resources in global state
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    amb_server = workflow.get('ambulance_server')
                    amb_id = workflow.get('ambulance_id')
                    
                    if amb_server in self.global_resources:
                        if amb_id in self.global_resources[amb_server].get("ambulances", {}):
                            self.global_resources[amb_server]["ambulances"][amb_id]["status"] = "available"
                            self.log.info(f"[LEADER] Released ambulance {amb_id}")
                self.log.debug(f"Event log before removal: {self.event_log}")
                self.log.debug(f"Active workflows before removal: {self.active_workflows}")
                del self.active_workflows[workflow_id]
                self.log.info(f"[STATE] Workflow {workflow_id} completed and removed")
    
    def rebuild_resource_state_from_event(self, event):
        """Leader failover: Reconstruct allocated resources from event log"""
        amb_server = event.get('ambulance_server')
        amb_id = event.get('ambulance_id')
        hosp_server = event.get('hospital_server')
        hosp_id = event.get('hospital_id')
        
        # Mark ambulance as allocated
        if amb_server and amb_id:
            if amb_server not in self.global_resources:
                self.global_resources[amb_server] = {"ambulances": {}, "hospitals": {}}
            if amb_id not in self.global_resources[amb_server]["ambulances"]:
                self.global_resources[amb_server]["ambulances"][amb_id] = {}
            self.global_resources[amb_server]["ambulances"][amb_id]["status"] = "allocated"
            self.log.info(f"[RECOVERY] Rebuilt state: ambulance {amb_id} allocated")

    async def send_execute_command(self, target_server_id, command, data):
        """Send execution command to specific server"""
        if target_server_id == self.serverInfo.server_id:
            # Execute locally
            await self.execute_command(command, data)
        else:
            # Send to remote server
            servers = [s.to_dict() for s in self.registry.get_all_servers()]
            target = next((s for s in servers if s['server_id'] == target_server_id), None)
            
            if target:
                try:
                    msg = ServerMessage(self.serverInfo.server_id, "execute_command", {
                        "command": command,
                        **data
                    })
                    _, w = await asyncio.open_connection(target['host'], target['ctrl_port'])
                    w.write(msg.serialize())
                    await w.drain()
                    w.close()
                    await w.wait_closed()
                    self.log.info(f"[COMMAND] Sent {command} to server {target_server_id[:8]}")
                except Exception as e:
                    self.log.error(f"Failed to send command to {target_server_id[:8]}: {e}")

    # ADD this NEW method to TCPServer class:
    async def execute_command(self, command, data):
        """Execute command from leader (server autonomy starts here)"""
        if command == "dispatch_ambulance":
            amb_id = data['ambulance_id']
            
            if amb_id in self.connected_ambulances:
                self.log.info(f"[EXEC] Dispatching ambulance {amb_id} per leader order")
                amb_writer = self.connected_ambulances[amb_id].get("writer")
                
                if amb_writer:
                    payload = {
                        "ambulance_id": amb_id,
                        "car_id": data['car_id'],
                        "crash_location": data['crash_location'],
                        "hospital_id": data.get('hospital_id')
                    }
                    msg = ServerMessage(self.serverInfo.server_id, "dispatch_ambulance", payload)
                    amb_writer.write(msg.serialize())
                    await amb_writer.drain()
                    
                    # Update local state
                    self.connected_ambulances[amb_id]["status"] = "dispatched"
                    self.connected_ambulances[amb_id]["assigned_hospital"] = data.get('hospital_id')
                    self.connected_ambulances[amb_id]["workflow_id"] = data.get('workflow_id')
                    
        elif command == "reserve_bed":
            hosp_id = data['hospital_id']
            
            if hosp_id in self.connected_hospitals:
                self.log.info(f"[EXEC] Reserving bed at hospital {hosp_id} per leader order")
                
                if self.connected_hospitals[hosp_id]["current_occupancy"] > 0:
                    self.connected_hospitals[hosp_id]["current_occupancy"] -= 1
                    
                # Notify hospital client
                hosp_writer = self.connected_hospitals[hosp_id].get("writer")
                if hosp_writer:
                    msg = ServerMessage(self.serverInfo.server_id, "assign_patient_to_hospital", {
                        "hospital_id": hosp_id,
                        "car_id": data['car_id'],
                        "crash_location": {}
                    })
                    hosp_writer.write(msg.serialize())
                    await hosp_writer.drain()
    
    async def send_message_to_server(self, target_server_id, status, payload):
        """Helper: Send message to specific server"""
        servers = [s.to_dict() for s in self.registry.get_all_servers()]
        target = next((s for s in servers if s['server_id'] == target_server_id), None)
        
        if not target:
            self.log.warning(f"Target server {target_server_id[:8]} not found")
            return
        
        try:
            msg = ServerMessage(self.serverInfo.server_id, status, payload)
            _, w = await asyncio.open_connection(target['host'], target['ctrl_port'])
            w.write(msg.serialize())
            await w.drain()
            w.close()
            await w.wait_closed()
        except Exception as e:
            self.log.error(f"Failed to send message to {target_server_id[:8]}: {e}")

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
        recovered_workflow = None
        for wf_id, workflow in self.active_workflows.items():
            # Check if this client is part of an active workflow
            if workflow.get('ambulance_id') == client_id:
                self.log.info(f"[RECOVERY] Ambulance {client_id} reconnected during workflow {wf_id}")
                recovered_workflow = workflow
                client_info["workflow_id"] = wf_id
                client_info["assigned_hospital"] = workflow.get('hospital_id')
                client_info["status"] = "reconnected"
                break
            elif workflow.get('car_id') == client_id:
                self.log.info(f"[RECOVERY] Car {client_id} reconnected during workflow {wf_id}")
                recovered_workflow = workflow
                break

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

        # NEW: If recovered, send workflow context to client
        if recovered_workflow and client_type == "Ambulance":
            # Resend dispatch info
            dispatch_msg = ServerMessage(self.serverInfo.server_id, "dispatch_ambulance", {
                "ambulance_id": client_id,
                "car_id": recovered_workflow['car_id'],
                "crash_location": recovered_workflow['crash_location'],
                "hospital_id": recovered_workflow.get('hospital_id')
            })
            writer.write(dispatch_msg.serialize())
            await writer.drain()
            self.log.info(f"[RECOVERY] Resent dispatch to ambulance {client_id}")

    async def handle_heartbeat(self, client_id):
        if client_id in self.clients:
            r, w, ct, st, _ = self.clients[client_id]
            self.clients[client_id] = (r, w, ct, st, datetime.now())
            self.log.debug(f"Heartbeat updated for {client_id}")

    async def assign_hospital(self, crash_location, local_crash_count, car_id):
        self.log.debug(f"Available hospitals {self.connected_hospitals}")
        """Pick the hospital with the highest bed count, reserve a bed, and notify it."""
        if not self.connected_hospitals:
            self.log.warning("No hospitals available for assignment")
            return None

        # Filter hospitals with available beds and sort by bed count (highest first)
        hospitals_with_beds = {
            hosp_id: hosp_info 
            for hosp_id, hosp_info in self.connected_hospitals.items() 
            if hosp_info.get("current_occupancy", 0) > 0
        }

        if not hospitals_with_beds:
            self.log.warning("No hospitals with available beds")
            return None

        # Select hospital with highest bed count
        hospital_id, hospital_info = max(
            hospitals_with_beds.items(), 
            key=lambda item: item[1].get("current_occupancy", 0)
        )
        
        current_occupancy = hospital_info.get("current_occupancy")
        
        # Reserve a bed
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
            self.log.info(f"Notified hospital {hospital_id} (beds: {current_occupancy}) about crash {local_crash_count}")
        else:
            self.log.warning(f"No writer found to notify hospital {hospital_id}")

        return hospital_id

    async def assign_ambulance(self, crash_location, hospital_id, local_crash_count, car_id):
        self.log.debug(f"Available ambulances {self.connected_ambulances}")
        """Pick the first available ambulance and dispatch it to crash location with hospital destination."""
        if not self.connected_ambulances:
            self.log.warning("No ambulances connected for dispatch")
            return

        # Filter for ambulances with "available" status
        available_ambulances = {
            amb_id: amb_info 
            for amb_id, amb_info in self.connected_ambulances.items() 
            if amb_info.get("status") == "available"
        }

        if not available_ambulances:
            self.log.warning("No ambulances with 'available' status for dispatch")
            return

        # Pick the first available ambulance in order
        ambulance_id, ambulance_info = next(iter(available_ambulances.items()))
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

        crash_location = {"latitude": latitude, "longitude": longitude}

        leader_id = self.registry.get_leader_id()
        req_payload = {
            "car_id": client_id,
            "crash_location": crash_location,
            "requesting_server": self.serverInfo.server_id,
        }        
        if leader_id == self.serverInfo.server_id:
            self.log.info(f"Leader processing crash reported by {client_id}")
            await self.leader_start_workflow(req_payload)
        else:
            self.log.info(f"Forwarding crash report from {client_id} to leader {leader_id}")
            await self.send_message_to_server(leader_id,"request_workflow", req_payload)


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
        if status == "available":
            # Check if this ambulance was in a workflow
            workflow_id = self.connected_ambulances.get(client_id, {}).get("workflow_id")
            
            if workflow_id and workflow_id in self.active_workflows:
                self.log.info(f"[WORKFLOW] Ambulance {client_id} completed workflow {workflow_id}")
                
                # Clear local workflow tracking
                self.connected_ambulances[client_id]["workflow_id"] = None
                self.connected_ambulances[client_id]["assigned_hospital"] = None
                
                # Notify leader of completion
                leader_id = self.registry.get_leader_id()
                
                if leader_id == self.serverInfo.server_id:
                    # We are leader - sequence and broadcast
                    self.global_seq_counter += 1
                    event = {
                        "seq": self.global_seq_counter,
                        "event_type": "workflow_completed",
                        "workflow_id": workflow_id,
                        "ambulance_id": client_id,
                        "timestamp": time.time()
                    }
                    await self.broadcast_global_event(event)
                else:
                    # Send to leader
                    await self.send_message_to_server(leader_id, "workflow_completed", {
                        "workflow_id": workflow_id,
                        "ambulance_id": client_id
                    })

        # Only allocate hospital bed when ambulance reaches scene
        if status == "arrived_at_scene":
            hosp_id = self.connected_ambulances.get(client_id, {}).get("assigned_hospital")
            car_id = payload.get("call_id") or payload.get("car_id")
            
            if hosp_id:
                # Notify ambulance of hospital assignment (autonomous execution)
                amb_writer = self.connected_ambulances.get(client_id, {}).get("writer")
                if amb_writer:
                    notify_payload = {
                        "hospital_id": hosp_id,
                        "car_id": car_id,
                        "crash_location": {}
                    }
                    msg = ServerMessage(self.serverInfo.server_id, "assign_patient_to_hospital", notify_payload)
                    amb_writer.write(msg.serialize())
                    await amb_writer.drain()
                    self.log.info(f"[AUTONOMY] Sent hospital {hosp_id} to ambulance {client_id}")

        # Notify hospital when ambulance arrives at hospital
        elif status == "at_hospital":
            hospital_id = None
            car_id = None
            if payload:
                hospital_id = payload.get("hospital_id")
                car_id = payload.get("call_id") or payload.get("car_id")

            if not hospital_id or not car_id:
                self.log.warning(f"at_hospital status missing hospital_id or car_id; hospital_id={hospital_id}, car_id={car_id}")
            elif hospital_id in self.connected_hospitals:
                hospital_info = self.connected_hospitals[hospital_id]
                hospital_writer = hospital_info.get("writer")
                if hospital_writer:
                    arrival_payload = {
                        "ambulance_id": client_id,
                        "car_id": car_id,
                        "hospital_id": hospital_id,
                    }
                    # Use an existing server status to avoid validation failures on clients
                    msg = ServerMessage(self.serverInfo.server_id, "ack_at_hospital", arrival_payload)
                    hospital_writer.write(msg.serialize())
                    await hospital_writer.drain()
                    self.log.info(f"Notified hospital {hospital_id} that ambulance {client_id} has arrived with patient from car {car_id}")
                else:
                    self.log.warning(f"No writer found to notify hospital {hospital_id}")
            else:
                self.log.warning(f"Hospital {hospital_id} not found in connected hospitals")

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

    def print_all_clients(self):
        """Print all connected clients and their statuses in a neat formatted table."""
        print("\n" + "="*100)
        print(f"{'CLIENT STATUS REPORT':^100}")
        print("="*100)
        
        total_clients = len(self.clients)
        print(f"\nTotal Connected Clients: {total_clients}\n")
        
        # Print Cars
        print("-" * 100)
        print(f"{'CARS':^100}")
        print("-" * 100)
        if self.connected_cars:
            print(f"{'Client ID':<30} {'Status':<20} {'Latitude':<15} {'Longitude':<15} {'Registered':<20}")
            print("-" * 100)
            for car_id, car_info in self.connected_cars.items():
                status = car_info.get("status", "N/A")
                latitude = car_info.get("latitude", "N/A")
                longitude = car_info.get("longitude", "N/A")
                if car_id in self.clients:
                    _, _, _, _, registered_time = self.clients[car_id]
                    registered_str = registered_time.strftime("%Y-%m-%d %H:%M:%S") if registered_time else "N/A"
                else:
                    registered_str = "N/A"
                print(f"{car_id:<30} {status:<20} {str(latitude):<15} {str(longitude):<15} {registered_str:<20}")
        else:
            print("No cars connected")
        
        # Print Ambulances
        print("\n" + "-" * 100)
        print(f"{'AMBULANCES':^100}")
        print("-" * 100)
        if self.connected_ambulances:
            print(f"{'Ambulance ID':<30} {'Status':<20} {'Registered':<20}")
            print("-" * 100)
            for amb_id, amb_info in self.connected_ambulances.items():
                status = amb_info.get("status", "N/A")
                if amb_id in self.clients:
                    _, _, _, _, registered_time = self.clients[amb_id]
                    registered_str = registered_time.strftime("%Y-%m-%d %H:%M:%S") if registered_time else "N/A"
                else:
                    registered_str = "N/A"
                print(f"{amb_id:<30} {status:<20} {registered_str:<20}")
        else:
            print("No ambulances connected")
        
        # Print Hospitals
        print("\n" + "-" * 100)
        print(f"{'HOSPITALS':^100}")
        print("-" * 100)
        if self.connected_hospitals:
            print(f"{'Hospital ID':<30} {'Status':<20} {'Beds Available':<15} {'Registered':<20}")
            print("-" * 100)
            for hosp_id, hosp_info in self.connected_hospitals.items():
                status = hosp_info.get("status", "N/A")
                occupancy = hosp_info.get("current_occupancy", "N/A")
                if hosp_id in self.clients:
                    _, _, _, _, registered_time = self.clients[hosp_id]
                    registered_str = registered_time.strftime("%Y-%m-%d %H:%M:%S") if registered_time else "N/A"
                else:
                    registered_str = "N/A"
                print(f"{hosp_id:<30} {status:<20} {str(occupancy):<15} {registered_str:<20}")
        else:
            print("No hospitals connected")
        
        print("\n" + "="*100 + "\n")

    async def periodic_status_print(self):
        """Periodically print the status of all connected clients."""
        try:
            while True:
                await asyncio.sleep(30)  # Print every 30 seconds
                self.print_all_clients()
        except asyncio.CancelledError:
            self.log.info("Periodic status print task cancelled")

    async def start(self):
        # Start monitoring and cleanup tasks
        monitor_task = asyncio.create_task(self.monitor_leader())
        cleanup_task = asyncio.create_task(self.cleanup_task())
        status_task = asyncio.create_task(self.periodic_status_print())
        resource_task = asyncio.create_task(self.update_global_resources_from_beacons())

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
                status_task.cancel()
                resource_task.cancel()
                await asyncio.gather(monitor_task, cleanup_task, status_task, resource_task, return_exceptions=True)