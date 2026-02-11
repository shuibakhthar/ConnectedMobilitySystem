import asyncio
from datetime import datetime, timedelta
import uuid
import time
from components.client_message import deserialize_client_message
from components.server_message import ServerMessage
from components.multicast_server import MulticastServer

from config.settings import (
    LEADER_MONITOR_INTERVAL,
    LEADER_HEARTBEAT_TIMEOUT,
    BEACON_INTERVAL,
    TCP_SERVER_LOGGER,
    RETRY_BUFFERED_EVENTS_INTERVAL,
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
        self.start_time = time.time()
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
        # self.global_seq_counter = 0
        self.active_workflows = {}

        self.global_resources = {}

        self.event_ordering_logger = EventOrderingLogger(self.serverInfo.server_id)
        self.workflow_last_event_seq = {}

        self.election = BullyElection(
            serverInfo=self.serverInfo,
            registry=self.registry,
        )

        self.local_events_to_sequence = []

        self.multicast = MulticastServer(
            server_id=self.serverInfo.server_id,
            host=self.serverInfo.host,
            logger=self.log
        )
        self.multicast.initialize_receiver()

    async def listen_multicast(self):
        """
        Background task: Listen for multicast events on all followers + leader.
        This runs continuously and processes incoming multicast batches.
        """
        while True:
            try:
                await asyncio.sleep(0.01)  # Yield to event loop
                
                batch = self.multicast.receive_batch()
                if batch:
                    self.log.info(
                        f"[MULTICAST-RX] Received batch: seq {batch['seq_range']} "
                        f"({batch['event_count']} events)"
                    )
                    
                    # Process the batch
                    await self.handle_global_event({
                        "events": batch['events']
                    })
                    
            except asyncio.CancelledError:
                self.log.info("[MULTICAST] Listener task cancelled")
                break
            except Exception as e:
                self.log.error(f"[MULTICAST] Listener error: {e}")
                await asyncio.sleep(1)

    def get_next_global_seq_counter(self):
        """Get max sequence number from local event log"""
        self.registry.global_seq_counter = self.registry.global_seq_counter + 1
        return self.registry.global_seq_counter
    
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
                self.log.info("My server ID: %s, Current leader ID: %s, HOST: %s, PORT: %s", self.serverInfo.server_id, self.registry.get_leader_id() if self.registry.get_leader_id() else None, self.serverInfo.host, self.serverInfo.port)
                # Startup grace: wait for beacon discovery before electing
                time_since_start = time.time() - self.start_time
                if time_since_start < (BEACON_INTERVAL * 2):
                    known_servers = self.get_local_server_list()
                    if len(known_servers) <= 1 and not self.registry.get_leader_id():
                        self.log.info("Discovery warm-up (%0.1fs). Skipping election.", time_since_start)
                        continue
                
                if self.is_leader_missing_or_dead():
                    self.log.warning("Leader missing or dead, starting election")
                    await self.election.start_election()
        except asyncio.CancelledError:
            self.log.info("Leader monitor task cancelled")
        except Exception as e:
            self.log.error(f"Error in leader monitor task: {e}")

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

    async def periodic_retry_buffered_events(self):
        try:
            while True:
                await asyncio.sleep(RETRY_BUFFERED_EVENTS_INTERVAL)
                leader_id = self.registry.get_leader_id()  # Retry every 10 seconds
                if self.local_events_to_sequence and leader_id and leader_id != self.serverInfo.server_id:
                    self.log.info(f"Retrying to send {len(self.local_events_to_sequence)} buffered events to leader")
                    await self.send_buffered_events_to_leader(leader_id=leader_id)
        except asyncio.CancelledError:
            self.log.info("Periodic retry of buffered events task cancelled")

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

    async def on_became_leader(self):
        """Called when this server becomes the new leader via election"""
        self.log.debug("=" * 60)
        self.log.debug(f"[LEADER TAKEOVER] Server {self.serverInfo.server_id[:8]} became leader")
        self.log.debug("=" * 60)

        # Step 0: Request buffered events from other servers
        await self.request_buffered_events_from_servers()

        if self.local_events_to_sequence:
            self.log.info(f"[LEADER TAKEOVER] There are {len(self.local_events_to_sequence)} local buffered events to sequence")
            events_by_workflow = {}
            for event in self.local_events_to_sequence:
                wf_id = event['workflow_id']
                if wf_id not in events_by_workflow:
                    events_by_workflow[wf_id] = []
                events_by_workflow[wf_id].append(event)
            for wf_id, events in events_by_workflow.items():
                await self.leader_sequence_status_events({
                    "workflow_id": wf_id,
                    "events": events
                })
            self.local_events_to_sequence = []  # Clear after sequencing
            self.log.info(f"[LEADER TAKEOVER] Cleared local buffered events after sequencing")
        
        # Step 1: Rebuild resource state from ALL active workflows
        self.log.info(f"[LEADER TAKEOVER] Rebuilding resources from {len(self.active_workflows)} active workflows")
        for wf_id, workflow in self.active_workflows.items():
            # Check if already completed
            completed = any(
                e['workflow_id'] == wf_id and e['event_type'] == 'workflow_completed'
                for e in self.event_log
            )
            if not completed:
                self.rebuild_resource_state_from_event(workflow)
                self.log.info(f"[LEADER TAKEOVER] Rebuilt resources for workflow {wf_id}")
        
        # Step 2: Check for workflows that need completion events
        await self.detect_completed_workflows()
        
        self.log.info(f"[LEADER TAKEOVER] Takeover complete. Managing {len(self.active_workflows)} active workflows")

    async def request_buffered_events_from_servers(self):
        """Request any buffered global events from other servers after becoming leader"""
        self.log.info(f"[LEADER] Requesting buffered events from other servers")
        servers = [s.to_dict() for s in self.registry.get_all_servers()]
        for server in servers:
            if server['server_id'] != self.serverInfo.server_id:
                try:
                    reader, writer = await asyncio.open_connection(server['host'], server['ctrl_port'])
                    msg = ServerMessage(
                        server_id=self.serverInfo.server_id,
                        status="request_buffered_events",
                        payload={}
                    )
                    writer.write(msg.serialize())
                    await writer.drain()
                    writer.close()
                    await writer.wait_closed()
                    self.log.debug(f"[LEADER] Requested buffered events from server {server['server_id'][:8]}")
                except Exception as e:
                    self.log.error(f"[LEADER] Failed to request buffered events from server {server['server_id'][:8]}: {e}")

    async def detect_completed_workflows(self):
        """Check if any ambulances are available but workflows not marked complete"""
        for wf_id, workflow in list(self.active_workflows.items()):
            # Skip if already has completion event
            completed = any(
                e['workflow_id'] == wf_id and e['event_type'] == 'workflow_completed'
                for e in self.event_log
            )
            if completed:
                continue
            
            # Check ambulance status
            amb_id = workflow.get('data', {}).get('ambulance_id')
            if amb_id in self.connected_ambulances:
                amb_status = self.connected_ambulances[amb_id].get('status')
                
                if amb_status == 'available':
                    # Ambulance finished but completion event was lost
                    self.log.warning(f"[RECOVERY] Workflow {wf_id}: ambulance {amb_id} is available but workflow not completed")
                    self.log.warning(f"[RECOVERY] Creating missing completion event for workflow {wf_id}")
                    
                    next_seq = self.get_next_global_seq_counter()
                    complete_event = self.event_ordering_logger.log_event(
                        sequence_counter=next_seq,
                        workflow_id=wf_id,
                        event_type="workflow_completed",
                        event_sub_type="workflow_completed",
                        description=f"[RECOVERY] Workflow {wf_id} completed (detected on leader takeover)",
                        depends_on=[],
                        data={
                            "workflow_id": wf_id,
                            "ambulance_id": amb_id,
                            "timestamp": time.time(),
                            "recovery": True
                        }
                    )
                    
                    event = {
                        "seq": next_seq,
                        "event_type": "workflow_completed",
                        "workflow_id": wf_id,
                        "ambulance_id": amb_id,
                        "timestamp": time.time()
                    }
                    
                    await self.broadcast_global_event([complete_event])
                    self.log.debug(f"[RECOVERY] Broadcast completion event for workflow {wf_id}")

    async def send_buffered_events_to_leader(self, leader_id=None, leader_info=None):
        """Send buffered global events to current leader"""
        leader_id = self.registry.get_leader_id() if leader_id is None else leader_id
        if self.local_events_to_sequence:
            events_by_workflow = {}
            for event in self.local_events_to_sequence:
                wf_id = event['workflow_id']
                if wf_id not in events_by_workflow:
                    events_by_workflow[wf_id] = []
                events_by_workflow[wf_id].append(event)
            try:
                for wf_id, events in events_by_workflow.items():
                        await self.send_message_to_server(leader_id, "status_update_report", {
                            "workflow_id": wf_id,
                            "events": events
                        })
                        self.log.info(f"[BUFFER RECOVERY] Sent {len(events)} buffered events for workflow {wf_id} to leader {leader_id[:8]}")
            except Exception as e:
                self.log.error(f"[BUFFER RECOVERY] Failed to send buffered events for workflow {wf_id} to leader {leader_id[:8]}: {e}")
            self.local_events_to_sequence =[]  # Clear after sending
            self.log.info(f"[BUFFER RECOVERY] Cleared local buffered events after sending to leader {leader_id[:8]}")
    
    async def handle_recovery_request(self, requesting_server_id):
        self.log.info(f"[RECOVERY] {requesting_server_id} requesting event log recovery")
                
        # Send full event log + metadata
        recovery_payload = {
            "last_seq": self.registry.global_seq_counter,
            "event_log": self.event_log,
            "active_workflows": self.active_workflows
        }
        
        response = ServerMessage(
            server_id = self.serverInfo.server_id,
            status = "event_log_recovery_response",
            payload = recovery_payload
        )
        
        # Extract requester address from active connections and send back
        # (Alternative: look up in registry and connect back)
        try:
            for client_conn in self.connected_servers.values():
                if client_conn.get('server_id') == requesting_server_id:
                    # Send via existing connection
                    client_conn['writer'].write(response.serialize())
                    await client_conn['writer'].drain()
                    break
        except Exception as e:
            self.log.error(f"[RECOVERY] Failed to send recovery response: {e}")
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
            
            elif msg.status == "workflow_event" or msg.status == "workflow_event_batch":
                await self.handle_global_event(msg.payload)
            
            elif msg.status == "workflow_completed":
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    workflow_id = msg.payload['workflow_id']
                    next_seq = self.get_next_global_seq_counter()
                    # Leader sequences completion
                    complete_event = self.event_ordering_logger.log_event(  
                        sequence_counter=next_seq,
                        workflow_id=workflow_id,
                        event_type="workflow_completed",
                        event_sub_type="workflow_completed",
                        description=f"[LEADER] Workflow {workflow_id} completed",
                        depends_on=[],
                        data={
                            "workflow_id": workflow_id,
                            "ambulance_id": msg.payload.get('data', {}).get('ambulance_id'),
                            "timestamp": time.time()
                        }
                    )
                    event = {
                        "seq": self.registry.global_seq_counter,
                        "event_type": "workflow_completed",
                        "workflow_id": msg.payload['workflow_id'],
                        "ambulance_id": msg.payload.get('data', {}).get('ambulance_id'),
                        "timestamp": time.time()
                    }
                    await self.broadcast_global_event([complete_event])
            
            elif msg.status == "execute_command":
                command = msg.payload.get("command")
                await self.execute_command(command, msg.payload)
            elif msg.status == "status_update_report":
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    await self.leader_sequence_status_events(msg.payload)
            elif msg.status == "request_buffered_events":
                await self.send_buffered_events_to_leader(msg.server_id)
            elif msg.status == "request_event_log_recovery":
                await self.handle_recovery_request(msg.server_id)
            elif msg.status == "event_log_recovery_response":
                payload = msg.payload
                self.log.info(f"Received event log recovery response with {len(payload.get('events', []))} events")
                self.event_log = payload.get("events", [])
                self.active_workflows = payload.get("active_workflows", {})
                self.registry.global_seq_counter = payload.get("global_seq_counter", self.registry.global_seq_counter)
                self.log.info(f"Updated local event log and active workflows from recovery response")
            elif msg.status == "client_reconnected":
                # Leader: client reconnected to different server
                if self.registry.get_leader_id() == self.serverInfo.server_id:
                    client_id = msg.payload.get('client_id')
                    workflow_id = msg.payload.get('workflow_id')
                    new_server_id = msg.payload.get('server_id')
                    
                    self.log.info(f"[LEADER] Client {client_id} reconnected to server {new_server_id[:8]} (workflow {workflow_id})")
                    
                    # Check if workflow needs completion
                    if workflow_id in self.active_workflows:
                        await self.detect_completed_workflows()
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
        next_seq = self.get_next_global_seq_counter()
        workflow_id = f"wf_{payload['car_id']}_{next_seq}"
        events_to_broadcast = []
        
        req_seq = self.event_ordering_logger.log_event(
            sequence_counter=next_seq,
            workflow_id=workflow_id,
            event_type="workflow_event",
            event_sub_type="request_received",
            description=f"[LEADER] Processing Workflow request for car {payload['car_id']}",
            depends_on=[],
            data={"car_id": payload['car_id'], "crash_location": payload['crash_location']}
        )
        
        amb_seq = self.event_ordering_logger.log_event(
            sequence_counter= self.get_next_global_seq_counter(),
            workflow_id=workflow_id,
            event_type = "workflow_event",
            event_sub_type="resource_found",
            description=f"[LEADER] Found ambulance {assigned_amb} on server {assigned_amb_server}",
            depends_on=[req_seq.get("seq")],
            data={"resource":"ambulance", "resource_id": assigned_amb, "server": assigned_amb_server}
        )
        if assigned_hosp:
            hosp_seq = self.event_ordering_logger.log_event(
                sequence_counter= self.get_next_global_seq_counter(),
                workflow_id=workflow_id,
                event_type="workflow_event",
                event_sub_type="resource_found",
                description=f"[LEADER] Found hospital {assigned_hosp} on server {assigned_hosp_server}",
                depends_on=[amb_seq.get("seq")],
                data={"resource":"hospital", "resource_id": assigned_hosp, "server": assigned_hosp_server}
            )
        else:
            hosp_seq = self.event_ordering_logger.log_event(
                sequence_counter= self.get_next_global_seq_counter(),
                workflow_id=workflow_id,
                event_type="workflow_event",
                event_sub_type="resource_found",
                description=f"[LEADER] No hospital found for workflow {workflow_id}, ambulance will handle.",
                depends_on=[amb_seq.get("seq")],
                data={"resource":"hospital", "resource_id": assigned_hosp, "server": assigned_hosp_server}
            )

        start_seq = self.event_ordering_logger.log_event(
            sequence_counter= self.get_next_global_seq_counter(),
            workflow_id=workflow_id,
            event_type="workflow_started",
            event_sub_type="workflow_started",
            description=f"[BROADCAST] EVENT #{next_seq}: Workflow {workflow_id} started",
            depends_on=[amb_seq.get("seq"), hosp_seq.get("seq")],
            data={
                "car_id": payload['car_id'],
                "crash_location": payload['crash_location'],
                "ambulance_id": assigned_amb,
                "ambulance_server": assigned_amb_server,
                "hospital_id": assigned_hosp,
                "hospital_server": assigned_hosp_server,
                "global_seq": self.registry.global_seq_counter,
                "timestamp": time.time()
            }
        )

        events_to_broadcast.extend([req_seq, amb_seq, hosp_seq, start_seq])

        event = {
            "seq": self.registry.global_seq_counter,
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
        # await self.broadcast_global_event(self.event_log)
        
        # 5. Send execution commands to specific servers

        cmd_seq = self.event_ordering_logger.log_event(
            sequence_counter= self.get_next_global_seq_counter(),
            workflow_id=workflow_id,
            event_type="workflow_event",
            event_sub_type="command_sent",
            description=f"[LEADER] Sending execute_command to ambulance server {assigned_amb_server} to dispatch ambulance {assigned_amb}",
            depends_on=[start_seq.get("seq")],
            data={"command":"dispatch_ambulance", "ambulance_id": assigned_amb, "ambulance_server": assigned_amb_server}
        )
        await self.send_execute_command(assigned_amb_server, "dispatch_ambulance", {
            "workflow_id": workflow_id,
            "ambulance_id": assigned_amb,
            "car_id": payload['car_id'],
            "crash_location": payload['crash_location'],
            "hospital_id": assigned_hosp
        })
        
        if assigned_hosp:
            hosp_cmd_seq = self.event_ordering_logger.log_event(
                sequence_counter= self.get_next_global_seq_counter(),
                workflow_id=workflow_id,
                event_type="workflow_event",
                event_sub_type="command_sent",
                description=f"[LEADER] Sending execute_command to hospital server {assigned_hosp_server} to reserve bed at hospital {assigned_hosp}",
                depends_on=[cmd_seq.get("seq")],
                data={"command":"reserve_bed", "hospital_id": assigned_hosp, "hospital_server": assigned_hosp_server}
            )
            await self.send_execute_command(assigned_hosp_server, "reserve_bed", {
                "workflow_id": workflow_id,
                "hospital_id": assigned_hosp,
                "car_id": payload['car_id']
            })
        events_to_broadcast.extend([cmd_seq, hosp_cmd_seq if assigned_hosp else cmd_seq])
        await self.broadcast_global_event(events_to_broadcast)
    
    async def leader_sequence_status_events(self, payload):
        """LEADER ONLY: Sequence status changes reported from servers"""
        # workflow_id = payload.get('workflow_id')
        status_events_data = payload.get('events', [])  # List of {client_id, status, description}
        
        
        events_to_broadcast = []
        workflow_last_seq={}
        
        for i, status_data in enumerate(status_events_data):
            next_seq = self.get_next_global_seq_counter()
            
            event_workflow_id = status_data.get('workflow_id')
            self.log.info(f"[LEADER] Sequencing {len(status_events_data)} status updates for workflow {event_workflow_id}")

            depends_on = []
            if event_workflow_id in workflow_last_seq:
                depends_on = [workflow_last_seq[event_workflow_id]]

            status_event = self.event_ordering_logger.log_event(
                sequence_counter=next_seq,
                workflow_id=event_workflow_id,
                event_type= status_data.get("event_type", "workflow_event"),
                event_sub_type=status_data.get("event_sub_type", "sequence_update"),
                description=status_data.get('description', ''),
                # depends_on=[next_seq - 1] if i > 0 else [],
                data= status_data.get('data', {})
            )
            events_to_broadcast.append(status_event)
            workflow_last_seq[event_workflow_id] = next_seq
            # self.registry.global_seq = next_seq
        
        # Broadcast all status changes together
        if events_to_broadcast:
            await self.broadcast_global_event(events_to_broadcast) 

    async def broadcast_global_event(self, events):
        """Broadcast event to ALL servers for state replication"""
        events_list = events if isinstance(events, list) else [events]

        for event in events_list:
            # self.event_log.append(event)
            if event.get('seq',0) > self.registry.global_seq_counter:
                self.registry.global_seq_counter = event['seq']

            self.log.info(f"[BROADCAST] Event #{event['seq']}: {event['event_type']} : {event.get('event_sub_type','')} for workflow {event['workflow_id']}")

        multicast_success =  self.multicast.send_batch(events_list)
        if multicast_success:
            self.log.debug(f"[MULTICAST] Successfully sent event batch via multicast")
            return
        else:            
            self.log.warning(f"[MULTICAST] Multicast failed, falling back to unicast for event batch")

            servers = [s.to_dict() for s in self.registry.get_all_servers()]
            
            for server in servers:
                if server['server_id'] == self.serverInfo.server_id:
                    # Apply locally
                    await self.handle_global_event({"events": events_list})
                else:
                    # Send to remote server
                    try:
                        msg = ServerMessage(self.serverInfo.server_id, "workflow_event_batch", {"events":events_list})
                        _, w = await asyncio.open_connection(server['host'], server['ctrl_port'])
                        w.write(msg.serialize())
                        await w.drain()
                        w.close()
                        await w.wait_closed()
                    except Exception as e:
                        self.log.error(f"Failed to broadcast to {server['server_id'][:8]}: {e}")


    async def handle_global_event(self, payload):
        if 'events' in payload:
            events_list = payload.get('events', [])
        else:
            events_list = [payload]

        # Missing dependency check

        missing_seqs = set()
        for event in events_list:
            depends_on = event.get('depends_on', [])
            for dep_seq in depends_on:
                # check both local event log and incoming batch for dependency
                if not any(e['seq'] == dep_seq for e in self.event_log) and not any(e['seq'] == dep_seq for e in events_list): 
                    missing_seqs.add(dep_seq)
        if missing_seqs:
            self.log.warning(f"[GLOBAL EVENT] Missing dependencies for event batch: {missing_seqs}. Requesting snapshot from leader.")
            await self.request_recovery_from_leader()
            return

        for event in events_list:
            """ALL SERVERS: Apply event to local state (no execution, just replication)"""
            if any(e['seq'] == event['seq'] for e in self.event_log):
                self.log.debug(f"[DUPLICATE] Event #{event['seq']} already processed, skipping.")
                continue  # Skip duplicate event

            self.event_log.append(event)
            if event.get('seq',0) > self.registry.global_seq_counter:
                self.registry.global_seq_counter = event['seq']

            # recv_seq = self.event_ordering_logger.log_event(
            #     sequence_counter=event['seq'],
            #     workflow_id=event['workflow_id'],
            #     event_type=event['event_type'],
            #     description=f"[RECEIVE] Event #{event['seq']}: {event['event_type']} : {event.get('event_sub_type','')} for workflow {event['workflow_id']}",
            #     data=event
            # )
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

                self.log.debug(f"[DEBUG] Received workflow_completed event for workflow_id: {workflow_id}")
                self.log.debug(f"[DEBUG] Current active_workflows keys: {list(self.active_workflows.keys())}")
                self.log.debug(f"[DEBUG] Full workflow_completed event: {event}")
                
                if workflow_id in self.active_workflows:
                    workflow = self.active_workflows[workflow_id]
                    
                    # If we are leader, release resources in global state
                    if self.registry.get_leader_id() == self.serverInfo.server_id:
                        workflow_data = workflow.get('data', {})
                        amb_server = workflow_data.get('ambulance_server')
                        amb_id = workflow_data.get('ambulance_id')
                        
                        if amb_server in self.global_resources:
                            if amb_id in self.global_resources[amb_server].get("ambulances", {}):
                                self.global_resources[amb_server]["ambulances"][amb_id]["status"] = "available"
                                self.log.info(f"[LEADER] Released ambulance {amb_id}")
                    self.log.debug(f"Event log before removal: {self.event_log}")
                    self.log.debug(f"Active workflows before removal: {self.active_workflows}")
                    del self.active_workflows[workflow_id]
                    self.log.info(f"[STATE] Workflow {workflow_id} completed and removed")
                else:
                    self.log.warning(f"[DEBUG] Workflow {workflow_id} NOT found in active_workflows")
                    self.log.warning(f"[DEBUG] Available workflows: {list(self.active_workflows.keys())}")
    
    async def request_recovery_from_leader(self):
        """
        Follower detects missing dependencies → request full event log from leader.
        This is the TCP recovery fallback when multicast packets are lost.
        """
        leader_id = self.registry.get_leader_id()
        if leader_id == self.serverInfo.server_id:
            self.log.debug("[RECOVERY] I am the leader, no need to request.")
            return
        
        # Find leader's address
        leader_info = None
        for server in self.registry.get_all_servers():
            if server.server_id == leader_id:
                leader_info = server
                break
        
        if not leader_info:
            self.log.error("[RECOVERY] Leader not found in server registry")
            return
        
        try:
            self.log.info(
                f"[RECOVERY] Requesting event log from leader "
                f"{leader_info.server_id[:8]} at {leader_info.host}:{leader_info.ctrl_port}"
            )
            
            # Request recovery
            msg = ServerMessage(
                server_id =self.serverInfo.server_id,
                status = "request_event_log_recovery",
                payload = {
                    "current_last_seq": self.registry.global_seq_counter,
                    "event_log_size": len(self.event_log)
                }
            )
            
            _, w = await asyncio.open_connection(
                leader_info.host,
                leader_info.ctrl_port
            )
            w.write(msg.serialize())
            await w.drain()
            w.close()
            await w.wait_closed()
            
            self.log.info("[RECOVERY] Event log recovery request sent to leader")
        except Exception as e:
            self.log.error(f"[RECOVERY] Failed to request leader: {e}")

    def rebuild_resource_state_from_event(self, event):
        """Leader failover: Reconstruct allocated resources from event log"""
        event_data = event.get('data', {})
        amb_server = event_data.get('ambulance_server')
        amb_id = event_data.get('ambulance_id')
        hosp_server = event_data.get('hospital_server')
        hosp_id = event_data.get('hospital_id')
        
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
        self.log.info(f"[RECOVERY DEBUG] Checking {len(self.active_workflows)} workflows for client {client_id}")
        for wf_id, workflow in self.active_workflows.items():
            workflow_data = workflow.get('data', {})
            amb_id = workflow_data.get('ambulance_id')
            car_id = workflow_data.get('car_id')
            self.log.info(f"[RECOVERY DEBUG] Workflow {wf_id}: amb_id={amb_id} (type={type(amb_id).__name__}), car_id={car_id}")
            self.log.info(f"[RECOVERY DEBUG] Comparing ambulance: '{amb_id}' == '{client_id}' ? {amb_id == client_id}")
            # Check if this client is part of an active workflow
            if workflow_data.get('ambulance_id') == client_id:
                self.log.info(f"[RECOVERY] Ambulance {client_id} reconnected during workflow {wf_id}")
                recovered_workflow = workflow
                client_info["workflow_id"] = wf_id
                client_info["assigned_hospital"] = workflow_data.get('hospital_id')
                client_info["status"] = "reconnected"
                break
            elif workflow_data.get('car_id') == client_id:
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
            workflow_data = recovered_workflow.get('data', {})
            dispatch_msg = ServerMessage(self.serverInfo.server_id, "dispatch_ambulance", {
                "ambulance_id": client_id,
                "car_id": workflow_data.get('car_id'),
                "crash_location": workflow_data.get('crash_location'),
                "hospital_id": workflow_data.get('hospital_id')
            })
            writer.write(dispatch_msg.serialize())
            await writer.drain()
            self.log.info(f"[RECOVERY] Resent dispatch to ambulance {client_id}")
        
        # # Notify leader that client reconnected (in case leader changed)
        #     leader_id = self.registry.get_leader_id()
        #     if leader_id and leader_id != self.serverInfo.server_id:
        #         try:
        #             await self.send_message_to_server(leader_id, "client_reconnected", {
        #                 "client_id": client_id,
        #                 "workflow_id": wf_id,
        #                 "server_id": self.serverInfo.server_id
        #             })
        #             self.log.info(f"[RECOVERY] Notified leader that {client_id} reconnected to this server")
        #         except Exception as e:
        #             self.log.warning(f"[RECOVERY] Failed to notify leader: {e}")

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

        workflow_id = self.connected_ambulances.get(client_id, {}).get("workflow_id")
        if workflow_id:
            self.log.info(f" Creating event for ambulance status update to {display_status} in workflow {workflow_id}")
            event = {
                "workflow_id": workflow_id,
                "event_type":"workflow_event",
                "event_sub_type":"status_update",
                "description":f"{client_type} {client_id} status changed to {display_status}",
                "data":{
                    "client_type": client_type,
                    "client_id": client_id,
                    "status": display_status
                }
            }
            # status_seq = self.event_ordering_logger.log_event(
            #     sequence_counter=self.get_next_global_seq_counter(),
            #     workflow_id=workflow_id,
            #     event_type="workflow_event",
            #     event_sub_type="status_update",
            #     description=f"{client_type} {client_id} status changed to {display_status}",
            #     data={
            #         "client_type": client_type,
            #         "client_id": client_id,
            #         "status": display_status
            #     }
            # )
            if len(self.local_events_to_sequence) > 5:
                self.log.info(f"[STATUS] Buffer has {len(self.local_events_to_sequence)} events, sending to leader for sequencing")
            
            self.local_events_to_sequence.append(event)


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
                # leader_id = self.registry.get_leader_id()
                
                # if leader_id == self.serverInfo.server_id:
                #     # We are leader - sequence and broadcast
                #     nxt_seq = self.get_next_global_seq_counter()
                #     comp_seq = self.event_ordering_logger.log_event(
                #         sequence_counter=nxt_seq,
                #         workflow_id=workflow_id,
                #         event_type="workflow_completed",
                #         event_sub_type="workflow_completed",
                #         description=f"[BROADCAST] Event #{nxt_seq}: workflow_completed for workflow {workflow_id} by ambulance {client_id}",
                #         depends_on=[status_seq.get("seq")] if status_seq else [],
                #         data={
                #             "workflow_id": workflow_id
                #         }
                #     )
                #     event = {
                #         "seq": nxt_seq,
                #         "event_type": "workflow_completed",
                #         "workflow_id": workflow_id,
                #         "ambulance_id": client_id,
                #         "timestamp": time.time()
                #     }
                #     events_to_sequence.append(comp_seq)
                # else:
                #     # Send to leader
                #     await self.send_message_to_server(leader_id, "workflow_completed", {
                #         "workflow_id": workflow_id,
                #         "ambulance_id": client_id
                #     })
                event = {
                    "workflow_id": workflow_id,
                    "event_type":"workflow_completed",
                    "event_sub_type":"workflow_completed",
                    "description":f"Workflow {workflow_id} completed by ambulance {client_id}",
                    "data":{
                        "workflow_id": workflow_id,
                        "ambulance_id": client_id
                    }
                }
                self.local_events_to_sequence.append(event)

                if self.local_events_to_sequence:
                    # open connection to leader and send events for sequencing
                    leader_id = self.registry.get_leader_id()
                    if leader_id == self.serverInfo.server_id:
                        await self.leader_sequence_status_events({
                            "workflow_id": workflow_id,
                            "events": self.local_events_to_sequence
                        })
                        self.local_events_to_sequence = []
                    else:
                        try:
                            await self.send_message_to_server(leader_id, "status_update_report", {
                                "workflow_id": workflow_id,
                                "events": self.local_events_to_sequence
                            })
                            self.local_events_to_sequence = []
                            self.log.info(f"[STATUS] Sent {len(self.local_events_to_sequence)} events to leader, buffer cleared")
                        except Exception as e:
                            self.log.error(f"Failed to send status update report to leader {leader_id}: {e}")


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
        """Print entire server status including clients, workflows, events, and resources."""
        print("\n" + "="*120)
        print(f"{'ENTIRE SERVER STATUS REPORT':^120}")
        print("="*120)
        
        total_clients = len(self.clients)
        print(f"\nTotal Connected Clients: {total_clients}")
        print(f"Total Active Workflows: {len(self.active_workflows)}")
        print(f"Total Events Logged: {len(self.event_log)}")
        print(f"Global Sequence Counter: {self.registry.global_seq_counter}\n")
        
        # Print All Known Servers in Cluster
        print("-" * 120)
        print(f"{'ALL KNOWN SERVERS IN CLUSTER':^120}")
        print("-" * 120)
        servers = self.get_local_server_list()
        if servers:
            print(f"{'Server ID':<30} {'Role':<12} {'Host':<20} {'Port':<8} {'Ctrl Port':<10} {'Active Clients':<15}")
            print("-" * 120)
            for server in servers:
                server_id = server['server_id']
                display_server_id = server_id[-8:] if server_id else "N/A"
                leader_id = self.registry.get_leader_id()
                is_leader = leader_id == server_id
                leader_status = "LEADER" if is_leader else "FOLLOWER"
                host = server.get('host', 'N/A')
                port = server.get('port', 'N/A')
                ctrl_port = server.get('ctrl_port', 'N/A')
                active_clients = server.get('active_clients', 0)
                print(f"{display_server_id:<30} {leader_status:<12} {str(host):<20} {str(port):<8} {str(ctrl_port):<10} {str(active_clients):<15}")
        else:
            print("No servers found in registry")
        
        # Print Current Server Status
        print("\n" + "-" * 120)
        print(f"{'CURRENT SERVER DETAILS':^120}")
        print("-" * 120)
        if self.serverInfo:
            server_id = self.serverInfo.server_id
            display_server_id = server_id[-8:] if server_id else "N/A"
            is_leader = self.registry.get_leader_id() == self.serverInfo.server_id
            leader_status = "LEADER" if is_leader else "FOLLOWER"
            print(f"{'Server ID':<30} {'Role':<12} {'Host':<20} {'Port':<8} {'Ctrl Port':<10} {'Last Crash':<5}")
            print("-" * 120)
            print(
                f"{display_server_id:<30} {leader_status:<12} {self.serverInfo.host:<20} "
                f"{str(self.serverInfo.port):<8} {str(self.serverInfo.ctrl_port):<10} {str(self.local_server_crash_count):<5}"
            )
        else:
            print("Server information not available")

        # Print Cars
        print("\n" + "-" * 120)
        print(f"{'CARS':^120}")
        print("-" * 120)
        if self.connected_cars:
            print(f"{'Car ID':<30} {'Status':<20} {'Latitude':<15} {'Longitude':<15} {'Registered':<20}")
            print("-" * 120)
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
        print("\n" + "-" * 120)
        print(f"{'AMBULANCES':^120}")
        print("-" * 120)
        if self.connected_ambulances:
            print(f"{'Ambulance ID':<30} {'Status':<20} {'Workflow':<30} {'Hospital':<20}")
            print("-" * 120)
            for amb_id, amb_info in self.connected_ambulances.items():
                status = amb_info.get("status", "N/A")
                workflow_id = amb_info.get("workflow_id", "N/A")
                hospital_id = amb_info.get("assigned_hospital", "N/A")
                if workflow_id:
                    workflow_id = workflow_id[-15:] if len(str(workflow_id)) > 15 else workflow_id
                if hospital_id:
                    hospital_id = hospital_id[-15:] if len(str(hospital_id)) > 15 else hospital_id
                print(f"{amb_id:<30} {status:<20} {str(workflow_id):<30} {str(hospital_id):<20}")
        else:
            print("No ambulances connected")
        
        # Print Hospitals
        print("\n" + "-" * 120)
        print(f"{'HOSPITALS':^120}")
        print("-" * 120)
        if self.connected_hospitals:
            print(f"{'Hospital ID':<30} {'Status':<20} {'Beds Available':<15} {'Registered':<20}")
            print("-" * 120)
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
        
        # Print Active Workflows
        print("\n" + "-" * 120)
        print(f"{'ACTIVE WORKFLOWS':^120}")
        print("-" * 120)
        if self.active_workflows:
            print(f"{'Workflow ID':<35} {'Car ID':<25} {'Ambulance':<25} {'Hospital':<25}")
            print("-" * 120)
            for wf_id, workflow in self.active_workflows.items():
                wf_id_short = wf_id[-30:] if len(wf_id) > 30 else wf_id
                car_id = (workflow.get('data', {}).get('car_id') or 'N/A')[-20:]
                amb_id = (workflow.get('data', {}).get('ambulance_id') or 'N/A')[-20:]
                hosp_id = (workflow.get('data', {}).get('hospital_id') or 'N/A')[-20:]
                print(f"{wf_id_short:<35} {str(car_id):<25} {str(amb_id):<25} {str(hosp_id):<25}")
        else:
            print("No active workflows")
        
        # Print Recent Events
        print("\n" + "-" * 120)
        print(f"{'RECENT EVENTS (Last 5)':^120}")
        print("-" * 120)
        if self.event_log:
            print(f"{'Seq':<8} {'Event Type':<20} {'Workflow':<30} {'Description':<60}")
            print("-" * 120)
            for event in self.event_log[-5:]:
                seq = event.get('seq', 'N/A')
                evt_type = event.get('event_type', 'N/A')
                wf_id = event.get('workflow_id', 'N/A')
                if wf_id and wf_id != 'N/A':
                    wf_id = wf_id[-25:]
                description = event.get('description', '')[:55]
                print(f"{str(seq):<8} {str(evt_type):<20} {str(wf_id):<30} {description:<60}")
        else:
            print("No events logged")
        
        # Print Global Resources (Leader only)
        if self.registry.get_leader_id() == self.serverInfo.server_id and self.global_resources:
            print("\n" + "-" * 120)
            print(f"{'GLOBAL RESOURCES (LEADER VIEW)':^120}")
            print("-" * 120)
            
            # Get list of alive server IDs from registry
            alive_server_ids = {s.server_id for s in self.registry.get_all_servers()}
            
            print(f"{'Server ID':<30} {'Status':<15} {'Ambulances':<15} {'Hospitals':<15}")
            print("-" * 120)
            
            for server_id, resources in self.global_resources.items():
                server_short = server_id[-8:]
                
                # Check if server is alive in registry
                if server_id in alive_server_ids:
                    status = "ALIVE"
                    amb_count = len(resources.get("ambulances", {}))
                    hosp_count = len(resources.get("hospitals", {}))
                    print(f"{server_short:<30} {status:<15} {str(amb_count):<15} {str(hosp_count):<15}")
                else:
                    status = "DEAD"
                    print(f"{server_short:<30} {status:<15} {'0 (stale)':<15} {'0 (stale)':<15}")
        
        # Print Buffered Events
        print("\n" + "-" * 120)
        print(f"{'LOCAL BUFFERED EVENTS':^120}")
        print("-" * 120)
        print(f"Events waiting to be sequenced: {len(self.local_events_to_sequence)}")
        
        print("\n" + "="*120 + "\n")

    async def periodic_status_print(self):
        """Periodically print the status of all connected clients."""
        try:
            while True:
                await asyncio.sleep(20)  # Print every 30 seconds
                self.log.info(f"Event log print: {self.event_log}")
                self.log.info(f"Active workflows print: {self.active_workflows}")
                self.print_all_clients()
        except asyncio.CancelledError:
            self.log.info("Periodic status print task cancelled")

    async def start(self):
        # Start monitoring and cleanup tasks
        monitor_task = asyncio.create_task(self.monitor_leader())
        cleanup_task = asyncio.create_task(self.cleanup_task())
        status_task = asyncio.create_task(self.periodic_status_print())
        resource_task = asyncio.create_task(self.update_global_resources_from_beacons())
        multicast = asyncio.create_task(self.listen_multicast())
        server = await asyncio.start_server(
            self.handle_client, "0.0.0.0", self.serverInfo.port
        )
        ctrl_server = await asyncio.start_server(
            self.handle_ctrl_message, "0.0.0.0", self.serverInfo.ctrl_port
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
                multicast.cancel()
                await asyncio.gather(monitor_task, cleanup_task, status_task, resource_task, multicast, return_exceptions=True)