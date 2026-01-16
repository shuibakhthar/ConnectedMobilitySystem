import uuid
import asyncio
import asyncio
from components.server_message import ServerMessage
from config.settings import BULLY_ELECTION_LOGGER, REGISTRY_HOST, REGISTRY_PORT
import json
'''
class BullyElection:
    def __init__(self, node_id, get_registry_func, host, ctrl_port, serverInfo = None,):
        
        self.serverInfo = serverInfo

        self.node_id = node_id
        self.get_registry = get_registry_func  # function returning list of server dicts
        self.host = host
        self.ctrl_port = ctrl_port

        self.log = BULLY_ELECTION_LOGGER

        self.coordinator_id = None
        self.election_in_progress = False
        self.ok_received = asyncio.Event()

    async def start_election(self):
        self.election_in_progress = True
        self.ok_received.clear()
        higher_nodes = [s for s in self.get_registry() if s['server_id'] > self.node_id]
        if not higher_nodes:
            await self.announce_coordinator()
        else:
            await self.send_election_messages(higher_nodes)
            try:
                await asyncio.wait_for(self.ok_received.wait(), timeout=3)
                # OK received, wait for coordinator message
                self.log.info("OK received, waiting for coordinator announcement.")
            except asyncio.TimeoutError:
                # No OK received, become coordinator
                await self.announce_coordinator()

    async def send_election_messages(self, nodes):
        for node in nodes:
            await self.send_message(node, "election", {"from": self.node_id})

    async def handle_election_message(self, from_id):
        if self.node_id > from_id:
            await self.send_message_to_id(from_id, "ok", {"from": self.node_id})
            await self.start_election()

    async def handle_ok_message(self, from_id):
        self.log.info(f"OK received from {from_id}")
        self.ok_received.set()

    async def announce_coordinator(self):
        self.coordinator_id = self.node_id
        self.election_in_progress = False
        for node in self.get_registry():
            if node['server_id'] != self.node_id:
                await self.send_message(node, "coordinator", {"from": self.node_id})
        self.log.info(f"I am the new coordinator: {self.node_id}")

    async def handle_coordinator_message(self, from_id):
        self.coordinator_id = from_id
        self.election_in_progress = False
        self.log.info(f"New coordinator is {from_id}")

    async def send_message(self, node, status, payload):
        msg = ServerMessage(
            server_id=self.node_id,
            status=status,
            payload=payload
        )
        try:
            reader, writer = await asyncio.open_connection(node['host'], node['ctrl_port'])
            writer.write(msg.serialize())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.log.debug(f"Sent {status} to {node['server_id']}")
        except Exception as e:
            self.log.error(f"Failed to send {status} to {node['server_id']}: {e}")

    async def send_message_to_id(self, server_id, status, payload):
        node = next((n for n in self.get_registry() if n['server_id'] == server_id), None)
        if node:
            await self.send_message(node, status, payload)'''

# revert 

class BullyElection:
    def __init__(self, node_id, get_registry_func, host, ctrl_port, serverInfo = None,):
        
        self.serverInfo = serverInfo

        # self.node_id = node_id
        # self.host = host
        # self.ctrl_port = ctrl_port

        self.get_registry = get_registry_func  # function returning list of server dicts
        self.log = BULLY_ELECTION_LOGGER

        self.coordinator_id = None
        self.election_in_progress = False
        self.ok_received = asyncio.Event()

    async def _get_servers(self):
        """Helper: always returns fresh server list"""
        registry_data = self.get_registry()()
        if asyncio.iscoroutine(registry_data):
            registry_data = await registry_data
        return (registry_data.get('server_list') if isinstance(registry_data, dict) else registry_data)

    async def start_election(self):
        self.election_in_progress = True
        self.ok_received.clear()
        servers = await self._get_servers()
        self.log.debug(f"[ELECTION] Servers type: {type(servers)}, len: {len(servers) if hasattr(servers, '__len__') else 'N/A'}")
        self.log.debug(f"[ELECTION] First item: {servers[0] if isinstance(servers, list) and servers else 'empty'}")
        self.log.debug(f"[ELECTION] Servers type: {type(servers)}, len: {len(servers) if hasattr(servers, '__len__') else 'N/A'}")
        higher_nodes = [s for s in servers if s['server_id'] > self.serverInfo.server_id]
        if not higher_nodes:
            await self.announce_coordinator()
        else:
            await self.send_election_messages(higher_nodes)
            try:
                await asyncio.wait_for(self.ok_received.wait(), timeout=3)
                # OK received, wait for coordinator message
                self.log.info("OK received, waiting for coordinator announcement.")
            except asyncio.TimeoutError:
                # No OK received, become coordinator
                await self.announce_coordinator()

    async def send_election_messages(self, nodes):
        for node in nodes:
            await self.send_message(node, "election_start", {"from": self.serverInfo.server_id})

    async def handle_election_message(self, from_id):
        if self.serverInfo.server_id > from_id:
            await self.send_message_to_id(from_id, "election_ack_ok", {"from": self.serverInfo.server_id})
            await self.start_election()

    async def handle_ok_message(self, from_id):
        self.log.info(f"OK received from {from_id}")
        self.ok_received.set()

    async def announce_coordinator(self):
        self.coordinator_id = self.serverInfo.server_id
        self.serverInfo.leaderId = self.coordinator_id
        self.serverInfo.leaderInfo = self.serverInfo.to_dict()
        self.election_in_progress = False

        servers = await self._get_servers()
        for node in servers:
            if node['server_id'] != self.serverInfo.server_id:
                await self.send_message(node, "election_coordinator", {"from": self.serverInfo.server_id})
        self.log.info(f"I am the new coordinator: {self.serverInfo.server_id}")
        await self.notify_leader_change()

    async def handle_coordinator_message(self, from_id):
        self.coordinator_id = from_id
        self.election_in_progress = False
        self.log.info(f"New coordinator is {from_id}")

    async def send_message(self, node, status, payload):
        msg = ServerMessage(
            server_id=self.serverInfo.server_id,
            status=status,
            payload=payload
        )
        try:
            reader, writer = await asyncio.open_connection(node['host'], node['ctrl_port'])
            writer.write(msg.serialize())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.log.debug(f"Sent {status} to {node['server_id']}")
        except Exception as e:
            self.log.error(f"Failed to send {status} to {node['server_id']}: {e}")

    async def send_message_to_id(self, server_id, status, payload):
        servers = await self._get_servers()
        node = next((n for n in servers if n['server_id'] == server_id), None)
        if node:
            await self.send_message(node, status, payload)

    async def notify_leader_change(self,  registry_host=REGISTRY_HOST, registry_port=REGISTRY_PORT):
        try:
            reader, writer = await asyncio.open_connection(registry_host, registry_port)
            request_msg = json.dumps({"request_type": "update_leader_info", "leader_id": self.serverInfo.server_id, "leader_info": self.serverInfo.to_dict()}).encode('utf-8')
            writer.write(request_msg)
            await writer.drain()
            data = await reader.read()
            response = json.loads(data.decode('utf-8'))
            self.log.info(f"Leader info update response: {response}")
            writer.close()
            await writer.wait_closed()
            self.log.info("Leader info updated in registry.")
        except Exception as e:
            self.log.error(f"Error updating leader info in registry: {e}")