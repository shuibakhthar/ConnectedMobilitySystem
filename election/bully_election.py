import asyncio
from components.server_message import ServerMessage
from config.settings import BULLY_ELECTION_LOGGER


class BullyElection:
    def __init__(self, serverInfo, registry):
        self.serverInfo = serverInfo
        self.registry = registry
        self.log = BULLY_ELECTION_LOGGER
        self.coordinator_id = None
        self.election_in_progress = False
        self.ok_received = asyncio.Event()

    async def _get_servers(self):
        """Get fresh server list from registry"""
        return [s.to_dict() for s in self.registry.get_all_servers()]

    async def start_election(self):
        self.election_in_progress = True
        self.ok_received.clear()
        servers = await self._get_servers()
        
        higher_nodes = [s for s in servers if s['server_id'] > self.serverInfo.server_id]
        
        if not higher_nodes:
            await self.announce_coordinator()
        else:
            # Retry election_start up to 3 times, 3s each
            max_attempts = 3
            for attempt in range(1, max_attempts + 1):
                self.ok_received.clear()
                await self.send_election_messages(higher_nodes)
                try:
                    await asyncio.wait_for(self.ok_received.wait(), timeout=3)
                    self.log.info("OK received, waiting for coordinator announcement.")
                    return
                except asyncio.TimeoutError:
                    self.log.warning(
                        "No OK received (attempt %d/%d).", attempt, max_attempts
                    )
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
        self.election_in_progress = False
        
        # Update local registry leader
        self.registry.set_leader(self.serverInfo.server_id, self.serverInfo)
        
        servers = await self._get_servers()
        for node in servers:
            if node['server_id'] != self.serverInfo.server_id:
                await self.send_message(node, "election_coordinator", {"from": self.serverInfo.server_id})
        
        if hasattr(self.serverInfo, 'server_ref') and self.serverInfo.server_ref:
            await self.serverInfo.server_ref.on_became_leader()
        self.log.info(f"I am the new coordinator: {self.serverInfo.server_id}")

    async def handle_coordinator_message(self, from_id):

        if from_id < self.serverInfo.server_id:
            self.log.warning(f"Received coordinator announcement from lower ID {from_id}. Ignoring and starting new election.")
            await self.start_election()
            return
        self.coordinator_id = from_id
        self.election_in_progress = False
        
        # Update local registry leader
        servers = await self._get_servers()
        new_leader = next((s for s in servers if s['server_id'] == from_id), None)
        if new_leader:
            from components.tcp_server import ServerInfo
            self.registry.set_leader(from_id, ServerInfo.from_dict(new_leader))
        
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