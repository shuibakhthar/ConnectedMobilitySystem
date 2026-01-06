import uuid


class BullyElection:
    pass

    # def __init__(self, node_id, nodes):
    #     self.node_id = node_id
    #     self.nodes = nodes  # List of (node_id, address) tuples
    #     self.coordinator_id = None

    # def start_election(self):
    #     higher_nodes = [node for node in self.nodes if node[0] > self.node_id]
    #     if not higher_nodes:
    #         self.coordinator_id = self.node_id
    #         print(f"Node {self.node_id} becomes the coordinator.")
    #         self.announce_coordinator()
    #     else:
    #         print(f"Node {self.node_id} is starting an election.")
    #         for node in higher_nodes:
    #             self.send_election_message(node)

    # def send_election_message(self, node):
    #     print(f"Node {self.node_id} sends election message to Node {node[0]}.")

    # def announce_coordinator(self):
    #     for node in self.nodes:
    #         if node[0] != self.node_id:
    #             self.send_coordinator_message(node)

    # def send_coordinator_message(self, node):
    #     print(f"Node {self.node_id} announces itself as coordinator to Node {node[0]}.")