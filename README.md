# ConnectedMobilitySystem

## Needed libraries / versions

- Python 3.14+ (UUIDv7 is used for server IDs via `uuid.uuid7()`)

## How to use

### 1) Start one or more servers

Each server starts a TCP server plus a control channel and broadcasts beacons. Example:

- `python main_server.py --tcp_port=8888`
- `python main_server.py --tcp_port=8890`

Notes:
- `--host` defaults to `127.0.0.1`.
- `--beacon_port` defaults to `9999` (see `BEACON_PORT` in settings).
- The control channel is `tcp_port + 1` and is used for election messages.

### 2) Start clients (car, ambulance, hospital)

Clients first checks the registry for the current leader, then request server assignment from the leader.

Examples:
- Ambulance:
	- `python main_client.py --client_id=ambulance_1 --client_type=Ambulance`
- Hospital:
	- `python main_client.py --client_id=hospital_1 --client_type=Hospital`
- Car:
	- `python main_client.py --client_id=car_1 --client_type=Car`

Notes:
- `--discover_timeout` can be updated if required, defaults to `15` seconds.
- `--heartbeat_interval` can be updated if required, defaults to `15` seconds.

## Requirements 

### Architectural model

The system follows a client–server architecture . Multiple servers run in parallel and each has a TCP endpoint for client traffic and a separate control port for inter‑server communication. A lightweight registry service tracks active servers and the current leader. Clients always connect to a server assigned by the leader, which allows the system to scale while keeping a single coordination point for membership and assignment.

The code separates concerns into discovery (UDP beacons + registry), election (Bully algorithm), messaging (JSON over TCP), and client/server transport. This keeps data‑plane traffic (client updates) and control‑plane traffic (election, leader changes, registry updates) distinct and easier to reason about.

### Dynamic discovery of hosts

Each server periodically broadcasts a UDP beacon containing its host, TCP port, control port, and server ID. Clients do not need static server addresses; they query the registry endpoint on TCP 9998 to discover the current leader, then ask that leader for a server assignment. No separate discovery process needs to be started.

The registry also prunes stale servers on a timer, ensuring the server list reflects only live instances. This makes host discovery dynamic and resilient to servers joining and leaving at runtime.

### Voting

Leader election uses a Bully‑style algorithm. When a server detects that the leader is missing or stale, it starts an election by notifying higher ID servers (UUIDv7 order). If no higher server responds, it declares itself coordinator and broadcasts the new leader decision. The leader information is then pushed to the registry so that new clients and other servers converge on the current coordinator.

This approach provides deterministic leadership based on server IDs and avoids split‑brain by ensuring that only the highest ID live server becomes the leader.

### Fault tolerance

Fault tolerance is provided through several mechanisms: periodic heartbeats from clients, leader health checks from servers, and registry cleanup of stale servers. If the leader fails or becomes unresponsive, followers initiate a new election. Clients that lose their connection can re‑discover the leader and request reassignment. The registry maintains a recent view of available servers, allowing quick recovery after failures or restarts.

The system is designed to degrade during crashes: if a server drops, it is removed from the registry, and clients can be re‑assigned to a live server. If the leader drops, a new leader is elected without manual intervention.

### Ordered reliable multicast

The leader server is the single source of truth for ordering. It assigns monotonically increasing sequence numbers to all events and broadcasts them to other servers. Each event includes a `depends_on` list that identifies prerequisite sequences, achieving **Total Order** with dependency based **Causal Ordering**. Servers autonomously process client requests and maintain a local event list, which is then sent to the leader for sequencing and broadcast to ensure consistency.

**Total Ordering** ensures that all servers execute identical allocation sequences via global sequence numbers from the leader. **Causal Ordering** ensures logical dependencies are preserved—for example: Car Accident → Ambulance Allocation → Hospital Allocation. This guarantees that dependent events are never processed out of order, maintaining application level consistency across the distributed system.
