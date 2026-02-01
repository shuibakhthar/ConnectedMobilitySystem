# ConnectedMobilitySystem

## Needed librarys / versions

- Python 3.14 (needed for UUID7)

## How to use it

### Starting a server

```python
python main_server.py --tcp_port=8000
```

All following servers are generated with the same command line but you need to change the port.

The standard port is 8888.

The local host ip is auto-detected. No need to specify.

To become a deeper understanding on the changes and the messages you can manually enable the DEBUG-logs. Therefore you have to pass the argument "--logging_level". With this you can also change the logging to only display Errormessages if you want.

Example:
```python
python main_server.py --logging_level=DEBUG
```


#### Example to start 3 servers

```python
python main_server.py --tcp_port=8000
```
This server will run on port 8000

```python
python main_server.py 
```
This server will run on port 8888

```python
python main_server.py --tcp_port=8080
```
This server will run on port 8080

### Starting a client

```python
python main_client.py
```

The client starts and trys to discover a server.

By starting it with no attributes, you have created a client of type "Ambulance" with the client_id "ambulance_1". The client sends a heartbeat every 15 ms and terminates if a server isn't discovery within the first 3 seconds after a beacon is sent.

To create a different client type you can specify this by using "--client_type" and choose from ["Ambulance", "Car", "TrafficLight", "Hospital"].
```python
python main_client.py --client_type=Car
```

Similarly you can choose the client id by "--client_id", the discovery timeout by "--discovery_timeout", the heartbeat interval by "--heartbeat_interval" and you can change the logging level likewise to the server with "--logging_level".

# Failures

Through continous heartbeats from every client and server a malfunction or loss of one can be detected within a short period.

A loss in a client will have no big chance except that the client is being removed from the list of clients.

However a loss in a "simple" server sets of a list of events. The server is removed from the list of servers and the clients that were connected to this server are being redistributed to other - still active - servers.

If the leader server shuts down unexpected every other server can trigger the bully leader election. The election algorithm then reassigns another server or itself as the leader. This will get broadcasted to every server.