# import asyncio
# from datetime import datetime
# from components.client_message import deserialize_client_message
# from components.server_message import ServerMessage
# import logging
# from datetime import datetime, timedelta

# class UDPServer(asyncio.DatagramProtocol):
#     def __init__(self, host, port):
#         self.host = host
#         self.port = port
#         self.log = logging.getLogger(f"UDPServer[{host}:{port}]")

#     def connection_made(self, transport):
#         self.transport = transport
#         self.log.info(f"UDP server started on {self.host}:{self.port}")

#     def datagram_received(self, data, addr):
#         message = data.decode()
#         self.log.info(f"Received {message} from {addr}")
#         # Process the message as needed
#         #  TODO: create message process function
#         # msg = deserialize_client_message(message)
#         # if msg:
#         #     self.log.info(f"Deserialized message: {msg}")
#         # else:
#         #     self.log.warning("Failed to deserialize client message.")

#     async def start(self):
#         loop = asyncio.get_running_loop()
#         await loop.create_datagram_endpoint(
#             lambda: self,
#             local_addr=(self.host, self.port)
#         )
        