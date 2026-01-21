import asyncio
import sys
import uuid

from components.client_message import ClientMessage
from components.server_message import deserialize_server_message

from config.settings import MAX_RETRIES, TCP_CLIENT_LOGGER
class TCPClient:
    def __init__(
        self,
        server_host,
        server_port,
        client_id,
        client_type="Car",
        heartbeat_interval=15,
        latitude: float = 0.0,
        longitude: float = 0.0,
        current_occupancy: int = 0,
    ):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.client_type = client_type
        self.heartbeat_interval = heartbeat_interval
        self.client_uid = uuid.uuid7()
        self.log = TCP_CLIENT_LOGGER
        self.latitude = latitude
        self.longitude = longitude
        self.current_occupancy = current_occupancy

    async def connect(self):

        server_host, server_port = await self.request_server_assignment()

        self.reader, self.writer = await asyncio.open_connection(server_host, server_port)
        await self.register()

    async def request_server_assignment(self, max_retries= MAX_RETRIES):
        leader_host = self.server_host
        leader_port = self.server_port

        for attempt in range(max_retries):
            try:
                self.log.info(f"Requesting server assignment from leader at {leader_host}:{leader_port}, attempt {attempt + 1}")
                reader, writer = await asyncio.open_connection(leader_host, leader_port)
                request_msg = ClientMessage(self.client_id, self.client_type, "request_server_assignment",  {})
                writer.write(request_msg.serialize())
                await writer.drain()

                response = await reader.readline()
                if not response:
                    raise ConnectionError("No response received from leader")
                
                response_msg = deserialize_server_message(response.decode())
                if response_msg:
                    if response_msg.status == "assign_server":
                        assigned_info = response_msg.payload
                        assigned_host = assigned_info.get("host")
                        assigned_port = assigned_info.get("port")
                        self.log.info(f"Assigned to server at {assigned_host}:{assigned_port}")
                        writer.close()
                        await writer.wait_closed()
                        return assigned_host, assigned_port
                    elif response_msg.status == "redirect_to_leader":
                        new_leader_info = response_msg.payload
                        self.log.info(f"Redirected to new leader {new_leader_info.get('host')}:{new_leader_info.get('port') }")
                        leader_host = new_leader_info.get("host")
                        leader_port = new_leader_info.get("port")
                        writer.close()
                        await writer.wait_closed()
                        await asyncio.sleep(1)  # brief pause before retrying
                        continue

                else:
                    raise ValueError("Invalid response or status from leader")
                
            except (ConnectionError, OSError) as e:
                self.log.error(f"Assignment attempt {attempt + 1} connection failed: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                else:
                        raise
            except Exception as e:
                self.log.error(f"Assignment attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(2)
        raise ConnectionError("Failed to get server assignment after all retries")

    async def register(self):
        reg_msg = ClientMessage(self.client_id, self.client_type, "register", {})
        #reg_msg = serialize_message("register", self.client_id, {"info": "Client registration"})
        self.writer.write(reg_msg.serialize())
        await self.writer.drain()
        response = await self.reader.readline()
        resp_msg = deserialize_server_message(response.decode())
        self.log.info(f"Registration response: {resp_msg.serialize() if resp_msg else 'None'}")
        # print(f"Registration response: {resp_msg.serialize()}")

    async def send_heartbeat(self):
        heartbeat_msg = ClientMessage(self.client_id, self.client_type, "heartbeat", {})
        self.writer.write(heartbeat_msg.serialize())
        await self.writer.drain()
        self.log.debug("Heartbeat sent.")
        # print("Heartbeat sent.")

    async def send_crash_report(self, latitude: float | None = None, longitude: float | None = None):
        """Send a crash report to the server.

        Note: This does not wait for an ack because `listen()` owns the reader.
        """
        if latitude is None:
            latitude = self.latitude
        if longitude is None:
            longitude = self.longitude

        crash_msg = ClientMessage(
            self.client_id,
            self.client_type,
            "report_crash",
            {"latitude": latitude, "longitude": longitude},
        )
        self.writer.write(crash_msg.serialize())
        await self.writer.drain()
        self.log.warning(f"Crash sent to server: lat={latitude}, lon={longitude}")

    async def send_occupancy_update(self):
        """Send bed occupancy information to the server (Hospital clients only)."""
        if self.client_type != "Hospital":
            return

        occupancy_msg = ClientMessage(
            self.client_id,
            self.client_type,
            "occupancy_update",
            {"current_occupancy": self.current_occupancy},
        )
        self.writer.write(occupancy_msg.serialize())
        await self.writer.drain()
        self.log.info(f"Occupancy update sent to server: {self.current_occupancy} beds available")

    async def send_ambulance_status(self, status, call_id="default_call", hospital_id="default_hospital"):
        """Send ambulance status update to the server."""
        if self.client_type != "Ambulance":
            return
        
        # Build payload based on what the status requires
        payload = {}
        if status in ["answer_call", "arrived_at_scene"]:
            payload["call_id"] = call_id
            self.log.debug(f"Preparing ambulance status '{status}' with payload: {payload}")
        elif status in ["transporting_patient", "at_hospital"]:
            payload["call_id"] = call_id
            payload["hospital_id"] = hospital_id
            self.log.debug(f"Preparing ambulance status '{status}' with payload: {payload}")
        else:
            self.log.debug(f"Preparing ambulance status '{status}' with empty payload")
        
        try:
            ambulance_msg = ClientMessage(
                self.client_id,
                self.client_type,
                status,
                payload,
            )
            self.writer.write(ambulance_msg.serialize())
            await self.writer.drain()
            self.log.info(f"Ambulance status sent to server: {status} with payload {payload}")
        except ValueError as e:
            self.log.error(f"Failed to send ambulance status '{status}': {e}")
            self.log.error(f"Status '{status}' requires specific payload fields. Check config/settings.py for required fields.")

    async def monitor_occupancy_key(self):
        """For Hospital clients: press +/- to update beds available, or enter a number."""
        if self.client_type != "Hospital":
            return

        self.log.info(f"Hospital controls: press '+' to increase beds available, '-' to decrease, or type a number and press Enter")

        # Windows: capture single keypress without Enter for +/-
        if sys.platform == "win32":
            import msvcrt

            try:
                while True:
                    if msvcrt.kbhit():
                        ch = msvcrt.getwch()
                        if ch == '+':
                            self.current_occupancy += 1
                            self.log.info(f"Beds available increased to {self.current_occupancy}")
                            await self.send_occupancy_update()
                        elif ch == '-':
                            if self.current_occupancy > 0:
                                self.current_occupancy -= 1
                            self.log.info(f"Beds available decreased to {self.current_occupancy}")
                            await self.send_occupancy_update()
                        elif ch.isdigit():
                            # Start collecting number input
                            num_str = ch
                            print(ch, end='', flush=True)  # Echo the first digit
                            while True:
                                if msvcrt.kbhit():
                                    ch2 = msvcrt.getwch()
                                    if ch2 == '\r':  # Enter key
                                        print()  # New line after Enter
                                        try:
                                            self.current_occupancy = int(num_str)
                                            self.log.info(f"Beds available set to {self.current_occupancy}")
                                            await self.send_occupancy_update()
                                        except ValueError:
                                            self.log.warning(f"Invalid number: {num_str}")
                                        break
                                    elif ch2.isdigit():
                                        num_str += ch2
                                        print(ch2, end='', flush=True)  # Echo each digit
                                    else:
                                        break
                                await asyncio.sleep(0.05)
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                return

        # Fallback (non-Windows): requires Enter for all input
        try:
            while True:
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:
                    return
                line = line.strip()
                if line == '+':
                    self.current_occupancy += 1
                    self.log.info(f"Beds available increased to {self.current_occupancy}")
                    await self.send_occupancy_update()
                elif line == '-':
                    if self.current_occupancy > 0:
                        self.current_occupancy -= 1
                    self.log.info(f"Beds available decreased to {self.current_occupancy}")
                    await self.send_occupancy_update()
                elif line.isdigit():
                    self.current_occupancy = int(line)
                    self.log.info(f"Beds available set to {self.current_occupancy}")
                    await self.send_occupancy_update()
        except asyncio.CancelledError:
            return

    async def monitor_crash_key(self):
        """For Car clients: pressing 'c' triggers a crash report."""
        if self.client_type != "Car":
            return

        self.log.info("Car controls: press 'c' to report a crash")

        # Windows: capture single keypress without Enter
        if sys.platform == "win32":
            import msvcrt

            try:
                while True:
                    if msvcrt.kbhit():
                        ch = msvcrt.getwch()
                        if ch and ch.lower() == "c":
                            await self.send_crash_report()
                            return
                            return
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                return

        # Fallback (non-Windows): requires Enter
        try:
            while True:
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:
                    return
                if line.strip().lower() == "c":
                    await self.send_crash_report()
                    return
                    return
        except asyncio.CancelledError:
            return

    async def monitor_ambulance_keys(self):
        """For Ambulance clients: press 'O' for On Duty, 'A' for Available, 'R' for Reached at scene, 'T' for Transporting patient."""
        if self.client_type != "Ambulance":
            return

        self.log.info("Ambulance controls: press 'O' for On Duty, 'A' for Available, 'R' for Reached at scene, 'T' for Transporting patient")

        # Windows: capture single keypress without Enter
        if sys.platform == "win32":
            import msvcrt

            try:
                while True:
                    if msvcrt.kbhit():
                        ch = msvcrt.getwch()
                        if ch and ch.upper() == "O":
                            self.log.info("Ambulance status: On Duty")
                            await self.send_ambulance_status("on_duty")
                        elif ch and ch.upper() == "A":
                            self.log.info("Ambulance status: Available")
                            await self.send_ambulance_status("available")
                        elif ch and ch.upper() == "R":
                            self.log.info("Ambulance status: Reached at scene")
                            await self.send_ambulance_status("arrived_at_scene", call_id="emergency_001")
                        elif ch and ch.upper() == "T":
                            self.log.info("Ambulance status: Transporting patient")
                            await self.send_ambulance_status("transporting_patient", call_id="emergency_001", hospital_id="hospital_001")
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                return

        # Fallback (non-Windows): requires Enter
        try:
            while True:
                line = await asyncio.to_thread(sys.stdin.readline)
                if not line:
                    return
                key = line.strip().upper()
                if key == "O":
                    self.log.info("Ambulance status: On Duty")
                    await self.send_ambulance_status("on_duty")
                elif key == "A":
                    self.log.info("Ambulance status: Available")
                    await self.send_ambulance_status("available")
                elif key == "R":
                    self.log.info("Ambulance status: Reached at scene")
                    await self.send_ambulance_status("arrived_at_scene", call_id="emergency_001")
                elif key == "T":
                    self.log.info("Ambulance status: Transporting patient")
                    await self.send_ambulance_status("transporting_patient", call_id="emergency_001", hospital_id="hospital_001")
        except asyncio.CancelledError:
            return

    #async def send_status(self, status):
    #    msg = serialize_message("status_update", self.client_id, status)
    #    self.writer.write(msg)
    #    await self.writer.drain()

    async def listen(self):
        try:
            while True:
                data = await self.reader.readline()
                if not data:
                    break
                msg = deserialize_server_message(data.decode())
                self.log.info(f"Received from server: {msg.serialize() if msg else 'None'}")
                # print(f"Received from server: {msg}")
        except asyncio.CancelledError:
            pass

    async def run(self):
        await self.connect()
        listener_task = asyncio.create_task(self.listen())
        crash_key_task = None
        occupancy_key_task = None
        ambulance_key_task = None
        
        if self.client_type == "Car":
            crash_key_task = asyncio.create_task(self.monitor_crash_key())
        elif self.client_type == "Hospital":
            occupancy_key_task = asyncio.create_task(self.monitor_occupancy_key())
        elif self.client_type == "Ambulance":
            ambulance_key_task = asyncio.create_task(self.monitor_ambulance_keys())
        
        # For demo: send periodic status updates every 15 seconds
        try:
            occupancy_counter = 0
            while True:
                await self.send_heartbeat()
                
                # Send occupancy update every 10 seconds for Hospital clients
                if self.client_type == "Hospital":
                    occupancy_counter += 15
                    if occupancy_counter >= 10:
                        await self.send_occupancy_update()
                        occupancy_counter = 0
                
                await asyncio.sleep(15)
        except KeyboardInterrupt:
            self.log.info("Client shutting down")
            # print("Client shutting down")
        except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError) as e:
            self.log.error(f"Connection lost: {e}. Server may have shut down.")
        except Exception as e:
            self.log.error(f"Unexpected error in client run: {e}", exc_info=True)
        finally:
            listener_task.cancel()
            if crash_key_task:
                crash_key_task.cancel()
                await asyncio.gather(crash_key_task, return_exceptions=True)
            if occupancy_key_task:
                occupancy_key_task.cancel()
                await asyncio.gather(occupancy_key_task, return_exceptions=True)
            if ambulance_key_task:
                ambulance_key_task.cancel()
                await asyncio.gather(ambulance_key_task, return_exceptions=True)
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception as e:
                self.log.debug(f"Error closing connection: {e}")
            self.log.info("Client connection closed")

# For manual test, run this code:
if __name__ == "__main__":
    client = TCPClient('127.0.0.1', 8888, client_id="ambulance_1", client_type="Ambulance")
    asyncio.run(client.run())
