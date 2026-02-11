import socket
import json
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

class MulticastServer:
    """
    Reliable ordered multicast for event replication.
    - Sends batches of events to all followers simultaneously via UDP
    - Followers detect gaps and fall back to TCP recovery
    - Idempotent: duplicate detection by seq number
    """
    
    MULTICAST_GROUP = "239.255.0.1"  # Administratively scoped multicast (works on routers)
    MULTICAST_PORT = 5555             # Custom port for distributed system
    TTL = 5                           # TTL for router traversal (5 hops)
    MAX_PACKET_SIZE = 1400            # Under UDP MTU (1500), leaves room for headers
    
    def __init__(self, server_id: str, host: str, logger=None):
        self.server_id = server_id
        self.host = host
        self.logger = logger or logging.getLogger(__name__)
        
        # Receiver socket (all servers including leader)
        self.recv_socket = None
        self.recv_thread = None
        
    def initialize_receiver(self):
        """Setup UDP multicast receiver (call this on every server)"""
        try:
            self.recv_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.recv_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # Bind to all interfaces on multicast port
            self.recv_socket.bind(("", self.MULTICAST_PORT))
            
            # Join multicast group on the specific interface
            # mreq = multicast_group_ip + interface_ip
            mreq = socket.inet_aton(self.MULTICAST_GROUP) + socket.inet_aton(self.host)
            self.recv_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            self.logger.info(f"[MULTICAST] Receiver joined {self.MULTICAST_GROUP} on interface {self.host}:{self.MULTICAST_PORT}")
            return True
        except Exception as e:
            self.logger.error(f"[MULTICAST] Failed to initialize receiver: {e}")
            return False
    
    def send_batch(self, events: List[Dict[str, Any]]) -> bool:
        """
        Broadcast a batch of events to all servers (leader only).
        Returns True if sent successfully, False otherwise.
        """
        if not events:
            return False
            
        try:
            # Build multicast message
            message = {
                "sender_id": self.server_id,
                "timestamp": datetime.utcnow().isoformat(),
                "seq_range": (events[0]['seq'], events[-1]['seq']),
                "event_count": len(events),
                "events": events
            }
            
            # Serialize to JSON
            payload = json.dumps(message).encode('utf-8')
            
            # Check packet size
            if len(payload) > self.MAX_PACKET_SIZE:
                self.logger.warning(
                    f"[MULTICAST] Batch too large ({len(payload)} bytes), "
                    f"splitting into smaller batches"
                )
                return self._send_large_batch(events)
            
            # Create sender socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.TTL)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            # Bind to correct interface (critical for router/hotspot support)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.host))

            # Send
            sock.sendto(payload, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
            sock.close()
            
            self.logger.info(
                f"[MULTICAST] Sent batch: seq {events[0]['seq']}-{events[-1]['seq']} "
                f"({len(events)} events, {len(payload)} bytes)"
            )
            return True
            
        except Exception as e:
            self.logger.error(f"[MULTICAST] Failed to broadcast: {e}")
            return False
    
    def _send_large_batch(self, events: List[Dict[str, Any]]) -> bool:
        """
        Handle large batches by splitting and sending multiple packets.
        """
        chunk_size = max(1, len(events) // 3)  # Split into ~3 packets
        success = True
        
        for i in range(0, len(events), chunk_size):
            chunk = events[i:i+chunk_size]
            try:
                message = {
                    "sender_id": self.server_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "seq_range": (chunk[0]['seq'], chunk[-1]['seq']),
                    "event_count": len(chunk),
                    "events": chunk
                }
                
                payload = json.dumps(message).encode('utf-8')
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, self.TTL)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
                # Bind to correct interface (critical for router/hotspot support)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(self.host))
                sock.sendto(payload, (self.MULTICAST_GROUP, self.MULTICAST_PORT))
                sock.close()
                
                self.logger.info(
                    f"[MULTICAST] Sent chunk: seq {chunk[0]['seq']}-{chunk[-1]['seq']} "
                    f"({len(chunk)} events)"
                )
            except Exception as e:
                self.logger.error(f"[MULTICAST] Failed chunk send: {e}")
                success = False
        
        return success
    
    def receive_batch(self) -> Dict[str, Any] or None:
        """
        Receive multicast event batch (non-blocking, call from async context).
        Returns dict with events and metadata, or None if no data.
        """
        if not self.recv_socket:
            return None
        
        try:
            self.recv_socket.setblocking(False)
            data, addr = self.recv_socket.recvfrom(self.MAX_PACKET_SIZE)
            message = json.loads(data.decode('utf-8'))
            
            return {
                "sender_id": message.get("sender_id"),
                "timestamp": message.get("timestamp"),
                "seq_range": message.get("seq_range"),
                "event_count": message.get("event_count"),
                "events": message.get("events", []),
                "source": addr
            }
        except BlockingIOError:
            return None  # No data available
        except Exception as e:
            self.logger.error(f"[MULTICAST] Failed to receive: {e}")
            return None
    
    def shutdown(self):
        """Cleanup multicast receiver"""
        if self.recv_socket:
            self.recv_socket.close()
            self.logger.info("[MULTICAST] Receiver shutdown")