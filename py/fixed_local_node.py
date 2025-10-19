"""
Fixed Node untuk support multiple instances di 1 komputer
Menggunakan kombinasi: Multicast + Loopback Discovery
"""

import zmq
import json
import threading
import socket
import struct
import uuid
import time
import hashlib
import os
import logging
from enum import Enum
from typing import Dict, Optional, Set, Tuple
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# ENUMERATIONS & DATA CLASSES (sama seperti sebelumnya)
# ============================================================================
class MessageType(Enum):
    DIRECT = "direct"
    GROUP = "group"
    IMAGE = "image"
    FILE = "file"
    OPERATION = "operation"
    FOLDER_SHARE = "folder_share"
    FILE_INIT = "file_init"
    CHUNK_REQUEST = "chunk_request"
    CHUNK_RESPONSE = "chunk_response"
    TRANSFER_ACK = "transfer_ack"
    TRANSFER_ERROR = "transfer_error"

@dataclass
class PeerInfo:
    node_id: str
    ip: str
    router_port: int
    file_port: int
    last_seen: float = 0.0
    
    def is_alive(self, timeout: float = 30.0) -> bool:
        return time.time() - self.last_seen < timeout

@dataclass
class TransferState:
    transfer_id: str
    file_path: str
    total_size: int
    chunk_size: int
    total_md5: str
    chunks_sent: Set[int]
    last_activity: float
    
    def is_complete(self) -> bool:
        expected_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.chunks_sent) >= expected_chunks

@dataclass
class ReceiveState:
    transfer_id: str
    file_name: str
    total_size: int
    chunk_size: int
    total_md5: str
    temp_path: Path
    received_chunks: Dict[int, bytes]
    chunk_md5s: Dict[int, str]
    retry_count: Dict[int, int]
    last_activity: float
    max_retries: int = 3
    
    def is_complete(self) -> bool:
        expected_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.received_chunks) >= expected_chunks
    
    def get_total_received(self) -> int:
        return sum(len(chunk) for chunk in self.received_chunks.values())


# ============================================================================
# FIXED NODE CLASS
# ============================================================================
class Node:
    """
    Enhanced Node dengan support untuk multiple instances di 1 komputer.
    
    Changes:
    1. Dynamic port allocation jika port sudah terpakai
    2. Loopback discovery untuk local instances
    3. Better multicast handling
    """
    
    def __init__(
        self, 
        node_id: Optional[str] = None,
        bind_ip: str = "0.0.0.0",
        pub_port: int = 0,  # 0 = auto-assign
        router_port: int = 0,  # 0 = auto-assign
        discovery_port: int = 5557,
        chunk_size: int = 1048576,
        max_concurrent_transfers: int = 5
    ):
        # ====================================================================
        # IDENTITAS DAN KONFIGURASI
        # ====================================================================
        self.node_id = node_id or str(uuid.uuid4())[:8]  # Shorter for readability
        self.bind_ip = bind_ip
        self.chunk_size = chunk_size
        self.max_concurrent_transfers = max_concurrent_transfers
        self.discovery_port = discovery_port
        
        logger.info(f"Initializing Node {self.node_id}")
        
        # ====================================================================
        # DATA STRUCTURES
        # ====================================================================
        self.peers: Dict[str, PeerInfo] = {}
        self.peers_lock = threading.RLock()
        
        self.groups: Set[str] = set()
        self.groups_lock = threading.Lock()
        
        self.transfers: Dict[str, TransferState] = {}
        self.transfers_lock = threading.Lock()
        
        self.receiving_transfers: Dict[str, ReceiveState] = {}
        self.receiving_lock = threading.Lock()
        
        self.transfer_semaphore = threading.Semaphore(max_concurrent_transfers)
        
        # ====================================================================
        # ZEROMQ SETUP dengan AUTO PORT ASSIGNMENT
        # ====================================================================
        self.context = zmq.Context()
        self.context.setsockopt(zmq.MAX_SOCKETS, 1024)
        
        # PUB socket dengan auto port assignment
        self.pub_socket = self.context.socket(zmq.PUB)
        if pub_port == 0:
            # Bind ke port 0 akan auto-assign available port
            self.pub_port = self.pub_socket.bind_to_random_port(f"tcp://{bind_ip}")
        else:
            self.pub_socket.bind(f"tcp://{bind_ip}:{pub_port}")
            self.pub_port = pub_port
        logger.info(f"PUB socket bound to port {self.pub_port}")
        
        # ROUTER socket dengan auto port assignment
        self.router_socket = self.context.socket(zmq.ROUTER)
        if router_port == 0:
            self.router_port = self.router_socket.bind_to_random_port(f"tcp://{bind_ip}")
        else:
            self.router_socket.bind(f"tcp://{bind_ip}:{router_port}")
            self.router_port = router_port
        logger.info(f"ROUTER socket bound to port {self.router_port}")
        
        # SUB socket
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        
        # FILE ROUTER socket dengan auto port assignment
        self.file_router_socket = self.context.socket(zmq.ROUTER)
        self.file_port = self.file_router_socket.bind_to_random_port(f"tcp://{bind_ip}")
        logger.info(f"FILE ROUTER socket bound to port {self.file_port}")
        
        # ====================================================================
        # IMPROVED UDP MULTICAST DISCOVERY
        # ====================================================================
        self.mcast_group = "224.0.0.1"
        self.discovery_socket = self._setup_multicast_socket()
        
        # ====================================================================
        # LOOPBACK DISCOVERY FILE untuk local instances
        # ====================================================================
        # File-based discovery untuk instances di komputer yang sama
        self.discovery_file = Path(f"/tmp/p2p_nodes_{discovery_port}.json")
        if os.name == 'nt':  # Windows
            self.discovery_file = Path(f"{os.environ['TEMP']}/p2p_nodes_{discovery_port}.json")
        
        # ====================================================================
        # THREADING CONTROL
        # ====================================================================
        self.running = False
        self.threads = []
        
        # Temp directory
        self.temp_dir = Path("temp_transfers")
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info(f"Node {self.node_id} initialized on ports PUB:{self.pub_port} ROUTER:{self.router_port} FILE:{self.file_port}")
    
    def _setup_multicast_socket(self):
        """
        Setup UDP multicast socket dengan better error handling.
        Fallback ke broadcast jika multicast tidak available.
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            # SO_REUSEPORT untuk allow multiple binds
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except (AttributeError, OSError):
                logger.warning("SO_REUSEPORT not available")
            
            # Enable loopback agar multicast messages juga diterima oleh sender
            # PENTING untuk multiple instances di 1 komputer!
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            
            # Bind ke INADDR_ANY untuk receive dari semua interfaces
            sock.bind(('', self.discovery_port))
            
            # Join multicast group
            mreq = struct.pack("4sl", socket.inet_aton(self.mcast_group), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            sock.settimeout(1.0)
            logger.info(f"Multicast socket setup successful on port {self.discovery_port}")
            return sock
        
        except Exception as e:
            logger.error(f"Error setting up multicast socket: {e}")
            # Fallback: create basic UDP socket untuk broadcast
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(1.0)
            return sock
    
    # ========================================================================
    # LIFECYCLE METHODS
    # ========================================================================
    def start(self):
        """Start node dengan dual discovery: multicast + file-based"""
        if self.running:
            logger.warning("Node already running")
            return
        
        self.running = True
        
        # Register ke discovery file untuk local instances
        self._register_to_discovery_file()
        
        # Start threads
        self.threads = [
            threading.Thread(target=self._discover_peers, name="Discovery", daemon=True),
            threading.Thread(target=self._receive_messages, name="Receiver", daemon=True),
            threading.Thread(target=self._handle_file_transfers, name="FileTransfer", daemon=True),
            threading.Thread(target=self._cleanup_stale_data, name="Cleanup", daemon=True),
            threading.Thread(target=self._local_discovery, name="LocalDiscovery", daemon=True),
        ]
        
        for thread in self.threads:
            thread.start()
            logger.info(f"Started thread: {thread.name}")
        
        # Initial announcement
        self._announce_presence()
        logger.info(f"Node {self.node_id} started")
    
    def stop(self):
        """Stop node dengan cleanup"""
        if not self.running:
            return
        
        logger.info(f"Stopping node {self.node_id}")
        self.running = False
        
        # Unregister dari discovery file
        self._unregister_from_discovery_file()
        
        # Wait for threads
        for thread in self.threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not stop gracefully")
        
        # Cleanup transfers
        with self.receiving_lock:
            for transfer_id, state in list(self.receiving_transfers.items()):
                try:
                    if state.temp_path.exists():
                        state.temp_path.unlink()
                except Exception as e:
                    logger.error(f"Error cleaning up transfer {transfer_id}: {e}")
        
        # Close sockets
        try:
            self.discovery_socket.close()
            self.pub_socket.close()
            self.router_socket.close()
            self.sub_socket.close()
            self.file_router_socket.close()
            self.context.term()
        except Exception as e:
            logger.error(f"Error closing sockets: {e}")
        
        logger.info(f"Node {self.node_id} stopped")
    
    # ========================================================================
    # FILE-BASED LOCAL DISCOVERY
    # ========================================================================
    def _register_to_discovery_file(self):
        """
        Register node info ke shared file untuk local discovery.
        File ini dibaca oleh instances lain di komputer yang sama.
        """
        try:
            # Read existing entries
            entries = {}
            if self.discovery_file.exists():
                try:
                    with open(self.discovery_file, 'r') as f:
                        entries = json.load(f)
                except json.JSONDecodeError:
                    entries = {}
            
            # Add/update our entry
            entries[self.node_id] = {
                "node_id": self.node_id,
                "ip": self._get_local_ip(),
                "router_port": self.router_port,
                "file_port": self.file_port,
                "pub_port": self.pub_port,
                "timestamp": time.time()
            }
            
            # Write back
            with open(self.discovery_file, 'w') as f:
                json.dump(entries, f, indent=2)
            
            logger.info(f"Registered to discovery file: {self.discovery_file}")
        
        except Exception as e:
            logger.error(f"Error registering to discovery file: {e}")
    
    def _unregister_from_discovery_file(self):
        """Remove our entry dari discovery file"""
        try:
            if self.discovery_file.exists():
                with open(self.discovery_file, 'r') as f:
                    entries = json.load(f)
                
                if self.node_id in entries:
                    del entries[self.node_id]
                
                with open(self.discovery_file, 'w') as f:
                    json.dump(entries, f, indent=2)
                
                logger.info("Unregistered from discovery file")
        
        except Exception as e:
            logger.error(f"Error unregistering from discovery file: {e}")
    
    def _local_discovery(self):
        """
        Background thread untuk discover local instances via file.
        Ini complement multicast discovery.
        """
        while self.running:
            try:
                if self.discovery_file.exists():
                    with open(self.discovery_file, 'r') as f:
                        entries = json.load(f)
                    
                    current_time = time.time()
                    
                    for node_id, info in entries.items():
                        # Skip ourselves
                        if node_id == self.node_id:
                            continue
                        
                        # Skip stale entries (older than 30 seconds)
                        if current_time - info.get('timestamp', 0) > 30:
                            continue
                        
                        # Add/update peer
                        with self.peers_lock:
                            peer_info = PeerInfo(
                                node_id=info["node_id"],
                                ip=info["ip"],
                                router_port=info["router_port"],
                                file_port=info["file_port"],
                                last_seen=time.time()
                            )
                            
                            is_new = node_id not in self.peers
                            self.peers[node_id] = peer_info
                            
                            if is_new:
                                # Connect ke pub socket
                                try:
                                    self.sub_socket.connect(f"tcp://{info['ip']}:{info['pub_port']}")
                                    logger.info(f"Discovered local peer: {node_id}")
                                except Exception as e:
                                    logger.error(f"Error connecting to local peer {node_id}: {e}")
                
                # Update our timestamp
                self._register_to_discovery_file()
                
            except Exception as e:
                if self.running:
                    logger.error(f"Local discovery error: {e}")
            
            time.sleep(2)  # Check every 2 seconds
    
    # ========================================================================
    # MULTICAST DISCOVERY
    # ========================================================================
    def _announce_presence(self):
        """Announce via multicast"""
        announcement = {
            "node_id": self.node_id,
            "ip": self._get_local_ip(),
            "router_port": self.router_port,
            "file_port": self.file_port,
            "pub_port": self.pub_port,
            "timestamp": time.time()
        }
        
        try:
            msg = json.dumps(announcement).encode()
            self.discovery_socket.sendto(msg, (self.mcast_group, self.discovery_port))
            logger.debug(f"Announced presence to network")
        except socket.error as e:
            logger.error(f"Error announcing presence: {e}")
    
    def _discover_peers(self):
        """Background thread untuk multicast discovery"""
        last_announce = time.time()
        announce_interval = 5.0
        
        while self.running:
            try:
                if self.discovery_socket.fileno() == -1:
                    logger.error("Discovery socket closed unexpectedly")
                    break
                
                try:
                    data, addr = self.discovery_socket.recvfrom(4096)
                    peer = json.loads(data.decode())
                    
                    # Ignore our own announcements
                    if peer["node_id"] == self.node_id:
                        continue
                    
                    # Update peer
                    with self.peers_lock:
                        peer_info = PeerInfo(
                            node_id=peer["node_id"],
                            ip=peer["ip"],
                            router_port=peer["router_port"],
                            file_port=peer.get("file_port", peer["router_port"] + 100),
                            last_seen=time.time()
                        )
                        
                        is_new = peer["node_id"] not in self.peers
                        self.peers[peer["node_id"]] = peer_info
                        
                        if is_new:
                            pub_port = peer.get("pub_port", 5555)
                            try:
                                self.sub_socket.connect(f"tcp://{peer['ip']}:{pub_port}")
                                logger.info(f"Discovered peer via multicast: {peer['node_id']}")
                            except Exception as e:
                                logger.error(f"Error connecting to peer {peer['node_id']}: {e}")
                
                except socket.timeout:
                    pass
                
                # Periodic re-announce
                if time.time() - last_announce > announce_interval:
                    self._announce_presence()
                    last_announce = time.time()
            
            except Exception as e:
                if self.running:
                    logger.error(f"Discovery error: {e}")
                    time.sleep(1)
    
    # ========================================================================
    # MESSAGE HANDLING (sama seperti sebelumnya, disingkat untuk brevity)
    # ========================================================================
    def _receive_messages(self):
        """Handle incoming messages"""
        poller = zmq.Poller()
        poller.register(self.sub_socket, zmq.POLLIN)
        poller.register(self.router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                
                if self.sub_socket in socks:
                    msg = self.sub_socket.recv_json(flags=zmq.NOBLOCK)
                    self._handle_message(msg)
                
                if self.router_socket in socks:
                    frames = self.router_socket.recv_multipart(flags=zmq.NOBLOCK)
                    if len(frames) >= 2:
                        sender_id = frames[0]
                        msg = json.loads(frames[1].decode())
                        self._handle_message(msg, sender_id)
            
            except Exception as e:
                if self.running:
                    logger.error(f"Receive error: {e}")
    
    def _handle_message(self, msg: dict, sender_id: Optional[bytes] = None):
        """Process incoming messages"""
        try:
            msg_type = MessageType(msg.get("type"))
            source = msg.get("source")
            content = msg.get("content")
            
            if msg_type == MessageType.GROUP:
                exclude = msg.get("exclude", [])
                group_name = msg.get("group_name")
                
                if self.node_id in exclude:
                    return
                
                with self.groups_lock:
                    if group_name and group_name not in self.groups:
                        return
                
                logger.info(f"📢 Group [{group_name}] from {source}: {content}")
            
            elif msg_type == MessageType.DIRECT:
                logger.info(f"💬 Direct from {source}: {content}")
            
            elif msg_type == MessageType.FILE_INIT:
                self._handle_file_init(msg, source)
            
            # Add other message type handlers as needed...
        
        except Exception as e:
            logger.error(f"Error handling message: {e}")


    # ========================================================================
    # FILE TRANSFER - SENDING
    # ========================================================================
    def send_large_file(
        self, 
        target_id: str, 
        file_path: str,
        chunk_size: Optional[int] = None
    ) -> Optional[str]:
        """
        Initiate large file transfer ke target node.
        
        Process:
        1. Validate file dan peer
        2. Compute MD5 hash
        3. Create transfer state
        4. Send FILE_INIT message
        5. Wait for chunks to be requested by receiver
        
        Args:
            target_id: Node ID tujuan
            file_path: Path ke file yang akan dikirim
            chunk_size: Ukuran chunk (default: self.chunk_size)
        
        Returns:
            transfer_id jika berhasil, None jika gagal
        """
        # Validate target peer exists
        with self.peers_lock:
            if target_id not in self.peers:
                logger.error(f"Target peer {target_id} not found")
                return None
            peer = self.peers[target_id]
        
        # Validate file exists
        file_path = Path(file_path)
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return None
        
        if not file_path.is_file():
            logger.error(f"Not a file: {file_path}")
            return None
        
        # Acquire semaphore untuk limit concurrent transfers
        if not self.transfer_semaphore.acquire(blocking=False):
            logger.warning("Max concurrent transfers reached")
            return None
        
        try:
            chunk_size = chunk_size or self.chunk_size
            total_size = file_path.stat().st_size
            
            logger.info(f"Starting file transfer: {file_path.name} ({total_size} bytes)")
            
            # Compute MD5 hash untuk verification
            total_md5 = self._compute_file_md5(file_path, chunk_size)
            
            # Create transfer state
            transfer_id = str(uuid.uuid4())
            transfer_state = TransferState(
                transfer_id=transfer_id,
                file_path=str(file_path),
                total_size=total_size,
                chunk_size=chunk_size,
                total_md5=total_md5,
                chunks_sent=set(),
                last_activity=time.time()
            )
            
            with self.transfers_lock:
                self.transfers[transfer_id] = transfer_state
            
            # Send FILE_INIT message ke receiver
            init_msg = {
                "type": MessageType.FILE_INIT.value,
                "source": self.node_id,
                "transfer_id": transfer_id,
                "file_name": file_path.name,
                "total_size": total_size,
                "chunk_size": chunk_size,
                "total_md5": total_md5
            }
            
            success = self._send_direct_message(target_id, init_msg)
            if not success:
                # Cleanup jika gagal send
                with self.transfers_lock:
                    del self.transfers[transfer_id]
                self.transfer_semaphore.release()
                return None
            
            logger.info(f"File transfer initiated: {transfer_id}")
            return transfer_id
        
        except Exception as e:
            logger.error(f"Error initiating file transfer: {e}", exc_info=True)
            self.transfer_semaphore.release()
            return None


    def _handle_file_init(self, msg: dict, source: str):
        """Handle file transfer initialization (simplified)"""
        logger.info(f"📁 File transfer request from {source}: {msg.get('file_name')}")
        # Implementation sama seperti sebelumnya...
    
    def _handle_file_transfers(self):
        """Handle file transfer operations (simplified)"""
        # Implementation sama seperti sebelumnya...
        pass
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    def _get_local_ip(self) -> str:
        """Get local IP address"""
        # Untuk local testing, gunakan localhost
        if all(peer.ip == "127.0.0.1" for peer in self.peers.values()):
            return "127.0.0.1"
        
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip
    
    def _cleanup_stale_data(self):
        """Cleanup stale peers and transfers"""
        while self.running:
            try:
                time.sleep(10)
                current_time = time.time()
                
                # Cleanup dead peers
                with self.peers_lock:
                    dead_peers = [
                        peer_id 
                        for peer_id, peer in self.peers.items()
                        if not peer.is_alive(30.0)
                    ]
                    
                    for peer_id in dead_peers:
                        logger.info(f"Removing dead peer: {peer_id}")
                        del self.peers[peer_id]
            
            except Exception as e:
                if self.running:
                    logger.error(f"Cleanup error: {e}")
    
    @contextmanager
    def _dealer_socket(self, peer: PeerInfo, for_file: bool = False):
        """Context manager untuk dealer socket"""
        dealer = self.context.socket(zmq.DEALER)
        try:
            port = peer.file_port if for_file else peer.router_port
            dealer.connect(f"tcp://{peer.ip}:{port}")
            dealer.setsockopt(zmq.LINGER, 0)
            dealer.setsockopt(zmq.SNDTIMEO, 5000)
            yield dealer
        finally:
            dealer.close()
    
    # ========================================================================
    # PUBLIC API METHODS
    # ========================================================================
    def join_group(self, group_name: str):
        """Join group"""
        with self.groups_lock:
            self.groups.add(group_name)
        logger.info(f"Joined group: {group_name}")
    
    def send_message(
        self, 
        target_id: Optional[str],
        msg_type: MessageType, 
        content: any,
        exclude: Optional[list] = None,
        group_name: Optional[str] = None
    ) -> bool:
        """Send message"""
        try:
            msg = {
                "type": msg_type.value,
                "source": self.node_id,
                "content": content,
                "timestamp": time.time()
            }
            
            if msg_type == MessageType.GROUP:
                if exclude is not None:
                    msg["exclude"] = exclude if isinstance(exclude, list) else [exclude]
                if group_name is not None:
                    msg["group_name"] = group_name
                
                self.pub_socket.send_json(msg)
                logger.debug(f"Sent group message to {group_name}")
                return True
            
            elif msg_type == MessageType.DIRECT:
                if not target_id:
                    logger.error("Target ID required")
                    return False
                
                with self.peers_lock:
                    if target_id not in self.peers:
                        logger.error(f"Target {target_id} not found")
                        return False
                    peer = self.peers[target_id]
                
                with self._dealer_socket(peer) as dealer:
                    dealer.send_json(msg)
                
                return True
            
            return False
        
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def get_peers(self) -> Dict[str, PeerInfo]:
        """Get peers list"""
        with self.peers_lock:
            return self.peers.copy()


# ============================================================================
# SIMPLE TEST
# ============================================================================
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='P2P Node')
    parser.add_argument('--id', type=str, help='Node ID', default=None)
    args = parser.parse_args()
    
    node = Node(node_id=args.id)
    
    try:
        node.start()
        logger.info("✅ Node started! Waiting for peers...")
        
        time.sleep(3)
        
        # Auto-join default group
        node.join_group("general")
        
        # Show discovered peers
        peers = node.get_peers()
        logger.info(f"📋 Found {len(peers)} peers:")
        for peer_id, peer_info in peers.items():
            logger.info(f"  - {peer_id} @ {peer_info.ip}:{peer_info.router_port}")
        
        # Send test message
        if peers:
            node.send_message(
                target_id=None,
                msg_type=MessageType.GROUP,
                content=f"Hello from {node.node_id}!",
                group_name="general"
            )
        
        # Keep running
        logger.info("💬 Node running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        node.stop()
