import zmq
import json
import threading
import socket
import struct
import uuid
import time
import hashlib
import base64
import os
import logging
from enum import Enum
from typing import Dict, Optional, Set, Tuple, List
from dataclasses import dataclass, field
from pathlib import Path
from contextlib import contextmanager

# ============================================================================
# KONFIGURASI LOGGING
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)-12s - %(levelname)-8s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# ENUMERATIONS
# ============================================================================
class MessageType(Enum):
    """Tipe-tipe pesan yang didukung oleh sistem"""
    DIRECT = "direct"
    GROUP = "group"
    IMAGE = "image"
    FILE = "file"
    OPERATION = "operation"
    FOLDER_SHARE = "folder_share"
    FILE_INIT = "file_init"
    CHUNK_REQUEST = "chunk_request"
    CHUNK_RESPONSE = "chunk_response"
    TRANSFER_COMPLETE = "transfer_complete"
    TRANSFER_ERROR = "transfer_error"

# ============================================================================
# DATA CLASSES
# ============================================================================
@dataclass
class PeerInfo:
    """Informasi tentang peer node"""
    node_id: str
    ip: str
    router_port: int
    file_port: int
    pub_port: int
    last_seen: float = 0.0
    
    def is_alive(self, timeout: float = 30.0) -> bool:
        """Check apakah peer masih aktif"""
        return time.time() - self.last_seen < timeout

@dataclass
class TransferState:
    """State untuk file transfer yang sedang berlangsung (sender side)"""
    transfer_id: str
    file_path: str
    total_size: int
    chunk_size: int
    total_md5: str
    target_node: str
    chunks_requested: Set[int] = field(default_factory=set)
    chunks_sent: Set[int] = field(default_factory=set)
    last_activity: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)
    
    def get_progress(self) -> float:
        """Calculate progress percentage"""
        total_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        if total_chunks == 0:
            return 100.0
        return (len(self.chunks_sent) / total_chunks) * 100.0
    
    def is_complete(self) -> bool:
        """Check apakah semua chunks sudah dikirim"""
        total_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.chunks_sent) >= total_chunks

@dataclass
class ReceiveState:
    """State untuk file yang sedang diterima (receiver side)"""
    transfer_id: str
    file_name: str
    total_size: int
    chunk_size: int
    total_md5: str
    sender_node: str
    temp_path: Path
    received_chunks: Dict[int, bytes] = field(default_factory=dict)
    requested_chunks: Set[int] = field(default_factory=set)
    chunk_md5s: Dict[int, str] = field(default_factory=dict)
    retry_count: Dict[int, int] = field(default_factory=dict)
    last_activity: float = field(default_factory=time.time)
    start_time: float = field(default_factory=time.time)
    max_retries: int = 3
    
    def get_progress(self) -> float:
        """Calculate progress percentage"""
        if self.total_size == 0:
            return 100.0
        return (self.get_total_received() / self.total_size) * 100.0
    
    def is_complete(self) -> bool:
        """Check apakah semua chunks sudah diterima"""
        total_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.received_chunks) >= total_chunks
    
    def get_total_received(self) -> int:
        """Hitung total bytes yang sudah diterima"""
        return sum(len(chunk) for chunk in self.received_chunks.values())
    
    def get_missing_chunks(self) -> List[int]:
        """Get list of chunk offsets yang belum diterima"""
        total_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        all_offsets = [i * self.chunk_size for i in range(total_chunks)]
        return [offset for offset in all_offsets if offset not in self.received_chunks]

# ============================================================================
# MAIN NODE CLASS
# ============================================================================
class Node:
    """
    P2P Node dengan reliable file transfer support
    """
    
    def __init__(
        self, 
        node_id: Optional[str] = None,
        bind_ip: str = "0.0.0.0",
        pub_port: int = 0,
        router_port: int = 0,
        file_port: int = 0,
        discovery_port: int = 5557,
        chunk_size: int = 524288,  # 512KB default
        max_concurrent_transfers: int = 5
    ):
        # ====================================================================
        # IDENTITAS DAN KONFIGURASI
        # ====================================================================
        self.node_id = node_id or str(uuid.uuid4())[:8]
        self.bind_ip = bind_ip
        self.discovery_port = discovery_port
        self.chunk_size = chunk_size
        self.max_concurrent_transfers = max_concurrent_transfers
        
        logger.info(f"Initializing Node {self.node_id}")
        
        # ====================================================================
        # DATA STRUCTURES dengan Thread-Safe Locks
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
        # ZEROMQ SETUP
        # ====================================================================
        self.context = zmq.Context()
        self.context.setsockopt(zmq.MAX_SOCKETS, 1024)
        
        # PUB socket untuk group messages
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.SNDHWM, 1000)
        if pub_port == 0:
            self.pub_port = self.pub_socket.bind_to_random_port(f"tcp://{bind_ip}")
        else:
            self.pub_socket.bind(f"tcp://{bind_ip}:{pub_port}")
            self.pub_port = pub_port
        
        # ROUTER socket untuk direct messages
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.router_socket.setsockopt(zmq.RCVHWM, 1000)
        self.router_socket.setsockopt(zmq.ROUTER_MANDATORY, 0)
        if router_port == 0:
            self.router_port = self.router_socket.bind_to_random_port(f"tcp://{bind_ip}")
        else:
            self.router_socket.bind(f"tcp://{bind_ip}:{router_port}")
            self.router_port = router_port
        
        # SUB socket untuk group messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub_socket.setsockopt(zmq.RCVHWM, 1000)
        
        # FILE ROUTER socket untuk file transfers
        self.file_router_socket = self.context.socket(zmq.ROUTER)
        self.file_router_socket.setsockopt(zmq.RCVHWM, 10000)
        self.file_router_socket.setsockopt(zmq.ROUTER_MANDATORY, 0)
        if file_port == 0:
            self.file_port = self.file_router_socket.bind_to_random_port(f"tcp://{bind_ip}")
        else:
            self.file_router_socket.bind(f"tcp://{bind_ip}:{file_port}")
            self.file_port = file_port
        
        logger.info(f"Sockets - PUB:{self.pub_port} ROUTER:{self.router_port} FILE:{self.file_port}")
        
        # ====================================================================
        # UDP MULTICAST DISCOVERY
        # ====================================================================
        self.mcast_group = "224.0.0.1"
        self.discovery_socket = self._setup_multicast_socket()
        
        # ====================================================================
        # FILE-BASED LOCAL DISCOVERY
        # ====================================================================
        self.discovery_file = Path(f"/tmp/p2p_nodes_{discovery_port}.json")
        if os.name == 'nt':
            self.discovery_file = Path(f"{os.environ['TEMP']}/p2p_nodes_{discovery_port}.json")
        
        # ====================================================================
        # THREADING CONTROL
        # ====================================================================
        self.running = False
        self.threads = []
        
        # Temp directory untuk file transfers
        self.temp_dir = Path("temp_transfers")
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info(f"Node {self.node_id} initialized successfully")

    def _setup_multicast_socket(self):
        """Setup UDP multicast socket"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except (AttributeError, OSError):
                pass
            
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            sock.bind(('', self.discovery_port))
            
            mreq = struct.pack("4sl", socket.inet_aton(self.mcast_group), socket.INADDR_ANY)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            
            sock.settimeout(1.0)
            logger.info(f"Multicast socket setup on port {self.discovery_port}")
            return sock
        
        except Exception as e:
            logger.error(f"Error setting up multicast: {e}")
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(1.0)
            return sock
    
    # ========================================================================
    # LIFECYCLE METHODS
    # ========================================================================
    def start(self):
        """Start node dan semua background threads"""
        if self.running:
            logger.warning("Node already running")
            return
        
        self.running = True
        
        self.threads = [
            threading.Thread(target=self._discover_peers, name="Discovery", daemon=True),
            threading.Thread(target=self._receive_messages, name="Receiver", daemon=True),
            threading.Thread(target=self._handle_file_transfers, name="FileHandler", daemon=True),
            threading.Thread(target=self._cleanup_stale_data, name="Cleanup", daemon=True),
            threading.Thread(target=self._local_discovery, name="LocalDiscovery", daemon=True),
        ]
        
        for thread in self.threads:
            thread.start()
            logger.info(f"Started thread: {thread.name}")
        
        self._announce_presence()
        logger.info(f"Node {self.node_id} started")

    def stop(self):
        """Stop node dengan graceful shutdown"""
        if not self.running:
            return
        
        logger.info(f"Stopping node {self.node_id}")
        self.running = False
        
        # Wait for threads
        for thread in self.threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not stop gracefully")
        
        # Cleanup receiving transfers
        with self.receiving_lock:
            for transfer_id, state in list(self.receiving_transfers.items()):
                try:
                    if state.temp_path.exists():
                        state.temp_path.unlink()
                except Exception as e:
                    logger.error(f"Error cleaning up {transfer_id}: {e}")
        
        # Unregister from discovery
        self._unregister_from_discovery_file()
        
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
        """Register node ke discovery file"""
        try:
            entries = {}
            if self.discovery_file.exists():
                try:
                    with open(self.discovery_file, 'r') as f:
                        entries = json.load(f)
                except json.JSONDecodeError:
                    entries = {}
            
            entries[self.node_id] = {
                "node_id": self.node_id,
                "ip": self._get_local_ip(),
                "router_port": self.router_port,
                "file_port": self.file_port,
                "pub_port": self.pub_port,
                "timestamp": time.time()
            }
            
            with open(self.discovery_file, 'w') as f:
                json.dump(entries, f, indent=2)
        
        except Exception as e:
            logger.error(f"Error registering to discovery file: {e}")
    
    def _unregister_from_discovery_file(self):
        """Remove entry dari discovery file"""
        try:
            if self.discovery_file.exists():
                with open(self.discovery_file, 'r') as f:
                    entries = json.load(f)
                
                if self.node_id in entries:
                    del entries[self.node_id]
                
                with open(self.discovery_file, 'w') as f:
                    json.dump(entries, f, indent=2)
        
        except Exception as e:
            logger.error(f"Error unregistering: {e}")
    
    def _local_discovery(self):
        """Background thread untuk discover local instances"""
        while self.running:
            try:
                if self.discovery_file.exists():
                    with open(self.discovery_file, 'r') as f:
                        entries = json.load(f)
                    
                    current_time = time.time()
                    
                    for node_id, info in entries.items():
                        if node_id == self.node_id:
                            continue
                        
                        if current_time - info.get('timestamp', 0) > 30:
                            continue
                        
                        with self.peers_lock:
                            peer_info = PeerInfo(
                                node_id=info["node_id"],
                                ip=info["ip"],
                                router_port=info["router_port"],
                                file_port=info["file_port"],
                                pub_port=info["pub_port"],
                                last_seen=time.time()
                            )
                            
                            is_new = node_id not in self.peers
                            self.peers[node_id] = peer_info
                            
                            if is_new:
                                try:
                                    # PERBAIKAN: Connect ke pub_port PEER, bukan self.pub_port
                                    self.sub_socket.connect(f"tcp://{info['ip']}:{info['pub_port']}")
                                    logger.info(f"Discovered local peer: {node_id}")
                                except Exception as e:
                                    logger.error(f"Error connecting to {node_id}: {e}")
                
                self._register_to_discovery_file()
                
            except Exception as e:
                if self.running:
                    logger.error(f"Local discovery error: {e}")
            
            time.sleep(2)
    
    # ========================================================================
    # PEER DISCOVERY
    # ========================================================================
    def _announce_presence(self):
        """Broadcast keberadaan node via UDP multicast"""
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
        except socket.error as e:
            logger.error(f"Error announcing presence: {e}")

    def _discover_peers(self):
        """Background thread untuk peer discovery"""
        last_announce = time.time()
        announce_interval = 5.0
        
        while self.running:
            try:
                if self.discovery_socket.fileno() == -1:
                    logger.error("Discovery socket closed")
                    break
                
                try:
                    data, addr = self.discovery_socket.recvfrom(4096)
                    peer = json.loads(data.decode())
                    
                    if peer["node_id"] == self.node_id:
                        continue
                    
                    with self.peers_lock:
                        peer_info = PeerInfo(
                            node_id=peer["node_id"],
                            ip=peer["ip"],
                            router_port=peer["router_port"],
                            file_port=peer.get("file_port", peer["router_port"]),
                            pub_port=peer.get("pub_port", peer["router_port"]),
                            last_seen=time.time()
                        )
                        
                        is_new = peer["node_id"] not in self.peers
                        self.peers[peer["node_id"]] = peer_info
                        
                        if is_new:
                            # PERBAIKAN: Connect ke pub_port PEER
                            self.sub_socket.connect(f"tcp://{peer['ip']}:{peer_info.pub_port}")
                            logger.info(f"Discovered peer: {peer['node_id']} at {peer['ip']}")
                
                except socket.timeout:
                    pass
                
                if time.time() - last_announce > announce_interval:
                    self._announce_presence()
                    last_announce = time.time()
                
            except Exception as e:
                if self.running:
                    logger.error(f"Discovery error: {e}")
                    time.sleep(1)

    # ========================================================================
    # MESSAGE HANDLING
    # ========================================================================
    def _receive_messages(self):
        """Handle incoming control messages"""
        poller = zmq.Poller()
        poller.register(self.sub_socket, zmq.POLLIN)
        poller.register(self.router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                
                if self.sub_socket in socks:
                    try:
                        msg = self.sub_socket.recv_json(flags=zmq.NOBLOCK)
                        self._handle_message(msg)
                    except zmq.ZMQError as e:
                        logger.error(f"Error receiving from sub: {e}")
                
                if self.router_socket in socks:
                    try:
                        frames = self.router_socket.recv_multipart(flags=zmq.NOBLOCK)
                        if len(frames) >= 2:
                            sender_id = frames[0]
                            msg = json.loads(frames[1].decode())
                            self._handle_message(msg, sender_id)
                    except zmq.ZMQError as e:
                        logger.error(f"Error receiving from router: {e}")
            
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
                
                logger.info(f"[{group_name}] {source}: {content}")
            
            elif msg_type == MessageType.DIRECT:
                logger.info(f"Direct from {source}: {content}")
            
            elif msg_type == MessageType.FILE_INIT:
                self._handle_file_init(msg, source)
            
            elif msg_type == MessageType.TRANSFER_COMPLETE:
                self._handle_transfer_complete(msg, source)
            
            elif msg_type == MessageType.TRANSFER_ERROR:
                self._handle_transfer_error(msg, source)
            
        except ValueError as e:
            logger.warning(f"Invalid message type: {e}")
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
        Initiate file transfer ke target node
        
        Returns:
            transfer_id if successful, None if failed
        """
        with self.peers_lock:
            if target_id not in self.peers:
                logger.error(f"Peer {target_id} not found")
                return None
            peer = self.peers[target_id]
        
        file_path = Path(file_path)
        if not file_path.exists() or not file_path.is_file():
            logger.error(f"Invalid file: {file_path}")
            return None
        
        if not self.transfer_semaphore.acquire(blocking=False):
            logger.warning("Max concurrent transfers reached")
            return None
        
        try:
            chunk_size = chunk_size or self.chunk_size
            total_size = file_path.stat().st_size
            
            logger.info(f"Starting transfer: {file_path.name} ({total_size} bytes) -> {target_id}")
            
            # Compute MD5
            total_md5 = self._compute_file_md5(file_path, chunk_size)
            
            # Create transfer state
            transfer_id = str(uuid.uuid4())
            transfer_state = TransferState(
                transfer_id=transfer_id,
                file_path=str(file_path),
                total_size=total_size,
                chunk_size=chunk_size,
                total_md5=total_md5,
                target_node=target_id
            )
            
            with self.transfers_lock:
                self.transfers[transfer_id] = transfer_state
            
            # Send FILE_INIT
            init_msg = {
                "type": MessageType.FILE_INIT.value,
                "source": self.node_id,
                "transfer_id": transfer_id,
                "file_name": file_path.name,
                "total_size": total_size,
                "chunk_size": chunk_size,
                "total_md5": total_md5
            }
            
            if not self._send_direct_message(target_id, init_msg):
                with self.transfers_lock:
                    del self.transfers[transfer_id]
                self.transfer_semaphore.release()
                return None
            
            logger.info(f"Transfer initiated: {transfer_id}")
            return transfer_id
        
        except Exception as e:
            logger.error(f"Error initiating transfer: {e}")
            self.transfer_semaphore.release()
            return None

    def _handle_file_transfers(self):
        """Background thread untuk handle file transfer messages"""
        poller = zmq.Poller()
        poller.register(self.file_router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                
                if self.file_router_socket in socks:
                    try:
                        frames = self.file_router_socket.recv_multipart(flags=zmq.NOBLOCK)
                        
                        if len(frames) < 2:
                            continue
                        
                        sender_id = frames[0]
                        metadata = json.loads(frames[1].decode())
                        chunk_data = frames[2] if len(frames) > 2 else None
                        
                        msg_type = MessageType(metadata.get("type"))
                        
                        if msg_type == MessageType.CHUNK_REQUEST:
                            transfer_id = metadata["transfer_id"]
                            offset = metadata["offset"]
                            chunk_size = metadata["chunk_size"]
                            
                            self._send_chunk(sender_id, transfer_id, offset, chunk_size)
                        
                        elif msg_type == MessageType.CHUNK_RESPONSE:
                            if chunk_data:
                                transfer_id = metadata["transfer_id"]
                                offset = metadata["offset"]
                                chunk_md5 = metadata["chunk_md5"]
                                
                                self._receive_chunk(
                                    sender_id, 
                                    transfer_id, 
                                    offset, 
                                    chunk_md5, 
                                    chunk_data
                                )
                    
                    except Exception as e:
                        logger.error(f"Error in file transfer handler: {e}")
            
            except Exception as e:
                if self.running:
                    logger.error(f"File transfer error: {e}")

    def _send_chunk(self, target_id: bytes, transfer_id: str, offset: int, chunk_size: int):
        """Send requested chunk"""
        with self.transfers_lock:
            if transfer_id not in self.transfers:
                logger.warning(f"Transfer {transfer_id} not found")
                return
            
            transfer = self.transfers[transfer_id]
            transfer.last_activity = time.time()
            transfer.chunks_requested.add(offset)
        
        try:
            with open(transfer.file_path, 'rb') as f:
                f.seek(offset)
                chunk = f.read(chunk_size)
            
            if not chunk:
                logger.warning(f"No data at offset {offset}")
                return
            
            chunk_md5 = hashlib.md5(chunk).hexdigest()
            
            metadata = {
                "type": MessageType.CHUNK_RESPONSE.value,
                "transfer_id": transfer_id,
                "offset": offset,
                "chunk_md5": chunk_md5,
                "chunk_size": len(chunk)
            }
            
            self.file_router_socket.send_multipart([
                target_id,
                json.dumps(metadata).encode(),
                chunk
            ])
            
            with self.transfers_lock:
                if transfer_id in self.transfers:
                    self.transfers[transfer_id].chunks_sent.add(offset)
                    
                    # Log progress setiap 10%
                    progress = self.transfers[transfer_id].get_progress()
                    if int(progress) % 10 == 0 and len(self.transfers[transfer_id].chunks_sent) % 5 == 0:
                        logger.info(f"Transfer {transfer_id[:8]}: {progress:.1f}%")
        
        except Exception as e:
            logger.error(f"Error sending chunk: {e}")

    def _handle_transfer_complete(self, msg: dict, source: str):
        """Handle notification bahwa transfer selesai di receiver side"""
        transfer_id = msg.get("transfer_id")
        
        with self.transfers_lock:
            if transfer_id in self.transfers:
                transfer = self.transfers[transfer_id]
                elapsed = time.time() - transfer.start_time
                speed = transfer.total_size / elapsed / 1024 / 1024  # MB/s
                
                logger.info(f"✓ Transfer {transfer_id[:8]} completed successfully")
                logger.info(f"  Time: {elapsed:.1f}s, Speed: {speed:.2f} MB/s")
                
                del self.transfers[transfer_id]
                self.transfer_semaphore.release()

    def _handle_transfer_error(self, msg: dict, source: str):
        """Handle notification bahwa transfer error di receiver side"""
        transfer_id = msg.get("transfer_id")
        error = msg.get("error")
        
        logger.error(f"Transfer {transfer_id[:8]} failed: {error}")
        
        with self.transfers_lock:
            if transfer_id in self.transfers:
                del self.transfers[transfer_id]
                self.transfer_semaphore.release()

    # ========================================================================
    # FILE TRANSFER - RECEIVING
    # ========================================================================
    def _handle_file_init(self, msg: dict, source: str):
        """Handle FILE_INIT message untuk memulai receiving"""
        try:
            transfer_id = msg["transfer_id"]
            file_name = msg["file_name"]
            total_size = msg["total_size"]
            chunk_size = msg["chunk_size"]
            total_md5 = msg["total_md5"]
            
            with self.peers_lock:
                if source not in self.peers:
                    logger.error(f"Unknown sender {source}")
                    return
                peer = self.peers[source]
            
            # Sanitize filename
            safe_filename = Path(file_name).name
            temp_path = self.temp_dir / f"temp_{transfer_id}_{safe_filename}"
            
            logger.info(f"Receiving: {safe_filename} ({total_size} bytes) from {source}")
            
            # Create receive state
            receive_state = ReceiveState(
                transfer_id=transfer_id,
                file_name=safe_filename,
                total_size=total_size,
                chunk_size=chunk_size,
                total_md5=total_md5,
                sender_node=source,
                temp_path=temp_path
            )
            
            with self.receiving_lock:
                self.receiving_transfers[transfer_id] = receive_state
            
            # Start chunk requester thread
            thread = threading.Thread(
                target=self._request_chunks_worker,
                args=(peer, transfer_id),
                name=f"ChunkReq-{transfer_id[:8]}",
                daemon=True
            )
            thread.start()
        
        except Exception as e:
            logger.error(f"Error handling FILE_INIT: {e}")

    def _request_chunks_worker(self, peer: PeerInfo, transfer_id: str):
        """Worker thread untuk request chunks dengan flow control"""
        window_size = 20  # Request 20 chunks at a time
        max_in_flight = 30  # Max chunks yang boleh di-request tapi belum diterima
        timeout = 10.0  # Timeout untuk chunk
        
        try:
            while self.running:
                with self.receiving_lock:
                    if transfer_id not in self.receiving_transfers:
                        logger.info(f"Transfer {transfer_id[:8]} finished")
                        break
                    
                    state = self.receiving_transfers[transfer_id]
                    
                    # Check jika sudah complete
                    if state.is_complete():
                        logger.info(f"All chunks received for {transfer_id[:8]}")
                        self._finalize_transfer(transfer_id, state)
                        break
                    
                    # Get missing chunks
                    missing = state.get_missing_chunks()
                    
                    # Filter chunks yang belum di-request atau perlu retry
                    to_request = []
                    in_flight = len(state.requested_chunks - state.received_chunks.keys())
                    
                    for offset in missing:
                        # Skip jika sudah requested dan belum timeout
                        if offset in state.requested_chunks:
                            # Check timeout untuk retry
                            if time.time() - state.last_activity < timeout:
                                continue
                        
                        # Limit in-flight requests
                        if in_flight >= max_in_flight:
                            break
                        
                        to_request.append(offset)
                        if len(to_request) >= window_size:
                            break
                    
                    # Request chunks
                    for offset in to_request:
                        request = {
                            "type": MessageType.CHUNK_REQUEST.value,
                            "transfer_id": transfer_id,
                            "offset": offset,
                            "chunk_size": state.chunk_size
                        }
                        
                        if self._send_file_message(peer, request):
                            state.requested_chunks.add(offset)
                            state.last_activity = time.time()
                            in_flight += 1
                
                # Small delay
                time.sleep(0.1)
        
        except Exception as e:
            logger.error(f"Error in chunk requester: {e}")
            self._cleanup_failed_transfer(transfer_id, f"Request error: {e}")

    def _receive_chunk(
        self, 
        sender_id: bytes, 
        transfer_id: str, 
        offset: int, 
        chunk_md5: str, 
        chunk: bytes
    ):
        """Process received chunk dengan verification"""
        with self.receiving_lock:
            if transfer_id not in self.receiving_transfers:
                logger.warning(f"Chunk for unknown transfer {transfer_id}")
                return
            
            state = self.receiving_transfers[transfer_id]
            state.last_activity = time.time()
            
            # Verify MD5
            computed_md5 = hashlib.md5(chunk).hexdigest()
            if computed_md5 != chunk_md5:
                logger.warning(f"MD5 mismatch at offset {offset}")
                
                # Retry logic
                retry_count = state.retry_count.get(offset, 0)
                if retry_count < state.max_retries:
                    state.retry_count[offset] = retry_count + 1
                    state.requested_chunks.discard(offset)  # Allow re-request
                    logger.info(f"Retry {retry_count + 1}/{state.max_retries} for offset {offset}")
                else:
                    error_msg = f"Too many retries at offset {offset}"
                    self._cleanup_failed_transfer(transfer_id, error_msg)
                return
            
            # Store chunk
            state.received_chunks[offset] = chunk
            state.chunk_md5s[offset] = chunk_md5
            
            # Log progress
            progress = state.get_progress()
            if int(progress) % 10 == 0 and len(state.received_chunks) % 5 == 0:
                logger.info(f"Receiving {transfer_id[:8]}: {progress:.1f}%")

    def _finalize_transfer(self, transfer_id: str, state: ReceiveState):
        """Finalize transfer dengan write to disk dan verify"""
        try:
            logger.info(f"Finalizing transfer {transfer_id[:8]}...")
            
            # Write chunks to file
            with open(state.temp_path, 'wb') as f:
                for offset in sorted(state.received_chunks.keys()):
                    f.seek(offset)
                    f.write(state.received_chunks[offset])
            
            # Verify total MD5
            final_md5 = self._compute_file_md5(state.temp_path, state.chunk_size)
            
            if final_md5 == state.total_md5:
                # Success - rename to final
                final_path = Path(state.file_name)
                
                # Handle existing file
                if final_path.exists():
                    counter = 1
                    stem = final_path.stem
                    suffix = final_path.suffix
                    while final_path.exists():
                        final_path = Path(f"{stem}_{counter}{suffix}")
                        counter += 1
                
                state.temp_path.rename(final_path)
                
                elapsed = time.time() - state.start_time
                speed = state.total_size / elapsed / 1024 / 1024
                
                logger.info(f"✓ Transfer {transfer_id[:8]} COMPLETE")
                logger.info(f"  File: {final_path}")
                logger.info(f"  Size: {state.total_size} bytes")
                logger.info(f"  Time: {elapsed:.1f}s, Speed: {speed:.2f} MB/s")
                logger.info(f"  MD5: {final_md5}")
                
                # Notify sender
                self._send_transfer_complete(state.sender_node, transfer_id)
            else:
                error_msg = f"MD5 mismatch - expected {state.total_md5}, got {final_md5}"
                logger.error(f"✗ Transfer {transfer_id[:8]} FAILED: {error_msg}")
                state.temp_path.unlink()
                
                # Notify sender
                self._send_transfer_error(state.sender_node, transfer_id, error_msg)
            
            # Cleanup
            with self.receiving_lock:
                if transfer_id in self.receiving_transfers:
                    del self.receiving_transfers[transfer_id]
        
        except Exception as e:
            logger.error(f"Error finalizing transfer: {e}")
            self._cleanup_failed_transfer(transfer_id, str(e))

    def _cleanup_failed_transfer(self, transfer_id: str, error: str):
        """Cleanup transfer yang gagal"""
        logger.error(f"Cleaning up failed transfer {transfer_id[:8]}: {error}")
        
        with self.receiving_lock:
            if transfer_id in self.receiving_transfers:
                state = self.receiving_transfers[transfer_id]
                
                # Remove temp file
                if state.temp_path.exists():
                    state.temp_path.unlink()
                
                # Notify sender
                self._send_transfer_error(state.sender_node, transfer_id, error)
                
                del self.receiving_transfers[transfer_id]

    def _send_transfer_complete(self, target_id: str, transfer_id: str):
        """Send notification bahwa transfer complete"""
        msg = {
            "type": MessageType.TRANSFER_COMPLETE.value,
            "source": self.node_id,
            "transfer_id": transfer_id
        }
        self._send_direct_message(target_id, msg)

    def _send_transfer_error(self, target_id: str, transfer_id: str, error: str):
        """Send notification bahwa transfer error"""
        msg = {
            "type": MessageType.TRANSFER_ERROR.value,
            "source": self.node_id,
            "transfer_id": transfer_id,
            "error": error
        }
        self._send_direct_message(target_id, msg)

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    def _compute_file_md5(self, file_path: Path, chunk_size: int) -> str:
        """Compute MD5 hash dari file"""
        md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    md5.update(chunk)
            return md5.hexdigest()
        except Exception as e:
            logger.error(f"Error computing MD5: {e}")
            return ""

    def _get_local_ip(self) -> str:
        """Get local IP address"""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

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

    def _send_direct_message(self, target_id: str, msg: dict) -> bool:
        """Send direct message ke target"""
        try:
            with self.peers_lock:
                if target_id not in self.peers:
                    logger.error(f"Target {target_id} not found")
                    return False
                peer = self.peers[target_id]
            
            with self._dealer_socket(peer) as dealer:
                dealer.send_json(msg)
            
            return True
        
        except zmq.ZMQError as e:
            logger.error(f"Error sending direct message: {e}")
            return False

    def _send_file_message(self, peer: PeerInfo, msg: dict) -> bool:
        """Send message via file channel"""
        try:
            with self._dealer_socket(peer, for_file=True) as dealer:
                dealer.send_json(msg)
            return True
        
        except zmq.ZMQError as e:
            logger.error(f"Error sending file message: {e}")
            return False

    def _cleanup_stale_data(self):
        """Background thread untuk cleanup"""
        cleanup_interval = 10.0
        peer_timeout = 30.0
        transfer_timeout = 120.0  # 2 minutes untuk transfers
        
        while self.running:
            try:
                time.sleep(cleanup_interval)
                
                current_time = time.time()
                
                # Cleanup dead peers
                with self.peers_lock:
                    dead_peers = [
                        peer_id 
                        for peer_id, peer in self.peers.items()
                        if not peer.is_alive(peer_timeout)
                    ]
                    
                    for peer_id in dead_peers:
                        logger.info(f"Removing dead peer: {peer_id}")
                        del self.peers[peer_id]
                
                # Cleanup stale outgoing transfers
                with self.transfers_lock:
                    stale = [
                        tid for tid, state in self.transfers.items()
                        if current_time - state.last_activity > transfer_timeout
                    ]
                    
                    for tid in stale:
                        logger.warning(f"Removing stale transfer: {tid[:8]}")
                        del self.transfers[tid]
                        self.transfer_semaphore.release()
                
                # Cleanup stale incoming transfers
                with self.receiving_lock:
                    stale = [
                        tid for tid, state in self.receiving_transfers.items()
                        if current_time - state.last_activity > transfer_timeout
                    ]
                    
                    for tid in stale:
                        logger.warning(f"Removing stale receive: {tid[:8]}")
                        state = self.receiving_transfers[tid]
                        if state.temp_path.exists():
                            state.temp_path.unlink()
                        del self.receiving_transfers[tid]
                
                # Cleanup orphaned temp files
                if self.temp_dir.exists():
                    for temp_file in self.temp_dir.glob("temp_*"):
                        if current_time - temp_file.stat().st_mtime > 3600:
                            logger.info(f"Removing orphaned: {temp_file.name}")
                            temp_file.unlink()
            
            except Exception as e:
                if self.running:
                    logger.error(f"Cleanup error: {e}")

    # ========================================================================
    # PUBLIC API
    # ========================================================================
    def join_group(self, group_name: str):
        """Join group untuk receive group messages"""
        with self.groups_lock:
            self.groups.add(group_name)
        logger.info(f"Joined group: {group_name}")

    def leave_group(self, group_name: str):
        """Leave group"""
        with self.groups_lock:
            self.groups.discard(group_name)
        logger.info(f"Left group: {group_name}")

    def send_message(
        self, 
        target_id: Optional[str],
        msg_type: MessageType, 
        content: any,
        exclude: Optional[list] = None,
        group_name: Optional[str] = None
    ) -> bool:
        """
        Send message
        
        Args:
            target_id: Target node ID (for DIRECT) or None (for GROUP)
            msg_type: Message type
            content: Message content
            exclude: List of node IDs to exclude (for GROUP)
            group_name: Group name (for GROUP)
        """
        try:
            msg = {
                "type": msg_type.value,
                "source": self.node_id,
                "content": content,
                "timestamp": time.time()
            }
            
            if msg_type == MessageType.GROUP:
                if exclude:
                    msg["exclude"] = exclude if isinstance(exclude, list) else [exclude]
                if group_name:
                    msg["group_name"] = group_name
                
                self.pub_socket.send_json(msg)
                return True
            
            elif msg_type == MessageType.DIRECT:
                if not target_id:
                    logger.error("Target ID required")
                    return False
                
                return self._send_direct_message(target_id, msg)
            
            else:
                logger.error(f"Unsupported message type: {msg_type}")
                return False
        
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return False

    def get_peers(self) -> Dict[str, PeerInfo]:
        """Get all active peers"""
        with self.peers_lock:
            return self.peers.copy()

    def get_transfer_status(self) -> Tuple[Dict, Dict]:
        """
        Get status of active transfers
        
        Returns:
            (outgoing_transfers, incoming_transfers)
        """
        with self.transfers_lock:
            outgoing = {
                tid: {
                    "file_path": state.file_path,
                    "total_size": state.total_size,
                    "progress": state.get_progress(),
                    "chunks_sent": len(state.chunks_sent),
                    "elapsed": time.time() - state.start_time
                }
                for tid, state in self.transfers.items()
            }
        
        with self.receiving_lock:
            incoming = {
                tid: {
                    "file_name": state.file_name,
                    "total_size": state.total_size,
                    "progress": state.get_progress(),
                    "received_bytes": state.get_total_received(),
                    "elapsed": time.time() - state.start_time
                }
                for tid, state in self.receiving_transfers.items()
            }
        
        return outgoing, incoming


# ============================================================================
# EXAMPLE USAGE & TESTING
# ============================================================================
if __name__ == "__main__":
    import sys
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(threadName)-12s - %(levelname)-8s - %(message)s'
    )
    
    # Gunakan command line argument untuk node ID
    node_id = sys.argv[1] if len(sys.argv) > 1 else "DummyNode"
    
    node = Node(
        node_id=node_id,
        chunk_size=512 * 1024,  # 512KB chunks
        max_concurrent_transfers=3
    )
    
    try:
        node.start()
        logger.info(f"Node {node_id} started successfully")
        logger.info("Waiting for peers...")
        
        time.sleep(3)
        
        # Join default group
        node.join_group("general")
        
        # Send group message
        node.send_message(
            target_id=None,
            msg_type=MessageType.GROUP,
            content=f"Hello from {node_id}!",
            group_name="general"
        )
        
        # Interactive loop
        logger.info("\nCommands:")
        logger.info("  peers - Show all peers")
        logger.info("  status - Show transfer status")
        logger.info("  send <peer_id> <file_path> - Send file")
        logger.info("  msg <peer_id> <message> - Send direct message")
        logger.info("  quit - Exit")
        
        while True:
            try:
                cmd = input("\n> ").strip().split()
                
                if not cmd:
                    continue
                
                if cmd[0] == "peers":
                    peers = node.get_peers()
                    logger.info(f"\nActive peers ({len(peers)}):")
                    for peer_id, peer in peers.items():
                        logger.info(f"  {peer_id} @ {peer.ip}:{peer.router_port}")
                
                elif cmd[0] == "status":
                    out, inc = node.get_transfer_status()
                    logger.info(f"\nOutgoing transfers: {len(out)}")
                    for tid, info in out.items():
                        logger.info(f"  {tid[:8]}: {info['progress']:.1f}%")
                    
                    logger.info(f"Incoming transfers: {len(inc)}")
                    for tid, info in inc.items():
                        logger.info(f"  {tid[:8]}: {info['progress']:.1f}%")
                
                elif cmd[0] == "send" and len(cmd) >= 3:
                    peer_id = cmd[1]
                    file_path = " ".join(cmd[2:])
                    
                    if not Path(file_path).exists():
                        logger.error(f"File not found: {file_path}")
                        continue
                    
                    tid = node.send_large_file(peer_id, file_path)
                    if tid:
                        logger.info(f"Transfer started: {tid[:8]}")
                    else:
                        logger.error("Failed to start transfer")
                
                elif cmd[0] == "msg" and len(cmd) >= 3:
                    peer_id = cmd[1]
                    message = " ".join(cmd[2:])
                    
                    if node.send_message(peer_id, MessageType.DIRECT, message):
                        logger.info("Message sent")
                    else:
                        logger.error("Failed to send message")
                
                elif cmd[0] == "quit":
                    break
                
                else:
                    logger.warning("Unknown command")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error: {e}")
    
    finally:
        logger.info("Shutting down...")
        node.stop()
        logger.info("Goodbye!")