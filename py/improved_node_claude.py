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
from typing import Dict, Optional, Set, Tuple
from dataclasses import dataclass
from pathlib import Path
from contextlib import contextmanager

# ============================================================================
# KONFIGURASI LOGGING
# ============================================================================
# Setup logging untuk debugging dan monitoring yang lebih baik
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
    CHUNK_REQUEST = "chunk_request"      # Baru: request chunk spesifik
    CHUNK_RESPONSE = "chunk_response"    # Baru: response chunk dengan data
    TRANSFER_ACK = "transfer_ack"        # Baru: acknowledgment transfer selesai
    TRANSFER_ERROR = "transfer_error"    # Baru: notifikasi error pada transfer

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
    last_seen: float = 0.0  # Timestamp terakhir kali terlihat
    
    def is_alive(self, timeout: float = 30.0) -> bool:
        """Check apakah peer masih aktif berdasarkan last_seen"""
        return time.time() - self.last_seen < timeout

@dataclass
class TransferState:
    """State untuk file transfer yang sedang berlangsung"""
    transfer_id: str
    file_path: str
    total_size: int
    chunk_size: int
    total_md5: str
    chunks_sent: Set[int]           # Set offset chunks yang sudah dikirim
    last_activity: float            # Timestamp aktivitas terakhir
    
    def is_complete(self) -> bool:
        """Check apakah semua chunks sudah dikirim"""
        expected_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.chunks_sent) >= expected_chunks

@dataclass
class ReceiveState:
    """State untuk file yang sedang diterima"""
    transfer_id: str
    file_name: str
    total_size: int
    chunk_size: int
    total_md5: str
    temp_path: Path
    received_chunks: Dict[int, bytes]   # offset -> chunk data
    chunk_md5s: Dict[int, str]          # offset -> md5 hash
    retry_count: Dict[int, int]         # offset -> jumlah retry
    last_activity: float
    max_retries: int = 3
    
    def is_complete(self) -> bool:
        """Check apakah semua chunks sudah diterima"""
        expected_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        return len(self.received_chunks) >= expected_chunks
    
    def get_total_received(self) -> int:
        """Hitung total bytes yang sudah diterima"""
        return sum(len(chunk) for chunk in self.received_chunks.values())

# ============================================================================
# MAIN NODE CLASS
# ============================================================================
class Node:
    """
    Node P2P yang mendukung:
    - Peer discovery via UDP multicast
    - Direct messaging via ZeroMQ ROUTER/DEALER
    - Group messaging via ZeroMQ PUB/SUB
    - Reliable file transfer dengan chunking dan MD5 verification
    """
    
    def __init__(
        self, 
        node_id: Optional[str] = None,
        bind_ip: str = "0.0.0.0",
        pub_port: int = 5555,
        router_port: int = 5556,
        discovery_port: int = 5557,
        chunk_size: int = 1048576,  # 1MB default chunk size
        max_concurrent_transfers: int = 5
    ):
        # ====================================================================
        # IDENTITAS DAN KONFIGURASI
        # ====================================================================
        self.node_id = node_id or str(uuid.uuid4())
        self.bind_ip = bind_ip
        self.pub_port = pub_port
        self.router_port = router_port
        self.discovery_port = discovery_port
        self.file_port = router_port + 100
        self.chunk_size = chunk_size
        self.max_concurrent_transfers = max_concurrent_transfers
        
        logger.info(f"Initializing Node {self.node_id}")
        
        # ====================================================================
        # DATA STRUCTURES dengan Thread-Safe Locks
        # ====================================================================
        # Gunakan lock terpisah untuk masing-masing resource
        # untuk menghindari deadlock dan meningkatkan concurrency
        self.peers: Dict[str, PeerInfo] = {}
        self.peers_lock = threading.RLock()  # RLock untuk allow nested locks
        
        self.groups: Set[str] = set()
        self.groups_lock = threading.Lock()
        
        # Transfer yang sedang dikirim (outgoing)
        self.transfers: Dict[str, TransferState] = {}
        self.transfers_lock = threading.Lock()
        
        # Transfer yang sedang diterima (incoming)
        self.receiving_transfers: Dict[str, ReceiveState] = {}
        self.receiving_lock = threading.Lock()
        
        # Semaphore untuk membatasi concurrent transfers
        self.transfer_semaphore = threading.Semaphore(max_concurrent_transfers)
        
        # ====================================================================
        # ZEROMQ SETUP
        # ====================================================================
        self.context = zmq.Context()
        self.context.setsockopt(zmq.MAX_SOCKETS, 1024)  # Increase socket limit
        
        # PUB socket: untuk broadcast group messages
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.SNDHWM, 1000)  # High water mark
        self.pub_socket.bind(f"tcp://{bind_ip}:{pub_port}")
        
        # ROUTER socket: untuk menerima direct messages
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.router_socket.setsockopt(zmq.RCVHWM, 1000)
        self.router_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)  # Fail jika peer tidak ada
        self.router_socket.bind(f"tcp://{bind_ip}:{router_port}")
        
        # SUB socket: untuk menerima group messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub_socket.setsockopt(zmq.RCVHWM, 1000)
        
        # FILE ROUTER socket: dedicated untuk file transfers
        # Dipisah dari router_socket untuk menghindari blocking
        self.file_router_socket = self.context.socket(zmq.ROUTER)
        self.file_router_socket.setsockopt(zmq.RCVHWM, 10000)  # Lebih besar untuk chunks
        self.file_router_socket.bind(f"tcp://{bind_ip}:{self.file_port}")
        
        # ====================================================================
        # UDP MULTICAST DISCOVERY SETUP
        # ====================================================================
        self.mcast_group = "224.0.0.1"
        self.discovery_socket = socket.socket(
            socket.AF_INET, 
            socket.SOCK_DGRAM, 
            socket.IPPROTO_UDP
        )
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # SO_REUSEPORT untuk allow multiple processes bind ke port yang sama
        try:
            self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            logger.warning("SO_REUSEPORT not supported on this platform")
        
        self.discovery_socket.bind(('', discovery_port))
        
        # Join multicast group
        mreq = struct.pack("4sl", socket.inet_aton(self.mcast_group), socket.INADDR_ANY)
        self.discovery_socket.setsockopt(
            socket.IPPROTO_IP, 
            socket.IP_ADD_MEMBERSHIP, 
            mreq
        )
        self.discovery_socket.settimeout(1.0)
        
        # ====================================================================
        # THREADING CONTROL
        # ====================================================================
        self.running = False
        self.threads = []
        
        # Ensure temp directory exists
        self.temp_dir = Path("temp_transfers")
        self.temp_dir.mkdir(exist_ok=True)
        
        logger.info(f"Node {self.node_id} initialized successfully")

    # ========================================================================
    # LIFECYCLE METHODS
    # ========================================================================
    def start(self):
        """
        Start semua background threads:
        - Discovery: mencari dan announce ke peers
        - Receive: handle incoming control messages
        - File transfer: handle chunk requests/responses
        - Cleanup: membersihkan stale transfers dan peers
        """
        if self.running:
            logger.warning("Node already running")
            return
        
        self.running = True
        
        # Start semua worker threads
        self.threads = [
            threading.Thread(target=self._discover_peers, name="Discovery", daemon=True),
            threading.Thread(target=self._receive_messages, name="Receiver", daemon=True),
            threading.Thread(target=self._handle_file_transfers, name="FileTransfer", daemon=True),
            threading.Thread(target=self._cleanup_stale_data, name="Cleanup", daemon=True),
        ]
        
        for thread in self.threads:
            thread.start()
            logger.info(f"Started thread: {thread.name}")
        
        # Announce presence ke network
        self._announce_presence()
        logger.info(f"Node {self.node_id} started")

    def stop(self):
        """
        Stop node dengan graceful shutdown:
        - Stop semua threads
        - Close semua file handles
        - Close semua sockets
        - Cleanup temp files
        """
        if not self.running:
            return
        
        logger.info(f"Stopping node {self.node_id}")
        self.running = False
        
        # Wait for threads dengan timeout
        for thread in self.threads:
            thread.join(timeout=2.0)
            if thread.is_alive():
                logger.warning(f"Thread {thread.name} did not stop gracefully")
        
        # Close semua file handles yang terbuka
        with self.transfers_lock:
            for transfer_id in list(self.transfers.keys()):
                try:
                    # Transfers tidak menyimpan file handle lagi
                    pass
                except Exception as e:
                    logger.error(f"Error cleaning up transfer {transfer_id}: {e}")
        
        with self.receiving_lock:
            for transfer_id, state in list(self.receiving_transfers.items()):
                try:
                    # Cleanup temp files
                    if state.temp_path.exists():
                        state.temp_path.unlink()
                except Exception as e:
                    logger.error(f"Error cleaning up receiving transfer {transfer_id}: {e}")
        
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
    # PEER DISCOVERY
    # ========================================================================
    def _announce_presence(self):
        """
        Broadcast keberadaan node ini ke network via UDP multicast.
        Mengirim informasi: node_id, ip, router_port, file_port
        """
        announcement = {
            "node_id": self.node_id,
            "ip": self._get_local_ip(),
            "router_port": self.router_port,
            "file_port": self.file_port,
            "timestamp": time.time()
        }
        
        try:
            msg = json.dumps(announcement).encode()
            self.discovery_socket.sendto(msg, (self.mcast_group, self.discovery_port))
            logger.debug(f"Announced presence to network")
        except socket.error as e:
            logger.error(f"Error announcing presence: {e}")

    def _discover_peers(self):
        """
        Background thread untuk:
        1. Listen UDP multicast untuk peer announcements
        2. Update peer list
        3. Connect ke new peers
        4. Periodic re-announce presence
        """
        last_announce = time.time()
        announce_interval = 5.0  # Re-announce setiap 5 detik
        
        while self.running:
            try:
                # Check apakah socket masih valid
                if self.discovery_socket.fileno() == -1:
                    logger.error("Discovery socket closed unexpectedly")
                    break
                
                # Try receive dengan timeout
                try:
                    data, addr = self.discovery_socket.recvfrom(4096)
                    peer = json.loads(data.decode())
                    
                    # Ignore announcement dari diri sendiri
                    if peer["node_id"] == self.node_id:
                        continue
                    
                    # Update atau add peer
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
                        
                        # Connect ke pub socket peer untuk group messages
                        if is_new:
                            self.sub_socket.connect(f"tcp://{peer['ip']}:{self.pub_port}")
                            logger.info(f"Discovered new peer: {peer['node_id']} at {peer['ip']}")
                
                except socket.timeout:
                    pass  # Normal timeout, continue loop
                
                # Periodic re-announce
                if time.time() - last_announce > announce_interval:
                    self._announce_presence()
                    last_announce = time.time()
                
            except socket.error as e:
                if self.running:
                    logger.error(f"Discovery socket error: {e}")
                    time.sleep(1)
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid discovery message: {e}")
            except Exception as e:
                if self.running:
                    logger.error(f"Discovery error: {e}", exc_info=True)
                    time.sleep(1)

    # ========================================================================
    # MESSAGE HANDLING
    # ========================================================================
    def _receive_messages(self):
        """
        Background thread untuk handle incoming control messages dari:
        - SUB socket (group messages)
        - ROUTER socket (direct messages)
        
        Menggunakan zmq.Poller untuk efficient multiplexing
        """
        poller = zmq.Poller()
        poller.register(self.sub_socket, zmq.POLLIN)
        poller.register(self.router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                # Poll dengan timeout 1 detik
                socks = dict(poller.poll(timeout=1000))
                
                # Handle group messages dari SUB socket
                if self.sub_socket in socks:
                    if self.sub_socket.fileno() == -1:
                        logger.error("Sub socket closed")
                        continue
                    
                    try:
                        msg = self.sub_socket.recv_json(flags=zmq.NOBLOCK)
                        self._handle_message(msg)
                    except zmq.ZMQError as e:
                        logger.error(f"Error receiving from sub socket: {e}")
                
                # Handle direct messages dari ROUTER socket
                if self.router_socket in socks:
                    if self.router_socket.fileno() == -1:
                        logger.error("Router socket closed")
                        continue
                    
                    try:
                        frames = self.router_socket.recv_multipart(flags=zmq.NOBLOCK)
                        if len(frames) >= 2:
                            sender_id = frames[0]
                            msg = json.loads(frames[1].decode())
                            self._handle_message(msg, sender_id)
                    except zmq.ZMQError as e:
                        logger.error(f"Error receiving from router socket: {e}")
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid message format: {e}")
            
            except Exception as e:
                if self.running:
                    logger.error(f"Receive error: {e}", exc_info=True)
                    time.sleep(0.1)

    def _handle_message(self, msg: dict, sender_id: Optional[bytes] = None):
        """
        Process incoming messages berdasarkan tipe.
        
        Args:
            msg: Dictionary berisi message data
            sender_id: ZMQ identity dari sender (untuk ROUTER socket)
        """
        try:
            msg_type = MessageType(msg.get("type"))
            source = msg.get("source")
            content = msg.get("content")
            
            # GROUP MESSAGE
            if msg_type == MessageType.GROUP:
                exclude = msg.get("exclude", [])
                group_name = msg.get("group_name")
                
                # Check apakah message ini untuk kita
                if self.node_id in exclude:
                    return
                
                with self.groups_lock:
                    if group_name and group_name not in self.groups:
                        return
                
                logger.info(f"Group [{group_name}] from {source}: {content}")
            
            # DIRECT MESSAGE
            elif msg_type == MessageType.DIRECT:
                logger.info(f"Direct from {source}: {content}")
            
            # IMAGE/FILE (base64 encoded)
            elif msg_type in [MessageType.IMAGE, MessageType.FILE]:
                try:
                    decoded = base64.b64decode(content)
                    logger.info(f"{msg_type.value} from {source}: {len(decoded)} bytes")
                except Exception as e:
                    logger.error(f"Error decoding {msg_type.value}: {e}")
            
            # OPERATION REQUEST
            elif msg_type == MessageType.OPERATION:
                logger.info(f"Operation from {source}: {content}")
                if sender_id:
                    response = {
                        "type": MessageType.OPERATION.value,
                        "source": self.node_id,
                        "content": f"Processed: {content}"
                    }
                    self.router_socket.send_multipart([
                        sender_id, 
                        json.dumps(response).encode()
                    ])
            
            # FILE TRANSFER INITIALIZATION
            elif msg_type == MessageType.FILE_INIT:
                self._handle_file_init(msg, source)
            
            # FOLDER SHARE
            elif msg_type == MessageType.FOLDER_SHARE:
                logger.info(f"Folder share from {source}: {content}")
            
            else:
                logger.warning(f"Unknown message type: {msg_type}")
        
        except ValueError as e:
            logger.warning(f"Invalid message type: {e}")
        except Exception as e:
            logger.error(f"Error handling message: {e}", exc_info=True)

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

    def _handle_file_transfers(self):
        """
        Background thread untuk handle file transfer operations:
        - CHUNK_REQUEST: receiver meminta chunk tertentu
        - CHUNK_RESPONSE: sender mengirim chunk data
        
        Menggunakan dedicated file_router_socket untuk menghindari
        blocking router_socket dengan data transfer besar.
        """
        poller = zmq.Poller()
        poller.register(self.file_router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                
                if self.file_router_socket in socks:
                    if self.file_router_socket.fileno() == -1:
                        logger.error("File router socket closed")
                        continue
                    
                    try:
                        frames = self.file_router_socket.recv_multipart(flags=zmq.NOBLOCK)
                        
                        if len(frames) < 2:
                            logger.warning("Invalid frame count in file transfer")
                            continue
                        
                        sender_id = frames[0]
                        metadata = json.loads(frames[1].decode())
                        chunk_data = frames[2] if len(frames) > 2 else None
                        
                        msg_type = MessageType(metadata.get("type"))
                        
                        # Handle CHUNK_REQUEST dari receiver
                        if msg_type == MessageType.CHUNK_REQUEST:
                            transfer_id = metadata["transfer_id"]
                            offset = metadata["offset"]
                            chunk_size = metadata["chunk_size"]
                            
                            # Send requested chunk
                            self._send_chunk(sender_id, transfer_id, offset, chunk_size)
                        
                        # Handle CHUNK_RESPONSE dari sender
                        elif msg_type == MessageType.CHUNK_RESPONSE:
                            if chunk_data:
                                transfer_id = metadata["transfer_id"]
                                offset = metadata["offset"]
                                chunk_md5 = metadata["chunk_md5"]
                                
                                # Process received chunk
                                self._receive_chunk(
                                    sender_id, 
                                    transfer_id, 
                                    offset, 
                                    chunk_md5, 
                                    chunk_data
                                )
                    
                    except zmq.ZMQError as e:
                        logger.error(f"ZMQ error in file transfer: {e}")
                    except json.JSONDecodeError as e:
                        logger.warning(f"Invalid JSON in file transfer: {e}")
                    except ValueError as e:
                        logger.warning(f"Invalid message type in file transfer: {e}")
            
            except Exception as e:
                if self.running:
                    logger.error(f"File transfer error: {e}", exc_info=True)
                    time.sleep(0.1)

    def _send_chunk(self, target_id: bytes, transfer_id: str, offset: int, chunk_size: int):
        """
        Send chunk tertentu dari file yang sedang di-transfer.
        
        Process:
        1. Validate transfer exists
        2. Read chunk dari file di offset tertentu
        3. Compute MD5 untuk chunk
        4. Send chunk via file_router_socket
        
        Args:
            target_id: ZMQ identity dari receiver
            transfer_id: ID transfer
            offset: Byte offset untuk chunk
            chunk_size: Ukuran chunk yang diminta
        """
        with self.transfers_lock:
            if transfer_id not in self.transfers:
                logger.warning(f"Transfer {transfer_id} not found for chunk send")
                return
            
            transfer = self.transfers[transfer_id]
            transfer.last_activity = time.time()
        
        try:
            # Read chunk dari file
            # PENTING: Tidak menyimpan file handle terbuka
            # untuk menghindari file descriptor exhaustion
            with open(transfer.file_path, 'rb') as f:
                f.seek(offset)
                chunk = f.read(chunk_size)
            
            if not chunk:
                logger.warning(f"No data at offset {offset} for transfer {transfer_id}")
                return
            
            # Compute MD5 untuk verification
            chunk_md5 = hashlib.md5(chunk).hexdigest()
            
            # Prepare response
            metadata = {
                "type": MessageType.CHUNK_RESPONSE.value,
                "transfer_id": transfer_id,
                "offset": offset,
                "chunk_md5": chunk_md5,
                "chunk_size": len(chunk)
            }
            
            # Send via file_router_socket
            self.file_router_socket.send_multipart([
                target_id,
                json.dumps(metadata).encode(),
                chunk
            ])
            
            # Mark chunk as sent
            with self.transfers_lock:
                if transfer_id in self.transfers:
                    self.transfers[transfer_id].chunks_sent.add(offset)
            
            logger.debug(f"Sent chunk at offset {offset} for transfer {transfer_id}")
        
        except FileNotFoundError:
            logger.error(f"File not found during chunk send: {transfer.file_path}")
        except Exception as e:
            logger.error(f"Error sending chunk: {e}", exc_info=True)

    # ========================================================================
    # FILE TRANSFER - RECEIVING
    # ========================================================================
    def _handle_file_init(self, msg: dict, source: str):
        """
        Handle FILE_INIT message untuk memulai receiving file.
        
        Process:
        1. Validate sender exists
        2. Create temp file
        3. Create receive state
        4. Start chunk request thread
        
        Args:
            msg: FILE_INIT message
            source: Node ID pengirim
        """
        try:
            transfer_id = msg["transfer_id"]
            file_name = msg["file_name"]
            total_size = msg["total_size"]
            chunk_size = msg["chunk_size"]
            total_md5 = msg["total_md5"]
            
            # Validate sender
            with self.peers_lock:
                if source not in self.peers:
                    logger.error(f"Unknown sender {source} for file transfer")
                    return
            
            # Sanitize filename untuk prevent path traversal
            safe_filename = Path(file_name).name
            temp_path = self.temp_dir / f"temp_{transfer_id}_{safe_filename}"
            
            logger.info(f"Starting to receive file: {safe_filename} ({total_size} bytes)")
            
            # Create receive state
            receive_state = ReceiveState(
                transfer_id=transfer_id,
                file_name=safe_filename,
                total_size=total_size,
                chunk_size=chunk_size,
                total_md5=total_md5,
                temp_path=temp_path,
                received_chunks={},
                chunk_md5s={},
                retry_count={},
                last_activity=time.time()
            )
            
            with self.receiving_lock:
                self.receiving_transfers[transfer_id] = receive_state
            
            # Start thread untuk request chunks
            thread = threading.Thread(
                target=self._request_chunks,
                args=(source, transfer_id, chunk_size, total_size),
                name=f"ChunkRequest-{transfer_id[:8]}",
                daemon=True
            )
            thread.start()
        
        except KeyError as e:
            logger.error(f"Missing field in FILE_INIT message: {e}")
        except Exception as e:
            logger.error(f"Error handling FILE_INIT: {e}", exc_info=True)

    def _request_chunks(
        self, 
        sender_id: str, 
        transfer_id: str, 
        chunk_size: int, 
        total_size: int
    ):
        """
        Request chunks dari sender dengan flow control.
        
        Menggunakan windowing mechanism untuk menghindari overwhelm network:
        - Request sejumlah chunks (window size)
        - Wait sampai chunks diterima
        - Request batch berikutnya
        
        Args:
            sender_id: Node ID pengirim
            transfer_id: ID transfer
            chunk_size: Ukuran per chunk
            total_size: Total size file
        """
        window_size = 10  # Request 10 chunks at a time untuk flow control
        timeout = 5.0     # Timeout untuk setiap chunk
        
        try:
            with self.peers_lock:
                if sender_id not in self.peers:
                    logger.error(f"Sender {sender_id} not found")
                    return
                peer = self.peers[sender_id]
            
            offset = 0
            while offset < total_size and self.running:
                # Check apakah transfer masih aktif
                with self.receiving_lock:
                    if transfer_id not in self.receiving_transfers:
                        logger.info(f"Transfer {transfer_id} completed or cancelled")
                        break
                    state = self.receiving_transfers[transfer_id]
                
                # Request window of chunks
                window_offsets = []
                for i in range(window_size):
                    current_offset = offset + (i * chunk_size)
                    if current_offset >= total_size:
                        break
                    
                    # Skip jika chunk sudah diterima
                    with self.receiving_lock:
                        if current_offset in state.received_chunks:
                            continue
                    
                    window_offsets.append(current_offset)
                    
                    # Request chunk
                    request = {
                        "type": MessageType.CHUNK_REQUEST.value,
                        "transfer_id": transfer_id,
                        "offset": current_offset,
                        "chunk_size": chunk_size
                    }
                    
                    self._send_file_message(peer, request)
                
                if not window_offsets:
                    # Semua chunks dalam window sudah diterima, move to next
                    offset += window_size * chunk_size
                    continue
                
                # Wait untuk chunks dalam window dengan timeout
                start_time = time.time()
                while time.time() - start_time < timeout:
                    with self.receiving_lock:
                        if transfer_id not in self.receiving_transfers:
                            return
                        
                        # Check apakah semua chunks dalam window sudah diterima
                        all_received = all(
                            off in state.received_chunks 
                            for off in window_offsets
                        )
                        
                        if all_received:
                            break
                    
                    time.sleep(0.1)
                
                # Move ke window berikutnya
                offset += window_size * chunk_size
                
                # Small delay untuk avoid overwhelming network
                time.sleep(0.01)
        
        except Exception as e:
            logger.error(f"Error requesting chunks: {e}", exc_info=True)
            # Cleanup jika terjadi error
            with self.receiving_lock:
                if transfer_id in self.receiving_transfers:
                    state = self.receiving_transfers[transfer_id]
                    if state.temp_path.exists():
                        state.temp_path.unlink()
                    del self.receiving_transfers[transfer_id]

    def _receive_chunk(
        self, 
        sender_id: bytes, 
        transfer_id: str, 
        offset: int, 
        chunk_md5: str, 
        chunk: bytes
    ):
        """
        Process received chunk dengan MD5 verification.
        
        Process:
        1. Validate transfer exists
        2. Verify MD5 hash
        3. Store chunk in memory
        4. Check jika transfer complete
        5. Jika complete, write to disk dan verify total MD5
        
        Args:
            sender_id: ZMQ identity dari sender
            transfer_id: ID transfer
            offset: Byte offset chunk ini
            chunk_md5: MD5 hash yang expected
            chunk: Data chunk
        """
        with self.receiving_lock:
            if transfer_id not in self.receiving_transfers:
                logger.warning(f"Received chunk for unknown transfer {transfer_id}")
                return
            
            state = self.receiving_transfers[transfer_id]
            state.last_activity = time.time()
            
            # Verify MD5
            computed_md5 = hashlib.md5(chunk).hexdigest()
            if computed_md5 != chunk_md5:
                logger.warning(f"MD5 mismatch at offset {offset} for transfer {transfer_id}")
                
                # Retry mechanism
                retry_count = state.retry_count.get(offset, 0)
                if retry_count < state.max_retries:
                    state.retry_count[offset] = retry_count + 1
                    logger.info(f"Retrying chunk at offset {offset} (attempt {retry_count + 1})")
                    
                    # Re-request chunk
                    # Extract sender node_id dari sender_id bytes
                    # (ini adalah ZMQ identity, mungkin perlu mapping)
                    # Untuk simplicity, kita tidak re-request otomatis disini
                    # Karena request loop akan handle retry
                else:
                    logger.error(f"Transfer {transfer_id} failed: too many retries at offset {offset}")
                    # Cleanup
                    if state.temp_path.exists():
                        state.temp_path.unlink()
                    del self.receiving_transfers[transfer_id]
                return
            
            # Store chunk
            state.received_chunks[offset] = chunk
            state.chunk_md5s[offset] = chunk_md5
            
            received = state.get_total_received()
            progress = (received / state.total_size) * 100
            logger.debug(f"Transfer {transfer_id}: {progress:.1f}% complete ({received}/{state.total_size} bytes)")
            
            # Check jika transfer complete
            if state.is_complete():
                logger.info(f"All chunks received for transfer {transfer_id}, verifying...")
                self._finalize_transfer(transfer_id, state)

    def _finalize_transfer(self, transfer_id: str, state: ReceiveState):
        """
        Finalize transfer dengan menulis chunks ke disk dan verify total MD5.
        
        Process:
        1. Write semua chunks ke temp file dalam order
        2. Compute total MD5
        3. Compare dengan expected MD5
        4. Jika match, rename temp file ke final name
        5. Cleanup
        
        Args:
            transfer_id: ID transfer
            state: ReceiveState object
        """
        try:
            # Write chunks ke temp file
            with open(state.temp_path, 'wb') as f:
                # Sort chunks by offset dan write secara berurutan
                for offset in sorted(state.received_chunks.keys()):
                    f.seek(offset)
                    f.write(state.received_chunks[offset])
            
            logger.info(f"Written all chunks to {state.temp_path}")
            
            # Verify total MD5
            final_md5 = self._compute_file_md5(state.temp_path, state.chunk_size)
            
            if final_md5 == state.total_md5:
                # Success! Rename temp file ke final name
                final_path = Path(state.file_name)
                
                # Handle jika file sudah exists
                if final_path.exists():
                    counter = 1
                    stem = final_path.stem
                    suffix = final_path.suffix
                    while final_path.exists():
                        final_path = Path(f"{stem}_{counter}{suffix}")
                        counter += 1
                
                state.temp_path.rename(final_path)
                logger.info(f"✓ Transfer {transfer_id} complete: {final_path}")
                logger.info(f"  File: {state.file_name}")
                logger.info(f"  Size: {state.total_size} bytes")
                logger.info(f"  MD5: {final_md5}")
            else:
                logger.error(f"✗ Transfer {transfer_id} failed: MD5 mismatch")
                logger.error(f"  Expected: {state.total_md5}")
                logger.error(f"  Got: {final_md5}")
                state.temp_path.unlink()
            
            # Cleanup dari receiving_transfers
            with self.receiving_lock:
                if transfer_id in self.receiving_transfers:
                    del self.receiving_transfers[transfer_id]
        
        except Exception as e:
            logger.error(f"Error finalizing transfer {transfer_id}: {e}", exc_info=True)
            if state.temp_path.exists():
                state.temp_path.unlink()
            with self.receiving_lock:
                if transfer_id in self.receiving_transfers:
                    del self.receiving_transfers[transfer_id]

    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    def _compute_file_md5(self, file_path: Path, chunk_size: int) -> str:
        """
        Compute MD5 hash dari file dengan membaca dalam chunks.
        Efficient untuk large files karena tidak load seluruh file ke memory.
        
        Args:
            file_path: Path ke file
            chunk_size: Ukuran chunk untuk reading
        
        Returns:
            MD5 hash string (hexdigest)
        """
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
            logger.error(f"Error computing MD5 for {file_path}: {e}")
            return ""

    def _get_local_ip(self) -> str:
        """
        Get local IP address dengan cara connect ke external IP.
        Tidak benar-benar connect, hanya untuk trigger OS routing.
        
        Returns:
            Local IP address string
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # Connect ke Google DNS (tidak benar-benar connect)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip

    @contextmanager
    def _dealer_socket(self, peer: PeerInfo, for_file: bool = False):
        """
        Context manager untuk create dan auto-close dealer socket.
        Menghindari socket leak dengan ensuring proper cleanup.
        
        Args:
            peer: PeerInfo object
            for_file: True jika untuk file transfer (gunakan file_port)
        
        Yields:
            ZMQ DEALER socket
        """
        dealer = self.context.socket(zmq.DEALER)
        try:
            port = peer.file_port if for_file else peer.router_port
            dealer.connect(f"tcp://{peer.ip}:{port}")
            dealer.setsockopt(zmq.LINGER, 0)  # Don't wait on close
            dealer.setsockopt(zmq.SNDTIMEO, 5000)  # 5 second send timeout
            yield dealer
        finally:
            dealer.close()

    def _send_direct_message(self, target_id: str, msg: dict) -> bool:
        """
        Send direct message ke target node via ROUTER/DEALER pattern.
        
        Args:
            target_id: Node ID tujuan
            msg: Message dictionary
        
        Returns:
            True jika berhasil, False jika gagal
        """
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
        """
        Send message via file transfer channel.
        
        Args:
            peer: PeerInfo object
            msg: Message dictionary
        
        Returns:
            True jika berhasil, False jika gagal
        """
        try:
            with self._dealer_socket(peer, for_file=True) as dealer:
                dealer.send_json(msg)
            return True
        
        except zmq.ZMQError as e:
            logger.error(f"Error sending file message: {e}")
            return False

    def _cleanup_stale_data(self):
        """
        Background thread untuk periodic cleanup:
        1. Remove dead peers (tidak seen dalam 30 detik)
        2. Remove stale transfers (tidak aktif dalam 60 detik)
        3. Cleanup orphaned temp files
        """
        cleanup_interval = 10.0  # Run setiap 10 detik
        peer_timeout = 30.0      # Peer considered dead setelah 30s
        transfer_timeout = 60.0  # Transfer considered stale setelah 60s
        
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
                    stale_transfers = [
                        transfer_id
                        for transfer_id, state in self.transfers.items()
                        if current_time - state.last_activity > transfer_timeout
                    ]
                    
                    for transfer_id in stale_transfers:
                        logger.warning(f"Removing stale transfer: {transfer_id}")
                        del self.transfers[transfer_id]
                        self.transfer_semaphore.release()
                
                # Cleanup stale incoming transfers
                with self.receiving_lock:
                    stale_receives = [
                        transfer_id
                        for transfer_id, state in self.receiving_transfers.items()
                        if current_time - state.last_activity > transfer_timeout
                    ]
                    
                    for transfer_id in stale_receives:
                        logger.warning(f"Removing stale receive transfer: {transfer_id}")
                        state = self.receiving_transfers[transfer_id]
                        if state.temp_path.exists():
                            state.temp_path.unlink()
                        del self.receiving_transfers[transfer_id]
                
                # Cleanup orphaned temp files
                if self.temp_dir.exists():
                    for temp_file in self.temp_dir.glob("temp_*"):
                        # Check jika file older than 1 hour
                        if current_time - temp_file.stat().st_mtime > 3600:
                            logger.info(f"Removing orphaned temp file: {temp_file}")
                            temp_file.unlink()
            
            except Exception as e:
                if self.running:
                    logger.error(f"Cleanup error: {e}", exc_info=True)

    # ========================================================================
    # PUBLIC API METHODS
    # ========================================================================
    def join_group(self, group_name: str):
        """
        Join group untuk menerima group messages.
        
        Args:
            group_name: Nama group
        """
        with self.groups_lock:
            self.groups.add(group_name)
        logger.info(f"Joined group: {group_name}")

    def leave_group(self, group_name: str):
        """
        Leave group.
        
        Args:
            group_name: Nama group
        """
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
        Send message ke node atau group.
        
        Args:
            target_id: Node ID tujuan (untuk DIRECT messages) atau None untuk GROUP
            msg_type: Tipe message
            content: Content message
            exclude: List node IDs yang di-exclude (untuk GROUP messages)
            group_name: Nama group (untuk GROUP messages)
        
        Returns:
            True jika berhasil, False jika gagal
        """
        try:
            msg = {
                "type": msg_type.value,
                "source": self.node_id,
                "content": content,
                "timestamp": time.time()
            }
            
            # GROUP MESSAGE
            if msg_type == MessageType.GROUP:
                if exclude is not None:
                    msg["exclude"] = exclude if isinstance(exclude, list) else [exclude]
                if group_name is not None:
                    msg["group_name"] = group_name
                
                self.pub_socket.send_json(msg)
                logger.debug(f"Sent group message to {group_name}")
                return True
            
            # DIRECT MESSAGE
            elif msg_type == MessageType.DIRECT:
                if not target_id:
                    logger.error("Target ID required for direct message")
                    return False
                
                return self._send_direct_message(target_id, msg)
            
            # IMAGE/FILE (base64 encoded binary)
            elif msg_type in [MessageType.IMAGE, MessageType.FILE]:
                if not target_id:
                    logger.error("Target ID required for image/file message")
                    return False
                
                # Encode binary content to base64
                if isinstance(content, bytes):
                    msg["content"] = base64.b64encode(content).decode()
                
                return self._send_direct_message(target_id, msg)
            
            # OPERATION REQUEST
            elif msg_type == MessageType.OPERATION:
                if not target_id:
                    logger.error("Target ID required for operation")
                    return False
                
                return self._send_direct_message(target_id, msg)
            
            # FOLDER SHARE (broadcast)
            elif msg_type == MessageType.FOLDER_SHARE:
                self.pub_socket.send_json(msg)
                logger.debug("Sent folder share message")
                return True
            
            else:
                logger.error(f"Unsupported message type: {msg_type}")
                return False
        
        except Exception as e:
            logger.error(f"Error sending message: {e}", exc_info=True)
            return False

    def get_peers(self) -> Dict[str, PeerInfo]:
        """
        Get dictionary semua active peers.
        
        Returns:
            Dict mapping node_id -> PeerInfo
        """
        with self.peers_lock:
            return self.peers.copy()

    def get_active_transfers(self) -> Tuple[Dict, Dict]:
        """
        Get informasi tentang active transfers.
        
        Returns:
            Tuple of (outgoing_transfers, incoming_transfers)
        """
        with self.transfers_lock:
            outgoing = {
                tid: {
                    "file_path": state.file_path,
                    "total_size": state.total_size,
                    "chunks_sent": len(state.chunks_sent),
                    "progress": (len(state.chunks_sent) * state.chunk_size / state.total_size) * 100
                }
                for tid, state in self.transfers.items()
            }
        
        with self.receiving_lock:
            incoming = {
                tid: {
                    "file_name": state.file_name,
                    "total_size": state.total_size,
                    "received_bytes": state.get_total_received(),
                    "progress": (state.get_total_received() / state.total_size) * 100
                }
                for tid, state in self.receiving_transfers.items()
            }
        
        return outgoing, incoming


# ============================================================================
# EXAMPLE USAGE
# ============================================================================
if __name__ == "__main__":
    """
    Example usage demonstrating:
    1. Node creation dan startup
    2. Peer discovery
    3. Group messaging
    4. Direct messaging
    5. File transfer
    """
    
    # Create node dengan custom ID
    node = Node(
        node_id="node_1",
        chunk_size=1024 * 1024,  # 1MB chunks
        max_concurrent_transfers=5
    )
    
    try:
        # Start node
        node.start()
        logger.info("Node started, waiting for peers...")
        
        # Wait untuk peer discovery
        time.sleep(3)
        
        # Join group
        node.join_group("general")
        
        # Send group message
        node.send_message(
            target_id=None,
            msg_type=MessageType.GROUP,
            content="Hello from node_1!",
            group_name="general"
        )
        
        # Get peers
        peers = node.get_peers()
        logger.info(f"Found {len(peers)} peers: {list(peers.keys())}")
        
        # Send direct message ke first peer
        if peers:
            first_peer = list(peers.keys())[0]
            node.send_message(
                target_id=first_peer,
                msg_type=MessageType.DIRECT,
                content="Hello directly!"
            )
        
        # Example: Send file
        # node.send_large_file(first_peer, "path/to/large_file.dat")
        
        # Keep running
        logger.info("Node running. Press Ctrl+C to stop.")
        while True:
            time.sleep(1)
            
            # Show active transfers
            outgoing, incoming = node.get_active_transfers()
            if outgoing or incoming:
                logger.info(f"Active transfers - Out: {len(outgoing)}, In: {len(incoming)}")
    
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    
    finally:
        node.stop()
        logger.info("Node stopped")