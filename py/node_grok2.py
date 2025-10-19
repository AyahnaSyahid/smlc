import zmq
import json
import threading
import socket
import struct
import uuid
import time
import hashlib
import base64
import sys
import os
from enum import Enum

class MessageType(Enum):
    DIRECT = "direct"
    GROUP = "group"
    IMAGE = "image"
    FILE = "file"
    OPERATION = "operation"
    FOLDER_SHARE = "folder_share"
    FILE_INIT = "file_init"

class Node:
    def __init__(self, node_id, bind_ip="0.0.0.0", pub_port=5555, router_port=5556, discovery_port=5557):
        self.node_id = node_id or str(uuid.uuid4())
        self.bind_ip = bind_ip
        self.pub_port = pub_port
        self.router_port = router_port
        self.discovery_port = discovery_port
        self.file_port = router_port + 100  # Dedicated port for file transfers
        self.peers = {}  # {node_id: (ip, router_port, file_port)}
        self.groups = set()
        self.transfers = {}  # {transfer_id: {file_path, file_handle, total_md5, chunk_size, total_size}}
        self.receiving_transfers = {}  # {transfer_id: {temp_file, file_name, total_md5, chunk_size, total_size, received_bytes, retries}}
        self.transfer_lock = threading.Lock()
        self.context = zmq.Context()

        # Pub socket for group messages
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{bind_ip}:{pub_port}")

        # Router socket for direct messages/requests
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.router_socket.bind(f"tcp://{bind_ip}:{router_port}")

        # Sub socket for receiving group messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

        # File router socket for file transfers
        self.file_router_socket = self.context.socket(zmq.ROUTER)
        self.file_router_socket.bind(f"tcp://{bind_ip}:{self.file_port}")

        # Discovery socket (UDP multicast)
        self.mcast_group = "224.0.0.1"
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self.discovery_socket.bind(('', discovery_port))
        mreq = struct.pack("4sl", socket.inet_aton(self.mcast_group), socket.INADDR_ANY)
        self.discovery_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.discovery_socket.settimeout(1.0)

        # Threads for async operation
        self.running = True
        self.discovery_thread = threading.Thread(target=self._discover_peers)
        self.receive_thread = threading.Thread(target=self._receive_messages)
        self.file_transfer_thread = threading.Thread(target=self._handle_file_transfers)

    def start(self):
        """Start the node, initiating discovery and message handling."""
        self.discovery_thread.start()
        self.receive_thread.start()
        self.file_transfer_thread.start()
        self._announce_presence()

    def stop(self):
        """Stop the node and clean up."""
        self.running = False
        try:
            self.discovery_socket.close()
        except Exception as e:
            print(f"Error closing discovery socket: {e}")
        self.discovery_thread.join(timeout=2.0)
        self.receive_thread.join(timeout=2.0)
        self.file_transfer_thread.join(timeout=2.0)
        with self.transfer_lock:
            for transfer in self.transfers.values():
                transfer["file_handle"].close()
            for transfer in self.receiving_transfers.values():
                transfer["temp_file"].close()
        try:
            self.pub_socket.close()
            self.router_socket.close()
            self.sub_socket.close()
            self.file_router_socket.close()
            self.context.term()
        except Exception as e:
            print(f"Error closing ZMQ sockets: {e}")

    def _announce_presence(self):
        """Announce this node to others via multicast."""
        msg = json.dumps({
            "node_id": self.node_id,
            "ip": self._get_local_ip(),
            "router_port": self.router_port,
            "file_port": self.file_port
        }).encode()
        try:
            self.discovery_socket.sendto(msg, (self.mcast_group, self.discovery_port))
        except socket.error as e:
            print(f"Error announcing presence: {e}")

    def _discover_peers(self):
        """Listen for peer announcements and connect to them."""
        while self.running:
            try:
                if self.discovery_socket.fileno() == -1:
                    print("Discovery socket closed, exiting discovery thread")
                    break
                data, addr = self.discovery_socket.recvfrom(1024)
                peer = json.loads(data.decode())
                if peer["node_id"] != self.node_id:
                    self.peers[peer["node_id"]] = (peer["ip"], peer["router_port"], peer.get("file_port", peer["router_port"] + 100))
                    self.sub_socket.connect(f"tcp://{peer['ip']}:{self.pub_port}")
            except socket.timeout:
                if self.running:
                    self._announce_presence()
            except socket.error as e:
                if self.running:
                    print(f"Discovery socket error: {e}")
            except Exception as e:
                if self.running:
                    print(f"Discovery error: {e}")

    def _receive_messages(self):
        """Handle incoming control messages (group, direct, file init)."""
        poller = zmq.Poller()
        poller.register(self.sub_socket, zmq.POLLIN)
        poller.register(self.router_socket, zmq.POLLIN)

        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                if self.sub_socket in socks:
                    if self.sub_socket.fileno() == -1:
                        print("Sub socket closed, skipping")
                        continue
                    msg = self.sub_socket.recv_json()
                    self._handle_message(msg)

                if self.router_socket in socks:
                    if self.router_socket.fileno() == -1:
                        print("Router socket closed, skipping")
                        continue
                    frames = self.router_socket.recv_multipart()
                    sender_id, msg = frames[0], json.loads(frames[1].decode())
                    self._handle_message(msg, sender_id)
            except Exception as e:
                if self.running:
                    print(f"Receive error: {e}")

    def _handle_file_transfers(self):
        """Handle file transfer requests and responses on file_router_socket."""
        poller = zmq.Poller()
        poller.register(self.file_router_socket, zmq.POLLIN)

        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                if self.file_router_socket in socks:
                    if self.file_router_socket.fileno() == -1:
                        print("File router socket closed, skipping")
                        continue
                    frames = self.file_router_socket.recv_multipart()
                    sender_id, metadata, chunk = frames[0], json.loads(frames[1].decode()), frames[2] if len(frames) > 2 else None
                    if metadata.get("action") == "fetch_chunk":
                        self._send_chunk(sender_id, metadata["transfer_id"], metadata["offset"], metadata["chunk_size"])
                    elif metadata.get("action") == "chunk_response" and chunk:
                        self._receive_chunk(sender_id, metadata["transfer_id"], metadata["offset"], metadata["chunk_md5"], chunk)
            except Exception as e:
                if self.running:
                    print(f"File transfer error: {e}")

    def join_group(self, group_name):
        self.groups.add(group_name)

    def send_large_file(self, target_id, file_path, chunk_size=1048576):
        """Initiate a large file transfer with chunked MD5 verification."""
        if target_id not in self.peers:
            print(f"Target {target_id} not found")
            return
        transfer_id = str(uuid.uuid4())
        total_size = os.path.getsize(file_path)
        total_md5 = self._compute_file_md5(file_path, chunk_size)

        with self.transfer_lock:
            self.transfers[transfer_id] = {
                "file_path": file_path,
                "file_handle": open(file_path, "rb"),
                "total_md5": total_md5,
                "chunk_size": chunk_size,
                "total_size": total_size
            }

        msg = {
            "type": MessageType.FILE_INIT.value,
            "source": self.node_id,
            "transfer_id": transfer_id,
            "file_name": os.path.basename(file_path),
            "total_size": total_size,
            "chunk_size": chunk_size,
            "total_md5": total_md5
        }
        ip, port = self.peers[target_id][:2]  # Use router_port
        dealer = self.context.socket(zmq.DEALER)
        dealer.connect(f"tcp://{ip}:{port}")
        dealer.send_json(msg)
        dealer.close()

    def _compute_file_md5(self, file_path, chunk_size):
        """Compute MD5 of a file by reading in chunks."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    def _handle_message(self, msg, sender_id=None):
        """Process incoming control messages."""
        msg_type = MessageType(msg.get("type"))
        source = msg.get("source")
        content = msg.get("content")
        exclude = msg.get("exclude", [])
        group_name = msg.get("group_name")

        if msg_type == MessageType.GROUP:
            if self.node_id in exclude or (group_name and group_name not in self.groups):
                return
            print(f"Group [{group_name}] from {source}: {content}")
        elif msg_type == MessageType.DIRECT:
            print(f"Direct from {source}: {content}")
        elif msg_type == MessageType.IMAGE or msg_type == MessageType.FILE:
            decoded = base64.b64decode(content)
            print(f"{msg_type.value} from {source}: {len(decoded)} bytes")
        elif msg_type == MessageType.OPERATION:
            if content.get("action") == "fetch_chunk" and sender_id:
                # Handled in _handle_file_transfers
                pass
            elif content.get("action") == "chunk_response" and sender_id:
                # Handled in _handle_file_transfers
                pass
            else:
                print(f"Operation request from {source}: {content}")
                if sender_id:
                    response = {"type": MessageType.OPERATION.value, "source": self.node_id, "content": f"Processed {content}"}
                    self.router_socket.send_multipart([sender_id, json.dumps(response).encode()])
        elif msg_type == MessageType.FILE_INIT:
            self._start_file_receive(sender_id, msg["transfer_id"], msg["file_name"], msg["total_size"], msg["chunk_size"], msg["total_md5"])
        elif msg_type == MessageType.FOLDER_SHARE:
            print(f"Folder share from {source}: {content}")

    def _send_chunk(self, target_id, transfer_id, offset, chunk_size):
        """Send a specific file chunk with MD5 on file_router_socket."""
        with self.transfer_lock:
            if transfer_id not in self.transfers:
                return
            transfer = self.transfers[transfer_id]
            f = transfer["file_handle"]
            f.seek(offset)
            chunk = f.read(chunk_size)
            if not chunk:
                return
            chunk_md5 = hashlib.md5(chunk).hexdigest()

        ip, _, file_port = self.peers[target_id]
        dealer = self.context.socket(zmq.DEALER)
        dealer.connect(f"tcp://{ip}:{file_port}")
        metadata = {
            "action": "chunk_response",
            "transfer_id": transfer_id,
            "offset": offset,
            "chunk_md5": chunk_md5
        }
        dealer.send_multipart([json.dumps(metadata).encode(), chunk])
        dealer.close()

    def _start_file_receive(self, sender_id, transfer_id, file_name, total_size, chunk_size, total_md5):
        """Start receiving a large file by requesting chunks."""
        temp_file = f"temp_{transfer_id}_{file_name}"
        with self.transfer_lock:
            self.receiving_transfers[transfer_id] = {
                "temp_file": open(temp_file, "wb"),
                "file_name": file_name,
                "total_md5": total_md5,
                "chunk_size": chunk_size,
                "total_size": total_size,
                "received_bytes": 0,
                "retries": {}
            }
        threading.Thread(target=self._request_chunks, args=(sender_id, transfer_id, chunk_size, total_size)).start()

    def _request_chunks(self, sender_id, transfer_id, chunk_size, total_size):
        """Request chunks for a file transfer."""
        offset = 0
        while offset < total_size and self.running:
            ip, _, file_port = self.peers[sender_id]
            dealer = self.context.socket(zmq.DEALER)
            dealer.connect(f"tcp://{ip}:{file_port}")
            dealer.send_json({
                "type": MessageType.OPERATION.value,
                "source": self.node_id,
                "content": {
                    "action": "fetch_chunk",
                    "transfer_id": transfer_id,
                    "offset": offset,
                    "chunk_size": chunk_size
                }
            })
            dealer.close()
            offset += chunk_size
            time.sleep(0.01)

    def _receive_chunk(self, sender_id, transfer_id, offset, chunk_md5, chunk):
        """Receive and verify a file chunk."""
        with self.transfer_lock:
            if transfer_id not in self.receiving_transfers:
                return
            transfer = self.receiving_transfers[transfer_id]
            computed_md5 = hashlib.md5(chunk).hexdigest()

            if computed_md5 != chunk_md5:
                retries = transfer["retries"].get(offset, 0)
                if retries < 3:
                    transfer["retries"][offset] = retries + 1
                    ip, _, file_port = self.peers[sender_id]
                    dealer = self.context.socket(zmq.DEALER)
                    dealer.connect(f"tcp://{ip}:{file_port}")
                    dealer.send_json({
                        "type": MessageType.OPERATION.value,
                        "source": self.node_id,
                        "content": {
                            "action": "fetch_chunk",
                            "transfer_id": transfer_id,
                            "offset": offset,
                            "chunk_size": transfer["chunk_size"]
                        }
                    })
                    dealer.close()
                else:
                    print(f"Transfer {transfer_id} failed: too many retries at offset {offset}")
                    transfer["temp_file"].close()
                    os.remove(f"temp_{transfer_id}_{transfer['file_name']}")
                    del self.receiving_transfers[transfer_id]
                return

            transfer["temp_file"].seek(offset)
            transfer["temp_file"].write(chunk)
            transfer["received_bytes"] += len(chunk)

            if transfer["received_bytes"] >= transfer["total_size"]:
                transfer["temp_file"].close()
                final_md5 = self._compute_file_md5(f"temp_{transfer_id}_{transfer['file_name']}", transfer["chunk_size"])
                if final_md5 == transfer["total_md5"]:
                    os.rename(f"temp_{transfer_id}_{transfer['file_name']}", transfer["file_name"])
                    print(f"Transfer {transfer_id} complete: {transfer['file_name']}")
                else:
                    print(f"Transfer {transfer_id} failed: MD5 mismatch")
                    os.remove(f"temp_{transfer_id}_{transfer['file_name']}")
                del self.receiving_transfers[transfer_id]

    def send_message(self, target_id, msg_type, content, exclude=None, group_name=None):
        """Send a message to a specific node or group."""
        msg = {
            "type": msg_type.value,
            "source": self.node_id,
            "content": content
        }
        if msg_type == MessageType.GROUP:
            if exclude is not None:
                msg["exclude"] = exclude if isinstance(exclude, list) else [exclude]
            if group_name is not None:
                msg["group_name"] = group_name

        if msg_type in [MessageType.DIRECT, MessageType.OPERATION, MessageType.FILE_INIT]:
            if target_id in self.peers:
                ip, port, _ = self.peers[target_id]
                dealer = self.context.socket(zmq.DEALER)
                dealer.connect(f"tcp://{ip}:{port}")
                dealer.send_json(msg)
                dealer.close()
        elif msg_type in [MessageType.GROUP, MessageType.FOLDER_SHARE]:
            self.pub_socket.send_json(msg)
        elif msg_type in [MessageType.IMAGE, MessageType.FILE]:
            msg["content"] = base64.b64encode(content).decode()
            if target_id in self.peers:
                ip, port, _ = self.peers[target_id]
                dealer = self.context.socket(zmq.DEALER)
                dealer.connect(f"tcp://{ip}:{port}")
                dealer.send_json(msg)
                dealer.close()

    def _get_local_ip(self):
        """Get the local IP address."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except Exception:
            ip = "127.0.0.1"
        finally:
            s.close()
        return ip