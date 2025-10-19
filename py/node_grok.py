import zmq
import json
import threading
import socket
import struct
import uuid
import time
from enum import Enum
import base64
import sys

class MessageType(Enum):
    DIRECT = "direct"
    GROUP = "group"
    IMAGE = "image"
    FILE = "file"
    OPERATION = "operation"
    FOLDER_SHARE = "folder_share"


class Node:    
    
    def __init__(self, node_id, bind_ip="0.0.0.0", pub_port=5555, router_port=5556, discovery_port=5557):
        self.node_id = node_id or str(uuid.uuid4())
        self.groups = set()  # Track groups this node belongs to
        self.bind_ip = bind_ip
        self.pub_port = pub_port
        self.router_port = router_port
        self.discovery_port = discovery_port
        self.peers = {}  # {node_id: (ip, router_port)}
        self.context = zmq.Context()
        
        # Pub socket for group messages
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://{bind_ip}:{pub_port}")
        
        # Router socket for direct messages/requests
        self.router_socket = self.context.socket(zmq.ROUTER)
        self.router_socket.bind(f"tcp://{bind_ip}:{router_port}")
        
        # Sub socket for receiving group messages
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")  # Subscribe to all
        
        # Discovery socket (UDP multicast)
        self.mcast_group = "224.0.0.1"
        self.discovery_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.discovery_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.discovery_socket.bind(('', discovery_port))
        mreq = struct.pack("4sl", socket.inet_aton(self.mcast_group), socket.INADDR_ANY)
        self.discovery_socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        self.discovery_socket.settimeout(1.0)
        
        # Threads for async operation
        self.running = True
        self.discovery_thread = threading.Thread(target=self._discover_peers)
        self.receive_thread = threading.Thread(target=self._receive_messages)
        
    def start(self):
        """Start the node, initiating discovery and message handling."""
        self.discovery_thread.start()
        self.receive_thread.start()
        self._announce_presence()
        
    def stop(self):
        """Stop the node and clean up."""
        self.running = False
        self.discovery_thread.join()
        self.receive_thread.join()
        self.discovery_socket.close()
        self.pub_socket.close()
        self.router_socket.close()
        self.sub_socket.close()
        self.context.term()
        
    def _announce_presence(self):
        """Announce this node to others via multicast."""
        msg = json.dumps({
            "node_id": self.node_id,
            "ip": self._get_local_ip(),
            "router_port": self.router_port
        }).encode()
        self.discovery_socket.sendto(msg, (self.mcast_group, self.discovery_port))
        
    def _discover_peers(self):
        """Listen for peer announcements and connect to them."""
        while self.running:
            try:
                data, addr = self.discovery_socket.recvfrom(1024)
                peer = json.loads(data.decode())
                if peer["node_id"] != self.node_id:
                    self.peers[peer["node_id"]] = (peer["ip"], peer["router_port"])
                    self.sub_socket.connect(f"tcp://{peer['ip']}:{self.pub_port}")
            except socket.timeout:
                self._announce_presence()  # Periodically re-announce
            except Exception as e:
                print(f"Discovery error: {e}")
                
    def _receive_messages(self):
        """Handle incoming messages (group via sub, direct via router)."""
        poller = zmq.Poller()
        poller.register(self.sub_socket, zmq.POLLIN)
        poller.register(self.router_socket, zmq.POLLIN)
        
        while self.running:
            try:
                socks = dict(poller.poll(timeout=1000))
                
                if self.sub_socket in socks:
                    msg = self.sub_socket.recv_json()
                    self._handle_message(msg)
                    
                if self.router_socket in socks:
                    frames = self.router_socket.recv_multipart()
                    sender_id, msg = frames[0], json.loads(frames[1].decode())
                    self._handle_message(msg, sender_id)
                    
            except Exception as e:
                print(f"Receive error: {e}")

    def join_group(self, group_name):
        """Join a group to receive its messages."""
        self.groups.add(group_name)

    def _handle_message(self, msg, sender_id=None):
        """Process incoming messages based on type."""
        msg_type = MessageType(msg.get("type"))
        source = msg.get("source")
        content = msg.get("content")
        exclude = msg.get("exclude", [])
        group_name = msg.get("group_name")  # Get group_name for group messages

        # Skip if node is excluded or not in the group for group messages
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
            print(f"Operation request from {source}: {content}")
            # Example: Process file or other operation, send response
            if sender_id:
                response = {"type": MessageType.OPERATION.value, "source": self.node_id, "content": f"Processed {content}"}
                self.router_socket.send_multipart([sender_id, json.dumps(response).encode()])
        elif msg_type == MessageType.FOLDER_SHARE:
            print(f"Folder share from {source}: {content}")
            
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
        
        if msg_type in [MessageType.DIRECT, MessageType.OPERATION]:
            if target_id in self.peers:
                ip, port = self.peers[target_id]
                dealer = self.context.socket(zmq.DEALER)
                dealer.connect(f"tcp://{ip}:{port}")
                dealer.send_json(msg)
                dealer.close()
        elif msg_type in [MessageType.GROUP, MessageType.FOLDER_SHARE]:
            self.pub_socket.send_json(msg)
        elif msg_type in [MessageType.IMAGE, MessageType.FILE]:
            # Assume content is binary, encode as base64
            msg["content"] = base64.b64encode(content).decode()
            if target_id in self.peers:
                ip, port = self.peers[target_id]
                dealer = self.context.socket(zmq.DEALER)
                dealer.connect(f"tcp://{ip}:{port}")
                dealer.send_json(msg)
                dealer.close()
                
    def _get_local_ip(self):
        """Get the local IP address."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))  # Dummy connection to get local IP
        ip = s.getsockname()[0]
        s.close()
        return ip
