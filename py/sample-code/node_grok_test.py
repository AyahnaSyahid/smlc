import time
import random
from node_grok import Node, MessageType

def main():
    # Create 20 nodes with unique ports
    nodes = []
    base_pub_port = 5555
    base_router_port = 5556
    base_discovery_port = 5557
    total_cl = 5
    for i in range(total_cl):
        print(f"PUB {base_pub_port + i} | ROUTE {base_router_port + i + total_cl}")
        node = Node(
            node_id=f"node_{i}",
            bind_ip="0.0.0.0",
            pub_port=base_pub_port + i,
            router_port=base_router_port + i + total_cl,
            discovery_port=base_discovery_port
        )
        nodes.append(node)
    
    # Start all nodes
    print(f"Starting {total_cl} nodes...")
    for node in nodes:
        node.start()
    
    # Wait for nodes to discover each other
    print("Waiting for peer discovery...")
    time.sleep(5)
    
    # Simulate various message types
    print("Simulating message exchanges...")
    
    # Send some direct messages
    for _ in range(5):
        sender = random.choice(nodes)
        target_id = random.choice(list(sender.peers.keys())) if sender.peers else None
        if target_id:
            sender.send_message(target_id, MessageType.DIRECT, f"Hello from {sender.node_id} to {target_id}")
    
    # Send some group messages
    for _ in range(3):
        sender = random.choice(nodes)
        sender.send_message(None, MessageType.GROUP, f"Group message from {sender.node_id}")
    
    # Send a sample file (simulated as bytes)
    sample_file = b"Sample file content"
    sender = random.choice(nodes)
    target_id = random.choice(list(sender.peers.keys())) if sender.peers else None
    if target_id:
        sender.send_message(target_id, MessageType.FILE, sample_file)
    
    # Send an operation request
    sender = random.choice(nodes)
    target_id = random.choice(list(sender.peers.keys())) if sender.peers else None
    if target_id:
        sender.send_message(target_id, MessageType.OPERATION, "Sample operation request")
    
    # Run for a bit to process messages
    print("Running for 10 seconds to process messages...")
    time.sleep(10)
    
    # Stop all nodes
    print("Stopping nodes...")
    for node in nodes:
        print(f"Stopping {node.node_id}...")
        node.stop()

if __name__ == "__main__":
    main()