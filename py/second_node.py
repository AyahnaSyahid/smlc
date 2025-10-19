from improved_node_claude import Node

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
        node_id="node_2",
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