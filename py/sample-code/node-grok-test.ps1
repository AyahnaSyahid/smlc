# test_nodes.ps1 - Automate launching and testing multiple nodes

# Launch node1
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python node-grok.py node1 5555 5556 5557"

# Launch node2
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python node-grok.py node2 5565 5566 5557"

# Launch node3
Start-Process powershell -ArgumentList "-NoExit", "-Command", "python node-grok.py node3 5575 5576 5557"

# Wait for nodes to start and discover each other
Start-Sleep -Seconds 5

# Simulate sending messages from node1 (requires manual addition or separate script, but for auto: use Invoke-Expression or pipe commands)
# Note: For full automation, you'd need to modify node-grok.py to accept send commands via args or use a wrapper.
# Here, we just launch and assume you check outputs manually. For basic test, uncomment example sends in node-grok.py.

Write-Host "Nodes launched. Check windows for discovery and messages. Press Ctrl+C in each to stop."