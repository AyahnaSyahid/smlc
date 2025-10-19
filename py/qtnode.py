# qtnode

from PySide6.QtCore import QObject, Qt, Slot, Signal
from fixed_local_node import Node

class QtNode(QObject):
    sendMessageError = Signal(str)

    def __init__(self,
        node_id = None,
        bind_ip = "0.0.0.0",
        pub_port = 0,
        router_port = 0,
        discovery_port = 5577,
        chunk_size = 1024 * 1024,
        max_concurrent_transfers = 5,
        parent=None
    ):
        super().__init__(parent)
        self.node = Node(
            node_id, 
            bind_ip, 
            pub_port, 
            router_port, 
            discovery_port, 
            chunk_size,
            max_concurrent_transfers 
        )

    @Slot()
    def start(self):
        self.node.start()
    
    @Slot(str)
    def join_group(self, gn):
        self.node.join_group(gn)
    
    @Slot(str, str, str, list, str)
    def send_message(self, target_id, msg_type, content, exclude, group_name):
        try:
            return self.node.send_message(target_id, msg_type, content, exclude, group_name)
        except Exception as e:
            self.sendMessageError.emit(e)
        return False
    