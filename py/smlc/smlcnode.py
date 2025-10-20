from PySide6.QtCore import QObject, Signal, Slot

class SMLCNode(QObject):
    
    def __init__( self, 
        node_id = "",
        multicast_address = None,
        bind_address = '0.0.0.0',
        pub_port = 0,
        sub_port = 0,
        