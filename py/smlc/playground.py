from PySide6.QtNetwork import QTcpSocket, QUdpSocket, QTcpServer, QHostAddress, QAbstractSocket
from PySide6.QtCore import QCoreApplication, QTimer, QThread, QMutex, QMutexLocker, \
                                Slot, Signal, QDateTime, QObject
import json
import logging

logger = logging.getLogger(__name__)

class ConnectionHanlder(QObject):
    pass
    
class Node(QObject):
    peerDiscovered = Signal(str)
    
    def __init__(self, node_name, parent=None):
        super().__init__(parent)
        # self.mutex = QMutex(self)
        self.node_id = node_name
        self.tcpServer = QTcpServer(self)
        self.udpSocket = QUdpSocket(self)
        self.started = False
        self.m_groups = ['224.1.1.1']
        self._local_ip_cache = {'ip':None, 'last_check':None}
        self.annouce_timer = QTimer(self)
        self.annouce_timer.timeout.connect(self.send_announce)
        self.annouce_timer.setInterval(5000)
        self.annouce_timer.start()
        self.peers = {} # node_id: ip_addr, port_addr, last_seen
        self.peers_mutex = QMutex(self)

    def localIp(self):
        if self._local_ip_cache['ip'] is None:
            s = QUdpSocket()
            s.connectToHost('8.8.8.8', 80)
            l_ip = s.localAddress().toString()
            l_check = QDateTime.currentDateTime()
            self._local_ip_cache = {'ip':l_ip, 'last_check':l_check}
            return l_ip
        elif QDateTime.currentDateTime() - self._local_ip_cache['last_check'] > 60000:
            self._local_ip_cache = {'ip':None, 'last_check':None}
            return self.localIp()
        else:
            return self._local_ip_cache['ip']
    
    @Slot(QAbstractSocket.SocketError)
    def onError(self, err):
        logger.warning(f"{self.node_id} ERROR {err}")
    
    @Slot()
    def send_announce(self):
        if not self.started:
            logger.warning("ERROR server not started yet")
            return 
        logger.info(f"{self.node_id} >> Sending Payload")
        payload = {'message_type':'announce', 
                   'node_id':self.node_id,
                   'ip_addr':self.localIp(),
                   'port_addr':self.tcpServer.serverPort() }
        data = json.dumps(payload)
        for group in self.m_groups:
            self.udpSocket.writeDatagram(data.encode(), QHostAddress(group), 55655)
        # self.udpSocket.writeDatagram(data.encode(), QHostAddress('255.255.255.255'), 55655)

    @Slot()
    def start(self):
        if self.started:
            return
        self.started = True
        logger.info(f"{self.node_id} >> Starting")
        self.tcpServer.acceptError.connect(self.onError)
        self.udpSocket.errorOccurred.connect(self.onError)
        self.tcpServer.listen(QHostAddress.SpecialAddress.AnyIPv4)
        self.udpSocket.bind(QHostAddress.SpecialAddress.AnyIPv4, 55655, QAbstractSocket.BindFlag.ReuseAddressHint)
        self.udpSocket.joinMulticastGroup(QHostAddress('224.1.1.1'))
        self.tcpServer.newConnection.connect(self.handleConnection)
        self.udpSocket.readyRead.connect(self.readDatagram)
    
    @Slot()
    def readDatagram(self):
        logger.info(f"{self.node_id} >> Receiving Datagrams")
        while self.udpSocket.hasPendingDatagrams():
            dg = self.udpSocket.receiveDatagram()
            data = dg.data().data()
            logger.debug(f"{data}")
            payload = None
            try:
                payload = json.loads(data.decode())
            except Exception as ex:
                logger.info(f"Gagal menerima payload {ex}")
                return
            if payload["message_type"] == 'announce':
                peer_node_id = payload["node_id"]
                peer_ip_addr = payload["ip_addr"]
                peer_port_addr = payload["port_addr"]
                
                if payload['node_id'] == self.node_id:
                    return
                lock = QMutexLocker(self.peers_mutex)
                if payload['node_id'] not in self.peers.keys():
                    logger.info(f"Discovered peer [{peer_node_id}]@{peer_ip_addr}:{peer_port_addr}")
                    peer_id = payload['node_id']
                    self.peers[node_id] = (payload['ip_addr'], payload[port_addr], QDateTime.currentDateTime())
                    return

    @Slot()
    def stop(self):
        if not self.started:
            return
        self.tcpServer.close()
        self.udpSocket.abort()
        self.started = False

    @Slot()
    def handleConnection(self):
        while self.tcpServer.hasPendingConnection():
            con = self.tcpServer.nextPendingConnection()
            con.abort()
            con.deleteLater()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    app = QCoreApplication([])
    node1 = Node("Node1")
    node2 = Node("Node2")
    node1.start()
    node2.start()
    QTimer.singleShot(60000, app.quit)
    app.exec()