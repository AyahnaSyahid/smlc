from PySide6.QtNetwork import QTcpSocket, QUdpSocket, QTcpServer, QHostAddress
from PySide6.QtCore import QCoreApplication, QTimer, QThread, QMutex, QMutexLocker, \
                                Slot, Signal, QDateTime
import json
import logging

logger = logging.getLogger(__name__)

class ConnectionHanlder(QObject):
    
class Node(QObject):
    peerDiscovered = Signal(str)
    
    def __init__(self, node_name, parent=None):
        super().__init__(parent)
        self.mutex = QMutex(self)
        self.node_id = node_name
        self.tcpServer = QTcpServer(self)
        self.udpSocket = QUdpSocket(self)
        self.started = False
        self.m_groups = ['224.1.1.1']
        self._local_ip_cache = {'ip':None, 'last_check':None}
        self.annouce_timer = QTimer(self)
        self.annouce_timer.setInterval(80000)
        self.annouce_timer.timeout.connect(self.send_announce)
        self.peers = {} # node_id: ip_addr, port_addr, last_seen
        self.peers_mutex = QMutex(self)
        

    def localIp(self):
        if self._loca_lip_cache['ip'] is None:
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
            return self._local_lip_cache['ip']
    
    
    @Slot()
    def send_announce(self):
        if not self.started:
            logger.warning("ERROR server not started yet")
            return 
        payload = {'message_type':'announce', 
                   'node_id':self.node_id,
                   'ip_addr':self.localIp(),
                   'port_addr':self.server.serverPort() }
        data = json.dumps(payload)
        for group in self.m_groups:
            self.udpSocket.writeDatagram(data.encode(), group, 55655)
        self.udpSocket.writeDatagram(data.encode(), '255.255.255.255', 55655)

    @Slot()
    def start(self):
        if self.started:
            return
        self.started = True
        self.server.listen(QHostAddress.SpecialAddress.AnyIPv4)
        self.udpSocket.bind(QHostAddress.SpecialAddress.AnyIPv4, 55655)
        self.udpSocket.joinMulticastGroup('224.1.1.1')
        self.server.newConnection.connect(self.handleConnection)
        self.udpSocket.readyRead.connect(self.readDatagram)
    
    @Slot()
    def readDatagram(self):
        while self.udpSocket.hasPendingDatagrams():
            data, host, port = self.udpSocket.readDatagram(-1)
            payload = None
            try:
                payload = json.loads(data.decode())
            except Exception as ex:
                log.info("Gagal menerima payload")
                return
            if payload is not None:
                if paylode["message_type"] == 'announce':
                    if payload['node_id'] == self.node_id:
                        return
                    if payload['node_id'] not in self.peers.keys()
                        lock = QMutexLocker(self.peers_mutex)
                        peer_id = payload['node_id']
                        peers[node_id] = (payload['ip_addr'], payload[port_addr], QDateTime.currentDateTime())
                        return
    @Slot()
    def stop(self):
        if not self.started:
            return
        self.server.close()
        self.udpSocket.abort()
        self.started = False

    @Slot()
    def handleConnection(self):
        while self.server.hasPendingConnection():
            con = self.server.nextPendingConnection()
            con.abort()
            con.deleteLater()
    

