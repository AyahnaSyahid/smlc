# node.py

import logging
from PySide6.QtCore import QObject, Qt, Signal, Slot
from PySide6.QtNetwork import QTcpServer, QUdpSocket, QTcpSocket, QHostAddress
logger = logging.getLogger(__name__)

class ChatNode(QObject):    
    tcpServerStartedListening = Signal(QHostAddress, int)
    udpServerStartedListening = Signal(QHostAddress, int)
    tcpServerError = Signal(QTcpSocket.SocketError)
    udpServerError = Signal(QTcpSocket.SocketError)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.tcpServer = QTcpServer(self)
        self.udpServer = QUdpSocket(self)
        self.tcpServer.newConnection.connect(self.onNewConnection)
        self.tcpServer.errorOccurred.connect(self.tcpServerError.emit)
        self.udpServer.readyRead.connect(self.onUdpReceived)
        self.udpServer.errorOccurred.connect(self.udpServerError.emit)

    @Slot(str, int)
    def listen(self, addr='0.0.0.0', port=32123):
        if self.tcpServer.isListening():
            self.tcpServer.close()
        listening = self.tcpServer.listen(QHostAddress(addr), port)
        if listening:
            self.tcpServerStartedListening.emit(self.tcpServer.serverAddress(), self.tcpServer.serverPort())
        else:
            print("TCP Server Error :", self.udpServer.errorString())
        udpStart = self.udpServer.bind(QHostAddress.LocalHost, 32234)
        if not udpStart:
            print("UDP Server Error :", self.udpServer.errorString())
    
    @Slot()
    def onNewConnection(self):
        while self.tcpServer.hasPendingConnections():
            con = self.tcpServer.nextPendingConnection()
            peerAddress, peerPort = con.peerAddress().toString(), con.peerPort()
            

    @Slot()
    def onUdpReceived(self):
        replyer = QUdpSocket()
        while self.udpServer.hasPendingDatagrams():
            datagram = self.udpServer.receiveDatagram()
            print(f'Menerima {datagram.data().size()} bytes datagram data')
            print(f'dari {datagram.senderAddress().toString()}:{datagram.senderPort()}')
            ba = datagram.data()
            msgid = ba.split(' ')[-1]
            replyer.writeDatagram(msgid, QHostAddress.LocalHost, 32223)

if __name__ == "__main__":
    from PySide6.QtCore import QCoreApplication, QTimer
    app = QCoreApplication([])
    node = ChatNode()
    node.listen('127.0.0.1', 32123)
    QTimer.singleShot(20000, app.quit)
    app.exec()
