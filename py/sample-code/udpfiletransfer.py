# udpfiletransfer.py

from PySide6.QtNetwork import QTcpSocket, QHostAddress, QAbstractSocket
from PySide6.QtCore import QObject, Slot, Signal

class FileSender(QObject):
    
    def __init__(self, file_path, address, port, parent=None):
        super().__init__(parent)
        self._target_addr = QHostAddress(address)
        self._target_port = port
        self._file_path = file_path
        self.socket = QTcpSocket(self)
        self.socket.connected.connect(self.onSocketConnected)
        self.socket.errorOcurred.connect(self.onErrorOccured)
        self.socket.connectToHost(self._target_addr, self._target_port)
        self.socket.readyRead.connect(self.onReadyRead)

    @pyqtSlot()
    def onSocketConnected(self):
        pass

    @pyqtSlot()
    def onErrorOccured(self):
        pass
    
    @pyqtSlot()
    def onReadyRead(self):
        pass


class FileReceiver(QObject):
    
    def __init__(self, )