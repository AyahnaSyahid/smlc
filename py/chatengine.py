import logging
from PySide6.QtCore import QObject, Slot, QThread
from PySide6.QtNetwork import QTcpSocket, QTcpServer, QHostAddress
from connectionhandler import ConnectionHandler
from filetransferhandler import FileTransferHandler

logger = logging.getLogger(__name__)

class ChatEngine(QObject):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.main_server = QTcpServer(self)
        # self.main_server.listen(QHostAddress.AnyIPv4, 52225)
        self.main_server.newConnection.connect(self.handleConnection)
        self.handlers = []  # ConnectionHandler instances
        self.file_servers = {}  # {port: QTcpServer}
        self.file_threads = {}  # {port: QThread}

    @Slot()
    def handleConnection(self):
        while self.main_server.hasPendingConnections():
            sock = self.main_server.nextPendingConnection()
            handler = ConnectionHandler(sock, self)
            handler.socketDisconnected.connect(self.onHandlerDisconnected)
            handler.startFileTransfer.connect(self.startFileTransferServer)
            handler.rejectFileTransfer.connect(self.onRejectFileTransfer)
            self.handlers.append(handler)
            logger.info(f"Handling {len(self.handlers)} connection")

    @Slot()
    def onHandlerDisconnected(self):
        handler = self.sender()
        if handler in self.handlers:
            self.handlers.remove(handler)
            handler.sock.deleteLater()
            handler.deleteLater()
            logger.info(f"Handler {handler.peerID} deleted")

    @Slot(str, int, dict)
    def startFileTransferServer(self, file_id: str, port: int, metadata: dict):
        if port in self.file_servers:
            logger.warning(f"Port {port} already in use")
            return
        file_server = QTcpServer(self)
        if not file_server.listen(QHostAddress.AnyIPv4, port):
            logger.error(f"Failed to start file server on port {port}")
            return
        thread = QThread(self)
        self.file_servers[port] = file_server
        self.file_threads[port] = thread
        file_server.newConnection.connect(lambda: self.handleFileConnection(file_id, port))
        thread.start()
        logger.info(f"Started file server on port {port} for {file_id}")

    @Slot()
    def handleFileConnection(self, file_id: str, port: int):
        server = self.file_servers.get(port)
        if not server or not server.hasPendingConnections():
            return
        sock = server.nextPendingConnection()
        handler = FileTransferHandler(sock, file_id)
        handler.transferComplete.connect(lambda fid: self.cleanupFileTransfer(fid, port))
        handler.transferFailed.connect(lambda fid, reason: self.cleanupFileTransfer(fid, port))
        handler.moveToThread(self.file_threads[port])
        sock.setParent(handler)  # Ensure socket follows handler to thread
        logger.info(f"File transfer handler created for {file_id} on port {port}")

    @Slot(str, int)
    def cleanupFileTransfer(self, file_id: str, port: int):
        if port in self.file_servers:
            server = self.file_servers.pop(port)
            server.close()
            server.deleteLater()
        if port in self.file_threads:
            thread = self.file_threads.pop(port)
            thread.quit()
            thread.wait()
            thread.deleteLater()
        logger.info(f"Cleaned up file transfer {file_id} on port {port}")

    @Slot(str, str)
    def onRejectFileTransfer(self, file_id: str, reason: str):
        logger.info(f"File transfer {file_id} rejected: {reason}")