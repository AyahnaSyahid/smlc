import os
import json
import uuid
import hashlib
from PySide6.QtCore import QCoreApplication, QTimer
from PySide6.QtNetwork import QTcpSocket, QHostAddress
from chatengine import ChatEngine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestClient:
    def __init__(self, node_id: str, port: int):
        self.node_id = node_id
        self.engine = ChatEngine()
        self.engine.main_server.listen(QHostAddress.AnyIPv4, port)
        self.client_socket = None
        self.file_id = str(uuid.uuid4())
        self.file_path = "testfile.dat"
        self.chunk_size = 1048576  # 1MB
        self.total_size = 1024 * 1024 * 1024  # 1GB
        self.num_chunks = (self.total_size + self.chunk_size - 1) // self.chunk_size
        self.checksum = self._calculate_file_checksum()

    def _calculate_file_checksum(self):
        hasher = hashlib.md5()
        with open(self.file_path, 'rb') as f:
            while chunk := f.read(4096):
                hasher.update(chunk)
        return hasher.hexdigest()

    def start_transfer(self, target_ip: str, target_port: int):
        self.client_socket = QTcpSocket()
        self.client_socket.connected.connect(self._send_transfer_request)
        self.client_socket.readyRead.connect(self._handle_response)
        self.client_socket.connectToHost(target_ip, target_port)
        logger.info(f"{self.node_id} connecting to {target_ip}:{target_port}")

    def _send_transfer_request(self):
        metadata = {
            "file_id": self.file_id,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "num_chunks": self.num_chunks,
            "filename": "testfile_output.dat",
            "checksum": self.checksum
        }
        payload = json.dumps(metadata).encode()
        msg = (int(11).to_bytes(4, 'big') +
               int.from_bytes(os.urandom(4), 'big').to_bytes(4, 'big') +
               len(payload).to_bytes(4, 'big') +
               payload)
        self.client_socket.write(msg)
        logger.info(f"{self.node_id} sent transfer request for {self.file_id}")

    def _handle_response(self):
        while self.client_socket.bytesAvailable() >= 12:
            header = self.client_socket.read(12)
            msgtype = int.from_bytes(header[:4], 'big')
            msg_len = int.from_bytes(header[8:12], 'big')
            if self.client_socket.bytesAvailable() < msg_len:
                return
            payload = self.client_socket.read(msg_len).decode()
            response = json.loads(payload)
            if msgtype == 12:  # Transfer Accept
                port = response["port"]
                self._start_file_transfer(port)
            elif msgtype == 13:  # Transfer Reject
                logger.warning(f"{self.node_id} transfer rejected: {response['reason']}")
                QTimer.singleShot(10000, lambda: self.start_transfer(self.client_socket.peerAddress().toString(), self.client_socket.peerPort()))
            elif msgtype == 14:  # Resume Status
                missing_chunks = response["missing_chunks"]
                self._start_file_transfer(response["port"], missing_chunks)

    def _start_file_transfer(self, port: int, missing_chunks=None):
        file_socket = QTcpSocket()
        file_socket.connected.connect(lambda: self._send_metadata(file_socket))
        file_socket.readyRead.connect(lambda: self._handle_file_response(file_socket, missing_chunks or []))
        file_socket.connectToHost(self.client_socket.peerAddress().toString(), port)
        logger.info(f"{self.node_id} connected to file transfer port {port}")

    def _send_metadata(self, file_socket: QTcpSocket):
        metadata = {
            "file_id": self.file_id,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "num_chunks": self.num_chunks,
            "filename": "testfile_output.dat",
            "checksum": self.checksum
        }
        payload = json.dumps(metadata).encode()
        msg = (int(5).to_bytes(4, 'big') +
               int.from_bytes(os.urandom(4), 'big').to_bytes(4, 'big') +
               len(payload).to_bytes(4, 'big') +
               payload)
        file_socket.write(msg)
        logger.info(f"{self.node_id} sent metadata for {self.file_id}")

    def _handle_file_response(self, file_socket: QTcpSocket, missing_chunks: list):
        while file_socket.bytesAvailable() >= 12:
            header = file_socket.read(12)
            msgtype = int.from_bytes(header[:4], 'big')
            msg_len = int.from_bytes(header[8:12], 'big')
            if file_socket.bytesAvailable() < msg_len:
                return
            payload = file_socket.read(msg_len).decode()
            response = json.loads(payload)
            if msgtype == 7:  # ACK
                if response["status"] == "metadata_ok":
                    self._send_chunks(file_socket, missing_chunks)
                elif response["status"] == "ok":
                    logger.info(f"{self.node_id} chunk {response['chunk_index']} ACKed")
            elif msgtype == 8:  # Retransmit
                chunk_index = response["chunk_index"]
                self._send_chunk(file_socket, chunk_index)
            elif msgtype == 9:  # Complete
                logger.info(f"{self.node_id} transfer complete for {self.file_id}")
                file_socket.close()
            elif msgtype == 15:  # Failed
                logger.error(f"{self.node_id} transfer failed: {response['reason']}")
                file_socket.close()

    def _send_chunks(self, file_socket: QTcpSocket, missing_chunks: list):
        chunks_to_send = missing_chunks if missing_chunks else range(self.num_chunks)
        with open(self.file_path, 'rb') as f:
            for chunk_index in chunks_to_send:
                f.seek(chunk_index * self.chunk_size)
                data = f.read(self.chunk_size)
                if not data:
                    break
                self._send_chunk(file_socket, chunk_index, data)

    def _send_chunk(self, file_socket: QTcpSocket, chunk_index: int, data: bytes = None):
        if data is None:
            with open(self.file_path, 'rb') as f:
                f.seek(chunk_index * self.chunk_size)
                data = f.read(self.chunk_size)
        chunk_checksum = hashlib.md5(data).hexdigest().encode()
        payload = chunk_index.to_bytes(4, 'big') + chunk_checksum[:16] + data
        msg = (int(6).to_bytes(4, 'big') +
               int.from_bytes(os.urandom(4), 'big').to_bytes(4, 'big') +
               len(payload).to_bytes(4, 'big') +
               payload)
        file_socket.write(msg)
        logger.info(f"{self.node_id} sent chunk {chunk_index}")

def create_dummy_file(filename: str, size: int):
    with open(filename, 'wb') as f:
        f.write(os.urandom(size))
    logger.info(f"Created dummy file {filename} ({size} bytes)")

if __name__ == "__main__":
    from PySide6.QtCore import QCoreApplication
    app = QCoreApplication([])

    # Buat file dummy 1GB
    create_dummy_file("testfile.dat", 64 * 1024 * 1024)

    # Inisialisasi dua node
    node_a = TestClient("NodeA", 52225)  # Pengirim
    node_b = TestClient("NodeB", 52226)  # Penerima

    # Mulai transfer dari Node A ke Node B
    QTimer.singleShot(1000, lambda: node_a.start_transfer("127.0.0.1", 52226))

    # Jalankan aplikasi selama 5 menit (untuk tes)
    timer = QTimer()
    timer.setSingleShot(True)
    timer.timeout.connect(app.quit)
    timer.start(60000)

    app.exec()

    # Cleanup
    if os.path.exists("testfile.dat"):
        os.remove("testfile.dat")