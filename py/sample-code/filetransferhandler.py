import logging
import json
import os
import hashlib
from PySide6.QtCore import QObject, Slot, Signal, QTimer, QDir
from PySide6.QtNetwork import QTcpSocket

logger = logging.getLogger(__name__)

_EMPTY_HEADER = {"fetch_header": True, "message_type": 0, "message_id": 0, "message_length": 0}

class FileTransferHandler(QObject):
    transferComplete = Signal(str)  # file_id
    transferFailed = Signal(str, str)  # file_id, reason
    transferProgress = Signal(str, int)  # file_id, percentage

    def __init__(self, socket: QTcpSocket, file_id: str, parent=None):
        super().__init__(parent)
        self.sock = socket
        self.file_id = file_id
        self.current_header = _EMPTY_HEADER.copy()
        self.metadata = {}
        self.received_chunks = set()
        self.chunk_size = 1048576  # 1MB
        self.temp_file_path = f"transfers/{self.file_id}.tmp"
        self.state_file_path = f"transfers/{self.file_id}.json"
        self.temp_file = None
        self.checksum_state = hashlib.md5()
        self.timeout_timer = QTimer(self)
        self.timeout_timer.setSingleShot(True)
        self.timeout_timer.timeout.connect(self._handle_timeout)
        self.retry_count = {}  # {chunk_index: count}

        # Buat direktori jika belum ada
        QDir().mkpath("transfers")

        # Load resume state jika ada
        self._load_resume_state()

        # Konek socket
        self.sock.readyRead.connect(self.readFileSocket)
        self.sock.disconnected.connect(self._handle_disconnect)
        self.sock.errorOccurred.connect(self._handle_error)

        # Mulai timeout
        self.timeout_timer.start(30000)  # 30 detik

    @Slot()
    def readFileSocket(self):
        self.timeout_timer.start(30000)  # Reset timeout
        while self.sock.bytesAvailable() >= 12:
            if self.current_header["fetch_header"]:
                if self.sock.bytesAvailable() < 12:
                    return
                readBytes = self.sock.read(12)
                message_type = int.from_bytes(readBytes[:4], 'big')
                message_id = int.from_bytes(readBytes[4:8], 'big')
                message_length = int.from_bytes(readBytes[8:12], 'big')
                self.current_header.update({
                    'fetch_header': False,
                    'message_type': message_type,
                    'message_length': message_length,
                    'message_id': message_id
                })

            msgtype = self.current_header["message_type"]
            if self.sock.bytesAvailable() < self.current_header["message_length"]:
                return

            if msgtype == 5:  # Metadata
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    self.metadata = json.loads(payload)
                    self.chunk_size = self.metadata.get("chunk_size", 1048576)
                    self.num_chunks = self.metadata["num_chunks"]
                    self.total_size = self.metadata["total_size"]
                    self.overall_checksum = self.metadata["checksum"]
                    self.temp_file = open(self.temp_file_path, 'ab' if os.path.exists(self.temp_file_path) else 'wb')
                    logger.info(f"Metadata received for {self.file_id}")
                    self.sock.write(self._build_message(7, json.dumps({"status": "metadata_ok", "file_id": self.file_id})))
                except Exception as e:
                    logger.error(f"Invalid metadata: {e}")
                    self._send_failed("invalid_metadata")
                self.current_header = _EMPTY_HEADER.copy()

            elif msgtype == 6:  # Chunk
                payload = self.sock.read(self.current_header["message_length"])
                try:
                    chunk_index = int.from_bytes(payload[:4], 'big')
                    chunk_checksum = payload[4:20].hex()  # MD5 16 bytes
                    data = payload[20:]
                    calculated_checksum = hashlib.md5(data).hexdigest()
                    if calculated_checksum != chunk_checksum:
                        self._request_retransmit(chunk_index)
                        return
                    if chunk_index in self.received_chunks:
                        # Sudah diterima, skip tapi ACK
                        self.sock.write(self._build_message(7, json.dumps({"status": "ok", "chunk_index": chunk_index})))
                        return
                    # Tulis ke file
                    self.temp_file.seek(chunk_index * self.chunk_size)
                    self.temp_file.write(data)
                    self.temp_file.flush()
                    # Update checksum state
                    self.checksum_state.update(data)
                    self.received_chunks.add(chunk_index)
                    # Save state
                    self._save_resume_state()
                    # Kirim ACK
                    self.sock.write(self._build_message(7, json.dumps({"status": "ok", "chunk_index": chunk_index})))
                    # Emit progress
                    progress = (len(self.received_chunks) / self.num_chunks) * 100
                    self.transferProgress.emit(self.file_id, int(progress))
                    # Cek complete
                    if len(self.received_chunks) == self.num_chunks:
                        self._verify_and_complete()
                except Exception as e:
                    logger.error(f"Chunk processing error: {e}")
                    self._request_retransmit(chunk_index)
                self.current_header = _EMPTY_HEADER.copy()

            elif msgtype == 8:  # Retransmit request (dari sender? Biasanya receiver kirim ini)
                # Jika sender minta retransmit (jarang, tapi handle)
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    request = json.loads(payload)
                    chunk_index = request["chunk_index"]
                    # Implement retransmit chunk jika ini receiver yang jadi sender (simetri P2P)
                    # Untuk sekarang, log saja (asumsi receiver tidak kirim chunk)
                    logger.warning(f"Retransmit request received for chunk {chunk_index}")
                except Exception as e:
                    logger.error(f"Invalid retransmit request: {e}")
                self.current_header = _EMPTY_HEADER.copy()

            else:
                logger.warning(f"Unknown msgtype {msgtype} in file transfer")
                self.current_header = _EMPTY_HEADER.copy()

    def _build_message(self, msgtype, payload):
        payload_bytes = payload.encode() if isinstance(payload, str) else payload
        msg_len = len(payload_bytes)
        return (msgtype.to_bytes(4, 'big') +
                int.from_bytes(os.urandom(4), 'big').to_bytes(4, 'big') +  # Random message_id
                msg_len.to_bytes(4, 'big') +
                payload_bytes)

    def _request_retransmit(self, chunk_index):
        if chunk_index not in self.retry_count:
            self.retry_count[chunk_index] = 0
        self.retry_count[chunk_index] += 1
        if self.retry_count[chunk_index] > 3:
            self._send_failed("max_retries_exceeded")
            return
        self.sock.write(self._build_message(8, json.dumps({"chunk_index": chunk_index, "file_id": self.file_id})))
        logger.info(f"Requested retransmit for chunk {chunk_index}")

    def _verify_and_complete(self):
        calculated_overall = self.checksum_state.hexdigest()
        if calculated_overall == self.overall_checksum:
            final_path = f"transfers/{self.metadata['filename']}"
            self.temp_file.close()
            os.rename(self.temp_file_path, final_path)
            self.sock.write(self._build_message(9, json.dumps({"status": "complete", "file_id": self.file_id})))
            self.transferComplete.emit(self.file_id)
            self._cleanup()
            logger.info(f"Transfer complete for {self.file_id}")
        else:
            self._send_failed("checksum_mismatch")
            logger.error(f"Checksum mismatch for {self.file_id}")

    def _send_failed(self, reason):
        self.sock.write(self._build_message(15, json.dumps({"reason": reason, "file_id": self.file_id})))
        self.transferFailed.emit(self.file_id, reason)
        self._cleanup()

    def _load_resume_state(self):
        if os.path.exists(self.state_file_path):
            with open(self.state_file_path, 'r') as f:
                state = json.load(f)
            self.received_chunks = set(state.get("received_chunks", []))
            self.metadata = state.get("metadata", {})
            self.num_chunks = self.metadata.get("num_chunks", 0)
            self.chunk_size = self.metadata.get("chunk_size", 1048576)
            # Restore checksum state (simplified, recalculate if needed)
            if os.path.exists(self.temp_file_path):
                with open(self.temp_file_path, 'rb') as f:
                    while chunk := f.read(4096):
                        self.checksum_state.update(chunk)
            logger.info(f"Resumed {self.file_id} with {len(self.received_chunks)} chunks")

    def _save_resume_state(self):
        state = {
            "received_chunks": list(self.received_chunks),
            "metadata": self.metadata
        }
        with open(self.state_file_path, 'w') as f:
            json.dump(state, f)

    def _cleanup(self):
        if self.temp_file:
            self.temp_file.close()
        if os.path.exists(self.state_file_path):
            os.remove(self.state_file_path)
        self.sock.close()
        self.deleteLater()

    @Slot()
    def _handle_timeout(self):
        self._send_failed("timeout")
        logger.warning(f"Timeout for {self.file_id}")

    @Slot()
    def _handle_disconnect(self):
        self._save_resume_state()  # Save for resume
        self.transferFailed.emit(self.file_id, "disconnected")
        self._cleanup()

    @Slot()
    def _handle_error(self):
        logger.error(f"Socket error in transfer: {self.sock.errorString()}")
        self._handle_disconnect()