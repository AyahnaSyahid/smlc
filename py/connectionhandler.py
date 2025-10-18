import logging
import json
import os
import uuid
from PySide6.QtCore import QObject, Slot, Signal, QDateTime
from filetransferhandler import FileTransferHandler  # Asumsi file ini ada

logger = logging.getLogger(__name__)

_EMPTY_HEADER = {"fetch_header": True, "message_type": 0, "message_id": 0, "message_length": 0, "part_length": 0, "part_received": []}

def msg4cb(hand):
    header = hand.current_header
    msglen = header["message_length"]
    soc = hand.sock
    if soc.bytesAvailable() < msglen:
        return
    logger.info("Got TEXT")
    buff = soc.read(msglen)
    print(buff.data())
    soc.write(b'\x00\x00\x00\x05\x00\x00\x00\x00\x00\x00\x00\x00')  # Text ACK
    hand.current_header = _EMPTY_HEADER.copy()

class ConnectionHandler(QObject):
    socketDisconnected = Signal()
    startFileTransfer = Signal(str, int, dict)  # file_id, port, metadata
    rejectFileTransfer = Signal(str, str)  # file_id, reason

    def __init__(self, socket, parent=None):
        super().__init__(parent)
        socket.readyRead.connect(self.readSocket)
        socket.errorOccurred.connect(self.errorHandler)
        socket.disconnected.connect(self.disconectedHandler)
        self.peerAddr = socket.peerAddress().toString()
        self.peerPort = socket.peerPort()
        self.peerID = f"{self.peerAddr}:{self.peerPort}"
        self.lastSeen = ''
        self.sock = socket
        self.current_header = _EMPTY_HEADER.copy()
        self.message_buffer = ''
        self.messageCallbacks = {}
        self.setMessageTypeCallback(4, msg4cb)
        self.active_transfers = {}  # {file_id: port}
        self.max_transfers = 5  # Batas transfer aktif

    def setMessageTypeCallback(self, mtype, funct):
        self.messageCallbacks[mtype] = funct

    @Slot()
    def readSocket(self):
        while self.sock.bytesAvailable() >= 12:
            if self.current_header["fetch_header"]:
                if self.sock.bytesAvailable() < 12:
                    return  # Incomplete Header
                logger.info("Reading header")
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
            if msgtype == 1:
                logger.info("Got PING")
                self.sock.write(b'\x00\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00')  # PONG
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 2:
                logger.info("Got PONG")
                self.lastSeen = QDateTime.currentDateTime().toString("yyyy-MM-ddTHH:mm:ss.zzz")
                self.sock.write(b'\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00')  # PONG ACK
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 3:
                logger.info("Got PONG ACK")
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 11:  # Transfer Request
                if self.sock.bytesAvailable() < self.current_header["message_length"]:
                    return
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    request = json.loads(payload)
                    file_id = request["file_id"]
                    total_size = request["total_size"]
                    if len(self.active_transfers) >= self.max_transfers:
                        self.rejectFileTransfer.emit(file_id, "Too many active transfers")
                        self.sock.write(self._build_message(13, json.dumps({"file_id": file_id, "reason": "too_many_transfers"})))
                    elif total_size > 5 * 1024 * 1024 * 1024:  # 5GB limit
                        self.rejectFileTransfer.emit(file_id, "File too large")
                        self.sock.write(self._build_message(13, json.dumps({"file_id": file_id, "reason": "file_too_large"})))
                    elif not self._check_disk_space(total_size):
                        self.rejectFileTransfer.emit(file_id, "Insufficient disk space")
                        self.sock.write(self._build_message(13, json.dumps({"file_id": file_id, "reason": "no_disk_space"})))
                    else:
                        port = self._allocate_port()
                        self.active_transfers[file_id] = port
                        self.startFileTransfer.emit(file_id, port, request)
                        self.sock.write(self._build_message(12, json.dumps({"file_id": file_id, "port": port})))
                except Exception as e:
                    logger.error(f"Invalid transfer request: {e}")
                    self.current_header = _EMPTY_HEADER.copy()
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 12:  # Transfer Accept
                if self.sock.bytesAvailable() < self.current_header["message_length"]:
                    return
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    response = json.loads(payload)
                    file_id = response["file_id"]
                    port = response["port"]
                    # Trigger client-side file transfer (handled externally, e.g., in app logic)
                    self.startFileTransfer.emit(file_id, port, {})
                except Exception as e:
                    logger.error(f"Invalid transfer accept: {e}")
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 13:  # Transfer Reject
                if self.sock.bytesAvailable() < self.current_header["message_length"]:
                    return
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    response = json.loads(payload)
                    self.rejectFileTransfer.emit(response["file_id"], response["reason"])
                except Exception as e:
                    logger.error(f"Invalid transfer reject: {e}")
                self.current_header = _EMPTY_HEADER.copy()
            elif msgtype == 14:  # Resume Status
                if self.sock.bytesAvailable() < self.current_header["message_length"]:
                    return
                payload = self.sock.read(self.current_header["message_length"]).data().decode()
                try:
                    request = json.loads(payload)
                    file_id = request["file_id"]
                    resume_state = self._get_resume_state(file_id)
                    self.sock.write(self._build_message(14, json.dumps({"file_id": file_id, "missing_chunks": resume_state})))
                except Exception as e:
                    logger.error(f"Invalid resume request: {e}")
                self.current_header = _EMPTY_HEADER.copy()
            else:
                if msgtype in self.messageCallbacks:
                    callbacks = self.messageCallbacks[msgtype]
                    for callback in callbacks:
                        callback(self)
                else:
                    logger.warning("UNKNOWN message type received")
                    self.current_header = _EMPTY_HEADER.copy()

    def _build_message(self, msgtype, payload):
        payload_bytes = payload.encode() if isinstance(payload, str) else payload
        msg_len = len(payload_bytes)
        return (msgtype.to_bytes(4, 'big') +
                uuid.uuid4().int.to_bytes(4, 'big')[:4] +  # Random message_id
                msg_len.to_bytes(4, 'big') +
                payload_bytes)

    def _check_disk_space(self, total_size):
        import shutil
        total, used, free = shutil.disk_usage("transfers/")
        return free > total_size * 1.1  # 10% margin

    def _allocate_port(self):
        # Simple port allocation (in real app, check port availability)
        used_ports = set(self.active_transfers.values())
        for port in range(50000, 60000):
            if port not in used_ports:
                return port
        raise RuntimeError("No available ports")

    def _get_resume_state(self, file_id):
        # Load resume state from JSON (simplified, assumes FileTransferHandler saves state)
        try:
            with open(f"transfers/{file_id}.json", 'r') as f:
                state = json.load(f)
            return state.get("missing_chunks", [])
        except FileNotFoundError:
            return []

    @Slot()
    def errorHandler(self):
        logger.error(f"Socket error: {self.sock.errorString()}")
        self.disconectedHandler()

    @Slot()
    def disconectedHandler(self):
        for file_id in list(self.active_transfers.keys()):
            self.rejectFileTransfer.emit(file_id, "Connection lost")
            del self.active_transfers[file_id]
        self.socketDisconnected.emit()