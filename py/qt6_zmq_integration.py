"""
Safe Integration: ZeroMQ Node + Qt6 GUI
Menggunakan Signals & Slots untuk thread-safe communication
"""

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, 
    QHBoxLayout, QTextEdit, QLineEdit, QPushButton,
    QLabel, QListWidget, QTabWidget, QProgressBar
)
from PySide6.QtCore import QObject, Signal, QTimer, Qt
from PySide6.QtGui import QTextCursor
import sys
import logging

# Import Node class dari file sebelumnya
# Asumsi: improved_node_claude.py ada di folder yang sama
from improved_node_claude import Node, MessageType

# ============================================================================
# WORKER OBJECT - Bridge antara ZeroMQ threads dan Qt GUI thread
# ============================================================================
class NodeWorker(QObject):
    """
    QObject yang bertindak sebagai bridge antara ZeroMQ background threads
    dan Qt GUI thread menggunakan signals.
    
    PENTING: Signals adalah thread-safe di Qt!
    Signal dapat di-emit dari thread mana saja, dan slot akan dieksekusi
    di thread dimana QObject ini di-create (main GUI thread).
    """
    
    # Define signals untuk berbagai events
    # Signals secara otomatis queue calls ke main thread
    message_received = Signal(str, str, str)  # (source, msg_type, content)
    peer_discovered = Signal(str, str)        # (node_id, ip)
    peer_lost = Signal(str)                   # (node_id)
    transfer_progress = Signal(str, float)    # (transfer_id, progress)
    transfer_complete = Signal(str, str)      # (transfer_id, filename)
    transfer_error = Signal(str, str)         # (transfer_id, error)
    log_message = Signal(str, str)            # (level, message)
    
    def __init__(self, node: Node):
        super().__init__()
        self.node = node
        self.previous_peers = set()
        
        # Setup custom logging handler untuk forward logs ke GUI
        self._setup_logging()
    
    def _setup_logging(self):
        """
        Setup custom logging handler yang emit signal untuk setiap log.
        Ini memungkinkan kita menampilkan logs di GUI secara real-time.
        """
        class QtLogHandler(logging.Handler):
            def __init__(self, signal):
                super().__init__()
                self.signal = signal
            
            def emit(self, record):
                msg = self.format(record)
                self.signal.emit(record.levelname, msg)
        
        handler = QtLogHandler(self.log_message)
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        logging.getLogger().addHandler(handler)
    
    def start_monitoring(self):
        """
        Start periodic monitoring untuk check perubahan state.
        Dipanggil via QTimer dari main thread.
        """
        # Check peer changes
        current_peers = set(self.node.get_peers().keys())
        
        # New peers
        new_peers = current_peers - self.previous_peers
        for peer_id in new_peers:
            peer_info = self.node.peers.get(peer_id)
            if peer_info:
                self.peer_discovered.emit(peer_id, peer_info.ip)
        
        # Lost peers
        lost_peers = self.previous_peers - current_peers
        for peer_id in lost_peers:
            self.peer_lost.emit(peer_id)
        
        self.previous_peers = current_peers
        
        # Check transfer progress
        outgoing, incoming = self.node.get_active_transfers()
        
        for transfer_id, info in outgoing.items():
            self.transfer_progress.emit(
                f"OUT:{transfer_id[:8]}", 
                info['progress']
            )
        
        for transfer_id, info in incoming.items():
            self.transfer_progress.emit(
                f"IN:{transfer_id[:8]}", 
                info['progress']
            )
    
    # Thread-safe methods yang bisa dipanggil dari GUI thread
    def send_group_message(self, group_name: str, message: str):
        """Send group message - thread-safe wrapper"""
        self.node.send_message(
            target_id=None,
            msg_type=MessageType.GROUP,
            content=message,
            group_name=group_name
        )
    
    def send_direct_message(self, target_id: str, message: str):
        """Send direct message - thread-safe wrapper"""
        self.node.send_message(
            target_id=target_id,
            msg_type=MessageType.DIRECT,
            content=message
        )
    
    def send_file(self, target_id: str, file_path: str):
        """Send file - thread-safe wrapper"""
        transfer_id = self.node.send_large_file(target_id, file_path)
        return transfer_id
    
    def join_group(self, group_name: str):
        """Join group - thread-safe wrapper"""
        self.node.join_group(group_name)
    
    def get_peers(self):
        """Get peers list - thread-safe wrapper"""
        return self.node.get_peers()


# ============================================================================
# MAIN GUI WINDOW
# ============================================================================
class P2PMessenger(QMainWindow):
    """
    Main window untuk P2P Messenger application.
    Mengintegrasikan ZeroMQ Node dengan Qt6 GUI secara thread-safe.
    """
    
    def __init__(self):
        super().__init__()
        
        # ====================================================================
        # INITIALIZE NODE
        # ====================================================================
        self.node = Node(
            node_id=f"qt_node_{id(self)}",
            chunk_size=1024 * 1024,  # 1MB chunks
            max_concurrent_transfers=3
        )
        
        # Create worker object
        self.worker = NodeWorker(self.node)
        
        # Connect signals ke slots
        # PENTING: Ini adalah magic nya! Signals secara otomatis
        # akan dieksekusi di main thread meskipun di-emit dari background thread
        self.worker.message_received.connect(self.on_message_received)
        self.worker.peer_discovered.connect(self.on_peer_discovered)
        self.worker.peer_lost.connect(self.on_peer_lost)
        self.worker.transfer_progress.connect(self.on_transfer_progress)
        self.worker.transfer_complete.connect(self.on_transfer_complete)
        self.worker.log_message.connect(self.on_log_message)
        
        # ====================================================================
        # SETUP UI
        # ====================================================================
        self.setWindowTitle("P2P Messenger - ZeroMQ + Qt6")
        self.setGeometry(100, 100, 1000, 700)
        
        self.setup_ui()
        
        # ====================================================================
        # START NODE & MONITORING
        # ====================================================================
        # Start ZeroMQ node
        self.node.start()
        
        # Setup timer untuk periodic monitoring
        # QTimer runs in main thread - SAFE!
        self.monitor_timer = QTimer(self)
        self.monitor_timer.timeout.connect(self.worker.start_monitoring)
        self.monitor_timer.start(1000)  # Update setiap 1 detik
        
        self.log("System", "Node started successfully", "green")
    
    def setup_ui(self):
        """Setup UI components"""
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # ====================================================================
        # TABS
        # ====================================================================
        tabs = QTabWidget()
        
        # Tab 1: Messaging
        messaging_tab = self.create_messaging_tab()
        tabs.addTab(messaging_tab, "💬 Messages")
        
        # Tab 2: Peers
        peers_tab = self.create_peers_tab()
        tabs.addTab(peers_tab, "👥 Peers")
        
        # Tab 3: File Transfer
        transfer_tab = self.create_transfer_tab()
        tabs.addTab(transfer_tab, "📁 File Transfer")
        
        # Tab 4: Logs
        logs_tab = self.create_logs_tab()
        tabs.addTab(logs_tab, "📋 Logs")
        
        main_layout.addWidget(tabs)
        
        # ====================================================================
        # STATUS BAR
        # ====================================================================
        self.statusBar().showMessage(f"Node ID: {self.node.node_id}")
    
    def create_messaging_tab(self):
        """Create messaging tab UI"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Chat display
        self.chat_display = QTextEdit()
        self.chat_display.setReadOnly(True)
        layout.addWidget(QLabel("💬 Chat Messages:"))
        layout.addWidget(self.chat_display)
        
        # Input area
        input_layout = QHBoxLayout()
        
        self.group_input = QLineEdit()
        self.group_input.setPlaceholderText("Group name (e.g., 'general')")
        input_layout.addWidget(QLabel("Group:"))
        input_layout.addWidget(self.group_input)
        
        join_btn = QPushButton("Join")
        join_btn.clicked.connect(self.join_group)
        input_layout.addWidget(join_btn)
        
        layout.addLayout(input_layout)
        
        # Message input
        msg_layout = QHBoxLayout()
        
        self.message_input = QLineEdit()
        self.message_input.setPlaceholderText("Type your message...")
        self.message_input.returnPressed.connect(self.send_group_message)
        msg_layout.addWidget(self.message_input)
        
        send_btn = QPushButton("Send")
        send_btn.clicked.connect(self.send_group_message)
        msg_layout.addWidget(send_btn)
        
        layout.addLayout(msg_layout)
        
        return widget
    
    def create_peers_tab(self):
        """Create peers list tab UI"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        layout.addWidget(QLabel("👥 Connected Peers:"))
        
        self.peers_list = QListWidget()
        layout.addWidget(self.peers_list)
        
        # Direct message section
        dm_layout = QHBoxLayout()
        
        self.dm_input = QLineEdit()
        self.dm_input.setPlaceholderText("Direct message...")
        dm_layout.addWidget(self.dm_input)
        
        dm_btn = QPushButton("Send DM")
        dm_btn.clicked.connect(self.send_direct_message)
        dm_layout.addWidget(dm_btn)
        
        layout.addLayout(dm_layout)
        
        return widget
    
    def create_transfer_tab(self):
        """Create file transfer tab UI"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        layout.addWidget(QLabel("📁 Active Transfers:"))
        
        self.transfer_list = QTextEdit()
        self.transfer_list.setReadOnly(True)
        layout.addWidget(self.transfer_list)
        
        # File send section
        send_layout = QHBoxLayout()
        
        self.file_path_input = QLineEdit()
        self.file_path_input.setPlaceholderText("File path to send...")
        send_layout.addWidget(self.file_path_input)
        
        send_file_btn = QPushButton("Send File")
        send_file_btn.clicked.connect(self.send_file)
        send_layout.addWidget(send_file_btn)
        
        layout.addLayout(send_layout)
        
        return widget
    
    def create_logs_tab(self):
        """Create logs tab UI"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        layout.addWidget(QLabel("📋 System Logs:"))
        
        self.logs_display = QTextEdit()
        self.logs_display.setReadOnly(True)
        layout.addWidget(self.logs_display)
        
        # Clear button
        clear_btn = QPushButton("Clear Logs")
        clear_btn.clicked.connect(self.logs_display.clear)
        layout.addWidget(clear_btn)
        
        return widget
    
    # ========================================================================
    # SLOTS - These are called in main thread via signals
    # ========================================================================
    
    def on_message_received(self, source: str, msg_type: str, content: str):
        """
        Slot dipanggil ketika message diterima.
        AMAN: Dieksekusi di main GUI thread via signal/slot mechanism.
        """
        self.log("Message", f"From {source}: {content}", "blue")
        self.chat_display.append(f"<b>{source}</b>: {content}")
        self.chat_display.moveCursor(QTextCursor.MoveOperation.End)
    
    def on_peer_discovered(self, node_id: str, ip: str):
        """Slot untuk peer baru ditemukan"""
        self.log("Peer", f"Discovered: {node_id} ({ip})", "green")
        self.peers_list.addItem(f"{node_id} - {ip}")
    
    def on_peer_lost(self, node_id: str):
        """Slot untuk peer hilang"""
        self.log("Peer", f"Lost: {node_id}", "red")
        # Remove dari list
        for i in range(self.peers_list.count()):
            if node_id in self.peers_list.item(i).text():
                self.peers_list.takeItem(i)
                break
    
    def on_transfer_progress(self, transfer_id: str, progress: float):
        """Slot untuk update progress transfer"""
        # Update di transfer list
        current_text = self.transfer_list.toPlainText()
        
        # Simple update - bisa lebih sophisticated dengan progress bars
        if transfer_id not in current_text:
            self.transfer_list.append(f"{transfer_id}: {progress:.1f}%")
        else:
            # Update existing line (simplified)
            lines = current_text.split('\n')
            for i, line in enumerate(lines):
                if transfer_id in line:
                    lines[i] = f"{transfer_id}: {progress:.1f}%"
            self.transfer_list.setText('\n'.join(lines))
    
    def on_transfer_complete(self, transfer_id: str, filename: str):
        """Slot untuk transfer selesai"""
        self.log("Transfer", f"Complete: {filename}", "green")
    
    def on_log_message(self, level: str, message: str):
        """Slot untuk system logs"""
        color_map = {
            'DEBUG': 'gray',
            'INFO': 'black',
            'WARNING': 'orange',
            'ERROR': 'red',
            'CRITICAL': 'darkred'
        }
        color = color_map.get(level, 'black')
        
        self.logs_display.append(
            f'<span style="color: {color};">[{level}] {message}</span>'
        )
        self.logs_display.moveCursor(QTextCursor.MoveOperation.End)
    
    # ========================================================================
    # USER ACTIONS
    # ========================================================================
    
    def join_group(self):
        """Join group yang diinput user"""
        group_name = self.group_input.text().strip()
        if group_name:
            self.worker.join_group(group_name)
            self.log("Group", f"Joined: {group_name}", "green")
    
    def send_group_message(self):
        """Send message ke group"""
        group_name = self.group_input.text().strip()
        message = self.message_input.text().strip()
        
        if group_name and message:
            self.worker.send_group_message(group_name, message)
            self.chat_display.append(f"<b style='color: green;'>You</b>: {message}")
            self.message_input.clear()
    
    def send_direct_message(self):
        """Send direct message ke selected peer"""
        current_item = self.peers_list.currentItem()
        if not current_item:
            self.log("Error", "Please select a peer first", "red")
            return
        
        # Extract node_id dari list item
        node_id = current_item.text().split(' - ')[0]
        message = self.dm_input.text().strip()
        
        if message:
            self.worker.send_direct_message(node_id, message)
            self.log("DM", f"Sent to {node_id}: {message}", "blue")
            self.dm_input.clear()
    
    def send_file(self):
        """Send file ke selected peer"""
        current_item = self.peers_list.currentItem()
        if not current_item:
            self.log("Error", "Please select a peer first", "red")
            return
        
        node_id = current_item.text().split(' - ')[0]
        file_path = self.file_path_input.text().strip()
        
        if file_path:
            transfer_id = self.worker.send_file(node_id, file_path)
            if transfer_id:
                self.log("Transfer", f"Started: {file_path}", "green")
            else:
                self.log("Error", f"Failed to start transfer", "red")
    
    def log(self, category: str, message: str, color: str = "black"):
        """Helper untuk log ke status dan logs tab"""
        self.statusBar().showMessage(f"[{category}] {message}", 3000)
        self.logs_display.append(
            f'<span style="color: {color};">[{category}] {message}</span>'
        )
    
    # ========================================================================
    # CLEANUP
    # ========================================================================
    
    def closeEvent(self, event):
        """
        Override closeEvent untuk proper cleanup saat window ditutup.
        PENTING: Stop node dan background threads sebelum exit.
        """
        self.log("System", "Shutting down...", "orange")
        
        # Stop monitoring timer
        self.monitor_timer.stop()
        
        # Stop node (akan stop semua background threads)
        self.node.stop()
        
        self.log("System", "Node stopped", "red")
        
        # Accept close event
        event.accept()


# ============================================================================
# MAIN APPLICATION
# ============================================================================
def main():
    """
    Main entry point untuk aplikasi.
    
    BEST PRACTICES untuk Qt + Threading:
    1. Create QApplication first
    2. Create QObjects dalam main thread
    3. Start background threads dari QObjects
    4. Use signals/slots untuk cross-thread communication
    5. Cleanup di closeEvent
    """
    app = QApplication(sys.argv)
    
    # Set application style (optional)
    app.setStyle('Fusion')
    
    # Create dan show main window
    window = P2PMessenger()
    window.show()
    
    # Run Qt event loop
    # Ini akan block sampai window ditutup
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
