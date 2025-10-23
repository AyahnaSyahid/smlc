# dumb window


from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtUiTools import loadUiType
from PySide6.QtCore import Qt, Slot, Signal
from pathlib import Path
import sys
import os

from .zmq_playground import *

root_dir = Path(__file__).parent
sys.path.append(root_dir.absolute())


ui_dumb, base = loadUiType(str(Path(__file__).parent / "dumbchatmainwindow.ui"))


class DumbChatWindow(QMainWindow):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = ui_dumb()
        self.ui.setupUi(self)
    
    @Slot(PeerInfo)
    def onPeerFound(self, peer):
        # add "Chat" category if there is no one
        # then add peer
        


