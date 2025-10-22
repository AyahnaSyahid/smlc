# dumbmain
import logging as l

l.basicConfig(level=l.DEBUG)

from PySide6.QtWidgets import QApplication, QMainWindow
from PySide6.QtUiTools import loadUiType
from pathlib import Path
import sys
import os

root_dir = Path(__file__).parent
sys.path.append(root_dir.absolute())
sys.path.append(str(root_dir / "smlc"))


ui_dumb, base = loadUiType(str(Path(__file__).parent / "smlc/dumbchatmainwindow.ui"))

class DumbChatWindow(QMainWindow):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.ui = ui_dumb()
        self.ui.setupUi(self)
        cmenu = self.ui.treeWidget
        cmenu.setHeaderHidden(1)
        cmenu.addCategory("Chat")
        cmenu.setBadge("Chat", "20")
        cmenu.addCategory("Folder")

if __name__ == "__main__":
    app = QApplication([])
    app.setStyle("Fusion")
    dumb = DumbChatWindow()
    dumb.show()
    app.exec()