from PySide6.QtWidgets import QWidget, QLabel, QHBoxLayout
from PySide6.QtCore import Slot, Qt


class ToolBoxPageHeader(QWidget):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.collapsed = False
        self.collapseMark = QLabel(">", self)
        self.titleLabel = QLabel("", self)
        self.badgeLabel = QLabel("", self)
        ly = QHBoxLayout()
        ly.addWidget(self.collapseMark)
        ly.addWidget(self.titleLabel, 1, Qt.Alignment.AlignCenter)
        ly.addWidget(self.badgeLabel)
        self.setLayout(ly)

    @Slot(str)
    def setText(self, tx):
        self.titleLabel.setText(tx)

    @Slot()
    def setCollapse(self, collapse=True):
        if self.collapsed == collapse:
            return
        self.collapsed = collapse
        if self.collapsed:
            self.collapseMark.setText("⌄")
        else:
            self.collapseMark.setText(">")
        font = self.font()
        font.setBold(self.collapsed)
    
    @Slot(int)
    def onToolboxCurrentChanged(self, ix):
        toolBox = self.sender()
        itsMe = toolBox.widget(ix) == self
        self.setCollapse(itsMe)
    
    @Slot(str)
    def setBadge(self, bg):
        self.badgeLabel.setText(bg)
