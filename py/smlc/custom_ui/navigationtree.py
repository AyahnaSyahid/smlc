from PySide6.QtWidgets import QTreeWidget, QTreeWidgetItem, QStyledItemDelegate, QWidget, \
                                QHBoxLayout, QLabel, QApplication
from PySide6.QtCore import Slot, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

import logging

log = logging.getLogger(__name__)

class TreeHeaderWidget(QWidget):
    
    def __init__(self, lab, parent=None):
        super().__init__(parent)
        self.carret = QLabel(">", self)
        self.text = QLabel(lab, self)
        # self.text.setAlignment(Qt.Alignment.AlignCenter)
        self.badge = QLabel("", self)
        self.badge.setMinimumWidth(40)
        
        layout = QHBoxLayout()
        layout.addWidget(self.carret)
        layout.addWidget(self.text, 1)
        layout.addWidget(self.badge)
        self.setLayout(layout)
    
    @Slot(str)
    def setBadge(self, b):
        self.badge.setText("4")
    

class NavigationTree(QTreeWidget):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.itemWidgets = {}
        
    @Slot(str)
    def addCategory(self, cat_name):
        item = QTreeWidgetItem(self, 1001)
        self.addTopLevelItem(item)
        hw = TreeHeaderWidget(cat_name)
        self.itemWidgets[cat_name] = hw
        self.setItemWidget(item, 0, hw)
    
    @Slot(str, str)
    def setBadge(self, wn, bdg):
        if wn in self.itemWidgets:
            self.itemWidgets[wn].setBadge(bdg)
    
    @Slot(str, str)
    def addCategoryItem(self, cat, name):
        pass