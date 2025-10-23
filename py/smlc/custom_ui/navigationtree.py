from PySide6.QtWidgets import QTreeView, QTreeWidgetItem, QStyledItemDelegate, QWidget, QToolButton, QPushButton, \
                                QHBoxLayout, QLabel, QApplication
from PySide6.QtCore import Slot, Qt, QSize
from PySide6.QtGui import QStandardItem, QStandardItemModel

import logging

log = logging.getLogger(__name__)


class NavigationTree(QTreeView):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._model = QStandardItemModel(0, 1, self)
        self.setModel(self._model)
        
    @Slot(str)
    def addCategory(self, cat_name):
        item = QStandardItem(cat_name)
        item.setEditable(False)
        item.setSelectable(False)
        item.setSizeHint(QSize(200, 40))
        self._model.appendRow(item)

    @Slot(str, str)
    def setBadge(self, wn, bdg):
        pass

    @Slot(str)
    def addChatPeer(self, name):
        pass