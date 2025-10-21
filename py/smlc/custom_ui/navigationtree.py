from PySide6.QtWidgets import QTreeView, QStyledItemDelegate, QWidget, \
                                QHBoxLayout, QLabel
from PySide6.QtCore import Slot, Qt
from PySide6.QtGui import QStandardItem, QStandardItemModel

import logging

log = logging.getLogger(__name__)

class IDelegate(QStyledItemDelegate):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._editor = None
        log.debug("IDelegate Created")
    
    def createEditor(self, par, option, mi):
        print(mi.parent())
        if not mi.parent().isValid(): 
            log.debug("mi is invalid, returning super()")
        else:
            log.debug("mi is valid")
            if not self._editor:
                wi = QWidget(par)
                ly = QHBoxLayout()
                lb1 = QLabel(">", wi)
                lb1.setObjectName('lb1')
                lb2 = QLabel("", wi)
                lb1.setObjectName('lb2')
                lb3 = QLabel("", wi)
                lb3.setObjectName('lb3')
                ly.addWidget(lb1)
                ly.addWidget(lb1, 1)
                ly.addWidget(lb1)
                wi.setLayout(ly)
                self._editor = wi
                wi.setGeometry(option.rect)
            return self._editor
        return super().createEditor(par, option, mi)


class NavigationTree(QTreeView):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self._model = QStandardItemModel(0, 1, self)
        self._deleg = IDelegate(self)
        self.setModel(self._model)
        self.setItemDelegate(self._deleg)
        
    @Slot(str)
    def addCategory(self, cat_name):
        item = QStandardItem(cat_name)
        item.setFlags(Qt.ItemFlag.ItemIsSelectable | Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsEditable)
        self._model.appendRow(item)
    
    @Slot(str, str)
    def addCategoryIntem(self, cat, name):
        pass