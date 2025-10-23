from PySide6.QtWidgets import QListView, QWidget, QLabel, QPushButton
from PySide6.QtCore import QStandardItemModel, QDateTime


class ChatItemWidget(QWidget):
    
    
    def __init__(self, parent=None):
        super().__init__(parent)
        profileIcon = QLabel(self)
        profileIcon.setObjectName("profileIcon")
        nameLabel = QLabel(self)
        nameLabel.setObjectName("nameLabel")
        timestampLabel = QLabel(self)
        moreoptButton = QPushButton('...')


class ChatModel(QStandardItemModel):
    
    
    def __init__(self, parent=None):
        super().__init__(parent)



class ChatView(QListView):
    
    
    def __init__(self, parent=None):
        super().__init__(parent)
        
    