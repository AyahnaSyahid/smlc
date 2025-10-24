from PySide6.QtUiTools import loadUiType
from PySide6.QtWidgets import QFrame
from pathlib import Path

chat_buble, _ = loadUiType(str(Path(__file__).parent / "chatbubles.ui"))

class ChatBubles(QWidget):
    
    def __init__(self, parent=None):
        super().__init__(parent)
        ui = chat_buble()
        ui.setupUi(self)
        self.ui = ui

if __name__ == "__main__":
    from PySide6.QtWidgets import QApplication, QListView
    from PySide6.QtCore import Qt
    from PySide6.QtGui import QStandardItemModel, QStandardItem 
    app = QApplication([])
    app.setStyle("Fusion")
    model = QStandardItemModel(0, 1, app)
    for i in range(12):
        # item = QStandardItem("""CSS selectors are used to "find" (or select) the HTML elements you want to style.\nUse our CSS Selector Tester to demonstrate the different selectors.""")
        item = QStandardItem()
        model.appendRow(item)
    view = QListView()
    view.setModel(model)
    view.setUniformItemSizes(False)
    view.setSpacing(5)
    for i in range(12):
        chb = ChatBubles()
        sizeHint = chb.sizeHint()
        if i == 10:
            chb.ui.messageLabel.setText("CSS selectors are used to \"find\" (or select) the HTML elements you want to style.\nUse our CSS Selector Tester to demonstrate the different selectors.")
        ix = model.index(i, 0)
        item = model.itemFromIndex(ix)
        item.setSizeHint(sizeHint)
        view.setIndexWidget(ix, chb)
        print(chb.sizeHint())
    view.adjustSize()
    view.show()
    app.exec()