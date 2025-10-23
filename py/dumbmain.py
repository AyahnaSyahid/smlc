# dumbmain
import logging as l
    
if __name__ == "__main__":
    
    l.basicConfig(level=l.DEBUG)
    app = QApplication([])
    app.setStyle("Fusion")
    dumb = DumbChatWindow()
    dumb.show()
    app.exec()