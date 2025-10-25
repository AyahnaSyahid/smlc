from PySide6.QtWidgets import QStyledItemDelegate, QApplication, QListView
from PySide6.QtCore import Qt, QSize, QRect
from PySide6.QtGui import QFont, QFontMetrics, QColor, QPainter, QPainterPath
import logging

logger = logging.getLogger(__name__)

class MessageDelegate(QStyledItemDelegate):
    # Definisi konstanta role
    SenderRole = 1201
    TextRole   = 1202
    InfoRole   = 1203
    PixmapRole   = 1204
    PixmapDataRole   = 1204
    FileRole   = 1204
    LocalFileRole = 
    PathFileRole =

    def __init__(self, parent=None):
        super().__init__(parent)
        self.padding = 2
        self.item_margin = 50
        self.bubble_margin = 5
        self.max_bubble_width_ratio = 0.7  # Lebar maksimum 70% dari view
        self.min_bubble_width = 0.6        # Lebar minimum 40% dari view
        self.corner_radius = 10            # Radius sudut membulat
        logging.info("MessageDelegate Created")

    def sizeHint(self, opt, ix):
        # logging.info("MessageDelegate::sizeHint Called")
        font = ix.data(Qt.ItemDataRole.FontRole) or opt.font
        if not isinstance(font, QFont):
            logging.debug("Unable to get font")
            font = QApplication.instance().font()
        
        # Font untuk header, pesan, dan waktu
        fHeader = QFont(font)
        fHeader.setPointSize(8)
        fMessage = QFont(font)
        fMessage.setPointSize(11)
        fTime = QFont(font)
        fTime.setPointSize(9)

        # Hitung lebar maksimum dan minimum bubble
        item_rect = opt.rect
        bubble_area = QRect(item_rect.x() + self.item_margin, 0, item_rect.width() - 2 * self.item_margin, item_rect.height())
        # logging.info("MessageDelegate::sizeHint line 44")
        content_area = bubble_area.adjusted(5, 5, -5, -5)
        
        fm_header = QFontMetrics(fHeader)
        fm_message = QFontMetrics(fMessage)
        fm_time = QFontMetrics(fTime)

        # Ambil data dari model menggunakan role
        header_text = ix.data(self.MessageHeaderRole) or ""
        message_text = ix.data(self.MessageRole) or ""
        time_text = ix.data(self.TimeStringRole) or "00:00"

        # Hitung lebar dan tinggi teks
        header_rect = fm_header.boundingRect(content_area, Qt.AlignLeft, header_text)
        header_height = fm_header.lineSpacing() if header_text else 0
        message_rect = fm_message.boundingRect(content_area.adjusted(header_height, 0, 0, 0), 
                                               Qt.AlignLeft | Qt.TextWordWrap, message_text)
        time_rect = fm_time.boundingRect(content_area, Qt.AlignLeft, time_text)

        # Hitung lebar berdasarkan konten
        header_rect.moveLeft(content_area.left() + 5)
        header_rect.moveTop(content_area.top() + 5)
        message_rect.moveLeft(header_rect.left())
        message_rect.moveTop(header_rect.top())
        if len(header_text):
            message_rect.moveTop(header_rect.bottom())
        time_rect.moveLeft(message_rect.right())
        time_rect.moveTop(message_rect.bottom() - int(time_rect.height() / 2))

        # Hitung tinggi
        res = QRect(header_rect.left(), header_rect.top(), time_rect.right() - header_rect.left(), time_rect.bottom() - header_rect.top())
        
        if ix.data(MessageDelegate.ImageAttachmentRole):
            size = ix.data(MessageDelegate.ImageSizeRole)
            # print(size)
            size.scale(250, 250, Qt.AspectRatioMode.KeepAspectRatio)
            res.setBottom(res.bottom() + size.height())
            res.setWidth(max(res.width(), size.width()))
        # print(res)
        return res.adjusted(-5, -5, 5, 5).size()

    def paint(self, painter, opt, index):
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        # Ambil ukuran dari sizeHint
        size = self.sizeHint(opt, index)
        is_sender = index.data(self.SenderRole)  # True untuk pengirim, False untuk penerima
        
        # Tentukan posisi bubble
        if is_sender:
            bubble_x = opt.rect.right() - size.width()
        else:
            bubble_x = opt.rect.left()

        bubble_rect = QRect(bubble_x, opt.rect.top(), size.width() , size.height())
        
        # Gambar bubble dengan tiga sudut membulat
        path = QPainterPath()
        radius = self.corner_radius
        x, y, w, h = bubble_rect.x(), bubble_rect.y(), bubble_rect.width(), bubble_rect.height()
        print(f'{size.height()=}')
        if is_sender:
            # Sudut kanan atas lancip, lainnya membulat
            path.moveTo(x + radius, y)
            path.arcTo(x, y, 2 * radius, 2 * radius, 90, 90)  # Kiri atas
            path.lineTo(x, y + h - radius)
            path.arcTo(x, y + h - 2 * radius, 2 * radius, 2 * radius, 180, 90)  # Kiri bawah
            path.lineTo(x + w - radius, y + h)
            path.arcTo(x + w - 2 * radius, y + h - 2 * radius, 2 * radius, 2 * radius, 270, 90)  # Kanan bawah
            path.lineTo(x + w, y)  # Kanan atas (lancip)
            path.closeSubpath()
        else:
            # Sudut kiri atas lancip, lainnya membulat
            path.moveTo(x, y)  # Kiri atas (lancip)
            path.lineTo(x + w - radius, y)
            path.arcTo(x + w - 2 * radius, y, 2 * radius, 2 * radius, 90, -90)  # Kanan atas
            path.lineTo(x + w, y + h - radius)
            path.arcTo(x + w - 2 * radius, y + h - 2 * radius, 2 * radius, 2 * radius, 0, -90)  # Kanan bawah
            path.lineTo(x + radius, y + h)
            path.arcTo(x, y + h - 2 * radius, 2 * radius, 2 * radius, -90, -90)  # Kiri bawah
            path.closeSubpath()

        painter.setPen(Qt.NoPen)
        painter.setBrush(QColor("#DCF8C6") if is_sender else QColor("#FFFFFF"))
        painter.drawPath(path)

        # Ambil font dan teks
        font = index.data(Qt.ItemDataRole.FontRole) or opt.font
        if not isinstance(font, QFont):
            font = QApplication.instance().font()

        fHeader = QFont(font)
        fHeader.setPointSize(8)
        fMessage = QFont(font)
        fMessage.setPointSize(11)
        fTime = QFont(font)
        fTime.setPointSize(9)

        fm_header = QFontMetrics(fHeader)
        fm_message = QFontMetrics(fMessage)
        fm_time = QFontMetrics(fTime)

        header_text = index.data(self.MessageHeaderRole) or ""
        message_text = index.data(self.MessageRole) or ""
        time_text = index.data(self.TimeStringRole) or "00:00"

        # Posisi teks
        text_x = bubble_rect.left() + self.padding
        text_width = bubble_rect.width() - 2 * self.padding
        y = bubble_rect.top() + self.padding

        # Gambar header
        painter.setFont(fHeader)
        painter.setPen(Qt.black)
        painter.drawText(QRect(text_x, y, text_width, fm_header.lineSpacing()), Qt.AlignLeft, header_text)
        y += fm_header.lineSpacing() if header_text else 0

        # Gambar attachment (jika ada)
        if index.data(self.ImageAttachmentRole):
            size = index.data(MessageDelegate.ImageSizeRole)
            size.scale(250, 250, Qt.AspectRatioMode.KeepAspectRatio)
            image_rect = QRect(bubble_rect.left(), bubble_rect.top(), size.width(), size.height())
            painter.drawRect(image_rect.adjusted(10, 10, 0, 0))  # Placeholder untuk gambar
            y += size.height()
       
       # Gambar pesan
        painter.setFont(fMessage)
        message_rect = fm_message.boundingRect(text_x, y, text_width, 0, Qt.AlignLeft | Qt.TextWordWrap, message_text)
        painter.drawText(message_rect, Qt.AlignLeft | Qt.TextWordWrap, message_text)
        y += message_rect.height()

        # Gambar waktu
        time_str_width = fm_time.horizontalAdvance(time_text)
        painter.setFont(fTime)
        painter.drawText(QRect(bubble_rect.right() - time_str_width + 5, message_rect.bottom() - 10, 50, fm_time.lineSpacing()), Qt.AlignLeft, time_text)

        painter.restore()



if __name__ == "__main__":
    import random
    from PySide6.QtGui import QStandardItemModel, QStandardItem 
    logging.basicConfig(level=logging.DEBUG)
    app = QApplication([])
    app.setStyle("Fusion")
    model = QStandardItemModel(0, 1, app)
    for i in range(12):
        item = QStandardItem()
        item.setData(f"Pesan ke {i}", MessageDelegate.MessageRole)
        # item.setData(f"Header {i}", MessageDelegate.MessageHeaderRole)
        item.setData("12:00", MessageDelegate.TimeStringRole)
        item.setData(i % 2 == 0, MessageDelegate.SenderRole)  # Pengirim jika genap, penerima jika ganjil
        hasAttachment = i % random.choices([1, 2, 3, 4, 5])[0]
        item.setData( hasAttachment, MessageDelegate.ImageAttachmentRole)  # Attachment jika ganjil
        if hasAttachment:
            item.setData( QSize(500, 200), MessageDelegate.ImageSizeRole)  # Attachment jika ganjil
            
        model.appendRow(item)
    view = QListView()
    delegate = MessageDelegate()
    view.setModel(model)
    view.setItemDelegate(delegate)
    view.setVerticalScrollMode(QListView.ScrollPerPixel)
    view.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOn)  # Selalu tampilkan scrollbar
    view.setSpacing(5)  # Atur jarak antar item sebesar 5px
    view.show()
    app.exec()