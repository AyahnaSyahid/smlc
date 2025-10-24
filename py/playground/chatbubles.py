from PySide6.QtWidgets import QStyledItemDelegate, QApplication, QListView
from PySide6.QtCore import Qt, QSize, QRect
from PySide6.QtGui import QFont, QFontMetrics, QColor, QPainter, QPainterPath
import logging

logger = logging.getLogger(__name__)

class MessageDelegate(QStyledItemDelegate):
    # Definisi konstanta role
    MessageRole = 1250
    TimeStringRole = 1251
    MessageHeaderRole = 1252
    ImageAttachmentRole = 1253
    SenderRole = 1254

    def __init__(self, parent=None):
        super().__init__(parent)
        self.padding = 10
        self.bubble_margin = 10
        self.max_bubble_width_ratio = 0.7  # Lebar maksimum 70% dari view
        self.min_bubble_width = 0.6        # Lebar minimum 40% dari view
        self.corner_radius = 10            # Radius sudut membulat

    def sizeHint(self, opt, ix):
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
        max_width = int(opt.rect.width() * self.max_bubble_width_ratio)
        min_width = int(opt.rect.width() * self.min_bubble_width)
        fm_header = QFontMetrics(fHeader)
        fm_message = QFontMetrics(fMessage)
        fm_time = QFontMetrics(fTime)

        # Ambil data dari model menggunakan role
        header_text = ix.data(self.MessageHeaderRole) or "Header"
        message_text = ix.data(self.MessageRole) or ""
        time_text = ix.data(self.TimeStringRole) or "00:00"

        # Hitung lebar dan tinggi teks
        header_rect = fm_header.boundingRect(0, 0, max_width - 2 * self.padding, 0, Qt.AlignLeft, header_text)
        message_rect = fm_message.boundingRect(0, 0, max_width - 2 * self.padding, 0, 
                                               Qt.AlignLeft | Qt.TextWordWrap, message_text)
        time_rect = fm_time.boundingRect(0, 0, max_width - 2 * self.padding, 0, Qt.AlignLeft, time_text)

        # Hitung lebar berdasarkan konten
        content_width = max(header_rect.width(), message_rect.width(), time_rect.width())
        bubble_width = content_width + 2 * self.padding
        bubble_width = max(min_width, min(bubble_width, max_width))  # Terapkan batas minimum dan maksimum

        # Hitung tinggi
        height = fm_header.lineSpacing()  # Header
        height += message_rect.height()    # Pesan
        height += fm_time.lineSpacing()    # Waktu
        if ix.data(self.ImageAttachmentRole):  # Attachment
            height += 250
        height += 2 * self.padding        # Padding atas dan bawah

        return QSize(bubble_width + 2 * self.bubble_margin, height)

    def paint(self, painter, opt, index):
        painter.save()
        painter.setRenderHint(QPainter.Antialiasing)

        # Ambil ukuran dari sizeHint
        size = self.sizeHint(opt, index)
        is_sender = index.data(self.SenderRole)  # True untuk pengirim, False untuk penerima

        # Tentukan posisi bubble
        if is_sender:
            bubble_x = opt.rect.right() - size.width() - self.bubble_margin
        else:
            bubble_x = opt.rect.left() + self.bubble_margin
        bubble_rect = QRect(bubble_x, opt.rect.top(), size.width() - 2 * self.bubble_margin, size.height())

        # Gambar bubble dengan tiga sudut membulat
        path = QPainterPath()
        radius = self.corner_radius
        x, y, w, h = bubble_rect.x(), bubble_rect.y(), bubble_rect.width(), bubble_rect.height()

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

        header_text = index.data(self.MessageHeaderRole) or "Header"
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
        y += fm_header.lineSpacing()

        # Gambar pesan
        painter.setFont(fMessage)
        message_rect = fm_message.boundingRect(text_x, y, text_width, 0, Qt.AlignLeft | Qt.TextWordWrap, message_text)
        painter.drawText(message_rect, Qt.AlignLeft | Qt.TextWordWrap, message_text)
        y += message_rect.height()

        # Gambar attachment (jika ada)
        if index.data(self.ImageAttachmentRole):
            painter.drawRect(QRect(text_x, y, text_width, 250))  # Placeholder untuk gambar
            y += 250

        # Gambar waktu
        painter.setFont(fTime)
        painter.drawText(QRect(text_x, y, text_width, fm_time.lineSpacing()), Qt.AlignRight, time_text)

        painter.restore()



if __name__ == "__main__":
    import random
    from PySide6.QtGui import QStandardItemModel, QStandardItem 
    app = QApplication([])
    app.setStyle("Fusion")
    model = QStandardItemModel(0, 1, app)
    for i in range(12):
        item = QStandardItem()
        item.setData(f"Pesan ke {i}", MessageDelegate.MessageRole)
        item.setData(f"Header {i}", MessageDelegate.MessageHeaderRole)
        item.setData("12:00", MessageDelegate.TimeStringRole)
        item.setData(i % 2 == 0, MessageDelegate.SenderRole)  # Pengirim jika genap, penerima jika ganjil
        item.setData(i % random.choices([1, 2, 3, 4, 5])[0], MessageDelegate.ImageAttachmentRole)  # Attachment jika ganjil
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