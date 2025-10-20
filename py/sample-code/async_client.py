import asyncio

async def dummy_client(message):
    try:
        reader, writer = await asyncio.open_connection('127.0.0.1', 32123)
    except ConnectionRefusedError:
        print(f'Koneksi ditolak: {message}')
        return 
    
    print(f'Mengirim: {message}')
    writer.write(message.encode())
    await writer.drain() # Tunggu sampai write buffer kosong

    # Loop untuk menerima pesan dari server (blokir sebentar, bisa diperbaiki dengan task terpisah)
    # Untuk uji coba sederhana
    while True:
        try:
            data = await reader.read(100)
            if not data:
                break
            print(f'Diterima: {data.decode()}')
        except ConnectionAbortedError:
            print(f'Koneksi diputus oleh server: {message}')
            break
    print('Menutup koneksi')
    writer.close()
    await writer.wait_closed()

async def create_dummy(many):
    
    clients = (dummy_client(f"C{x}") for x in range(many))
    await asyncio.gather(*clients)

def create_dummy_main(how_many = 10):
    try:
        asyncio.run(create_dummy(200))
    except KeyboardInterrupt:
        print("\nServer dihentikan oleh pengguna.")

def udp_reliability_test():
    from PySide6.QtNetwork import QUdpSocket, QHostAddress
    from PySide6.QtCore import Qt, Signal, Slot, QCoreApplication, QObject, QMutex, QMutexLocker, QTimer
    reader, sender, state, mutex = None, None, {}, QMutex()
    
    def readData():
        # global reader, sender, state, mutex, state['retry_count']
        received = []
        locker = QMutexLocker(mutex)
        
        while reader.hasPendingDatagrams():
            datagram = reader.receiveDatagram()
            ba = datagram.data().data().decode()
            print(f"readed {ba}")
            received.append(int(ba))
        
        for i in received:
            state['toBeSent'].remove(i)
            state['confirmedReceived'].append(i)

        if len(state['toBeSent']):
            if state['retry_count'] > 20:
                raise RuntimeError("Retry count exceed 20")
            state['retry_count'] += 1
            print(f"Retry Count {state['retry_count']}")
            sendTimer.start(200)
        else:
            print("done Sending Data")
            app.quit()
            
    def startSending():
        # global reader, sender, state, mutex
        print(f"data toBeSent {len(state['toBeSent'])}" )
        for i in state['toBeSent']:
            sender.writeDatagram(f'Sending {i}'.encode(), QHostAddress.LocalHost, 32234)
    
    app = QCoreApplication([])
    
    state = {}
    state['toBeSent'] = [i for i in range(200)]
    state['confirmedReceived'] = []
    state['retry_count'] = 0
    
    udpReader = QUdpSocket()
    reader = udpReader
    udpSender = QUdpSocket()
    sender = udpSender
    reader.readyRead.connect(readData)
    if not udpReader.bind(QHostAddress.AnyIPv4, 32223):
        print("Error :", udpReader.errorString())
        app.quit()
        sys.exit(0)
    
    sendTimer = QTimer()
    sendTimer.setInterval(1000)
    sendTimer.timeout.connect(startSending)
    sendTimer.start()
    app.exec()

if __name__ == '__main__':
    import sys
    try:
        udp_reliability_test()
    except Exception as e:
        print(e)
        sys.exit(1)