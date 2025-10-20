import asyncio
import itertools

# Set untuk menyimpan semua Writers (koneksi klien)
clients = set()

# Fungsi utama untuk menangani setiap koneksi klien
async def handle_client(reader, writer):
    # Dapatkan alamat klien untuk logging
    addr = writer.get_extra_info('peername')
    print(f"Koneksi baru dari {addr}")

    # Tambahkan koneksi klien ke set global
    clients.add(writer)

    try:
        # Loop utama untuk membaca data dari klien
        while True:
            # Baca hingga 1024 byte
            data = await reader.read(1024)
            if not data:
                # Klien terputus
                break

            # Decode data byte menjadi string, strip whitespace
            message = data.decode().strip()
            
            # Buat pesan lengkap untuk broadcast
            full_message = f"[{addr[0]}:{addr[1]}] {message}\n"
            print(f"Menerima: {full_message.strip()}")
            writer.write(b"Hello From Server")
            
            # Broadcast pesan ke semua klien yang terhubung
            await broadcast(full_message, writer)

    except ConnectionResetError:
        # Tangani jika klien menutup koneksi secara paksa
        print(f"Koneksi {addr} terputus secara paksa.")
    except Exception as e:
        # Tangani pengecualian lain
        print(f"Terjadi error pada koneksi {addr}: {e}")
    finally:
        # Tutup koneksi dan hapus dari set
        print(f"Menutup koneksi {addr}")
        writer.close()
        await writer.wait_closed() # Tunggu sampai writer benar-benar tertutup
        clients.remove(writer)

# Fungsi untuk mengirim pesan ke semua klien kecuali pengirim (opsional)
async def broadcast(message, sender_writer=None):
    # Konversi pesan string menjadi bytes
    data = message.encode()
    
    # Membuat daftar tugas (tasks) pengiriman
    # Gunakan list comprehension untuk efisiensi
    tasks = [
        writer.write(data)
        for writer in clients
        if writer is not sender_writer # Jangan kirim ke pengirim (opsional)
    ]
    
    # Tunggu semua operasi 'write' selesai
    await asyncio.gather(*tasks)

# Fungsi untuk menjalankan server
async def main():
    # Buat dan mulai server
    server = await asyncio.start_server(
        handle_client, 
        '127.0.0.1', # Ganti dengan '0.0.0.0' jika ingin bisa diakses dari luar
        8888         # Port yang digunakan
    )

    addr = server.sockets[0].getsockname()
    print(f"Server chat sedang berjalan di {addr}")

    # Jalankan server secara permanen
    async with server:
        await server.serve_forever()

# Jalankan fungsi main
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nServer dihentikan oleh pengguna.")