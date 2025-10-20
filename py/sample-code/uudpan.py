import asyncio

class UdpServerProtocol:
    
    def __init__(self, done_future):
        print("Server initialized")
        self.future_done = done_future
    
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = data.decode()
        print('Server Received %r from %s' % (message, addr))
        print('Server Send %r to %s' % (message, addr))
        self.transport.sendto(data, addr)
        self.transport.close()
        self.future_done.set_result(True)
    
    def connection_lost(self, exc):
        print("Server Clossed", exc)


class UdpClientProtocol:
    
    def __init__(self, message, on_con_lost):
        self.message = message
        self.on_con_lost = on_con_lost
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        print('Client Send:', self.message)
        self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):
        print("Client Receive:", data.decode())
        print("Client Close the socket")
        self.transport.close()

    def error_received(self, exc):
        print('Client Error received:', exc)

    def connection_lost(self, exc):
        print("Client Connection closed")
        self.on_con_lost.set_result(True)


async def test_udp():
    loop = asyncio.get_running_loop()
    
    on_con_lost = loop.create_future()
    server_done = loop.create_future()
    
    server_transport, server_proto = await loop.create_datagram_endpoint( lambda: UdpServerProtocol(server_done), local_addr=('127.0.0.1', 9191) )
    
    # client_transport, client_proto = await loop.create_datagram_endpoint( lambda: UdpClientProtocol("Ini Budi", on_con_lost), remote_addr=('127.0.0.1', 9191) )
    
    # await on_con_lost
    # await server_done
    try:
        await asyncio.wait_for(server_done, 10)
    except TimeoutError:
        print("Client took too long to connect")
    finally:
        server_transport.close()

if __name__ == "__main__" :
    
    asyncio.run(test_udp())
    print("Done")