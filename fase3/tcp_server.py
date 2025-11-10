"""
Exemplo de servidor que usa SimpleTCPSocket.
Ele funciona com UnreliableChannel (passado como channel) ou com UDP real (channel=None).
"""
from typing import Optional
from fase3.tcp_socket import SimpleTCPSocket
from utils.simulator import UnreliableChannel
import sys

def run_server(port: int = 8000, channel: Optional[UnreliableChannel] = None):
    server = SimpleTCPSocket(port, verbose=True, channel=channel)
    server.listen()
    print(f"Servidor: ouvindo na porta {server.port}")
    conn, addr = server.accept()
    print(f"Conexão aceita de {addr}")
    try:
        while True:
            data = conn.recv(4096, timeout=5.0)
            if not data:
                break
            print(f"Servidor recebeu {len(data)} bytes; ecoando")
            conn.send(data)  # eco
    except KeyboardInterrupt:
        pass
    finally:
        try:
            conn.close()
        except Exception:
            pass
        server.close()
        print("Servidor encerrado")


if __name__ == "__main__":
    # Exemplo: python tcp_server.py [port]
    port = 8000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    run_server(port)
