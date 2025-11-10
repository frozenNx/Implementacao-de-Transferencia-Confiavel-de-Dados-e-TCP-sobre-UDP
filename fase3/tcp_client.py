"""
Exemplo de cliente que usa SimpleTCPSocket.
Exemplo de uso:
  python tcp_client.py localhost 8000
ou usar channel=UnreliableChannel(...)
"""

from typing import Optional
from fase3.tcp_socket import SimpleTCPSocket
from utils.simulator import UnreliableChannel
import sys
import time

def run_client(server_host='localhost', server_port=8000, channel: Optional[UnreliableChannel] = None):
    client = SimpleTCPSocket(0, verbose=True, channel=channel)
    client.connect((server_host, server_port))
    print(f"Conectado a {(server_host, server_port)}")
    try:
        msg = b"Hello server! " * 800  # ~12KB
        t0 = time.time()
        client.send(msg)
        t_total = time.time() - t0
        print(f"Enviado {len(msg)} bytes em {t_total:.3f}s")
        # tentar receber eco
        received = b''
        while len(received) < len(msg):
            part = client.recv(4096, timeout=2.0)
            if not part:
                break
            received += part
        print(f"Recebido {len(received)} bytes")
    finally:
        client.close()
        print("Cliente encerrado")

if __name__ == "__main__":
    host = 'localhost'
    port = 8000
    if len(sys.argv) >= 3:
        host = sys.argv[1]; port = int(sys.argv[2])
    run_client(host, port)