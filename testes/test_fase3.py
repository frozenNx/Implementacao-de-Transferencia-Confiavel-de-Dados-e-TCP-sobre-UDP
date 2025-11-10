"""
testes/test_fase3.py

Testes da Fase 3 (todos em um arquivo). Usa um InMemoryChannel
simulador local (loss/corrupt/delay) que registra endpoints por porta
e chama endpoint.on_channel_receive(packet, src_port).
"""

import threading
import time
import random
from typing import Dict, Tuple
from fase3.tcp_socket import SimpleTCPSocket, FLAG_SYN, FLAG_ACK, FLAG_FIN, TCP_HDR_SIZE
import sys

# ---------------------------
# In-memory unreliable channel
# ---------------------------
class InMemoryChannel:
    """
    Canal simples que roteia segmentos entre endpoints registrados por porta.
    Simula perda, corrupção (bit flip), e atraso assíncrono.
    API:
      - register(endpoint, port)
      - unregister(port)
      - send_to(dst_port, packet, src_port)
    """

    def __init__(self, loss_rate=0.0, corrupt_rate=0.0, delay_range=(0.0, 0.0)):
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range
        self._endpoints: Dict[int, object] = {}
        self._lock = threading.Lock()

    def register(self, endpoint, port: int):
        with self._lock:
            self._endpoints[int(port)] = endpoint

    def unregister(self, port: int):
        with self._lock:
            self._endpoints.pop(int(port), None)

    def _corrupt(self, packet: bytes) -> bytes:
        b = bytearray(packet)
        if len(b) == 0:
            return packet
        flips = random.randint(1, max(1, min(5, len(b))))
        for _ in range(flips):
            idx = random.randrange(len(b))
            b[idx] ^= 0xFF
        return bytes(b)

    def send_to(self, dst_port: int, packet: bytes, src_port: int):
        # simulate loss
        if random.random() < self.loss_rate:
            # dropped
            # print("[CHANNEL] packet lost")
            return
        # simulate corruption
        if random.random() < self.corrupt_rate:
            packet = self._corrupt(packet)
        # simulate delay
        delay = random.uniform(*self.delay_range)
        def deliver():
            time.sleep(delay)
            with self._lock:
                endpoint = self._endpoints.get(int(dst_port))
            if endpoint is None:
                return
            try:
                endpoint.on_channel_receive(packet, src_port)
            except Exception as e:
                print("Error delivering to endpoint:", e, file=sys.stderr)
        threading.Thread(target=deliver, daemon=True).start()

# ---------------------------
# Helpers (small)
# ---------------------------

def run_handshake_test():
    print("Teste 1: Handshake")
    ch = InMemoryChannel(loss_rate=0.0, corrupt_rate=0.0, delay_range=(0.0, 0.01))
    server_port = 5000
    client_port = 5001
    server = SimpleTCPSocket(server_port, channel=ch, verbose=False)
    client = SimpleTCPSocket(client_port, channel=ch, verbose=False)

    server.listen()
    # accept in background thread (server will set state when SYN arrives)
    t_server = threading.Thread(target=lambda: server.accept(timeout=5.0), daemon=True)
    t_server.start()
    time.sleep(0.01)
    client.connect(server_port, timeout=2.0)
    time.sleep(0.1)
    assert client.state == 'ESTABLISHED', f"cliente state {client.state}"
    assert server.state == 'ESTABLISHED', f"server state {server.state}"
    client.close()
    server.close()
    print("✓ Handshake OK\n")

def run_transfer_10kb_test():
    print("Teste 2: Transferência 10KB")
    ch = InMemoryChannel(loss_rate=0.0, corrupt_rate=0.0, delay_range=(0.0, 0.005))
    server_port = 5010
    client_port = 5011
    server = SimpleTCPSocket(server_port, channel=ch, verbose=False)
    client = SimpleTCPSocket(client_port, channel=ch, verbose=False)

    server.listen()
    # server accept and receive in background
    def server_task(holder):
        conn, addr = server.accept(timeout=5.0)
        buf = b''
        while True:
            chunk = server.recv(4096, timeout=1.0)
            if not chunk:
                break
            buf += chunk
        holder.append(buf)
        server.close()
    holder = []
    t = threading.Thread(target=server_task, args=(holder,), daemon=True)
    t.start()
    time.sleep(0.01)
    client.connect(server_port, timeout=2.0)
    data = b'x' * 10240
    client.send(data)
    # give server time to receive
    time.sleep(0.5)
    client.close()
    time.sleep(0.1)
    assert holder and holder[0] == data, f"server received len {len(holder[0]) if holder else 0}"
    print("✓ Transferência 10KB OK\n")

def run_flow_control_test():
    print("Teste 3: Controle de Fluxo (rwnd reduzido)")
    ch = InMemoryChannel(loss_rate=0.0, corrupt_rate=0.0, delay_range=(0.0, 0.01))
    server_port = 5020
    client_port = 5021
    server = SimpleTCPSocket(server_port, channel=ch, verbose=False)
    client = SimpleTCPSocket(client_port, channel=ch, verbose=False)

    server.listen()
    def server_task(holder):
        conn, addr = server.accept(timeout=5.0)
        # reduce recv window on connection object (the server-side socket)
        conn.recv_window = 1024
        buf = b''
        while True:
            chunk = conn.recv(512, timeout=1.0)
            if not chunk:
                break
            time.sleep(0.01)  # slow processing to enforce backpressure
            buf += chunk
        holder.append(buf)
        conn.close()
    holder = []
    t = threading.Thread(target=server_task, args=(holder,), daemon=True)
    t.start()
    time.sleep(0.01)
    client.connect(server_port, timeout=2.0)
    data = b'y' * 10240
    client.send(data)
    time.sleep(1.0)
    client.close()
    time.sleep(0.2)
    assert holder and holder[0] == data
    print("✓ Controle de fluxo OK\n")

def run_retransmission_test():
    print("Teste 4: Retransmissão com perda (20%)")
    ch = InMemoryChannel(loss_rate=0.2, corrupt_rate=0.0, delay_range=(0.0, 0.05))
    server_port = 5030
    client_port = 5031
    server = SimpleTCPSocket(server_port, channel=ch, verbose=False)
    client = SimpleTCPSocket(client_port, channel=ch, verbose=False)

    server.listen()
    holder = []
    def server_task():
        conn, addr = server.accept(timeout=8.0)
        buf = b''
        while True:
            chunk = conn.recv(4096, timeout=2.0)
            if not chunk:
                break
            buf += chunk
        holder.append(buf)
        conn.close()

    t = threading.Thread(target=server_task, daemon=True)
    t.start()
    time.sleep(0.02)
    client.connect(server_port, timeout=5.0)
    data = b'z' * 5120
    t0 = time.time()
    client.send(data)
    t_total = time.time() - t0
    client.close()
    time.sleep(0.5)
    assert holder and holder[0] == data, f"received len {len(holder[0]) if holder else 0}"
    print(f"✓ Retransmissão OK (tempo total ~{t_total:.2f}s)\n")

def run_fin_close_test():
    print("Teste 5: Encerramento correto (FIN handshake)")
    ch = InMemoryChannel(loss_rate=0.0, corrupt_rate=0.0, delay_range=(0.0, 0.01))
    server_port = 5040
    client_port = 5041
    server = SimpleTCPSocket(server_port, channel=ch, verbose=False)
    client = SimpleTCPSocket(client_port, channel=ch, verbose=False)

    server.listen()
    threading.Thread(target=lambda: server.accept(timeout=5.0), daemon=True).start()
    time.sleep(0.01)
    client.connect(server_port, timeout=2.0)
    time.sleep(0.05)
    client.close()
    time.sleep(0.05)
    assert client.state in ('CLOSED',), f"client state {client.state}"
    # server may be CLOSED after exchange
    print("✓ Encerramento OK\n")

if __name__ == "__main__":
    print("Executando testes Fase 3 (pode demorar alguns segundos)...\n")
    run_handshake_test()
    run_transfer_10kb_test()
    run_flow_control_test()
    run_retransmission_test()
    run_fin_close_test()
    print("Todos os testes concluídos.")
