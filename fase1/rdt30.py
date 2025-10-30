"""
RDT 3.0 - Reliable Data Transfer com tratamento de perda de pacotes
-------------------------------------------------------------------
Objetivo:
Adicionar tolerância a perdas (de dados ou ACKs).

Características:
 - Temporizador (timeout) para retransmissões automáticas.
 - Suporte à perda e atraso de pacotes.
 - Alternância de sequência (0/1).
"""

import socket 
import struct 
import hashlib 
import threading 
import time 
from utils.simulator import UnreliableChannel

TYPE_DATA = 0
TYPE_ACK = 1

HEADER_FMT = "!BB8s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


# ==============================================================
# Funções auxiliares
# ==============================================================

def checksum(data: bytes) -> bytes:
    """Calcula um checksum de 8 bytes (MD5 truncado)."""
    if isinstance(data, str):
        data = data.encode()
    return hashlib.md5(data).digest()[:8]


def make_packet(pkt_type: int, seqnum: int = 0, data: bytes = b'') -> bytes:
    """Pacote: [1B tipo][1B seq][8B checksum][dados]."""
    chksum = checksum(data)
    header = struct.pack(HEADER_FMT, pkt_type, seqnum, chksum)
    return header + data


def parse_packet(packet: bytes):
    """Desmonta pacote em (tipo, seq, checksum, dados)."""
    if len(packet) < HEADER_SIZE:
        raise ValueError("Pacote muito curto")
    pkt_type, seqnum, chksum = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
    data = packet[HEADER_SIZE:]
    return pkt_type, seqnum, chksum, data


# ==============================================================
# Emissor
# ==============================================================

class RDT30Sender:
    """Emissor do protocolo RDT 3.0."""

    def __init__(self, simulator: UnreliableChannel,
                 local_port=13002, dest=('localhost', 13001),
                 timeout=2.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.seq = 0  # alternating bit (0 ou 1)
        self.timeout = timeout
        self.last_packet = None
        self.retransmissions = 0  # ← contador de retransmissões

    def send(self, msg: str):
        data = msg.encode()
        packet = make_packet(TYPE_DATA, self.seq, data)
        self.last_packet = packet
        attempts = 0

        while True:
            attempts += 1
            if attempts > 1:
                self.retransmissions += 1

            print(f"[SENDER] Enviando (seq={self.seq}): {msg} (tentativa {attempts})")

            self.simulator.send(packet, self.sock, self.dest)

            self.sock.settimeout(self.timeout)
            try:
                resp, _ = self.sock.recvfrom(4096)
                pkt_type, ack_seq, recv_chksum, _ = parse_packet(resp)

                if recv_chksum != checksum(b''):
                    print("[SENDER] ACK corrompido → retransmitindo")
                    self.retransmissions += 1
                    continue

                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    print(f"[SENDER] ACK recebido (seq={ack_seq}) ✓")
                    self.seq = 1 - self.seq
                    break
                else:
                    print(f"[SENDER] ACK duplicado/incorreto → retransmitindo")
                    self.retransmissions += 1

            except socket.timeout:
                print("[SENDER] Timeout → retransmitindo pacote")
                self.retransmissions += 1

        print(f"[SENDER] Mensagem '{msg}' entregue com sucesso.\n")


# ==============================================================
# Receptor
# ==============================================================

class RDT30Receiver:
    """Receptor do protocolo RDT 3.0."""

    def __init__(self, local_port=13001):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.expected_seq = 0
        self.received = []
        self.last_ack_seq = 1  
        self.running = False

    def start(self):
        self.running = True
        while self.running:
            try:
                pkt, addr = self.sock.recvfrom(4096)
            except OSError:
                break

            try:
                pkt_type, seqnum, recv_chksum, data = parse_packet(pkt)
            except Exception as e:
                print("[RECEIVER] Pacote inválido recebido:", e)
                continue

            if recv_chksum != checksum(data):
                print("[RECEIVER] Pacote corrompido → reenviando último ACK")
                ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                self.sock.sendto(ack, addr)
                continue

            if pkt_type == TYPE_DATA:
                if seqnum == self.expected_seq:
                    msg = data.decode(errors='replace')
                    print(f"[RECEIVER] Pacote OK (seq={seqnum}): {msg}")
                    self.received.append(msg)
                    ack = make_packet(TYPE_ACK, seqnum, b'')
                    self.sock.sendto(ack, addr)
                    self.last_ack_seq = seqnum
                    self.expected_seq = 1 - self.expected_seq
                else:
                    print(f"[RECEIVER] Pacote duplicado (seq={seqnum}) → reenviando ACK({self.last_ack_seq})")
                    ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                    self.sock.sendto(ack, addr)

    def stop(self):
        self.running = False
        try:
            self.sock.close()
        except:
            pass