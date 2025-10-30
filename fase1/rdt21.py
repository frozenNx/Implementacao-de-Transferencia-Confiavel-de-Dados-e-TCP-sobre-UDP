
"""
RDT 2.1 - Reliable Data Transfer com sequência e ACK/NAK numerados
------------------------------------------------------------------
Objetivo:
Corrigir duplicações causadas por retransmissões indevidas (erro do 2.0).

Características:
 - Usa número de sequência (0/1) alternado.
 - ACKs e NAKs contêm número de sequência esperado.
 - Corrupção de dados e ACKs são tratados.
"""

import socket
import struct
import hashlib
import time
from utils.simulator import UnreliableChannel

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2

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
        raise ValueError("Pacote muito curto para análise.")
    pkt_type, seqnum, chksum = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
    data = packet[HEADER_SIZE:]
    return pkt_type, seqnum, chksum, data


# ==============================================================
# Emissor
# ==============================================================

class RDT21Sender:
    """Emissor do protocolo RDT 2.1."""

    def __init__(self, simulator: UnreliableChannel, local_port=12000,
                 dest=('localhost', 12001), timeout=2.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.seq = 0  # Número de sequência atual (0 ou 1)
        self.timeout = timeout
        self.retransmissions = 0  # contador de retransmissões

    def packet_header_size(self):
        return 1 + 1 + 8

    def send(self, msg: str):
        data = msg.encode()
        packet = make_packet(TYPE_DATA, self.seq, data)
        attempts = 0

        while True:
            attempts += 1
            print(f"[SENDER] Enviando (seq={self.seq}): {msg} (tentativa {attempts})")
            self.simulator.send(packet, self.sock, self.dest)

            try:
                self.sock.settimeout(self.timeout)
                resp, _ = self.sock.recvfrom(1024)
                pkt_type, ack_seq, _, _ = parse_packet(resp)

                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    print(f"[SENDER] ACK recebido (seq={ack_seq})")
                    self.seq = 1 - self.seq
                    break
                elif pkt_type == TYPE_NAK:
                    print("[SENDER] NAK recebido → retransmitindo")
                    self.retransmissions += 1
                else:
                    print("[SENDER] ACK inválido/corrompido → retransmitindo")
                    self.retransmissions += 1

            except socket.timeout:
                print("[SENDER] Timeout → retransmitindo")
                self.retransmissions += 1



# ==============================================================
# Receptor
# ==============================================================

class RDT21Receiver:
    """Receptor do protocolo RDT 2.1."""

    def __init__(self, local_port=12001):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.expected_seq = 0  # Próximo número de sequência esperado
        self.received = []     # Lista de mensagens entregues à aplicação
        self.last_ack_seq = 1  # Último ACK enviado
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

            calc_chksum = checksum(data)
            if recv_chksum != calc_chksum:
                print("[RECEIVER] Pacote Corrompido -> enviando NAK")
                self.sock.sendto(make_packet(TYPE_NAK, seqnum, b''), addr)
                continue

            if pkt_type == TYPE_DATA:
                if seqnum == self.expected_seq:
                    msg = data.decode(errors='replace')
                    print(f"[RECEIVER] Pacote OK (seq={seqnum}): {msg}")
                    self.received.append(msg)
                    # Envia ACK e alterna sequência esperada
                    ack = make_packet(TYPE_ACK, seqnum, b'')
                    self.sock.sendto(ack, addr)
                    self.last_ack_seq = seqnum
                    self.expected_seq = 1 - self.expected_seq
                else:
                    # Pacote duplicado → reenvia último ACK
                    print(f"[RECEIVER] Pacote duplicado (seq={seqnum}) → reenviando ACK(seq={self.last_ack_seq})")
                    ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                    self.sock.sendto(ack, addr)
            else:
                print("[RECEIVER] Recebeu pacote não-DATA no receptor → ignorando")

    def stop(self):
        self.running = False
        try:
            self.sock.close()
        except:
            pass