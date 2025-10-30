"""
RDT 2.0 - Reliable Data Transfer com detecção de erro
-----------------------------------------------------
Objetivo:
Implementar comunicação confiável sobre um canal não confiável que
pode corromper pacotes, mas não os perder.

Características:
 - Utiliza checksum (MD5 truncado) para detectar corrupção.
 - O receptor envia ACK (confirmação) ou NAK (erro).
 - O emissor retransmite mensagens em caso de NAK ou timeout.

Limitações:
 - Não trata perda de pacotes ou ACKs (corrigido no RDT 3.0).
 - Pode haver duplicação em cenários específicos.
"""

import socket
import hashlib
import struct
import time
from utils.simulator import UnreliableChannel

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2


# ==============================================================
# Funções auxiliares
# ==============================================================

def checksum(data: bytes) -> bytes:
    """Calcula um checksum de 8 bytes (MD5 truncado)."""
    if isinstance(data, str):
        data = data.encode()
    chksum = hashlib.md5(data).hexdigest()[:8].encode()  # 8 bytes
    return chksum.ljust(8, b'\x00')


def make_packet(pkt_type: int, data: bytes = b'') -> bytes:
    """Monta um pacote no formato: [1B tipo][8B checksum][dados]."""
    chksum = checksum(data)
    header = struct.pack("!B8s", pkt_type, chksum)
    return header + data


def parse_packet(packet: bytes):
    """Desmonta o pacote em (tipo, checksum, dados)."""
    pkt_type, chksum = struct.unpack("!B8s", packet[:9])
    data = packet[9:]
    return pkt_type, chksum, data


# ==============================================================
# Emissor
# ==============================================================

class RDT20Sender:
    """Emissor do protocolo RDT 2.0."""

    def __init__(self, simulator: UnreliableChannel, local_port=10000, dest=('localhost', 10001)):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.retransmissions = 0

    def packet_header_size(self):
        return 1 + 8

    def send(self, msg: str):
        data = msg.encode()
        packet = make_packet(TYPE_DATA, data)

        while True:
            print("[SENDER] Eviando:", msg)
            self.simulator.send(packet, self.sock, self.dest)
            try:
                self.sock.settimeout(2)
                ack_pkt, _ = self.sock.recvfrom(1024)
                pkt_type, _, _ = parse_packet(ack_pkt)

                if pkt_type == TYPE_ACK:
                    print("[SENDER] ACK recebido")
                    break
                else:
                    print("[SENDER] NAK recebido → retransmitindo")
                    self.retransmissions += 1
            except socket.timeout:
                print("[SENDER] Timeout → retransmitindo")
                self.retransmissions += 1


# ==============================================================
# Receptor
# ==============================================================

class RDT20Receiver:
    """Receptor do protocolo RDT 2.0."""

    def __init__(self, local_port=10001):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.received = []

    def start(self):
        while True:
            pkt, addr = self.sock.recvfrom(1024)
            pkt_type, chksum, data = parse_packet(pkt)

            if checksum(data) == chksum:
                try:
                    msg = data.decode()
                except UnicodeDecodeError:
                    msg = "<dados corrompidos>"
                print("[RECEIVER] Pacote OK:", msg)
                self.received.append(msg)
                self.sock.sendto(make_packet(TYPE_ACK), addr)
            else:
                print("[RECEIVER] Pacote Corrompido -> enviando NAK")
                # Envia NAK
                self.sock.sendto(make_packet(TYPE_NAK), addr)