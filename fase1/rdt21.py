"""
RDT 2.1 - Reliable Data Transfer com sequência e ACK/NAK numerados
------------------------------------------------------------------
Objetivo:
Corrigir duplicações causadas por retransmissões indevidas (erro do RDT 2.0).

Características:
 - Usa número de sequência (0/1) alternado.
 - ACKs e NAKs contêm número de sequência esperado.
 - Corrupção de dados e ACKs são tratados.
"""

import socket
from typing import Tuple
from utils.simulator import UnreliableChannel
from utils.packet import TYPE_DATA, TYPE_ACK, TYPE_NAK, checksum, make_packet, parse_packet
from utils import logger


# =========================
# Emissor
# =========================
class RDT21Sender:
    """Emissor do protocolo RDT 2.1."""

    def __init__(self, simulator: UnreliableChannel, local_port=12000,
                 dest=('localhost', 12001), timeout=2.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.seq = 0
        self.timeout = timeout
        self.retransmissions = 0

    def packet_header_size(self) -> int:
        """
        Retorna o tamanho do cabeçalho do pacote (bytes extras por mensagem).

        - 1 byte tipo
        - 1 byte seqnum
        - 8 bytes checksum (MD5 truncado)
        """
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
                else:
                    print("[SENDER] NAK ou ACK inválido → retransmitindo")
                    self.retransmissions += 1
            except socket.timeout:
                print("[SENDER] Timeout → retransmitindo")
                self.retransmissions += 1

# =========================
# Receptor
# =========================
class RDT21Receiver:
    """Receptor do protocolo RDT 2.1."""

    def __init__(self, local_port: int = 12001) -> None:
        """
        Inicializa o receptor RDT 2.1.

        Args:
            local_port (int): porta local para bind
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.expected_seq = 0
        self.received = []
        self.last_ack_seq = 1
        self.running = False

    def start(self) -> None:
        """Inicia loop de recepção de pacotes."""
        self.running = True
        while self.running:
            try:
                pkt, addr = self.sock.recvfrom(4096)
            except OSError:
                break

            try:
                pkt_type, seqnum, recv_chksum, data = parse_packet(pkt)
            except Exception as e:
                logger.log_corrupt(seqnum=0)
                continue

            if recv_chksum != checksum(data):
                logger.log_corrupt(seqnum=seqnum, pkt_type=pkt_type)
                nak_pkt = make_packet(TYPE_NAK, seqnum, b'')
                self.sock.sendto(nak_pkt, addr)
                continue

            if pkt_type == TYPE_DATA:
                if seqnum == self.expected_seq:
                    msg = data.decode(errors='replace')
                    logger.log_received(seqnum=seqnum, pkt_type=TYPE_DATA)
                    self.received.append(msg)
                    ack_pkt = make_packet(TYPE_ACK, seqnum, b'')
                    self.sock.sendto(ack_pkt, addr)
                    self.last_ack_seq = seqnum
                    self.expected_seq = 1 - self.expected_seq
                else:
                    # Pacote duplicado → reenvia último ACK
                    ack_pkt = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                    self.sock.sendto(ack_pkt, addr)
                    logger.log_sent(seqnum=self.last_ack_seq, pkt_type=TYPE_ACK)
            else:
                # Ignora pacotes não-DATA
                continue

    def stop(self) -> None:
        """Para o receptor e fecha o socket."""
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass