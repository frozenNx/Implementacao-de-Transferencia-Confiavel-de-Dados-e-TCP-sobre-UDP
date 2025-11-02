"""
RDT 3.0 - Reliable Data Transfer com tratamento de perda de pacotes
-------------------------------------------------------------------
Objetivo:
Adicionar tolerância a perdas de pacotes ou ACKs.

Características:
 - Temporizador (timeout) para retransmissões automáticas.
 - Suporte à perda e atraso de pacotes.
 - Alternância de sequência (0/1).
"""

import socket
from typing import Tuple
from utils.simulator import UnreliableChannel
from utils.packet import TYPE_DATA, TYPE_ACK, checksum, make_packet, parse_packet
from utils import logger


# =========================
# Emissor
# =========================
class RDT30Sender:
    """Emissor do protocolo RDT 3.0."""

    def __init__(
        self,
        simulator: UnreliableChannel,
        local_port: int = 13002,
        dest: Tuple[str, int] = ('localhost', 13001),
        timeout: float = 2.0
    ) -> None:
        """
        Inicializa o emissor RDT 3.0.

        Args:
            simulator (UnreliableChannel): canal não confiável
            local_port (int): porta local para bind
            dest (Tuple[str, int]): endereço do receptor (host, porta)
            timeout (float): tempo máximo de espera por ACK
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.seq = 0
        self.timeout = timeout
        self.last_packet = None
        self.retransmissions = 0

    def send(self, msg: str) -> None:
        """
        Envia mensagem confiável com retransmissão automática em caso de perda ou ACK corrompido.

        Args:
            msg (str): mensagem a ser enviada
        """
        data = msg.encode()
        packet = make_packet(TYPE_DATA, self.seq, data)
        self.last_packet = packet
        attempts = 0

        while True:
            attempts += 1
            if attempts > 1:
                self.retransmissions += 1

            logger.log_sent(seqnum=self.seq, pkt_type=TYPE_DATA)
            self.simulator.send(packet, self.sock, self.dest)

            try:
                self.sock.settimeout(self.timeout)
                resp, _ = self.sock.recvfrom(4096)
                pkt_type, ack_seq, recv_chksum, _ = parse_packet(resp)

                if recv_chksum != checksum(b''):
                    logger.log_corrupt(seqnum=ack_seq, pkt_type=pkt_type)
                    self.retransmissions += 1
                    continue

                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    logger.log_received(seqnum=ack_seq, pkt_type=TYPE_ACK)
                    self.seq = 1 - self.seq
                    break
                else:
                    logger.log_lost(seqnum=ack_seq, pkt_type=pkt_type)
                    self.retransmissions += 1

            except socket.timeout:
                logger.log_lost(seqnum=self.seq, pkt_type=TYPE_DATA)
                self.retransmissions += 1


# =========================
# Receptor
# =========================
class RDT30Receiver:
    """Receptor do protocolo RDT 3.0."""

    def __init__(self, local_port: int = 13001) -> None:
        """
        Inicializa o receptor RDT 3.0.

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
            except Exception:
                logger.log_corrupt(seqnum=0)
                continue

            if recv_chksum != checksum(data):
                logger.log_corrupt(seqnum=seqnum, pkt_type=pkt_type)
                ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                self.sock.sendto(ack, addr)
                continue

            if pkt_type == TYPE_DATA:
                if seqnum == self.expected_seq:
                    msg = data.decode(errors='replace')
                    logger.log_received(seqnum=seqnum, pkt_type=TYPE_DATA)
                    self.received.append(msg)
                    ack = make_packet(TYPE_ACK, seqnum, b'')
                    self.sock.sendto(ack, addr)
                    self.last_ack_seq = seqnum
                    self.expected_seq = 1 - self.expected_seq
                else:
                    # Pacote duplicado → reenvia último ACK
                    ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                    self.sock.sendto(ack, addr)
                    logger.log_sent(seqnum=self.last_ack_seq, pkt_type=TYPE_ACK)

    def stop(self) -> None:
        """Para o receptor e fecha o socket."""
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass
