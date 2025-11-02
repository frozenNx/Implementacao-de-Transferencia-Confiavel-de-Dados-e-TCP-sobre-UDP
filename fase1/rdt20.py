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

import threading
import socket
from typing import Tuple
from utils.simulator import UnreliableChannel
from utils.packet import TYPE_DATA, TYPE_ACK, TYPE_NAK, checksum, make_packet, parse_packet
from utils import logger


# =========================
# Emissor
# =========================
class RDT20Sender:
    """Emissor do protocolo RDT 2.0."""

    def __init__(
        self,
        simulator: UnreliableChannel,
        local_port: int = 10000,
        dest: Tuple[str, int] = ('localhost', 10001)
    ) -> None:
        """
        Inicializa o emissor RDT 2.0.

        Args:
            simulator (UnreliableChannel): canal não confiável
            local_port (int): porta local para bind
            dest (Tuple[str, int]): endereço do receptor (host, porta)
        """
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.retransmissions = 0

    def send(self, msg: str) -> None:
        """
        Envia mensagem confiável para o receptor.

        Args:
            msg (str): mensagem a ser enviada
        """
        data = msg.encode()
        seqnum = 0
        packet = make_packet(TYPE_DATA, seqnum=seqnum, data=data)

        while True:
            logger.log_sent(seqnum=seqnum, pkt_type=TYPE_DATA)
            self.simulator.send(packet, self.sock, self.dest)

            try:
                self.sock.settimeout(2.0)
                ack_pkt, _ = self.sock.recvfrom(1024)
                pkt_type, recv_seq, _, _ = parse_packet(ack_pkt)

                if pkt_type == TYPE_ACK and recv_seq == seqnum:
                    logger.log_received(seqnum=recv_seq, pkt_type=TYPE_ACK)
                    break
                else:
                    logger.log_received(seqnum=recv_seq, pkt_type=TYPE_NAK)
                    self.retransmissions += 1

            except socket.timeout:
                logger.log_lost(seqnum=seqnum, pkt_type=TYPE_DATA)
                self.retransmissions += 1


# =========================
# Receptor
# =========================
class RDT20Receiver:
    def __init__(self, local_port: int = 10001) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.received = []
        self._running = False  # flag de controle
        self._thread = None

    def start(self) -> None:
        """Inicia thread de recepção."""
        self._running = True
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()

    def _receive_loop(self) -> None:
        while self._running:
            try:
                pkt, addr = self.sock.recvfrom(1024)
            except OSError:
                # socket foi fechado
                break

            pkt_type, seqnum, chksum, data = parse_packet(pkt)

            if checksum(data) == chksum:
                msg = data.decode(errors="replace")
                logger.log_received(seqnum=0, pkt_type=pkt_type)
                self.received.append(msg)
                ack_pkt = make_packet(TYPE_ACK, seqnum=0)
                self.sock.sendto(ack_pkt, addr)
            else:
                logger.log_corrupt(seqnum=0, pkt_type=pkt_type)
                nak_pkt = make_packet(TYPE_NAK, seqnum=0)
                self.sock.sendto(nak_pkt, addr)

    def stop(self) -> None:
        """Para a thread de recepção e fecha o socket."""
        self._running = False
        self.sock.close()
        if self._thread:
            self._thread.join()