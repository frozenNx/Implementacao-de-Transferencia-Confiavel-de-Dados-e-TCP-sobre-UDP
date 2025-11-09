"""
RDT 2.0 - Reliable Data Transfer com detecção de erro
-----------------------------------------------------
Objetivo: Implementar comunicação confiável sobre um canal que
pode corromper pacotes, mas não os perder.

Utiliza ACK e NAK para controle de erros.
"""

import threading
import socket
from typing import Tuple
from utils.simulator import UnreliableChannel
# Importação da função de validação é crucial aqui
from utils.packet import TYPE_DATA, TYPE_ACK, TYPE_NAK, make_packet, validate_packet 
from utils import logger


# =========================
# Emissor (Sender) - FSM: Wait for call from above; Wait for ACK or NAK
# =========================
class RDT20Sender:
    """Emissor do protocolo RDT 2.0 (Canal com erros, sem perdas)."""

    def __init__(
        self,
        simulator: UnreliableChannel,
        local_port: int = 10000,
        dest: Tuple[str, int] = ('localhost', 10001)
    ) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.retransmissions = 0
        # RDT 2.0 não usa timeout (não há perdas), espera indefinidamente.
        self.sock.settimeout(None) 

    def send(self, msg: str) -> None:
        data = msg.encode()
        seqnum = 0 # SeqNum fixo, pois não há risco de ACK/NAK corrompido (ainda)
        packet = make_packet(TYPE_DATA, seqnum=seqnum, data=data)

        while True:
            logger.log_sent(seqnum=seqnum, pkt_type=TYPE_DATA)
            self.simulator.send(packet, self.sock, self.dest)
            
            # --- Espera Bloqueante por ACK ou NAK ---
            try:
                ack_pkt, _ = self.sock.recvfrom(1024)
                
                # O ACK/NAK TAMBÉM pode estar corrompido!
                # Note: RDT 2.0 não consegue lidar com ACKs/NAKs corrompidos,
                # o remetente fica preso. Essa falha será corrigida no RDT 2.1.
                
                # Tenta validar o pacote (se corrompido, validate_packet retornará False)
                # No RDT 2.0, ACKs/NAKs corrompidos são tratados como NAKs.
                pkt_type, recv_seq, _, is_valid = validate_packet(ack_pkt)

                if is_valid and pkt_type == TYPE_ACK:
                    logger.log_received(seqnum=recv_seq, pkt_type=TYPE_ACK)
                    break # Sucesso: Sai do loop e aceita novos dados
                elif is_valid and pkt_type == TYPE_NAK:
                    logger.log_received(seqnum=recv_seq, pkt_type=TYPE_NAK)
                    logger.log_retransmit(seqnum, TYPE_DATA)
                    self.retransmissions += 1
                    # Continua o loop: reenvia o pacote
                else:
                    # Pacote de controle (ACK/NAK) corrompido (falha do RDT 2.0)
                    # No RDT 2.0, não há como saber o que enviar.
                    # Simplesmente retransmitimos, tratando-o como NAK.
                    logger.log_corrupt(seqnum=recv_seq, pkt_type=pkt_type)
                    logger.log_retransmit(seqnum, TYPE_DATA)
                    self.retransmissions += 1

            except socket.error as e:
                # Se acontecer algum erro de socket não relacionado a timeout, logar
                logger.error(f"Erro de socket inesperado: {e}")
                break


# =========================
# Receptor (Receiver) - FSM: Wait for 0 from below
# =========================
class RDT20Receiver:
    def __init__(self, local_port: int = 10001) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.received = []
        self.expected_seq = 0
        self._running = False
        self._thread = None

    def start(self) -> None:
        self._running = True
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()

    def _receive_loop(self) -> None:
        while self._running:
            try:
                pkt, addr = self.sock.recvfrom(1024)
            except OSError:
                break # socket foi fechado

            # --- Validação do Pacote (Check Integridade) ---
            # Usa validate_packet para verificar se o pacote DATA está corrompido.
            pkt_type, seqnum_rcv, data, is_valid = validate_packet(pkt)
            
            # --- Se o pacote DATA estiver íntegro ---
            if is_valid and pkt_type == TYPE_DATA:
                msg = data.decode(errors="replace")
                logger.log_received(seqnum=seqnum_rcv, pkt_type=TYPE_DATA)
                self.received.append(msg)
                
                # Ação: Entrega à aplicação e Envia ACK
                ack_pkt = make_packet(TYPE_ACK, seqnum=self.expected_seq) # O seqnum é irrelevante, mas usamos 0
                self.sock.sendto(ack_pkt, addr)
                
            # --- Se o pacote DATA estiver corrompido ---
            else:
                logger.log_corrupt(seqnum=seqnum_rcv, pkt_type=TYPE_DATA)
                
                # Ação: Envia NAK
                nak_pkt = make_packet(TYPE_NAK, seqnum=self.expected_seq) # O seqnum é irrelevante
                self.sock.sendto(nak_pkt, addr)

    def stop(self) -> None:
        self._running = False
        self.sock.close()
        if self._thread:
            self._thread.join()

    def get_all_messages(self) -> list:
        return self.received