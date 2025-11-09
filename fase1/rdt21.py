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
import threading
from typing import Tuple
from utils.simulator import UnreliableChannel
from utils.packet import TYPE_DATA, TYPE_ACK, TYPE_NAK, HEADER_SIZE, make_packet, validate_packet
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
        self.seq = 0  # Próximo número de sequência a ser enviado (0 ou 1)
        self.timeout = timeout
        self.retransmissions = 0
        self.last_sent_packet = None
        # O sender RDT 2.1 deve ter um estado, mas vamos usar 'seq' como estado implícito

    def packet_header_size(self) -> int:
        """
        Retorna o tamanho do cabeçalho do pacote (HEADER_SIZE de utils/packet.py).
        """
        return HEADER_SIZE

    def send(self, msg: str):
        data = msg.encode()
        
        # 1. Cria e armazena o pacote
        packet = make_packet(TYPE_DATA, self.seq, data)
        self.last_sent_packet = packet
        attempts = 0

        while True:
            attempts += 1
            
            # 2. Envia o pacote (retransmissão se attempts > 1)
            self.simulator.send(self.last_sent_packet, self.sock, self.dest)
            logger.log_sent(self.seq, TYPE_DATA)
            
            try:
                self.sock.settimeout(self.timeout)
                resp, _ = self.sock.recvfrom(1024)
                
                # 3. Valida a Resposta
                pkt_type, ack_seq, _, is_valid = validate_packet(resp)

                # 3a. Pacote de controle Corrompido
                if not is_valid:
                    logger.info("[SND] Pacote de controle corrompido. Retransmitindo.")
                    self.retransmissions += 1
                    continue # Volta ao início do loop para retransmitir
                
                # 3b. NAK: Recebeu NAK (NAK tem o SeqNum do pacote esperado)
                if pkt_type == TYPE_NAK:
                    logger.info("[SND] NAK recebido. Retransmitindo.")
                    self.retransmissions += 1
                    continue # Volta ao início do loop para retransmitir
                
                # 3c. ACK CORRETO: Recebeu ACK válido para o pacote atual
                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    logger.log_received(ack_seq, TYPE_ACK)
                    
                    # Ação: Avança para o próximo estado/seq
                    self.seq = 1 - self.seq
                    break # SUCCESS: Sai do loop e passa para a próxima mensagem
                
                # 3d. ACK Duplicado ou SeqNum Errado
                if pkt_type == TYPE_ACK and ack_seq != self.seq:
                    # Este é o ACK do pacote anterior. O Receptor o reenvia se receber uma duplicata.
                    logger.info(f"[SND] ACK duplicado/errado recebido (seq={ack_seq}). Ignorando.")
                    # Continua esperando o ACK correto para self.seq.
                    # No RDT 2.1, ignorar é a ação mais segura, pois o Sender ainda está no estado de espera.
                    continue
                         
            except socket.timeout:
                # O timeout só deveria ser usado no RDT 3.0, mas já que está aqui,
                # força a retransmissão se a resposta foi perdida/demorou (erro do canal não tratado pelo 2.1)
                logger.log_timeout(self.seq)
                self.retransmissions += 1
                # Continua o loop para retransmitir

# =========================
# Receptor
# =========================
class RDT21Receiver:
    """Receptor do protocolo RDT 2.1. Usa números de sequência para evitar duplicação."""

    def __init__(self, local_port: int = 12001) -> None:
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.expected_seq = 0  # O número de sequência do pacote DATA que o receptor espera
        self.received = []
        self.running = False
        self._thread = None 

    def start(self) -> None:
        """Inicia thread de recepção de pacotes."""
        self.running = True
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()

    def _receive_loop(self) -> None:
        while self.running:
            try:
                self.sock.settimeout(1.0) 
                pkt, addr = self.sock.recvfrom(4096)
            except socket.timeout:
                continue 
            except OSError:
                break 

            # --- Validação do Pacote ---
            pkt_type, seqnum, data, is_valid = validate_packet(pkt)
            
            # --- Se Não for DATA, Ignora ---
            if pkt_type != TYPE_DATA:
                logger.info(f"[RCV] Recebeu pacote de controle ({pkt_type}). Ignorando.")
                continue

            # 1. Pacote DATA Corrompido
            if not is_valid:
                logger.log_corrupt(seqnum=seqnum if seqnum is not None else -1, pkt_type=TYPE_DATA)
                
                # Ação: Envia NAK para o número de sequência que está esperando (expected_seq)
                nak_pkt = make_packet(TYPE_NAK, self.expected_seq, b'')
                self.sock.sendto(nak_pkt, addr)
                logger.log_sent(self.expected_seq, TYPE_NAK)
                continue

            # 2. Pacote DATA Íntegro e é o esperado (seqnum == expected_seq)
            if seqnum == self.expected_seq:
                msg = data.decode(errors='replace')
                logger.log_received(seqnum=seqnum, pkt_type=TYPE_DATA)
                self.received.append(msg)
                
                # Ação: Envia ACK para o pacote recebido (seqnum)
                ack_pkt = make_packet(TYPE_ACK, seqnum, b'')
                self.sock.sendto(ack_pkt, addr)
                logger.log_sent(seqnum=seqnum, pkt_type=TYPE_ACK)
                
                # Atualiza o estado: Espera pelo próximo
                self.expected_seq = 1 - self.expected_seq
                
            # 3. Pacote DATA Íntegro, mas duplicado (seqnum != expected_seq)
            else:
                ack_seq_to_send = 1 - self.expected_seq # Este é o SeqNum do pacote ANTERIOR que o Emissor pode estar esperando
                logger.info(f"[RCV] Pacote duplicado (recebido={seqnum}, esperado={self.expected_seq}). Reenviando ACK {ack_seq_to_send}.")
                
                # Ação: Reenvia ACK do pacote JÁ ENTREGUE (1 - expected_seq)
                ack_pkt = make_packet(TYPE_ACK, ack_seq_to_send, b'')
                self.sock.sendto(ack_pkt, addr)
                logger.log_sent(seqnum=ack_seq_to_send, pkt_type=TYPE_ACK)
    
    def get_all_messages(self) -> list:
        """Retorna a lista de mensagens entregues à camada de aplicação."""
        return self.received

    def stop(self) -> None:
        """Para o receptor e fecha o socket."""
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass
        if self._thread and threading.current_thread() != self._thread:
             self._thread.join()