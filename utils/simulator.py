"""
utils/simulator.py
-----------------------------------
Simulador de canal não confiável para testar protocolos RDT, GBN, SR e TCP simplificado.
Simula perda, corrupção e atraso de pacotes.
"""

import random
import threading
import time
import struct
from utils.packet import TYPE_DATA, TYPE_ACK, TYPE_NAK, HEADER_FMT, HEADER_SIZE
from utils import logger


class UnreliableChannel:
    """
    Canal não confiável que simula perda, corrupção e atraso de pacotes.
    """

    def __init__(self, loss_rate=0.1, corrupt_rate=0.1, delay_range=(0.01, 0.5)):
        """Inicializa o canal."""
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range
        
        # Variáveis para roteamento GBN/SR/Local
        self.gbn_sender = None
        self.gbn_receiver = None

    def register_gbn_endpoints(self, sender, receiver):
        """
        Registra os pontos finais do GBN (Sender e Receiver) para que 
        o canal possa rotear pacotes DATA e ACK corretamente (Fase 2).
        """
        self.gbn_sender = sender
        self.gbn_receiver = receiver


    def send(self, packet: bytes, dest=None, dest_addr=None):
        """
        Envia pacote através do canal não confiável.
        
        Args:
            packet (bytes): pacote a ser enviado.
            dest: socket (Fase 1) ou None (Fase 2).
            dest_addr (tuple | None): endereço de destino (usado apenas na Fase 1).
        """
        
        # --- Extração segura do tipo e SeqNum para Logging ---
        pkt_type = None
        seqnum = -1
        try:
            # Desempaca usando o formato do cabeçalho definido em utils/packet.py
            # Pk_type (B), SeqNum (I), Checksum (8s)
            pkt_type, seqnum, _ = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
        except (struct.error, IndexError):
            # Não pudemos ler o header. Se o pacote não for perdido, ele será entregue corrompido.
            logger.info("Pacote com cabeçalho ilegível. Tratando como potencialmente corrompido.")

        # --- Simula perda ---
        if random.random() < self.loss_rate:
            logger.log_lost(seqnum if seqnum != -1 else 'UNK', pkt_type)
            return

        # --- Simula corrupção ---
        if random.random() < self.corrupt_rate:
            # A corrupção ocorre no canal e o Receptor deve detectá-la.
            # O log de corrupção deve ser feito no Receptor (CHK) quando detectado.
            # Aqui, apenas logamos que a corrupção foi induzida no canal (INFO).
            packet = self._corrupt_packet(packet)
            logger.info(f"Corrupção induzida no pacote Seq={seqnum}.")

        # --- Simula atraso ---
        delay = random.uniform(*self.delay_range)

        def send_action():
            time.sleep(delay)
            
            # --- ROTEAMENTO (Fase 2: GBN/SR - Simulação Local) ---
            if self.gbn_sender and self.gbn_receiver:
                if pkt_type == TYPE_DATA:
                    # Envia DATA para o Receptor
                    self.gbn_receiver.receive(packet)
                elif pkt_type == TYPE_ACK or pkt_type == TYPE_NAK:
                    # Envia ACK/NAK para o Emissor
                    self.gbn_sender.receive_control_packet(packet)
                else:
                    logger.warning(f"Tipo de pacote {pkt_type} desconhecido no roteador GBN.")
            
            # --- ROTEAMENTO (Fase 1: RDT - Sockets UDP) ---
            elif hasattr(dest, "sendto") and dest_addr is not None:
                try:
                    dest.sendto(packet, dest_addr)
                except Exception as e:
                    logger.error(f"Erro ao enviar via socket: {e}")
            else:
                logger.warning("Destino de pacote não roteável ou mal configurado.")


        # Executa envio com atraso simulado em uma nova thread
        threading.Thread(target=send_action, daemon=True).start()

    def _corrupt_packet(self, packet: bytes) -> bytes:
        """Corrompe 1 a 5 bytes aleatórios do pacote."""
        packet_list = bytearray(packet) # Usa bytearray para modificação eficiente
        num_corruptions = random.randint(1, 5)
        
        for _ in range(num_corruptions):
            idx = random.randint(0, len(packet_list) - 1)
            # Inverte bits: XOR com 0xFF
            packet_list[idx] ^= 0xFF 
        return bytes(packet_list)