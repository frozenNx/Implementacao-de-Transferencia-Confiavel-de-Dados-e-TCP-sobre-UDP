"""
utils/simulator.py
-----------------------------------
Simulador de canal não confiável para testar protocolos RDT, GBN, SR e TCP simplificado.
Simula perda, corrupção e atraso de pacotes.
"""

import random
import threading
import struct
from utils import logger


class UnreliableChannel:
    """
    Canal não confiável que simula perda, corrupção e atraso de pacotes.
    """

    def __init__(self, loss_rate=0.1, corrupt_rate=0.1, delay_range=(0.01, 0.5)):
        """
        Inicializa o canal.

        Args:
            loss_rate (float): probabilidade de perda de pacote (0.0 a 1.0)
            corrupt_rate (float): probabilidade de corrupção (0.0 a 1.0)
            delay_range (tuple): (min_delay, max_delay) em segundos
        """
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range

    def send(self, packet: bytes, dest, dest_addr=None):
        """
        Envia pacote através do canal não confiável.

        Compatível com:
        - Fase 1: 'dest' é um socket, e 'dest_addr' é o endereço (tupla)
        - Fase 2: 'dest' é uma função (callback local, ex: receiver.receive)

        Args:
            packet (bytes): pacote a ser enviado
            dest: socket (Fase 1) ou função callback (Fase 2)
            dest_addr (tuple | None): endereço de destino (usado apenas na Fase 1)
        """
        # Desempacota tipo e seqnum do header (para logging)
        pkt_type = packet[0]  # 1 byte tipo
        seqnum = struct.unpack('!I', packet[1:5])[0]  # 4 bytes seqnum

        # --- Simula perda ---
        if random.random() < self.loss_rate:
            logger.log_lost(seqnum, pkt_type)
            return

        # --- Simula corrupção ---
        if random.random() < self.corrupt_rate:
            packet = self._corrupt_packet(packet)
            logger.log_corrupt(seqnum, pkt_type)

        # --- Simula atraso ---
        delay = random.uniform(*self.delay_range)

        def send_action():
            # Envio local (callback direto)
            if callable(dest):
                dest(packet)
            # Envio real (via socket UDP)
            elif hasattr(dest, "sendto") and dest_addr is not None:
                dest.sendto(packet, dest_addr)
            else:
                raise TypeError("Destino inválido para envio no canal.")

            logger.log_sent(seqnum, pkt_type)

        # Executa envio com atraso simulado
        threading.Timer(delay, send_action).start()

    def _corrupt_packet(self, packet: bytes) -> bytes:
        """
        Corrompe 1 a 5 bytes aleatórios do pacote.

        Args:
            packet (bytes): pacote original

        Returns:
            bytes: pacote corrompido
        """
        packet_list = list(packet)
        num_corruptions = random.randint(1, 5)
        for _ in range(num_corruptions):
            idx = random.randint(0, len(packet_list) - 1)
            packet_list[idx] ^= 0xFF  # Inverte bits
        return bytes(packet_list)
