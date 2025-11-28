"""
===========================================================
Módulo: simulator.py
===========================================================

Implementa um canal de comunicação não confiável sobre
UDP real, capaz de simular:

    - Perda de pacotes
    - Corrupção de bits
    - Atrasos naturais (por timeout)
    - Logging detalhado de eventos

Este módulo é utilizado pelos protocolos RDT (2.0, 2.1,
3.0) para testar seu comportamento em ambiente adverso.

Cada instância cria um socket UDP real, com endereço local
e remoto, permitindo testes completos de fim a fim.
===========================================================
"""

import socket
import random
from utils.packet import Packet
from utils.logger import Logger


class UnreliableChannel:
    """
    Representa um canal de comunicação UDP com possibilidade de:

        - Perder pacotes (loss_prob)
        - Corromper pacotes (corrupt_prob)
        - Registrar logs detalhados

    O canal é unidirecional. Para comunicação bidirecional,
    devem ser criados dois canais independentes.

    Args:
        local_addr (tuple): (host, porta) do socket local.
        remote_addr (tuple): (host, porta) do destino.
        loss_prob (float): probabilidade de perda (0.0–1.0).
        corrupt_prob (float): probabilidade de corrupção (0.0–1.0).
        logger (Logger | None): logger opcional.
    """

    def __init__(
        self,
        local_addr: tuple,
        remote_addr: tuple,
        loss_prob: float = 0.0,
        corrupt_prob: float = 0.0,
        logger: Logger | None = None,
    ):
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        self.loss_prob = loss_prob
        self.corrupt_prob = corrupt_prob

        self.logger = logger or Logger(prefix="channel", origin="CHANNEL")

        # Socket UDP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)
        self.sock.settimeout(1.0)

        self.logger.info(
            f"Canal iniciado em {local_addr}, remoto={remote_addr}, "
            f"loss={loss_prob * 100:.1f}%, corrupt={corrupt_prob * 100:.1f}%."
        )

    # ============================================================ #
    # Envio
    # ============================================================ #
    def send(self, packet: Packet):
        """
        Envia um pacote pelo canal.

        Pode aplicar:
            - Perda simulada
            - Corrupção simulada

        Args:
            packet (Packet): Pacote já construído.
        """
        # Perda simulada
        if random.random() < self.loss_prob:
            self.logger.loss(
                f"Pacote seq={packet.seq_num} perdido (simulado)."
            )
            return

        # Serializa
        raw = packet.to_bytes()

        # Corrupção simulada
        if random.random() < self.corrupt_prob and len(raw) > 0:
            idx = random.randint(0, len(raw) - 1)
            corrupted = bytearray(raw)
            corrupted[idx] ^= 0xFF
            raw = bytes(corrupted)
            self.logger.corrupt(
                f"Pacote seq={packet.seq_num} corrompido (simulado)."
            )
        else:
            self.logger.send(
                f"Pacote seq={packet.seq_num} enviado."
            )

        # Envio real
        self.sock.sendto(raw, self.remote_addr)

    # ============================================================ #
    # Recebimento
    # ============================================================ #
    def recv(self) -> Packet | None:
        """
        Aguarda e recebe um pacote UDP.

        Returns:
            Packet | None:
                - Pacote válido
                - None em caso de timeout
                - None se o pacote estiver corrompido
        """
        try:
            raw, addr = self.sock.recvfrom(2048)
            pkt = Packet.from_bytes(raw)

            self.logger.recv(
                f"Pacote recebido de {addr}: seq={pkt.seq_num}"
            )

            if pkt.is_corrupt():
                self.logger.corrupt(
                    f"Pacote seq={pkt.seq_num} corrompido (detectado)."
                )
                return None

            return pkt

        except socket.timeout:
            return None
        
        except OSError:
            return None

    # ============================================================ #
    # Fechamento
    # ============================================================ #
    def close(self):
        """
        Fecha o socket e o logger.

        Pode ser chamado múltiplas vezes.
        """
        self.logger.info("Encerrando canal.")
        try:
            self.sock.close()
        except Exception:
            pass
        finally:
            self.logger.close()

    # ============================================================ #
    # Suporte ao uso com 'with'
    # ============================================================ #
    def __enter__(self):
        """Permite uso com 'with UnreliableChannel() as chan:'."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Fecha o canal ao sair de um bloco 'with'."""
        self.close()
