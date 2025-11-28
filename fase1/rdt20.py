"""
===========================================================
RDT 2.0 – Stop-and-Wait (UDP com canal não confiável)
===========================================================

Implementação da versão 2.0 do protocolo RDT (Reliable Data
Transfer). O RDT 2.0 detecta corrupção usando checksum e
usa mensagens NAK para solicitar retransmissão.

Características principais:
    - Stop-and-Wait (um pacote por vez).
    - ACK confirma entrega correta.
    - NAK solicita retransmissão.
    - Usa UnreliableChannel que simula perda e corrupção.

Classes:
    RDT20Sender   – Emissor RDT 2.0.
    RDT20Receiver – Receptor RDT 2.0.
"""

import time
from typing import List, Optional

from utils.packet import Packet
from utils.logger import Logger
from utils.simulator import UnreliableChannel


# ======================================================================
#                                SENDER
# ======================================================================

class RDT20Sender:
    """
    Emissor RDT 2.0 utilizando UnreliableChannel (Stop-and-Wait).

    Parameters
    ----------
    channel : UnreliableChannel
        Canal não confiável do sender para o receiver.
    logger : Logger
        Instância de logger para mensagens de depuração.
    timeout : float
        Tempo máximo de espera por ACK antes de retransmitir.
    """

    def __init__(self, channel: UnreliableChannel,
                 logger: Logger, timeout: float = 2.0) -> None:
        self.channel = channel
        self.logger = logger
        self.timeout = float(timeout)

        self.seq: int = 0
        self.retransmissions: int = 0

    # ------------------------------------------------------------------

    def send_message(self, payload: str) -> bool:
        """
        Envia um payload único e bloqueia até receber ACK correspondente.

        Processo Stop-and-Wait:
            1. Envia DATA(seq)
            2. Aguarda ACK(seq) ou NAK
            3. Retransmite se: timeout, NAK ou ACK corrompido
            4. Se ACK válido chega, avança seq e retorna True

        Parameters
        ----------
        payload : str
            Conteúdo textual a ser enviado.

        Returns
        -------
        bool
            True se o ACK correto for recebido.
        """
        packet = Packet.make_data(self.seq, payload.encode("utf-8"))

        while True:
            # ----------------------------------------------------------
            # (1) Envio do pacote DATA
            # ----------------------------------------------------------
            self.logger.send(f"Enviando pacote seq={self.seq}.")
            self.channel.send(packet)
            send_time = time.time()

            # ----------------------------------------------------------
            # (2) Aguarda ACK/NAK durante o timeout
            # ----------------------------------------------------------
            while time.time() - send_time < self.timeout:
                received = self.channel.recv()

                if received is None:
                    time.sleep(0.01)
                    continue

                # Pacote corrompido → retransmitir
                if received.is_corrupt():
                    self.logger.corrupt(
                        "ACK/NAK corrompido recebido no sender. Retransmitindo."
                    )
                    self.retransmissions += 1
                    break

                # NAK explícito
                if not (received.flags & Packet.FLAG_ACK):
                    self.logger.recv(
                        f"NAK recebido para seq={self.seq}. Retransmitindo."
                    )
                    self.retransmissions += 1
                    break

                # ACK correto
                if received.ack_num == self.seq:
                    self.logger.recv(f"ACK válido recebido para seq={self.seq}.")
                    self.seq += 1
                    return True

                # ACK inesperado (duplicado ou atrasado)
                self.logger.info(
                    f"ACK inesperado recebido ack_num={received.ack_num}; "
                    f"esperado={self.seq}. Ignorando."
                )

            # ----------------------------------------------------------
            # (3) Timeout → retransmitir
            # ----------------------------------------------------------
            if time.time() - send_time >= self.timeout:
                self.logger.timeout(
                    f"Timeout aguardando ACK de seq={self.seq}. Retransmitindo."
                )
                self.retransmissions += 1


# ======================================================================
#                                RECEIVER
# ======================================================================

class RDT20Receiver:
    """
    Receptor RDT 2.0.

    Mantém estado de recepção:
        expected_seq : seq esperado para próxima entrega
        delivered    : lista das mensagens entregues à aplicação
        last_ack_packet : último ACK enviado, usado para duplicatas
    """

    def __init__(self, channel: UnreliableChannel, logger: Logger) -> None:
        self.channel = channel
        self.logger = logger

        self.expected_seq: int = 0
        self.delivered: List[str] = []
        self.last_ack_packet: Optional[Packet] = None

    # ------------------------------------------------------------------

    def serve_once(self, blocking: bool = True,
                   wait_timeout: float = 0.5) -> bool:
        """
        Processa uma única iteração de recepção.

        Parameters
        ----------
        blocking : bool
            True  → aguarda até wait_timeout por um pacote.
            False → retorna imediatamente se canal estiver vazio.
        wait_timeout : float
            Tempo máximo de espera em modo bloqueante (segundos).

        Returns
        -------
        bool
            True se algum pacote (DATA ou NAK/duplicado) foi processado.
            False se nada foi recebido.
        """
        pkt = None

        # -----------------------------
        # (1) Recepção com ou sem bloqueio
        # -----------------------------
        if blocking:
            start = time.time()
            while time.time() - start < wait_timeout:
                pkt = self.channel.recv()
                if pkt is not None:
                    break
                time.sleep(0.01)
        else:
            pkt = self.channel.recv()

        if pkt is None:
            return False

        # -----------------------------
        # (2) Ignorar ACKs (sentido oposto)
        # -----------------------------
        if pkt.flags & Packet.FLAG_ACK:
            return False

        # -----------------------------
        # (3) Pacote corrompido → enviar NAK
        # -----------------------------
        if pkt.is_corrupt():
            self.logger.corrupt(
                "Pacote corrompido detectado no receiver. Enviando NAK."
            )
            nak = Packet.make_nak(self.expected_seq)
            self.channel.send(nak)
            return True

        # -----------------------------
        # (4) Pacote correto e esperado
        # -----------------------------
        if pkt.seq_num == self.expected_seq:
            payload = pkt.data.decode("utf-8", errors="ignore")
            self.delivered.append(payload)

            self.logger.recv(
                f"Pacote seq={pkt.seq_num} entregue à aplicação: {payload!r}"
            )

            ack = Packet.make_ack(pkt.seq_num)
            self.channel.send(ack)
            self.logger.send(f"ACK enviado para seq={pkt.seq_num}")

            self.last_ack_packet = ack
            self.expected_seq += 1
            return True

        # -----------------------------
        # (5) Duplicata / fora de ordem
        # -----------------------------
        self.logger.info(
            f"Pacote fora de ordem/duplicado seq={pkt.seq_num} "
            f"(esperado={self.expected_seq}). Reenviando último ACK."
        )

        if self.last_ack_packet is not None:
            self.channel.send(self.last_ack_packet)

        return True
