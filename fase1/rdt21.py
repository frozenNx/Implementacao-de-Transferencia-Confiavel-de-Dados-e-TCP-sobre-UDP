"""
===========================================================
RDT 2.1 — Alternating Bit Protocol (UDP não confiável)
===========================================================

Implementação do protocolo RDT 2.1, baseado em alternating-bit,
utilizando um UnreliableChannel que simula perda e corrupção.

Diferente do RDT 2.0:
    - Não utiliza NAK explícito.
    - Toda a sinalização é feita via ACK contendo o número do
      último pacote recebido corretamente (ACK cumulativo).
    - Duplicatas e pacotes fora de ordem são tratados com reenvio
      do último ACK válido.

Classes:
    RDT21Sender   — Emissor (alternating-bit)
    RDT21Receiver — Receptor (alternating-bit)
"""

from __future__ import annotations

import time
from typing import Optional

from utils.logger import Logger
from utils.packet import Packet
from utils.simulator import UnreliableChannel


# ======================================================================
#                               SENDER
# ======================================================================

class RDT21Sender:
    """
    Emissor do protocolo RDT 2.1 (alternating-bit).

    Máquina de estados do sender:
        - Possui um seqnum ∈ {0, 1}
        - Para enviar um pacote:
            1. Envia DATA(seqnum)
            2. Aguarda ACK(seqnum)
               - ACK válido → alterna seqnum e conclui
               - ACK corrompido → ignora e continua esperando
               - ACK inesperado → retransmite imediatamente
            3. Timeout → retransmite pkt

    Parameters
    ----------
    channel : UnreliableChannel
        Canal não confiável do sender → receiver.
    logger : Logger, optional
        Logger para eventos. Se None, é criado automaticamente.
    timeout : float
        Timeout (s) para retransmissão.
    """

    def __init__(
        self,
        channel: UnreliableChannel,
        logger: Optional[Logger] = None,
        timeout: float = 1.0,
    ) -> None:

        self.channel = channel
        self.logger = logger or Logger(prefix="RDT21-SENDER", origin="SENDER")
        self.seqnum = 0
        self.timeout = float(timeout)
        self.retransmissions = 0

    # ------------------------------------------------------------------

    def send(self, data: bytes) -> bool:
        """
        Envia `data` de forma confiável usando alternating-bit.

        Bloqueia até receber ACK correto ou continuar retransmitindo
        conforme timeout.

        Parameters
        ----------
        data : bytes
            Payload a ser enviado.

        Returns
        -------
        bool
            True se ACK(seqnum) for recebido.
        """
        pkt = Packet(seq_num=self.seqnum, ack_num=0, flags=0, data=data)

        while True:
            # ----------------------------------------------------------
            # (1) Envia ou retransmite DATA(seqnum)
            # ----------------------------------------------------------
            self.channel.send(pkt)
            self.logger.send(
                f"Pacote seq={self.seqnum} enviado "
                f"(payload_len={len(pkt.data)})."
            )
            send_time = time.time()

            # ----------------------------------------------------------
            # (2) Loop de espera por ACK(seqnum)
            # ----------------------------------------------------------
            while time.time() - send_time < self.timeout:
                ack = self.channel.recv()

                if ack is None:
                    time.sleep(0.01)
                    continue

                # ACK corrompido → ignorar
                if ack.is_corrupt():
                    self.logger.corrupt("ACK corrompido recebido; ignorando.")
                    continue

                # Esperamos somente ACKs
                if not (ack.flags & Packet.FLAG_ACK):
                    self.logger.info(
                        "Pacote não-ACK recebido no sender; ignorando."
                    )
                    continue

                # ACK esperado → sucesso
                if ack.ack_num == self.seqnum:
                    self.logger.recv(
                        f"ACK válido recebido para seq={ack.ack_num}."
                    )
                    self.seqnum = 1 - self.seqnum
                    return True

                # ACK duplicado/inesperado
                self.logger.info(
                    f"ACK inesperado ack_num={ack.ack_num}, "
                    f"esperado={self.seqnum}. Retransmitindo."
                )
                self.retransmissions += 1
                break  # sair do loop de espera e retransmitir imediatamente

            # ----------------------------------------------------------
            # (3) Timeout → retransmitir
            # ----------------------------------------------------------
            if time.time() - send_time >= self.timeout:
                self.logger.timeout(
                    f"Timeout aguardando ACK seq={self.seqnum}. Retransmitindo."
                )

            self.retransmissions += 1
            # loop while True continua e reenviará o mesmo pkt


# ======================================================================
#                               RECEIVER
# ======================================================================

class RDT21Receiver:
    """
    Receptor do protocolo RDT 2.1 (alternating-bit).

    Máquina de estados:
        - expected_seq ∈ {0, 1}
        - last_ack_sent guarda o último ACK válido reenviado
        - Ao receber pacote:
            1. Se corrompido → reenviar último ACK
            2. Se seq != expected_seq → duplicado/out-of-order → reenviar ACK
            3. Caso contrário → entregar, enviar ACK(expected_seq),
               atualizar estados.

    Parameters
    ----------
    channel : UnreliableChannel
        Canal não confiável do receiver → sender.
    logger : Logger, optional
        Logger. Se None, cria um logger padrão.
    """

    def __init__(
        self,
        channel: UnreliableChannel,
        logger: Optional[Logger] = None
    ) -> None:

        self.channel = channel
        self.logger = logger or Logger(prefix="RDT21-RECEIVER", origin="RECEIVER")

        self.expected_seq = 0
        self.last_ack_sent: Optional[int] = None
        self.delivered_count = 0

    # ------------------------------------------------------------------

    def receive(self) -> Optional[bytes]:
        """
        Processa um único pacote recebido do canal.

        Returns
        -------
        bytes or None
            Os bytes entregues à aplicação, ou None caso:
                - o canal esteja vazio,
                - o pacote seja inválido,
                - seja duplicado/out-of-order.
        """
        pkt = self.channel.recv()
        if pkt is None:
            return None

        # ----------------------------------------------------------
        # (1) Pacote corrompido → reenviar último ACK válido
        # ----------------------------------------------------------
        if pkt.is_corrupt():
            self.logger.corrupt(
                "Pacote corrompido recebido no receiver. "
                "Reenviando último ACK."
            )
            if self.last_ack_sent is not None:
                ack = Packet(
                    seq_num=0,
                    ack_num=self.last_ack_sent,
                    flags=Packet.FLAG_ACK,
                    data=b""
                )
                self.channel.send(ack)
            return None

        # ----------------------------------------------------------
        # (2) Pacote fora de ordem (duplicado)
        # ----------------------------------------------------------
        if pkt.seq_num != self.expected_seq:
            self.logger.info(
                f"Pacote duplicado/out-of-order seq={pkt.seq_num}, "
                f"esperado={self.expected_seq}. Reenviando último ACK."
            )
            if self.last_ack_sent is not None:
                ack = Packet(
                    seq_num=0,
                    ack_num=self.last_ack_sent,
                    flags=Packet.FLAG_ACK,
                    data=b""
                )
                self.channel.send(ack)
            return None

        # ----------------------------------------------------------
        # (3) Pacote correto e esperado → entregar
        # ----------------------------------------------------------
        payload = pkt.data
        self.delivered_count += 1
        self.logger.recv(
            f"Pacote seq={pkt.seq_num} entregue à aplicação: {payload!r}"
        )

        # Envia ACK confirmando o seq recebido
        ack = Packet(
            seq_num=0,
            ack_num=self.expected_seq,
            flags=Packet.FLAG_ACK,
            data=b""
        )
        self.channel.send(ack)
        self.logger.send(f"ACK enviado para seq={self.expected_seq}.")

        # Atualiza estado
        self.last_ack_sent = self.expected_seq
        self.expected_seq = 1 - self.expected_seq

        return payload
