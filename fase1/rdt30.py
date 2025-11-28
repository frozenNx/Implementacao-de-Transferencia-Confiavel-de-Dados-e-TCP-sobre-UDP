"""
===========================================================
RDT 3.0 — Reliable Data Transfer (stop-and-wait com perda)
===========================================================

Implementação do protocolo RDT 3.0 usando um UnreliableChannel
(simulando perda/corrupção de pacotes via UDP).

Características:
    - Stop-and-wait.
    - Tolerância a perda de DATA e ACK.
    - Timeout + retransmissão.
    - ACK cumulativo (alternating-bit: seqnum 0/1).
    - Thread paralela dedicada à recepção de ACKs.

Classes:
    RDT30Sender   — Emissor RDT 3.0
    RDT30Receiver — Receptor RDT 3.0
"""

from __future__ import annotations

import time
import random
import threading
from typing import Optional

from utils.packet import Packet
from utils.simulator import UnreliableChannel

TIMEOUT = 2.0


# ======================================================================
#                               SENDER
# ======================================================================

class RDT30Sender:
    """
    Emissor do protocolo RDT 3.0.

    Máquina de estados:
        - seqnum ∈ {0, 1}
        - Para enviar:
            1. Envia DATA(seqnum) — perda proposital opcional (15%).
            2. Espera ACK(seqnum).
            3. Se timeout ou ACK incorreto → retransmite.
            4. Se ACK correto → alterna seqnum.

    Atributos
    ---------
    channel : UnreliableChannel
        Canal não confiável.
    seqnum : int
        Número de sequência atual (0/1).
    timeout : float
        Tempo de timeout para retransmissão.
    logger : Logger
        Logger associado ao canal.
    ack_event : threading.Event
        Sinalizado quando um ACK válido chega.
    last_ack : int | None
        Último ACK recebido pela thread de escuta.
    running : bool
        Controla o loop da thread de ACK.
    retransmissions : int
        Contador total de retransmissões.
    """

    def __init__(self, channel: UnreliableChannel, timeout: float = TIMEOUT) -> None:
        self.channel = channel
        self.seqnum = 0
        self.timeout = float(timeout)
        self.logger = channel.logger

        self.ack_event = threading.Event()
        self.last_ack: Optional[int] = None
        self.running = True
        self.retransmissions = 0

        # Thread dedicada para ouvir ACKs
        self._ack_thread = threading.Thread(
            target=self._listen_ack,
            daemon=True
        )
        self._ack_thread.start()

    # ------------------------------------------------------------------

    def _listen_ack(self) -> None:
        """
        Thread auxiliar para receber ACKs continuamente.

        Comportamento:
            - Ignora pacotes corrompidos.
            - Quando recebe ACK válido, armazena ack_num e sinaliza ack_event.
        """
        while self.running:
            try:
                pkt = self.channel.recv()
            except OSError:
                break
            except Exception as exc:
                if not self.running:
                    break
                try:
                    self.logger.info(f"_listen_ack: exceção: {exc}")
                except Exception:
                    pass
                continue

            if pkt is None:
                continue

            if pkt.is_corrupt():
                self.logger.corrupt("ACK corrompido recebido; ignorando.")
                continue

            if pkt.flags & Packet.FLAG_ACK:
                self.last_ack = pkt.ack_num
                self.ack_event.set()

    # ------------------------------------------------------------------

    def send(self, data: str) -> bool:
        """
        Envia uma string de forma confiável usando RDT 3.0.

        Parameters
        ----------
        data : str
            Mensagem a enviar.

        Returns
        -------
        bool
            True se o ACK correto for recebido.
        """
        pkt = Packet.make_data(self.seqnum, data)
        ack_received = False

        while self.running and not ack_received:

            # Simula atraso natural de envio
            time.sleep(random.uniform(0.05, 0.5))

            # Simula perda proposital (15%)
            if random.random() < 0.15:
                self.logger.info(
                    f"Pacote seq={pkt.seq_num} perdido propositalmente (simulado)."
                )
            else:
                self.logger.send(f"ENVIANDO DATA seq={pkt.seq_num}")
                self.channel.send(pkt)

            # Reset do evento
            self.ack_event.clear()
            start = time.time()

            # Espera por ACK até timeout
            while time.time() - start < self.timeout:
                if self.ack_event.wait(timeout=0.01):
                    if self.last_ack == self.seqnum:
                        ack_received = True
                        self.seqnum = 1 - self.seqnum
                        break

            # Se não recebeu ACK correto
            if not ack_received:
                self.retransmissions += 1
                self.logger.timeout(
                    f"Timeout/ACK incorreto. Retransmitindo seq={pkt.seq_num}..."
                )

        return ack_received

    # ------------------------------------------------------------------

    def close(self) -> None:
        """
        Encerra o emissor:

        - Finaliza thread de ACK.
        - Fecha o canal.
        """
        self.running = False

        try:
            self._ack_thread.join(timeout=1.0)
        except Exception:
            pass

        try:
            self.channel.close()
        except Exception:
            pass


# ======================================================================
#                               RECEIVER
# ======================================================================

class RDT30Receiver:
    """
    Receptor do protocolo RDT 3.0.

    Máquina de estados:
        - expected_seq ∈ {0, 1}
        - Para cada DATA recebido:
            1. Se corrompido → reenviar último ACK.
            2. Se seq != expected_seq → duplicado → reenviar último ACK.
            3. Senão → entregar, enviar ACK(expected_seq), alternar estado.

    Atributos
    ---------
    channel : UnreliableChannel
        Canal de recepção.
    expected_seq : int
        Pròximo seq esperado pelo receptor.
    last_ack_sent : int
        Último ACK enviado (para duplicatas).
    logger : Logger
        Logger associado ao canal.
    """

    def __init__(self, channel: UnreliableChannel) -> None:
        self.channel = channel
        self.expected_seq = 0
        self.last_ack_sent = 0
        self.logger = channel.logger

    # ------------------------------------------------------------------

    def receive(self) -> Optional[str]:
        """
        Aguarda um pacote válido e o entrega para a aplicação.

        Returns
        -------
        str | None
            O payload decodificado, ou None caso o canal esteja vazio.
        """
        while True:
            pkt = self.channel.recv()
            if pkt is None:
                continue

            # Simula atraso realista de processamento
            time.sleep(random.uniform(0.05, 0.5))

            # -----------------------------
            # (1) Corrupção → reenviar ACK
            # -----------------------------
            if pkt.is_corrupt():
                self.logger.corrupt(
                    "Pacote corrompido! Reenviando último ACK."
                )
                ack = Packet.make_ack(self.last_ack_sent)
                self.channel.send(ack)
                continue

            # -----------------------------
            # (2) Duplicado/out-of-order
            # -----------------------------
            if pkt.seq_num != self.expected_seq:
                self.logger.info(
                    f"Pacote duplicado seq={pkt.seq_num}, "
                    "reenviando último ACK."
                )
                ack = Packet.make_ack(self.last_ack_sent)
                self.channel.send(ack)
                continue

            # -----------------------------
            # (3) Pacote correto → entregar
            # -----------------------------
            self.logger.recv(f"RECEBIDO DATA seq={pkt.seq_num}")

            ack = Packet.make_ack(pkt.seq_num)
            self.channel.send(ack)

            self.last_ack_sent = pkt.seq_num
            self.expected_seq = 1 - self.expected_seq

            return pkt.data.decode()
