# ============================================================
# Módulo: fase2/gbn.py
# Implementação Go-Back-N (GBN) para a Fase 2 do projeto.
#
# Características:
#   - Janela deslizante de tamanho N
#   - ACKs cumulativos
#   - Timer único para o pacote base
#   - Retransmissão de toda a janela no timeout
#   - Compatível com utils.packet, utils.simulator e test_fase2.py
# ============================================================

from __future__ import annotations

import threading
import time
from typing import Dict, Optional

from utils.packet import Packet
from utils.simulator import UnreliableChannel
from utils.logger import Logger

DEFAULT_WINDOW_SIZE = 5
DEFAULT_TIMEOUT = 1.5


# ============================================================
#                           GBNSender
# ============================================================
class GBNSender:
    """
    Implementação do remetente Go-Back-N.

    Funcionalidades:
      - Mantém janela com até N pacotes não confirmados.
      - Envia pacotes imediatamente enquanto houver espaço.
      - Usa um timer único para o pacote base.
      - ACKs cumulativos avançam a base.
      - Timeout provoca retransmissão da janela inteira.
    """

    def __init__(
        self,
        chan_data: UnreliableChannel,
        chan_ack: UnreliableChannel,
        N: int = DEFAULT_WINDOW_SIZE,
        timeout: float = DEFAULT_TIMEOUT,
        logger: Optional[Logger] = None,
    ):
        # canais
        self.chan_data = chan_data
        self.chan_ack = chan_ack

        # parâmetros do protocolo
        self.N = int(N)
        self.timeout = float(timeout)

        # logger (usa o fornecido, ou herdado do canal, ou cria um novo)
        self.logger = (
            logger
            or getattr(chan_data, "logger", None)
            or Logger(prefix="GBN", origin="SENDER")
        )

        # estado do GBN
        self.base = 0
        self.nextseqnum = 0
        self._buffer: Dict[int, Packet] = {}

        # sincronização
        self._lock = threading.Lock()
        self._timer: Optional[threading.Timer] = None

        # métricas
        self.retransmissions = 0

        # controle de threads
        self._running = True

        # thread passiva para manter o objeto vivo
        self._sender_thread = threading.Thread(
            target=self._send_loop, daemon=True
        )
        self._sender_thread.start()

        # thread que escuta ACKs
        self._ack_thread = threading.Thread(
            target=self._ack_listener, daemon=True
        )
        self._ack_thread.start()

    # ============================================================
    #                           Timer
    # ============================================================
    def _start_timer(self) -> None:
        """Inicia o timer para o pacote base."""
        self._cancel_timer()
        self._timer = threading.Timer(self.timeout, self._on_timeout)
        self._timer.daemon = True
        self._timer.start()
        if self.logger:
            self.logger.info(
                f"Timer iniciado (timeout={self.timeout}s) base={self.base}"
            )

    def _cancel_timer(self) -> None:
        """Cancela o timer, se existir."""
        if self._timer:
            try:
                self._timer.cancel()
            except Exception:
                pass
            self._timer = None

    def _restart_timer(self) -> None:
        """Reinicia o timer se ainda houver pacotes pendentes."""
        self._cancel_timer()
        if self.base != self.nextseqnum:
            self._start_timer()

    def _stop_timer(self) -> None:
        """Para o timer quando não há pacotes pendentes."""
        self._cancel_timer()

    # ============================================================
    #                           Timeout
    # ============================================================
    def _on_timeout(self) -> None:
        """Retransmite todos os pacotes não confirmados."""
        with self._lock:
            if self.base == self.nextseqnum:
                self._timer = None
                return

            if self.logger:
                self.logger.timeout(
                    f"Timeout: retransmitindo janela "
                    f"{self.base}..{self.nextseqnum - 1}"
                )

            for seq in range(self.base, self.nextseqnum):
                pkt = self._buffer.get(seq)
                if not pkt:
                    continue
                try:
                    self.chan_data.send(pkt)
                    self.retransmissions += 1
                    if self.logger:
                        self.logger.send(f"Retransmitido DATA seq={seq}")
                except Exception as exc:
                    if self.logger:
                        self.logger.info(
                            f"Erro ao retransmitir seq={seq}: {exc}"
                        )

            # reiniciar timer
            if self.base != self.nextseqnum:
                self._start_timer()
            else:
                self._timer = None

    # ============================================================
    #                       Thread auxiliar
    # ============================================================
    def _send_loop(self) -> None:
        """Thread passiva apenas para manter a instância viva."""
        while self._running:
            time.sleep(0.05)

    # ============================================================
    #                      Listener de ACKs
    # ============================================================
    def _ack_listener(self) -> None:
        """Escuta ACKs e atualiza a base."""
        while self._running:
            try:
                pkt = self.chan_ack.recv()
            except Exception:
                if not self._running:
                    break
                time.sleep(0.01)
                continue

            if pkt is None:
                time.sleep(0.001)
                continue

            if pkt.is_corrupt():
                continue

            if not (pkt.flags & Packet.FLAG_ACK):
                continue

            acknum = int(pkt.ack_num)

            with self._lock:
                if acknum >= self.base:
                    old = self.base
                    self.base = min(acknum + 1, self.nextseqnum)

                    # limpar buffer
                    for s in list(self._buffer.keys()):
                        if s < self.base:
                            del self._buffer[s]

                    # timer
                    if self.base == self.nextseqnum:
                        self._stop_timer()
                    else:
                        self._restart_timer()

                    if self.logger:
                        self.logger.info(
                            f"ACK recebido acknum={acknum} "
                            f"base {old} -> {self.base}"
                        )

    # ============================================================
    #                       Envio de dados
    # ============================================================
    def send(self, data: bytes) -> None:
        """
        Envia um pacote DATA.
        Bloqueia se a janela estiver cheia.
        """
        payload = data.encode() if isinstance(data, str) else data

        while True:
            with self._lock:
                if self.nextseqnum < self.base + self.N:
                    seq = self.nextseqnum
                    pkt = Packet.make_data(seq, payload)
                    self._buffer[seq] = pkt

                    try:
                        self.chan_data.send(pkt)
                        if self.logger:
                            self.logger.send(
                                f"ENVIANDO DATA seq={seq} "
                                f"(len={len(payload)})"
                            )
                    except Exception as exc:
                        if self.logger:
                            self.logger.info(
                                f"Erro ao enviar DATA seq={seq}: {exc}"
                            )

                    if self.base == seq:
                        self._start_timer()

                    self.nextseqnum += 1
                    return
            time.sleep(0.001)

    # ============================================================
    #                          Fechamento
    # ============================================================
    def close(self, wait: float = 3.0) -> None:
        """
        Finaliza o sender, para threads e cancela timer.
        """
        self._running = False
        self._cancel_timer()

        try:
            self._ack_thread.join(timeout=wait)
        except Exception:
            pass

        try:
            self._sender_thread.join(timeout=wait)
        except Exception:
            pass


# ============================================================
#                          GBNReceiver
# ============================================================
class GBNReceiver:
    """
    Implementação do receptor Go-Back-N.

    Funcionalidades:
      - Entrega apenas pacotes em ordem.
      - Envia ACK cumulativo para cada DATA válido.
      - Reenvia ACK anterior para pacotes fora de ordem.
      - Retorna None quando não há pacotes úteis.
    """

    def __init__(
        self,
        data_channel: UnreliableChannel,
        ack_channel: UnreliableChannel,
        logger: Optional[Logger] = None,
    ):
        self.data_channel = data_channel
        self.ack_channel = ack_channel

        self.logger = (
            logger
            or getattr(data_channel, "logger", None)
            or Logger(prefix="GBN", origin="RECV")
        )

        self.expectedseqnum = 0
        self.running = True

    def receive(self) -> Optional[bytes]:
        """
        Recebe pacote DATA em ordem.
        Retorna:
            - bytes(payload) se pacote correto chegou
            - None caso contrário
        """
        if not self.running:
            return None

        try:
            pkt = self.data_channel.recv()
        except Exception:
            self.running = False
            return None

        if pkt is None:
            return None

        if pkt.is_corrupt():
            if self.expectedseqnum > 0:
                self._send_ack(self.expectedseqnum - 1)
            return None

        if pkt.flags & Packet.FLAG_DATA:
            if pkt.seq_num == self.expectedseqnum:
                payload = pkt.data
                self._send_ack(pkt.seq_num)

                if self.logger:
                    try:
                        self.logger.log_recv_ack(pkt.seq_num)
                    except Exception:
                        self.logger.info(
                            f"Recebido DATA seq={pkt.seq_num}, ACK enviado"
                        )

                self.expectedseqnum += 1
                return payload

        # fora de ordem
        if self.expectedseqnum > 0:
            self._send_ack(self.expectedseqnum - 1)

        return None

    def _send_ack(self, n: int) -> None:
        """Envia ACK cumulativo para seq n."""
        try:
            self.ack_channel.send(Packet.make_ack(n))
        except Exception:
            pass

    def close(self) -> None:
        """Finaliza o receiver."""
        self.running = False
