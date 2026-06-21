"""
===========================================================
Módulo: fase1/rdt30.py
===========================================================

RDT 3.0 - Reliable Data Transfer (stop-and-wait com perda)

Implementação do protocolo RDT 3.0 usando um UnreliableChannel
(simulando perda/corrupção de pacotes via UDP).

Características:
    - Stop-and-wait.
    - Tolerância a perda de DATA e ACK.
    - Timeout + retransmissão.
    - ACK cumulativo (alternating-bit: seqnum 0/1).
    - Thread paralela dedicada à recepção de ACKs.

Classes:
    RDT30Sender   - Emissor RDT 3.0
    RDT30Receiver - Receptor RDT 3.0
===========================================================
"""

from __future__ import annotations

import time
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
            1. Envia DATA(seqnum) pelo canal.
            2. Espera ACK(seqnum) até o timeout.
            3. Se timeout ou ACK incorreto -> retransmite.
            4. Se ACK correto -> alterna seqnum.

    Observação:
        Toda simulação de falha de rede (perda, corrupção, atraso)
        é responsabilidade exclusiva do UnreliableChannel injetado
        via construtor - o protocolo em si não decide perder ou
        atrasar seus próprios pacotes. Isso segue a arquitetura
        descrita no enunciado (seção 4.3) e mantém o RDT 3.0
        agnóstico em relação à qualidade do canal subjacente.

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

        # Usamos um Condition (em vez de um Event simples) para evitar a
        # clássica race condition de "lost wakeup": com Event, é possível
        # que _listen_ack chame set() exatamente entre o clear() e o
        # wait() da thread principal, perdendo o sinal para sempre e
        # travando send() indefinidamente (foi exatamente isso que causou
        # o travamento observado no sweep da Fase 2 - ver
        # fase2_sweep_20260616_185825.log). Com Condition, a checagem da
        # condição (last_ack == seqnum esperado) e a espera são atômicas
        # em relação ao lock, então nenhum ACK pode "passar despercebido".
        self._ack_cond = threading.Condition()
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

        Observação importante:
            Um OSError isolado (ex.: ConnectionRefusedError originado de um
            ICMP "port unreachable" associado de forma assíncrona pelo
            kernel a um sendto() anterior, comum em UDP no Linux) NÃO deve
            encerrar esta thread. Se a thread morrer aqui, o sender nunca
            mais recebe ACKs e fica retransmitindo para sempre, em
            silêncio, sem qualquer indício no log - foi exatamente esse o
            comportamento observado no teste de sweep da Fase 2 (ver log
            fase2_sweep_20260616_185825.log). Por isso, tratamos qualquer
            OSError aqui como evento transitório: registramos e
            continuamos, em vez de sair do laço.
        """
        while self.running:
            try:
                pkt = self.channel.recv()
            except OSError as exc:
                if not self.running:
                    break
                try:
                    self.logger.info(
                        f"_listen_ack: OSError transitório ignorado: "
                        f"{type(exc).__name__}: {exc}"
                    )
                except Exception:
                    pass
                time.sleep(0.01)
                continue
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
                with self._ack_cond:
                    self.last_ack = pkt.ack_num
                    self._ack_cond.notify_all()

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

            # A perda, corrupção e atraso são responsabilidade exclusiva
            # do UnreliableChannel (configurado via loss_prob,
            # corrupt_prob e delay_range) - o protocolo apenas envia.
            self.logger.send(f"ENVIANDO DATA seq={pkt.seq_num}")
            self.channel.send(pkt)

            # Espera por ACK correto até o timeout, usando wait_for() do
            # Condition: a checagem do predicado (last_ack == seqnum) e a
            # espera por notify_all() são atômicas em relação ao lock, de
            # forma que nenhum ACK que já tenha chegado (e atualizado
            # last_ack) antes mesmo de entrarmos no wait_for() pode passar
            # despercebido - diferente do Event antigo, que tinha uma
            # janela real entre clear() e wait() onde um ACK podia se
            # perder para sempre.
            target_seq = self.seqnum
            with self._ack_cond:
                ack_received = self._ack_cond.wait_for(
                    lambda: self.last_ack == target_seq,
                    timeout=self.timeout,
                )

            if ack_received:
                self.seqnum = 1 - self.seqnum

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
            1. Se corrompido -> reenviar último ACK.
            2. Se seq != expected_seq -> duplicado -> reenviar último ACK.
            3. Senão -> entregar, enviar ACK(expected_seq), alternar estado.

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
                # Cobre tanto timeout do socket quanto pacotes corrompidos:
                # o UnreliableChannel já detecta corrupção internamente
                # (ver utils/simulator.py) e retorna None nesse caso, sem
                # nunca expor um Packet com is_corrupt()==True ao
                # protocolo. Por isso não há aqui um tratamento explícito
                # de corrupção - essa responsabilidade já é do canal,
                # mantendo a separação de camadas descrita no enunciado.
                continue

            # -----------------------------
            # Duplicado/out-of-order
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
            # Pacote correto -> entregar
            # -----------------------------
            self.logger.recv(f"RECEBIDO DATA seq={pkt.seq_num}")

            ack = Packet.make_ack(pkt.seq_num)
            self.channel.send(ack)

            self.last_ack_sent = pkt.seq_num
            self.expected_seq = 1 - self.expected_seq

            return pkt.data.decode()
