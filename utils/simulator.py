"""
===========================================================
Módulo: simulator.py
===========================================================

Implementa um canal de comunicação não confiável sobre
UDP real, capaz de simular:

    - Perda de pacotes
    - Corrupção de bits
    - Atraso variável de rede (delay_range)
    - Logging detalhado de eventos

Este módulo é utilizado pelos protocolos RDT (2.0, 2.1,
3.0) para testar seu comportamento em ambiente adverso.

Cada instância cria um socket UDP real, com endereço local
e remoto, permitindo testes completos de fim a fim.

Observação de design:
    Toda a simulação de falhas de rede (perda, corrupção,
    atraso) deve ficar centralizada aqui, no canal - nunca
    dentro dos protocolos RDT/TCP. Os protocolos devem
    assumir que estão falando com uma rede real e desconhecer
    se/como ela está sendo degradada; é assim que o enunciado
    descreve a arquitetura (ver classe UnreliableChannel de
    referência na especificação, com loss_rate, corrupt_rate
    e delay_range).
===========================================================
"""

import queue
import random
import socket
import threading
import time

from utils.logger import Logger
from utils.packet import Packet


class UnreliableChannel:
    """
    Representa um canal de comunicação UDP com possibilidade de:

        - Perder pacotes (loss_prob)
        - Corromper pacotes (corrupt_prob)
        - Atrasar a entrega de pacotes (delay_range)
        - Registrar logs detalhados

    O canal é unidirecional. Para comunicação bidirecional,
    devem ser criados dois canais independentes.

    Args:
        local_addr (tuple): (host, porta) do socket local.
        remote_addr (tuple): (host, porta) do destino.
        loss_prob (float): probabilidade de perda (0.0-1.0).
        corrupt_prob (float): probabilidade de corrupção (0.0-1.0).
        delay_range (tuple | None): (min_delay, max_delay) em segundos.
            Quando definido, cada pacote enviado (que não tenha sido
            perdido) é despachado após um atraso aleatório dentro
            desse intervalo, simulando latência variável de rede.
            O atraso é assíncrono (threading.Timer) e não bloqueia
            quem chamou send().
        logger (Logger | None): logger opcional.
    """

    def __init__(
        self,
        local_addr: tuple,
        remote_addr: tuple,
        loss_prob: float = 0.0,
        corrupt_prob: float = 0.0,
        delay_range: tuple | None = None,
        logger: Logger | None = None,
    ):
        """Inicializa o canal UDP não confiável.

        Args:
            local_addr: Tupla (host, port) para bind local do socket.
            remote_addr: Tupla (host, port) do destino remoto.
            loss_prob: Probabilidade de perda de pacote (0.0-1.0).
            corrupt_prob: Probabilidade de corrupção de bits (0.0-1.0).
            delay_range: Tupla (min_delay, max_delay) em segundos para
                atraso de entrega; None desativa o atraso (padrão).
            logger: Instância de Logger; cria uma padrão se não fornecida.
        """
        self.local_addr = local_addr
        self.remote_addr = remote_addr
        self.loss_prob = loss_prob
        self.corrupt_prob = corrupt_prob
        self.delay_range = delay_range

        self.logger = logger or Logger(prefix="channel", origin="CHANNEL")

        # Socket UDP
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # SO_REUSEADDR reduz o risco de o SO manter a porta em um estado
        # transitório (relevante quando vários canais são abertos e
        # fechados em sequência rápida, como no sweep de janela da Fase 2).
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(local_addr)
        self.sock.settimeout(1.0)

        # Fila de despacho com atraso: usamos UMA única thread worker que
        # consome os pacotes em ordem FIFO, aplicando o atraso aleatório
        # antes de cada sendto(). Isso é deliberadamente diferente de
        # disparar um threading.Timer independente por pacote: com Timers
        # independentes, cada pacote sorteia seu próprio atraso e pode
        # ULTRAPASSAR um pacote enviado depois dele (reordenação), o que
        # quebra protocolos como o RDT 3.0/alternating-bit, que assumem
        # canal FIFO (só perda/corrupção, nunca reordenação). Com uma fila
        # serializada, a ORDEM de entrega respeita a ordem de chamadas a
        # send(), mesmo com atraso variável - exatamente o que a Tarefa 1C
        # do enunciado pede ("simular atraso variável na rede"), sem
        # introduzir um modo de falha que o protocolo não foi desenhado
        # para tolerar.
        self._dispatch_queue: "queue.Queue[bytes | None]" = queue.Queue()
        self._dispatch_thread = threading.Thread(
            target=self._dispatch_worker, daemon=True
        )
        self._dispatch_thread.start()

        delay_txt = (
            f", delay={delay_range[0]*1000:.0f}-{delay_range[1]*1000:.0f}ms"
            if delay_range else ""
        )
        self.logger.info(
            f"Canal iniciado em {local_addr}, remoto={remote_addr}, "
            f"loss={loss_prob * 100:.1f}%, corrupt={corrupt_prob * 100:.1f}%"
            f"{delay_txt}."
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
            - Atraso simulado (fila FIFO com atraso variável)

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

        # Enfileira para despacho assíncrono (não bloqueia quem chamou
        # send()); a thread worker aplica o atraso e preserva a ordem.
        self._dispatch_queue.put(raw)

    def _dispatch_worker(self) -> None:
        """
        Consome a fila de despacho em ordem FIFO, aplicando o atraso
        configurado (delay_range) antes de cada envio real. Roda em uma
        única thread dedicada por canal, garantindo que pacotes nunca
        sejam entregues fora da ordem em que send() foi chamado.
        """
        while True:
            raw = self._dispatch_queue.get()
            if raw is None:  # sentinela de encerramento (ver close())
                break

            if self.delay_range:
                time.sleep(random.uniform(*self.delay_range))

            try:
                self.sock.sendto(raw, self.remote_addr)
            except OSError as exc:
                # Mesmo cuidado do recv(): registrar em vez de deixar a
                # exceção se propagar silenciosamente (ou travar o
                # despacho sem nenhum indício no log).
                try:
                    self.logger.info(
                        f"[ERRO SOCKET] send() falhou em {self.local_addr} -> "
                        f"{self.remote_addr}: {type(exc).__name__}: {exc}"
                    )
                except Exception:
                    pass

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

        except OSError as exc:
            # NUNCA engolir silenciosamente: um OSError aqui pode indicar
            # socket fechado, porta inválida ou outro estado anômalo que,
            # se ignorado, produz um travamento silencioso no chamador
            # (o lado afetado para de receber pacotes sem nenhum aviso).
            try:
                self.logger.info(
                    f"[ERRO SOCKET] recv() falhou em {self.local_addr}: "
                    f"{type(exc).__name__}: {exc}"
                )
            except Exception:
                pass
            return None

    # ============================================================ #
    # Fechamento
    # ============================================================ #
    def close(self):
        """
        Fecha o socket, encerra a thread de despacho e o logger.

        Pode ser chamado múltiplas vezes.
        """
        self.logger.info("Encerrando canal.")
        try:
            self._dispatch_queue.put(None)  # sentinela: encerra o worker
        except Exception:
            pass
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
