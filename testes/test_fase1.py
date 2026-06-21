"""
===========================================================
Módulo: testes/test_fase1.py
===========================================================

Descrição Geral:
    Testes automatizados da Fase 1 do projeto de RDT.

    São validados três protocolos:
        - RDT 2.0  - Canal com corrupção (sem números de sequência)
        - RDT 2.1  - Canal com corrupção + números de sequência
        - RDT 3.0  - Canal com perda, atraso e timeout (Stop-and-Wait)

    Os testes executam uma comunicação completa:
        - Sender envia N mensagens sequenciais
        - Receiver coleta e devolve os dados processados
        - Canal simula perda, atraso ou corrupção
        - Métricas coletadas:
              - Retransmissões totais
              - Overhead médio por mensagem (RDT 2.1)
              - Throughput (RDT 3.0)

    Observação:
        A execução via CLI é mantida exatamente como no arquivo original.

Execução:
    python -m testes.test_fase1
===========================================================
"""

import time
import random
import threading

from utils.packet import Packet
from utils.simulator import UnreliableChannel
from utils.logger import Logger

from fase1.rdt20 import RDT20Sender, RDT20Receiver
from fase1.rdt21 import RDT21Sender, RDT21Receiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver


# ==========================================================
# Função auxiliar de suporte
# ==========================================================
def _create_bidirectional_channels(sender_addr, receiver_addr, loss_prob, corrupt_prob, logger):
    """
    Cria par de canais bidirecionais não confiáveis.

    Parameters
    ----------
    sender_addr : tuple(str, int)
        Endereço local do sender (IP, porta).
    receiver_addr : tuple(str, int)
        Endereço local do receiver.
    loss_prob : float
        Probabilidade de perda de pacotes.
    corrupt_prob : float
        Probabilidade de corrupção.
    logger : Logger
        Logger compartilhado.

    Returns
    -------
    tuple(UnreliableChannel, UnreliableChannel)
        Canais (sender → receiver) e (receiver → sender).
    """
    chan_s_to_r = UnreliableChannel(
        local_addr=sender_addr,
        remote_addr=receiver_addr,
        loss_prob=loss_prob,
        corrupt_prob=corrupt_prob,
        logger=logger,
    )

    chan_r_to_s = UnreliableChannel(
        local_addr=receiver_addr,
        remote_addr=sender_addr,
        loss_prob=loss_prob,
        corrupt_prob=corrupt_prob,
        logger=logger,
    )

    return chan_s_to_r, chan_r_to_s


# ==========================================================
# TESTE RDT 2.0
# ==========================================================
def run_test_rdt20(num_messages=10, corrupt_prob=0.0, loss_prob=0.0):
    """
    Executa teste funcional do RDT 2.0.

    O protocolo precisa funcionar em canal com corrupção,
    sem números de sequência nem retransmissão explícita
    do lado do receiver.

    Parameters
    ----------
    num_messages : int
        Quantidade de mensagens enviadas.
    corrupt_prob : float
        Probabilidade de corrupção no canal.
    loss_prob : float
        Probabilidade de perda no canal.

    Returns
    -------
    tuple(bool, int, list[str])
        (mensagens_ok, retransmissoes, mensagens_recebidas)
    """
    logger = Logger(prefix=f"rdt20_c{int(corrupt_prob * 100)}_l{int(loss_prob * 100)}")

    sender_addr = ("127.0.0.1", 12000 + random.randint(0, 1000))
    receiver_addr = ("127.0.0.1", sender_addr[1] + 1)

    sender_chan, receiver_chan = _create_bidirectional_channels(
        sender_addr, receiver_addr, loss_prob, corrupt_prob, logger
    )

    sender = RDT20Sender(sender_chan, logger, timeout=1.0)
    receiver = RDT20Receiver(receiver_chan, logger)

    stop_event = threading.Event()

    def receiver_loop():
        """Loop assíncrono do Receiver."""
        while not stop_event.is_set():
            processed = receiver.serve_once(blocking=False)
            if not processed:
                time.sleep(0.01)

    threading.Thread(target=receiver_loop, daemon=True).start()

    # Envio das mensagens
    messages = [f"Mensagem {i}" for i in range(num_messages)]
    for m in messages:
        sender.send_message(m)
        time.sleep(0.01)

    all_received = receiver.delivered == messages

    time.sleep(0.5)
    stop_event.set()

    logger.info(
        f"RESUMO: OK={all_received} | "
        f"Retransmissões={sender.retransmissions}"
    )

    sender_chan.close()
    receiver_chan.close()
    logger.close()

    return all_received, sender.retransmissions, receiver.delivered


# ==========================================================
# TESTE RDT 2.1
# ==========================================================
def run_test_rdt21(num_messages=10, corrupt_prob_data=0.0, corrupt_prob_ack=0.0):
    """
    Executa teste funcional do RDT 2.1.

    Verifica:
        - Alternância correta de seqnum
        - Duplicação não permitida
        - Retransmissões esperadas
        - Cálculo de overhead médio por mensagem

    Returns
    -------
    tuple(bool, int, list[str], float)
        (OK, retransmissoes, msgs_recebidas, overhead)
    """
    logger = Logger(prefix=f"rdt21_d{int(corrupt_prob_data * 100)}_a{int(corrupt_prob_ack * 100)}")

    sender_addr = ("127.0.0.1", 13000 + random.randint(0, 1000))
    receiver_addr = ("127.0.0.1", sender_addr[1] + 1)

    chan_data = UnreliableChannel(
        sender_addr, receiver_addr, corrupt_prob=corrupt_prob_data,
        loss_prob=0.0, logger=logger
    )
    chan_ack = UnreliableChannel(
        receiver_addr, sender_addr, corrupt_prob=corrupt_prob_ack,
        loss_prob=0.0, logger=logger
    )

    sender = RDT21Sender(chan_ack, logger)
    receiver = RDT21Receiver(chan_data, logger)

    received = []
    retransmissions = 0
    sender_done = threading.Event()

    def receiver_loop():
        """
        Loop assíncrono do Receiver.

        Observação importante:
            Não basta parar assim que `len(received) == num_messages`.
            Se o ACK da ÚLTIMA mensagem entregue for perdido ou
            corrompido, o sender vai retransmitir aquele DATA - e se
            esta thread já tiver encerrado naquele instante, não há
            mais ninguém para responder com o ACK correto, e o sender
            fica retransmitindo para sempre (foi exatamente esse o
            travamento intermitente observado nesta fixture). Por
            isso, após entregar a última mensagem esperada, a thread
            continua viva por uma janela extra (aguardando
            `sender_done` E um pequeno buffer de tempo) para conseguir
            reenviar o último ACK em caso de retransmissão tardia.
        """
        while len(received) < num_messages:
            data = receiver.receive()
            if data:
                received.append(data.decode())
            else:
                time.sleep(0.005)

        # Janela de segurança: continua respondendo a possíveis
        # retransmissões tardias do sender até ele sinalizar que já
        # recebeu o ACK de tudo (sender_done) ou até um teto de tempo.
        grace_deadline = time.time() + 3.0
        while not sender_done.is_set() and time.time() < grace_deadline:
            receiver.receive()  # reenvia ACK se for duplicado; no-op se vazio

    t = threading.Thread(target=receiver_loop, daemon=True)
    t.start()

    messages = [f"Mensagem {i}" for i in range(num_messages)]

    for msg in messages:
        before = sender.retransmissions
        sender.send(msg.encode())
        retransmissions += sender.retransmissions - before
        time.sleep(0.01)

    sender_done.set()
    t.join(timeout=4.0)

    # Overhead médio aproximado
    size_data = len(Packet.make_data(0, "X").to_bytes())
    size_ack = len(Packet.make_ack(0).to_bytes())
    total_bytes = num_messages * (size_data + size_ack)
    payload_bytes = sum(len(m.encode()) for m in messages)
    overhead = (total_bytes - payload_bytes) / num_messages

    all_received = received == messages

    logger.info(
        f"RESUMO: OK={all_received} | Retransmissões={retransmissions} | "
        f"Overhead={overhead:.2f}"
    )

    chan_data.close()
    chan_ack.close()
    logger.close()

    return all_received, retransmissions, received, overhead


# ==========================================================
# TESTE RDT 3.0
# ==========================================================
def run_test_rdt30(num_messages=10):
    """
    Executa teste funcional e métrico do RDT 3.0.

    Mede:
        - tempo total
        - retransmissões
        - throughput efetivo (bytes/segundo)

    Returns
    -------
    tuple(list[str], int, float)
        (mensagens_recebidas, retransmissoes, throughput)
    """
    logger = Logger(prefix="rdt30")

    sender_addr = ("127.0.0.1", 15000 + random.randint(0, 1000))
    receiver_addr = ("127.0.0.1", sender_addr[1] + 1)

    sender_chan = UnreliableChannel(
        sender_addr, receiver_addr, loss_prob=0.15,
        corrupt_prob=0.0, delay_range=(0.05, 0.5), logger=logger
    )
    receiver_chan = UnreliableChannel(
        receiver_addr, sender_addr, loss_prob=0.15,
        corrupt_prob=0.0, delay_range=(0.05, 0.5), logger=logger
    )

    sender = RDT30Sender(sender_chan)
    receiver = RDT30Receiver(receiver_chan)

    received = []
    sender_done = threading.Event()
    start = time.time()

    def receiver_loop():
        """
        Loop assíncrono do Receiver.

        Observação importante (mesma causa raiz já corrigida em
        run_test_rdt21): não basta parar assim que
        `len(received) == num_messages`. Se o ACK da ÚLTIMA mensagem
        entregue for perdido (aqui loss_prob=0.15 em ambas direções,
        então é um evento plausível, não raro), o sender vai
        retransmitir aquele DATA - e se esta thread já tiver
        encerrado naquele instante, não há mais ninguém para
        responder com o ACK correto, e o sender fica retransmitindo
        para sempre. Por isso, após entregar a última mensagem
        esperada, a thread continua viva por uma janela extra
        (aguardando `sender_done` e um pequeno buffer de tempo) para
        conseguir reenviar o último ACK em caso de retransmissão
        tardia.
        """
        while len(received) < num_messages:
            msg = receiver.receive()
            if msg:
                received.append(msg)
            else:
                time.sleep(0.01)

        # Janela de segurança: continua respondendo a possíveis
        # retransmissões tardias do sender até ele sinalizar que já
        # recebeu o ACK de tudo (sender_done) ou até um teto de tempo.
        grace_deadline = time.time() + 5.0
        while not sender_done.is_set() and time.time() < grace_deadline:
            receiver.receive()  # reenvia ACK se for duplicado; no-op se vazio

    t = threading.Thread(target=receiver_loop, daemon=True)
    t.start()

    for i in range(num_messages):
        sender.send(f"Mensagem {i}")

    sender_done.set()
    end = time.time()
    t.join(timeout=6.0)

    total_bytes = sum(len(m.encode()) for m in received)
    elapsed = max(end - start, 1e-6)
    throughput = total_bytes / elapsed

    logger.info(
        f"RESUMO: OK={len(received) == num_messages} | "
        f"Retransmissões={sender.retransmissions} | "
        f"Throughput={throughput:.2f} bytes/s"
    )

    sender.close()
    receiver_chan.close()
    logger.close()

    return received, sender.retransmissions, throughput


# ==========================================================
# EXECUÇÃO DIRETA (CLI)
# ==========================================================
if __name__ == "__main__":
    print("\n=== RDT 2.0 ===")
    ok, retx, msgs = run_test_rdt20()
    print(f"OK? {ok} | Retransmissões={retx}")

    print("\n=== RDT 2.1 ===")
    ok, retx, msgs, overhead = run_test_rdt21(corrupt_prob_data=0.2)
    print(f"OK? {ok} | Retransmissões={retx} | Overhead={overhead:.2f}")

    print("\n=== RDT 3.0 ===")
    msgs, retx, thr = run_test_rdt30()
    print(f"OK? {len(msgs)==10} | Retransmissões={retx} | Throughput={thr:.2f} bytes/s")
