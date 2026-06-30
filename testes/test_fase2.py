"""
===========================================================
Módulo: testes/test_fase2.py
===========================================================

Descrição:
    Testes da Fase 2 - Implementação Go-Back-N (GBN) comparada
    com RDT 3.0. Inclui:
        - Teste de transferência de 1 MiB
        - Teste com perda de 10%
        - Teste de throughput variando janela (sweep)

Execução:
    python -m testes.test_fase2
===========================================================
"""

import os
import time
import random
import threading
from typing import List, Tuple

import matplotlib.pyplot as plt

from utils.packet import Packet
from utils.simulator import UnreliableChannel
from utils.logger import Logger

from fase2.gbn import GBNSender, GBNReceiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver


# Diretório único para armazenar logs e gráficos
OUT_DIR = "logs"
os.makedirs(OUT_DIR, exist_ok=True)


# ============================================================
#                 Canais Bidirecionais Isolados
# ============================================================
def _make_bidirectional(base_port: int, loss: float, corrupt: float, logger: Logger) \
        -> Tuple[Tuple[UnreliableChannel, UnreliableChannel], Tuple[UnreliableChannel, UnreliableChannel]]:
    """
    Cria canais independentes DATA e ACK para SENDER e RECEIVER.

    Estrutura criada:

      Sender DATA --> Receiver DATA
      Receiver DATA --> Sender DATA   (apenas para simulação)

      Receiver ACK --> Sender ACK
      Sender ACK --> Receiver ACK     (não utilizado)

    Essa separação evita o erro WinError 10054 no Windows,
    garantindo que cada lado possua seus próprios sockets.
    """

    sender_data_addr = ("127.0.0.1", base_port)
    receiver_data_addr = ("127.0.0.1", base_port + 1)

    chan_data_sender = UnreliableChannel(sender_data_addr, receiver_data_addr,
                                         loss_prob=loss, corrupt_prob=corrupt, logger=logger)
    chan_data_receiver = UnreliableChannel(receiver_data_addr, sender_data_addr,
                                           loss_prob=loss, corrupt_prob=corrupt, logger=logger)

    sender_ack_addr = ("127.0.0.1", base_port + 2)
    receiver_ack_addr = ("127.0.0.1", base_port + 3)

    chan_ack_receiver = UnreliableChannel(receiver_ack_addr, sender_ack_addr,
                                          loss_prob=loss, corrupt_prob=corrupt, logger=logger)
    chan_ack_sender = UnreliableChannel(sender_ack_addr, receiver_ack_addr,
                                        loss_prob=loss, corrupt_prob=corrupt, logger=logger)

    return (chan_data_sender, chan_ack_sender), (chan_data_receiver, chan_ack_receiver)


# ============================================================
#        Loop genérico para parear Sender/Receiver
# ============================================================
def _run_pair(sender, receiver, messages: List[bytes],
              delay_between: float = 0.0, overall_timeout: float | None = None):
    """
    Envia todos os messages via sender e coleta entregas via receiver.

    Retorna:
      delivered: lista de mensagens entregues (bytes decodificados)
      elapsed: tempo total da transferência
    """

    delivered: List[str] = []
    total = len(messages)
    stop_event = threading.Event()

    def recv_loop():
        while len(delivered) < total and not stop_event.is_set():
            try:
                msg = receiver.receive()
            except Exception:
                msg = None

            if msg is not None:
                delivered.append(msg.decode() if isinstance(msg, bytes) else msg)
            else:
                time.sleep(0.002)

    # Thread de recebimento
    t = threading.Thread(target=recv_loop, daemon=True)
    t.start()

    t0 = time.time()

    # Envio de todas as mensagens
    for m in messages:
        sender.send(m)
        if delay_between:
            time.sleep(delay_between)

    # Cálculo do timeout geral (adaptativo)
    if overall_timeout is None:
        overall_timeout = max(5.0, total * 0.02)

    deadline = time.time() + overall_timeout
    last_report = time.time()

    # Aguardando todas entregas
    while time.time() < deadline and len(delivered) < total:
        if time.time() - last_report > 0.5:
            last_report = time.time()
            try:
                print(f"[PROGRESS] delivered={len(delivered)}/{total} "
                      f"base={getattr(sender, 'base', None)} nextseq={getattr(sender, 'nextseqnum', None)} "
                      f"retrans={getattr(sender, 'retransmissions', None)}")
            except Exception:
                print(f"[PROGRESS] delivered={len(delivered)}/{total}")
        time.sleep(0.05)

    stop_event.set()
    t.join(timeout=1.0)

    # Fechamento do receiver (se existir)
    try:
        receiver.close()
    except Exception:
        pass

    elapsed = time.time() - t0
    return delivered, elapsed


# ============================================================
#             Teste 1 - Transferência de 1 MiB
# ============================================================
def test_transfer_1mb_compare():
    logger = Logger(prefix="fase2_1mb", origin="TEST")

    pkt_size = 1024
    total_bytes = 1024 * 1024
    num_messages = total_bytes // pkt_size
    messages = [b"X" * pkt_size for _ in range(num_messages)]

    # ---------------- GBN ----------------
    (ds, as_), (dr, ar) = _make_bidirectional(
        16000 + random.randint(0, 2000), 0.0, 0.0, logger
    )

    gbn_sender = GBNSender(ds, as_, N=5, timeout=1.5, logger=logger)
    gbn_receiver = GBNReceiver(dr, ar, logger=logger)

    delivered_gbn, elapsed_gbn = _run_pair(
        gbn_sender, gbn_receiver, messages, overall_timeout=30.0
    )

    gbn_sender.close()
    time.sleep(0.05)

    throughput_gbn = sum(len(m) for m in delivered_gbn) / max(elapsed_gbn, 1e-9)

    # ---------------- RDT 3.0 ----------------
    (ds, as_), (dr, ar) = _make_bidirectional(
        17000 + random.randint(0, 2000), 0.0, 0.0, logger
    )

    rdt_sender = RDT30Sender(ds, timeout=2.0)
    rdt_receiver = RDT30Receiver(dr)

    delivered_rdt, elapsed_rdt = _run_pair(rdt_sender, rdt_receiver, messages)

    throughput_rdt = sum(len(m) for m in delivered_rdt) / max(elapsed_rdt, 1e-9)

    # Registro em log
    logger.info("=" * 60)
    logger.info("Resumo Transferência 1MiB")
    logger.info(f"GBN:   elapsed={elapsed_gbn:.3f}s  thr={throughput_gbn:.2f}B/s  retrans={gbn_sender.retransmissions}")
    logger.info(f"RDT3:  elapsed={elapsed_rdt:.3f}s  thr={throughput_rdt:.2f}B/s  retrans={rdt_sender.retransmissions}")
    logger.info("=" * 60)

    rdt_sender.close()
    try:
        ds.close(); as_.close()
        dr.close(); ar.close()
    except Exception:
        pass

    assert len(delivered_gbn) == num_messages
    assert len(delivered_rdt) == num_messages
    
    # Salva resumo simples
    summary_path = os.path.join(OUT_DIR, "transfer1mb_summary.txt")
    with open(summary_path, "w") as f:
        f.write(f"GBN: elapsed={elapsed_gbn:.3f}s delivered={len(delivered_gbn)} "
                f"throughput={throughput_gbn:.2f} retransmissions={gbn_sender.retransmissions}\n")
        f.write(f"RDT3.0: elapsed={elapsed_rdt:.3f}s delivered={len(delivered_rdt)} "
                f"throughput={throughput_rdt:.2f} retransmissions={rdt_sender.retransmissions}\n")

    print("[Resumo salvo em]", summary_path)

# ============================================================
#        Teste 2 - Perda de 10% (GBN)
# ============================================================
def test_loss_10pct_check():
    logger = Logger(prefix="fase2_loss10", origin="TEST")

    num_messages = 200
    payload = b"Hello-GBN-" * 10
    messages = [payload for _ in range(num_messages)]

    (ds, as_), (dr, ar) = _make_bidirectional(
        18000 + random.randint(0, 2000), 0.10, 0.0, logger
    )

    sender = GBNSender(ds, as_, N=5, timeout=1.0, logger=logger)
    receiver = GBNReceiver(dr, ar, logger=logger)

    delivered, elapsed = _run_pair(sender, receiver, messages, delay_between=0.001)

    sender.close()
    time.sleep(0.05)

    # Cálculo de utilização aproximada
    sent_packets = num_messages + sender.retransmissions
    pkt_len = len(Packet.make_data(0, payload).to_bytes())
    total_sent_bytes = sent_packets * pkt_len

    utilization = (sum(len(m) for m in delivered) / total_sent_bytes) if total_sent_bytes > 0 else 0.0

    logger.info("=" * 55)
    logger.info(f"Loss 10% -> delivered={len(delivered)}/{num_messages}")
    logger.info(f"Elapsed={elapsed:.3f}s  Retrans={sender.retransmissions}")
    logger.info(f"Utilization={utilization:.6f}")
    logger.info("=" * 55)

    assert len(delivered) == num_messages

    # Salva resumo simples
    summary_path = os.path.join(OUT_DIR, "loss10_summary.txt")
    with open(summary_path, "w") as f:
        f.write(f"delivered={len(delivered)}/{num_messages} "
                f"elapsed={elapsed:.3f}s retransmissions={sender.retransmissions} "
                f"utilization={utilization:.6f}\n")

    print("[Resumo salvo em]", summary_path)

# ==========================================================
# Test 3: Sweep Window + RDT 3.0 no plot
# ==========================================================
def test_throughput_vs_window():
    logger = Logger(prefix="fase2_sweep", origin="TEST")
    window_sizes = [1, 5, 10, 20]
    results = []

    # --------------------------
    # Teste GBN para vários N
    # --------------------------
    for N in window_sizes:
        (chan_data_s, chan_ack_s), (chan_data_r, chan_ack_r) = \
            _make_bidirectional(19000 + random.randint(0, 2000), 0.05, 0.0, logger)

        sender = GBNSender(chan_data_s, chan_ack_s, N=N, timeout=1.0, logger=logger)
        receiver = GBNReceiver(chan_data_r, chan_ack_r, logger=logger)

        pkt_size = 512
        num_messages = (32 * 1024) // pkt_size
        messages = [b"M" * pkt_size for _ in range(num_messages)]

        delivered, elapsed = _run_pair(sender, receiver, messages)
        throughput = sum(len(m) for m in delivered) / max(elapsed, 1e-9)

        results.append((N, throughput, sender.retransmissions, len(delivered), elapsed))

        sender.close()
        receiver.close()
        time.sleep(0.05)

    # --------------------------
    # Teste RDT 3.0 (stop-and-wait)
    # --------------------------
    (chan_data_s, chan_ack_s), (chan_data_r, chan_ack_r) = \
        _make_bidirectional(21000 + random.randint(0, 2000), 0.05, 0.0, logger)

    pkt_size = 512
    num_messages = (32 * 1024) // pkt_size
    messages = [b"M" * pkt_size for _ in range(num_messages)]

    rdt_sender = RDT30Sender(chan_data_s, timeout=2.0)
    rdt_receiver = RDT30Receiver(chan_data_r)

    delivered_rdt, elapsed_rdt = _run_pair(rdt_sender, rdt_receiver, messages)
    throughput_rdt = sum(len(m) for m in delivered_rdt) / max(elapsed_rdt, 1e-9)

    logger.info("=" * 60)
    logger.info("Resumo Sweep Throughput vs Janela")
    for N, thr, ret, d, e in results:
        logger.info(f"GBN N={N}: thr={thr:.2f}B/s retrans={ret} delivered={d} elapsed={e:.3f}s")
    logger.info(f"RDT3.0: thr={throughput_rdt:.2f}B/s delivered={len(delivered_rdt)} elapsed={elapsed_rdt:.3f}s")
    logger.info("=" * 60)

    rdt_sender.close()
    try:
        chan_data_s.close(); chan_ack_s.close()
        chan_data_r.close(); chan_ack_r.close()
    except Exception:
        pass

    # --------------------------
    # Plot
    # --------------------------
    Ns = [r[0] for r in results]
    throughputs = [r[1] for r in results]

    plt.figure(figsize=(9, 4))
    plt.plot(Ns, throughputs, marker="o", label="GBN Throughput")
    plt.scatter([1], [throughput_rdt], label="RDT 3.0 (Stop-and-Wait)")
    plt.title("Throughput vs Window Size (loss=5%)")
    plt.xlabel("Window (N)")
    plt.ylabel("Throughput (B/s)")
    plt.grid(True)
    plt.legend()

    out_png = os.path.join(OUT_DIR, "throughput_vs_window.png")
    plt.savefig(out_png)
    plt.close()

    print("Sweep concluído - plot salvo em:", out_png)
    assert len(results) == len(window_sizes)

    # Salva resumo do sweep
    summary_path = os.path.join(OUT_DIR, "throughput_sweep_summary.txt")
    with open(summary_path, "w") as f:
        f.write("GBN Sweep Results (loss=5%)\n")
        f.write("N | throughput(B/s) | retrans | delivered | elapsed(s)\n")
        for N, thr, ret, d, e in results:
            f.write(f"{N} | {thr:.2f} | {ret} | {d} | {e:.3f}\n")

        f.write("\nRDT3.0:\n")
        f.write(f"throughput={throughput_rdt:.2f} delivered={len(delivered_rdt)} "
                f"elapsed={elapsed_rdt:.3f}\n")

    print("[Resumo salvo em]", summary_path)


# Execução manual
if __name__ == "__main__":
    print("Executando testes de fase 2 manualmente...")
    test_transfer_1mb_compare()
    test_loss_10pct_check()
    test_throughput_vs_window()
