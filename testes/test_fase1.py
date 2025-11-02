"""
Fase 1 - Testes de protocolos RDT
=================================
Testa RDT 2.0, 2.1 e 3.0 sobre canal não confiável.
"""

import threading
import time
from utils.simulator import UnreliableChannel
from utils import logger
from fase1.rdt20 import RDT20Sender, RDT20Receiver
from fase1.rdt21 import RDT21Sender, RDT21Receiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver


# =============================
# TESTE RDT 2.0
# =============================
def test_rdt20() -> None:
    print("\n===== TESTE RDT 2.0 =====")
    sim = UnreliableChannel(loss_rate=0.0, corrupt_rate=0.3)
    recv = RDT20Receiver()
    send = RDT20Sender(sim)

    recv_thread = threading.Thread(target=recv.start, daemon=True)
    recv_thread.start()

    msgs = [f"Msg {i}" for i in range(10)]
    for msg in msgs:
        send.send(msg)

    time.sleep(3)
    recv.stop()

    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print("✓ RDT 2.0 finalizado\n")


# =============================
# TESTE RDT 2.1
# =============================
def test_rdt21() -> None:
    print("\n===== TESTE RDT 2.1 =====")
    sim = UnreliableChannel(loss_rate=0.0, corrupt_rate=0.2)
    recv = RDT21Receiver()
    send = RDT21Sender(sim)

    recv_thread = threading.Thread(target=recv.start, daemon=True)
    recv_thread.start()

    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes_extras = 0
    for msg in msgs:
        total_bytes_extras += send.packet_header_size()
        send.send(msg)

    overhead = total_bytes_extras / len(msgs)
    time.sleep(3)
    recv.stop()

    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print(f"Overhead (bytes extras / mensagem útil): {overhead}")
    print("✓ RDT 2.1 finalizado\n")


# =============================
# TESTE RDT 3.0
# =============================
def test_rdt30() -> None:
    print("\n===== TESTE RDT 3.0 =====")
    sim = UnreliableChannel(loss_rate=0.15, corrupt_rate=0.0, delay_range=(0.05, 0.5))
    recv = RDT30Receiver()
    send = RDT30Sender(sim, timeout=2.0)

    recv_thread = threading.Thread(target=recv.start, daemon=True)
    recv_thread.start()

    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes = sum(len(m.encode()) for m in msgs)
    start_time = time.time()

    for msg in msgs:
        logger.log_sent(send.seq, 0)  # Tipo 0 = DATA
        send.send(msg)

    end_time = time.time()
    total_time = end_time - start_time
    throughput = total_bytes / total_time

    time.sleep(2)
    recv.stop()

    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print(f"Throughput efetivo (bytes úteis / tempo total): {throughput:.2f}")
    print("✓ RDT 3.0 finalizado\n")


# =============================
# EXECUÇÃO PRINCIPAL
# =============================
if __name__ == "__main__":
    test_rdt20()
    test_rdt21()
    test_rdt30()
    print("==== TODOS OS TESTES DA FASE 1 FINALIZADOS ====")
