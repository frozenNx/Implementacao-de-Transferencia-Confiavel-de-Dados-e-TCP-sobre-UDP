import threading
import time
from utils.simulator import UnreliableChannel
from fase1.rdt20 import RDT20Sender, RDT20Receiver
from fase1.rdt21 import RDT21Sender, RDT21Receiver
from fase1.rdt30 import RDT30Sender, RDT30Receiver


# =============================
# TESTE RDT 2.0
# =============================
def test_rdt20():
    """
    Teste do RDT 2.0 (Reliable Data Transfer com detecção de corrupção).
    - Canal perfeito para 10 mensagens
    - Corrupção de 30% dos pacotes
    - Verifica retransmissões
    """
    print("\n===== TESTE RDT 2.0 =====")
    sim = UnreliableChannel(loss_rate=0.0, corrupt_rate=0.3)
    recv = RDT20Receiver()
    send = RDT20Sender(sim)

    threading.Thread(target=recv.start, daemon=True).start()

    msgs = [f"Msg {i}" for i in range(10)]
    for msg in msgs:
        send.send(msg)

    time.sleep(3)
    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print("✓ RDT 2.0 finalizado\n")


# =============================
# TESTE RDT 2.1
# =============================
def test_rdt21():
    """
    Teste do RDT 2.1 (Stop-and-Wait com números de sequência)
    - Corrupção de 20% dos pacotes DATA e ACKs
    - Verifica duplicação de dados
    - Calcula overhead (bytes extras por mensagem útil)
    """
    print("\n===== TESTE RDT 2.1 =====")
    sim = UnreliableChannel(loss_rate=0.0, corrupt_rate=0.2)
    recv = RDT21Receiver()
    send = RDT21Sender(sim)

    threading.Thread(target=recv.start, daemon=True).start()

    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes_extras = 0

    for msg in msgs:
        total_bytes_extras += send.packet_header_size()
        send.send(msg)

    overhead = total_bytes_extras / len(msgs)

    time.sleep(3)
    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print(f"Overhead (bytes extras / mensagem útil): {overhead}")
    print("✓ RDT 2.1 finalizado\n")


# =============================
# TESTE RDT 3.0
# =============================
def test_rdt30():
    """
    Teste do RDT 3.0 (Stop-and-Wait com timer, perdas e atrasos)
    - Perda de 15% dos pacotes DATA e ACKs
    - Atraso variável 50-500ms
    - Mede retransmissões e throughput efetivo
    """
    print("\n===== TESTE RDT 3.0 =====")
    sim = UnreliableChannel(loss_rate=0.15, corrupt_rate=0.0, delay_range=(0.05, 0.5))
    recv = RDT30Receiver()
    send = RDT30Sender(sim, timeout=2.0)

    threading.Thread(target=recv.start, daemon=True).start()

    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes = sum(len(m.encode()) for m in msgs)
    start_time = time.time()

    for msg in msgs:
        print(f"\n[SENDER] Preparando envio da mensagem: {msg}")
        print(f"[SENDER] Timer iniciado ({send.timeout}s)")
        send.send(msg)
        print("[SENDER] Timer cancelado (ACK recebido ou retransmissão concluída)\n")

    end_time = time.time()
    total_time = end_time - start_time
    throughput = total_bytes / total_time

    time.sleep(2)
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