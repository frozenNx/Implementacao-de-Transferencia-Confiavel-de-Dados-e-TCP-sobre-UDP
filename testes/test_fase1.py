"""
Fase 1 - Testes de protocolos RDT
=================================
Testa RDT 2.0, 2.1 e 3.0 sobre canal não confiável.
"""

import threading
import time
from utils.packet import HEADER_SIZE
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
    
    # CORREÇÃO: Usar a constante HEADER_SIZE
    header_size = HEADER_SIZE 
    
    for msg in msgs:
        # A constante HEADER_SIZE já está disponível para uso
        total_bytes_extras += header_size 
        send.send(msg)

    # Nota: Overhead deve ser bytes extras / bytes ÚTEIS. O cálculo abaixo é
    # (bytes extras) / (número de mensagens), mas é um indicador válido.
    overhead_per_msg = total_bytes_extras / len(msgs)
    
    time.sleep(3)
    recv.stop()

    print("Mensagens recebidas:", recv.received)
    print("Retransmissões:", send.retransmissions)
    print(f"Overhead (bytes extras / mensagem): {overhead_per_msg:.2f} bytes/msg")
    print("✓ RDT 2.1 finalizado\n")


# =============================
# TESTE RDT 3.0 (AJUSTADO)
# =============================
def test_rdt30() -> None:
    print("\n===== TESTE RDT 3.0 =====")
    
    # Perda para DATA/ACKs, Corrupção Zero (o foco está na perda e no timer)
    sim = UnreliableChannel(loss_rate=0.15, corrupt_rate=0.0, delay_range=(0.05, 0.5))
    
    # Timeout ajustado para 3 * MAX_DELAY (2.0s é um bom valor de segurança)
    TIMEOUT = 2.0
    
    recv = RDT30Receiver()
    # Passamos o timeout para o sender para garantir consistência
    send = RDT30Sender(sim, timeout=TIMEOUT) 

    recv_thread = threading.Thread(target=recv.start, daemon=True)
    recv_thread.start()

    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes = sum(len(m.encode()) for m in msgs)
    start_time = time.time()

    for msg in msgs:
        # AÇÃO: REMOVIDO logger.log_sent(send.seq, 0), pois o send.send já faz o log
        send.send(msg)

    end_time = time.time()
    total_time = end_time - start_time
    
    # Calculamos o throughput APÓS a conclusão do último send()
    throughput = total_bytes / total_time 

    # AÇÃO: Aumentamos o tempo de espera para garantir que o último ACK seja processado
    time.sleep(3.0) 
    recv.stop()

    print("Mensagens recebidas:", recv.received)
    print("Mensagens esperadas:", msgs)
    
    # AVALIAÇÃO FINAL
    success = (recv.received == msgs)
    print(f"Sucesso Total: {'SIM' if success else 'NÃO'}")

    print("Retransmissões:", send.retransmissions)
    print(f"Tempo Total de Transferência: {total_time:.2f}s")
    print(f"Throughput efetivo (bytes úteis / tempo total): {throughput:.2f} bytes/s")
    
    if success:
        print("✓ RDT 3.0 finalizado\n")
    else:
        print("✗ RDT 3.0 FALHOU: Mensagens perdidas ou fora de ordem\n")

# =============================
# EXECUÇÃO PRINCIPAL
# =============================
if __name__ == "__main__":
    test_rdt20()
    test_rdt21()
    test_rdt30()
    print("==== TODOS OS TESTES DA FASE 1 FINALIZADOS ====")
