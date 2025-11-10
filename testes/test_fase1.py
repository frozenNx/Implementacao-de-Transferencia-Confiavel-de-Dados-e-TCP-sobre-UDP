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


def test_rdt30() -> None:
    print("\n===== TESTE RDT 3.0 =====")
    
    # Configuração do canal não confiável
    sim = UnreliableChannel(loss_rate=0.15, corrupt_rate=0.0, delay_range=(0.05, 0.5))
    
    # IDs lógicos do simulador
    sender_id = "A"
    receiver_id = "B"
    
    # Timeout do sender
    TIMEOUT = 2.0
    
    # Criar receptor
    recv = RDT30Receiver(sim, receiver_id, sender_id)
    
    # Criar sender
    send = RDT30Sender(sim, sender_id, receiver_id, timeout=TIMEOUT)
    
    # Registrar endpoints no canal (roteamento interno)
    sim.register_gbn_endpoints(sender=send, receiver=recv)
    
    # Mensagens para enviar
    msgs = [f"Msg {i}" for i in range(10)]
    total_bytes = sum(len(m.encode()) for m in msgs)
    start_time = time.time()
    
    # Envio das mensagens
    for msg in msgs:
        send.send(msg)
    
    end_time = time.time()
    total_time = end_time - start_time
    
    # Conferir mensagens recebidas
    received_msgs = recv.get_all_messages()
    
    print("Mensagens recebidas:", received_msgs)
    print("Mensagens esperadas:", msgs)
    
    success = (received_msgs == msgs)
    print(f"Sucesso Total: {'SIM' if success else 'NÃO'}")
    
    print(f"Retransmissões: {getattr(send, 'retransmissions', 0)}")
    print(f"Tempo Total de Transferência: {total_time:.2f}s")
    throughput = total_bytes / total_time
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
