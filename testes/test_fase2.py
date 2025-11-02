"""
Testes da Fase 2 - Protocolo Go-Back-N (GBN)
-----------------------------------------------------
Simula o envio de pacotes através de um canal não confiável,
usando as classes implementadas em gbn.py.
"""

import time
from fase2.gbn import GBN_Sender, GBN_Receiver
from utils.simulator import UnreliableChannel

if __name__ == "__main__":
    print("\n==== INÍCIO DA SIMULAÇÃO Go-Back-N ====\n")

    # Criação dos componentes principais
    channel = UnreliableChannel(loss_rate=0.2, corrupt_rate=0.1, delay_range=(0.1, 0.3))
    sender = GBN_Sender(channel, window_size=4, timeout=2.0)
    receiver = GBN_Receiver(channel)

    # Associação cruzada (simula comunicação bidirecional)
    sender.set_receiver(receiver)
    receiver.set_sender(sender)

    # Envio de mensagens de teste
    for i in range(10):
        sender.send_data(f"Mensagem {i}")
        time.sleep(0.2)

    # Espera processamento dos pacotes restantes
    time.sleep(5)
    print("\n==== FIM DA SIMULAÇÃO ====\n")
