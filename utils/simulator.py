import random
import threading

class UnreliableChannel:
    def __init__(self, loss_rate=0.1, corrupt_rate=0.1, delay_range=(0.01, 0.5)):
        self.loss_rate = loss_rate
        self.corrupt_rate = corrupt_rate
        self.delay_range = delay_range

    def send(self, packet, dest_socket, dest_addr):
        # Simula perda
        if random.random() < self.loss_rate:
            print("[CANAL] Pacote perdido")
            return

        # Simula corrupção
        if random.random() < self.corrupt_rate:
            packet = self._corrupt_packet(packet)
            print("[CANAL] Pacote corrompido")

        # Simula atraso
        delay = random.uniform(*self.delay_range)
        threading.Timer(delay, lambda: dest_socket.sendto(packet, dest_addr)).start()

    def _corrupt_packet(self, packet):
        packet_list = list(packet)
        idx = random.randint(0, len(packet_list) - 1)
        packet_list[idx] ^= 0xFF  # Inverte bits
        return bytes(packet_list)
