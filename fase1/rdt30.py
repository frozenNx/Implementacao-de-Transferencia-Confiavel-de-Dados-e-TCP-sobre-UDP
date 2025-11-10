"""
RDT 3.0 - Stop-and-Wait com sequência alternada (0/1)
------------------------------------
Funciona com o UnreliableChannel original da Fase 1.
Não depende de sim_receive ou buffer interno.
"""

import queue
import time
from utils.packet import TYPE_DATA, TYPE_ACK, make_packet, parse_packet
from utils import logger

import queue

class RDT30Sender:
    def __init__(self, channel, sender_id, receiver_id, timeout=1.0):
        self.channel = channel
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.timeout = timeout
        self.seq = 0
        self.retransmissions = 0
        self.ack_queue = queue.Queue()  # fila de ACKs recebidos

    def receive_control_packet(self, packet):
        """Chamado pelo simulador para entregar ACKs"""
        self.ack_queue.put(packet)

    def send(self, msg):
        if isinstance(msg, str):
            data = msg.encode()
        else:
            data = bytes(msg)

        pkt = make_packet(TYPE_DATA, self.seq, data)

        while True:
            # envia DATA
            logger.log_sent(self.seq, TYPE_DATA)
            self.channel.send(pkt, dest=self.sender_id, dest_addr=self.receiver_id)

            start = time.time()
            while True:
                try:
                    ack_pkt = self.ack_queue.get(timeout=self.timeout)
                except queue.Empty:
                    # timeout → retransmitir
                    logger.log_timeout(self.seq)
                    self.retransmissions += 1
                    break

                info = parse_packet(ack_pkt)
                if info['type'] == TYPE_ACK and not info['corrupt'] and info['seq'] == self.seq:
                    logger.log_received(info['seq'], TYPE_ACK)
                    self.seq = 1 - self.seq
                    return
                else:
                    logger.info(f"Ignorando ACK inválido ou corrompido: Seq={info['seq']}")



class RDT30Receiver:
    def __init__(self, channel, receiver_id, sender_id, deliver_callback=None):
        self.channel = channel
        self.receiver_id = receiver_id
        self.sender_id = sender_id
        self.expected = 0
        self.received_messages = []
        self.running = True
        self.deliver_callback = deliver_callback

    def receive(self, pkt_bytes):
        """Método chamado pelo canal quando DATA chega."""
        info = parse_packet(pkt_bytes)
        seq = info['seq']

        # Pacote corrompido ou tipo errado → ACK último correto
        if info['corrupt'] or info['type'] != TYPE_DATA:
            # ACK cumulativo para o último pacote correto
            ack = make_packet(TYPE_ACK, (self.expected - 1) % 2)
            self.channel.send(ack, dest=self.receiver_id, dest_addr=self.sender_id)
            return  # sai do método, não continue

        if seq == self.expected:
            # Entrega a mensagem
            msg = info['payload'].decode()
            if self.deliver_callback:
                self.deliver_callback(msg)
            else:
                self.received_messages.append(msg)

            # Atualiza a sequência esperada
            self.expected = 1 - self.expected

        # Sempre envia ACK do último pacote recebido corretamente
        ack = make_packet(TYPE_ACK, (self.expected - 1) % 2)
        self.channel.send(ack, dest=self.receiver_id, dest_addr=self.sender_id)
            
    def stop(self):
        self.running = False

    def get_all_messages(self):
        return list(self.received_messages)
