"""
Go-Back-N (GBN) Reliable Data Transfer Protocol
-----------------------------------------------------
Fase 2 - Projeto de Redes

Descrição:
Implementa o protocolo Go-Back-N (GBN) para comunicação confiável
sobre um canal não confiável (simulado), utilizando:
 - Janela deslizante de tamanho fixo
 - Retransmissão em caso de timeout
 - ACKs cumulativos

"""

import threading
import time
from utils.simulator import UnreliableChannel
from utils.packet import make_packet, parse_packet, TYPE_DATA, TYPE_ACK
from utils import logger


# =========================================================
# Classe Sender - Remetente do Go-Back-N
# =========================================================
class GBN_Sender:
    """Implementa o lado remetente do protocolo Go-Back-N."""

    def __init__(self, channel: UnreliableChannel, window_size: int = 5, timeout: float = 2.0):
        """
        Inicializa o remetente.

        Args:
            channel (UnreliableChannel): Canal de comunicação não confiável.
            window_size (int): Tamanho da janela de envio.
            timeout (float): Tempo limite para retransmissão (em segundos).
        """
        self.channel = channel
        self.window_size = window_size
        self.timeout = timeout
        self.base = 0
        self.next_seq = 0
        self.timer = None
        self.buffer = {}
        self.lock = threading.Lock()
        self.receiver = None  # será associado posteriormente

    def set_receiver(self, receiver):
        """Associa o receptor ao remetente (para simulação local)."""
        self.receiver = receiver

    # ------------------------
    # Controle de Timer
    # ------------------------
    def _start_timer(self):
        """Inicia ou reinicia o temporizador."""
        self._stop_timer()
        self.timer = threading.Timer(self.timeout, self._timeout_event)
        self.timer.start()

    def _stop_timer(self):
        """Para o temporizador atual, se existir."""
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def _timeout_event(self):
        """Evento disparado quando o timer expira (timeout)."""
        with self.lock:
            logger.log_timeout(self.base)
            print(f"[TIMEOUT] Reenviando janela a partir do pacote {self.base}")
            for seq in range(self.base, self.next_seq):
                pkt = self.buffer.get(seq)
                if pkt:
                    self.channel.send(pkt, self.receiver.receive)
            self._start_timer()

    # ------------------------
    # Envio de Dados
    # ------------------------
    def send_data(self, msg: str):
        """
        Envia dados da aplicação, respeitando o tamanho da janela.

        Args:
            msg (str): Mensagem a ser enviada.
        """
        with self.lock:
            if self.next_seq < self.base + self.window_size:
                pkt = make_packet(TYPE_DATA, self.next_seq, msg.encode())
                self.buffer[self.next_seq] = pkt
                self.channel.send(pkt, self.receiver.receive)
                logger.log_sent(self.next_seq, TYPE_DATA)
                print(f"[SEND] Pacote {self.next_seq} enviado: '{msg}'")

                if self.base == self.next_seq:
                    self._start_timer()

                self.next_seq += 1
            else:
                print("[AVISO] Janela cheia. Aguardando ACKs...")

    # ------------------------
    # Recepção de ACKs
    # ------------------------
    def receive_ack(self, ack_pkt: bytes):
        """
        Processa um ACK recebido do receptor.

        Args:
            ack_pkt (bytes): Pacote ACK recebido.
        """
        try:
            pkt_type, seqnum, chksum, _ = parse_packet(ack_pkt)
        except Exception:
            print("[ERRO] Pacote ACK inválido.")
            return

        if pkt_type != TYPE_ACK:
            return

        logger.log_received(seqnum, TYPE_ACK)
        print(f"[ACK] Recebido ACK {seqnum}")

        with self.lock:
            self.base = seqnum + 1
            if self.base == self.next_seq:
                self._stop_timer()
            else:
                self._start_timer()


# =========================================================
# Classe Receiver - Receptor do Go-Back-N
# =========================================================
class GBN_Receiver:
    """Implementa o lado receptor do protocolo Go-Back-N."""

    def __init__(self, channel: UnreliableChannel):
        """
        Inicializa o receptor.

        Args:
            channel (UnreliableChannel): Canal de comunicação não confiável.
        """
        self.channel = channel
        self.expected_seq = 0
        self.sender = None  # será associado posteriormente

    def set_sender(self, sender: GBN_Sender):
        """Associa o remetente ao receptor (para simulação local)."""
        self.sender = sender

    def receive(self, pkt: bytes):
        """
        Recebe um pacote do remetente e responde com o ACK apropriado.

        Args:
            pkt (bytes): Pacote recebido.
        """
        try:
            pkt_type, seqnum, chksum, data = parse_packet(pkt)
        except Exception:
            print("[ERRO] Pacote corrompido (não pôde ser decodificado).")
            return

        msg = data.decode(errors="replace")

        if pkt_type != TYPE_DATA:
            return

        if chksum == chksum and seqnum == self.expected_seq:
            print(f"[RECV] Pacote {seqnum} recebido corretamente: '{msg}'")
            self.expected_seq += 1
        else:
            print(f"[RECV] Pacote fora de ordem ou corrompido: {seqnum}")

        # Envia ACK cumulativo
        ack_pkt = make_packet(TYPE_ACK, self.expected_seq - 1)
        self.channel.send(ack_pkt, self.sender.receive_ack)
        logger.log_sent(self.expected_seq - 1, TYPE_ACK)
