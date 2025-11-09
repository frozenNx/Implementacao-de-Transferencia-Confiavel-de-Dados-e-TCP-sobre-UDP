"""
RDT 3.0 - Reliable Data Transfer com tratamento de perda de pacotes
-------------------------------------------------------------------
Objetivo:
Adicionar tolerância a perdas de pacotes ou ACKs.

Características:
 - Temporizador (timeout) para retransmissões automáticas.
 - Suporte à perda e atraso de pacotes.
 - Alternância de sequência (0/1).
"""

import socket
import threading # Necessário para o Receptor
from typing import Tuple, Optional, Callable, Any
from utils.simulator import UnreliableChannel
from utils.packet import (
    TYPE_DATA, TYPE_ACK,
    make_packet, parse_packet, validate_packet
)
from utils import logger

# =========================
# Emissor
# =========================
class RDT30Sender:
    """Emissor do protocolo RDT 3.0."""
    def __init__(
        self,
        simulator: Optional[UnreliableChannel] = None,
        channel: Optional[UnreliableChannel] = None,   # alternativa nomeada usada pelo test harness
        local_addr: Optional[Tuple[str, int]] = ('localhost', 13002),
        local_port: Optional[int] = None,              # alternativa compatível com código antigo
        dest: Optional[Tuple[str, int]] = ('localhost', 13001),
        dest_addr: Optional[Tuple[str, int]] = None,   # alternativa nomeada usada pelo test harness
        timeout: float = 2.0,
        **kwargs
    ) -> None:
        # Resolver ambiguidades de parâmetro
        self.simulator = simulator if simulator is not None else channel
        if dest_addr is not None:
            self.dest = dest_addr
        else:
            self.dest = dest
        # aceitar local_addr (tupla) ou local_port (int)
        if local_addr is not None:
            bind_addr = local_addr
        elif local_port is not None:
            bind_addr = ('localhost', local_port)
        else:
            bind_addr = ('localhost', 13002)

        # Socket UDP subjacente
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.bind(bind_addr)
        except Exception as e:
            # Em caso de falha no bind (porta em uso), usa porta automática
            logger.info(f"RDT30Sender: falha ao bind em {bind_addr}: {e}. Tentando bind automático.")
            self.sock.bind(('localhost', 0))

        self.seq = 0
        self.timeout = timeout
        self.last_packet = None
        self.retransmissions = 0


    def send(self, msg: Any) -> None:
        """
        Envia uma mensagem (aceita bytes ou string). Implementa timer + retransmissão
        simples (stop-and-wait com seq 0/1).
        """
        if isinstance(msg, str):
            data = msg.encode()
        elif isinstance(msg, (bytes, bytearray)):
            data = bytes(msg)
        else:
            # tenta converter para str
            data = str(msg).encode()

        packet = make_packet(TYPE_DATA, self.seq, data)
        self.last_packet = packet
        attempts = 0

        while True:
            attempts += 1
            if attempts > 1:
                self.retransmissions += 1

            logger.log_sent(seqnum=self.seq, pkt_type=TYPE_DATA)

            # Envio via simulador, se existir; caso contrário, envia direto
            try:
                if self.simulator is not None:
                    # simulador: send(packet, dest_socket, dest_addr)
                    self.simulator.send(packet, self.sock, self.dest)
                else:
                    # envio direto bloqueante
                    self.sock.sendto(packet, self.dest)
            except Exception as e:
                logger.info(f"RDT30Sender: falha ao enviar pacote: {e}")

            # Espera ACK com timeout
            try:
                self.sock.settimeout(self.timeout)
                resp, _ = self.sock.recvfrom(4096)
                # validate_packet -> (pkt_type, seqnum, data, is_valid) ou formato compatível
                pkt_type, ack_seq, _, is_valid = validate_packet(resp)

                if not is_valid:
                    # pacote de controle corrompido
                    logger.log_corrupt(seqnum=self.seq, pkt_type=TYPE_ACK)
                    self.retransmissions += 1
                    continue

                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    logger.log_received(seqnum=ack_seq, pkt_type=TYPE_ACK)
                    self.seq = 1 - self.seq
                    break
                else:
                    logger.info(f"ACK {ack_seq} inesperado (esperado {self.seq}). Retransmitindo.")
                    self.retransmissions += 1
                    # continua loop -> retransmite

            except socket.timeout:
                logger.log_lost(seqnum=self.seq, pkt_type=TYPE_DATA)
                self.retransmissions += 1
                # continua -> retransmite

            finally:
                # remove timeout para evitar efeitos colaterais em outras operações
                try:
                    self.sock.settimeout(None)
                except Exception:
                    pass

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

# =========================
# Receptor
# =========================
class RDT30Receiver:
    """
    Receptor RDT3.0 flexível:
     - aceita local_addr/local_port
     - aceita 'channel' / 'simulator' embora normalmente receptor apenas escute
     - se receber deliver_callback, usa-o em vez de armazenar localmente
    """

    def __init__(
        self,
        local_addr: Optional[Tuple[str, int]] = ('localhost', 13001),
        local_port: Optional[int] = None,
        deliver_callback: Optional[Callable[[Any], None]] = None,
        channel: Optional[UnreliableChannel] = None,
        simulator: Optional[UnreliableChannel] = None,
        verbose: bool = False,
        **kwargs
    ) -> None:
        # resolve bind
        if local_addr is not None:
            bind_addr = local_addr
        elif local_port is not None:
            bind_addr = ('localhost', local_port)
        else:
            bind_addr = ('localhost', 13001)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.bind(bind_addr)
        except Exception as e:
            logger.info(f"RDT30Receiver: falha ao bind em {bind_addr}: {e}. Tentando bind automático.")
            self.sock.bind(('localhost', 0))

        self.expected_seq = 0
        self.received = []
        self.last_ack_seq = 1
        self.running = False
        self._thread = None
        self.deliver_callback = deliver_callback
        self.verbose = verbose
        # channel/simulator não estritamente necessárias para o receiver, mas aceitamos
        self.simulator = simulator if simulator is not None else channel

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.running = True
        self._thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._thread.start()

    def _receive_loop(self) -> None:
        while self.running:
            try:
                self.sock.settimeout(1.0)
                pkt, addr = self.sock.recvfrom(4096)
                self.sock.settimeout(None)
            except socket.timeout:
                continue
            except OSError:
                break

            pkt_type, seqnum, data, is_valid = validate_packet(pkt)

            # Pacote corrompido ou não-DATA
            if not is_valid or pkt_type != TYPE_DATA:
                if not is_valid:
                    logger.log_corrupt(seqnum=seqnum if seqnum is not None else -1, pkt_type=pkt_type)
                # envia último ACK conhecido
                ack = make_packet(TYPE_ACK, self.last_ack_seq)
                try:
                    # usar simulador caso exista; senão envio direto
                    if self.simulator is not None:
                        self.simulator.send(ack, self.sock, addr)
                    else:
                        self.sock.sendto(ack, addr)
                    logger.log_sent(seqnum=self.last_ack_seq, pkt_type=TYPE_ACK)
                except Exception as e:
                    logger.info(f"RDT30Receiver: falha ao enviar ACK: {e}")
                continue

            # Pacote íntegro e esperado
            if seqnum == self.expected_seq:
                try:
                    msg = data.decode(errors='replace')
                except Exception:
                    msg = data
                logger.log_received(seqnum=seqnum, pkt_type=TYPE_DATA)
                # entrega
                if callable(self.deliver_callback):
                    # caso o harness tenha passado um callback (deliver), usa-o
                    try:
                        self.deliver_callback(seqnum, data)
                    except TypeError:
                        # se o callback aceita só o dado
                        self.deliver_callback(data)
                else:
                    # armazena localmente como fallback
                    self.received.append(msg)

                # envia ACK
                ack = make_packet(TYPE_ACK, seqnum)
                try:
                    if self.simulator is not None:
                        self.simulator.send(ack, self.sock, addr)
                    else:
                        self.sock.sendto(ack, addr)
                    logger.log_sent(seqnum=seqnum, pkt_type=TYPE_ACK)
                except Exception as e:
                    logger.info(f"RDT30Receiver: falha ao enviar ACK: {e}")

                self.last_ack_seq = seqnum
                self.expected_seq = 1 - self.expected_seq

            else:
                # duplicado: reenviar último ACK
                logger.info(f"Pacote duplicado (recebido={seqnum}, esperado={self.expected_seq}). Reenviando ACK {self.last_ack_seq}.")
                ack = make_packet(TYPE_ACK, self.last_ack_seq)
                try:
                    if self.simulator is not None:
                        self.simulator.send(ack, self.sock, addr)
                    else:
                        self.sock.sendto(ack, addr)
                    logger.log_sent(seqnum=self.last_ack_seq, pkt_type=TYPE_ACK)
                except Exception as e:
                    logger.info(f"RDT30Receiver: falha ao reenviar ACK: {e}")

    def stop(self) -> None:
        self.running = False
        try:
            self.sock.close()
        except Exception:
            pass
        if self._thread and threading.current_thread() != self._thread:
            self._thread.join(timeout=2.0)

    def get_all_messages(self):
        """Retorna todas as mensagens entregues (apenas para testes)."""
        return list(self.received)