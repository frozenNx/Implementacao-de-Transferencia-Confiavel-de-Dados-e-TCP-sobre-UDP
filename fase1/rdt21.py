
"""
rdt21.py — Implementação do protocolo RDT 2.1 (Reliable Data Transfer 2.1)

Este módulo implementa o protocolo RDT 2.1, responsável por comunicação confiável
em um canal não confiável, considerando perdas e corrupção de pacotes.
A versão 2.1 introduz numeração de sequência para lidar com retransmissões
duplicadas e ACKs corrompidos, garantindo entrega correta e ordenada.

Componentes:
- RDT21Sender: Emissor com retransmissão e controle de sequência.
- RDT21Receiver: Receptor com verificação de integridade e controle de duplicatas.
"""

import socket
import struct
import hashlib
import time
from utils.simulator import UnreliableChannel


# ========================
# CONSTANTES E ESTRUTURAS
# ========================

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2

# Formato: Tipo (1 byte), SeqNum (1 byte), Checksum (8 bytes)
HEADER_FMT = "!BB8s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


# ========================
# FUNÇÕES AUXILIARES
# ========================

def checksum(data: bytes) -> bytes:
    """
    Calcula o checksum de 8 bytes de um dado utilizando o algoritmo MD5.

    Parâmetros:
        data (bytes | str): Dados a serem verificados.

    Retorna:
        bytes: Checksum de 8 bytes.
    """
    if isinstance(data, str):
        data = data.encode()
    return hashlib.md5(data).digest()[:8]


def make_packet(pkt_type: int, seqnum: int = 0, data: bytes = b'') -> bytes:
    """
    Monta um pacote com cabeçalho e dados.

    Parâmetros:
        pkt_type (int): Tipo do pacote (DATA, ACK, NAK).
        seqnum (int): Número de sequência (0 ou 1).
        data (bytes): Dados da mensagem.

    Retorna:
        bytes: Pacote completo pronto para envio.
    """
    chksum = checksum(data)
    header = struct.pack(HEADER_FMT, pkt_type, seqnum, chksum)
    return header + data


def parse_packet(packet: bytes):
    """
    Lê um pacote e separa seus campos.

    Parâmetros:
        packet (bytes): Pacote recebido.

    Retorna:
        tuple: (pkt_type, seqnum, chksum, data)
    """
    if len(packet) < HEADER_SIZE:
        raise ValueError("Pacote muito curto para análise.")
    pkt_type, seqnum, chksum = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
    data = packet[HEADER_SIZE:]
    return pkt_type, seqnum, chksum, data


# ========================
# CLASSE DO EMISSOR
# ========================

class RDT21Sender:
    """
    Implementa o protocolo RDT 2.1 no lado emissor.

    Funções principais:
    - Envia mensagens utilizando numeração alternada (0/1).
    - Retransmite em caso de timeout, NAK ou ACK inválido.
    - Utiliza checksum para detecção de corrupção.
    """

    def __init__(self, simulator: UnreliableChannel, local_port=12000,
                 dest=('localhost', 12001), timeout=2.0):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.simulator = simulator
        self.dest = dest
        self.seq = 0  # Número de sequência atual (0 ou 1)
        self.timeout = timeout
        self.retransmissions = 0  # contador de retransmissões

    def packet_header_size(self):
        """Retorna tamanho do header do pacote: tipo(1) + seq(1) + checksum(8)"""
        return 1 + 1 + 8

    def send(self, msg: str):
        """
        Envia uma mensagem confiável pelo canal.

        Parâmetros:
            msg (str): Mensagem a ser transmitida.
        """
        data = msg.encode()
        packet = make_packet(TYPE_DATA, self.seq, data)
        attempts = 0

        while True:
            attempts += 1
            print(f"[SENDER] Enviando (seq={self.seq}): {msg} (tentativa {attempts})")
            self.simulator.send(packet, self.sock, self.dest)

            try:
                self.sock.settimeout(self.timeout)
                resp, _ = self.sock.recvfrom(1024)
                pkt_type, ack_seq, _, _ = parse_packet(resp)

                # Verifica se é ACK válido
                if pkt_type == TYPE_ACK and ack_seq == self.seq:
                    print(f"[SENDER] ACK recebido (seq={ack_seq})")
                    self.seq = 1 - self.seq  # Alterna número de sequência
                    break
                elif pkt_type == TYPE_NAK:
                    print("[SENDER] NAK recebido → retransmitindo")
                    self.retransmissions += 1
                else:
                    print("[SENDER] ACK inválido/corrompido → retransmitindo")
                    self.retransmissions += 1

            except socket.timeout:
                print("[SENDER] Timeout → retransmitindo")
                self.retransmissions += 1



# ========================
# CLASSE DO RECEPTOR
# ========================

class RDT21Receiver:
    """
    Implementa o protocolo RDT 2.1 no lado receptor.

    Funções principais:
    - Detecta pacotes corrompidos via checksum.
    - Rejeita duplicatas com base no número de sequência.
    - Envia ACK ou NAK conforme o estado do pacote.
    """

    def __init__(self, local_port=12001):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', local_port))
        self.expected_seq = 0  # Próximo número de sequência esperado
        self.received = []     # Lista de mensagens entregues à aplicação
        self.last_ack_seq = 1  # Último ACK enviado
        self.running = False

    def start(self):
        """Inicia o loop principal do receptor."""
        self.running = True
        while self.running:
            try:
                pkt, addr = self.sock.recvfrom(4096)
            except OSError:
                break

            try:
                pkt_type, seqnum, recv_chksum, data = parse_packet(pkt)
            except Exception as e:
                print("[RECEIVER] Pacote inválido recebido:", e)
                continue

            # Verifica integridade do pacote
            calc_chksum = checksum(data)
            if recv_chksum != calc_chksum:
                print("[RECEIVER] Pacote Corrompido -> enviando NAK")
                self.sock.sendto(make_packet(TYPE_NAK, seqnum, b''), addr)
                continue

            # Processa pacotes DATA válidos
            if pkt_type == TYPE_DATA:
                if seqnum == self.expected_seq:
                    msg = data.decode(errors='replace')
                    print(f"[RECEIVER] Pacote OK (seq={seqnum}): {msg}")
                    self.received.append(msg)
                    # Envia ACK e alterna sequência esperada
                    ack = make_packet(TYPE_ACK, seqnum, b'')
                    self.sock.sendto(ack, addr)
                    self.last_ack_seq = seqnum
                    self.expected_seq = 1 - self.expected_seq
                else:
                    # Pacote duplicado → reenvia último ACK
                    print(f"[RECEIVER] Pacote duplicado (seq={seqnum}) → reenviando ACK(seq={self.last_ack_seq})")
                    ack = make_packet(TYPE_ACK, self.last_ack_seq, b'')
                    self.sock.sendto(ack, addr)
            else:
                print("[RECEIVER] Recebeu pacote não-DATA no receptor → ignorando")

    def stop(self):
        """Encerra o receptor e fecha o socket."""
        self.running = False
        try:
            self.sock.close()
        except:
            pass