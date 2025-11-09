"""
utils/packet.py
-----------------------------------
Módulo utilitário para criação, parsing e validação de pacotes.

Formato do pacote:
+--------+---------+------------+
| Tipo   | SeqNum  | Checksum   |
| (1B)   | (4B)    | (8B)       |
+--------+---------+------------+
| Dados (variável) |
+------------------+
"""
import struct
import hashlib
from typing import Dict, Any, Optional, Tuple

# ============================
# Definições globais
# ============================
# !BI8s: ! (Network Byte Order), B (1 byte tipo), I (4 bytes seqnum - unsigned int), 8s (8 bytes checksum)
HEADER_FMT = "!BI8s"
HEADER_SIZE = struct.calcsize(HEADER_FMT)

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2


# ============================
# Funções auxiliares (Logger Simplificado)
# ============================

class SimplifiedLogger:
    def __init__(self, verbose):
        self.verbose = verbose
    def info(self, msg):
        if self.verbose: print(f"[INFO] {msg}")
    def log_sent(self, seq, type):
        if self.verbose: print(f"[SENT] Seq={seq}, Type={type}")
    def log_received(self, seq, type):
        if self.verbose: print(f"[RCV] ACK={seq}, Type={type}")
    def log_timeout(self, base):
        print(f"[TIMEOUT] Base={base} - Retransmitindo janela.")
    def log_retransmit(self, seq, reason):
        print(f"[RTR] Seq={seq} - Motivo: {reason}")
    def log_duplicate_ack(self, seq, count):
        if self.verbose: print(f"[DUP ACK] Seq={seq}, Count={count}")
    def log_fast_retransmit(self, base):
        print(f"[FAST RTR] Base={base} - 3 ACKs duplicados. Retransmitindo janela.")

logger = SimplifiedLogger(verbose=True)

# ============================
# Funções de Pacote
# ============================

def checksum(data: bytes) -> bytes:
    """
    Calcula um checksum de 8 bytes (MD5 truncado).
    
    Args:
        data (bytes): Dados para calcular checksum
    
    Returns:
        bytes: Checksum de 8 bytes
    """
    if isinstance(data, str):
        data = data.encode()
    # Retorna os primeiros 8 bytes do hash MD5
    return hashlib.md5(data).digest()[:8]

def validate_checksum(chksum_rcv: bytes, data: bytes) -> bool:
    """Valida se o checksum recebido corresponde aos dados."""
    return chksum_rcv == checksum(data)


def make_packet(pkt_type: int, seqnum: int, data: bytes = b'') -> bytes:
    """
    Monta um pacote serializado no formato binário.
    O checksum é calculado sobre o payload (data).
    """
    chksum = checksum(data) 
    
    # Empacota cabeçalho: tipo, seqnum e checksum
    header = struct.pack(HEADER_FMT, pkt_type, seqnum, chksum)
    return header + data


def parse_packet(packet: bytes) -> Optional[Dict[str, Any]]:
    """
    Desmonta pacote em seus campos individuais e verifica o tamanho.
    
    Returns:
        Optional[Dict]: Dicionário com (type, seq, payload, corrupt) ou None se muito curto.
    """
    if len(packet) < HEADER_SIZE:
        logger.info(f"Pacote muito curto (tamanho {len(packet)} < {HEADER_SIZE})")
        return None

    try:
        # Desempacota: tipo (int), seqnum (int), checksum (bytes)
        pkt_type, seqnum, chksum_rcv = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
        data = packet[HEADER_SIZE:]
    except struct.error:
        # Em caso de corrupção extrema que afeta o header (improvável com struct.unpack), mas seguro.
        logger.info("Erro ao desempacotar o cabeçalho do pacote.")
        return None

    is_valid = validate_checksum(chksum_rcv, data)

    return {
        'type': pkt_type,
        'seq': seqnum,        
        'payload': data,
        'corrupt': not is_valid
    }


def validate_packet(packet: bytes) -> Tuple[Optional[int], Optional[int], Optional[bytes], bool]:
    """
    Analisa e valida um pacote recebido.
    
    Returns:
        tuple: (tipo, seqnum, dados, valido=True/False)
    """
    info = parse_packet(packet)
    
    # 1. Pacote muito curto ou erro de parsing
    if info is None:
        return None, None, None, False
        
    # 2. Pacote parseado, mas corrompido (checksum falhou)
    if info['corrupt']:
        return info['type'], info['seq'], info['payload'], False
        
    # 3. Pacote parseado e válido
    return info['type'], info['seq'], info['payload'], True


def packet_header_size() -> int:
    """
    Retorna o tamanho do cabeçalho (em bytes).
    """
    return HEADER_SIZE