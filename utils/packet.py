"""
utils/packet.py
-----------------------------------
Módulo utilitário para criação, parsing e validação de pacotes.

Fornece funções genéricas usadas pelos protocolos RDT, Go-Back-N e Selective Repeat.
Segue o formato padrão de cabeçalho usado em todas as fases do projeto.

Formato do pacote:
+--------+---------+------------+
| Tipo   | SeqNum  | Checksum   |
| (1B)   | (4B)    | (8B)       |
+--------+---------+------------+
| Dados (variável) |
+------------------+
"""

import struct
import hashlib

# ============================
# Definições globais
# ============================
HEADER_FMT = "!BI8s"  # (1 byte tipo, 4 bytes seqnum, 8 bytes checksum)
HEADER_SIZE = struct.calcsize(HEADER_FMT)

TYPE_DATA = 0
TYPE_ACK = 1
TYPE_NAK = 2


# ============================
# Funções auxiliares
# ============================
def checksum(data: bytes) -> bytes:
    """
    Calcula um checksum de 8 bytes (MD5 truncado).
    Usa o conteúdo de 'data' para gerar um hash que permite verificar corrupção.
    
    Args:
        data (bytes | str): Dados para calcular checksum
    
    Returns:
        bytes: Checksum de 8 bytes
    """
    if isinstance(data, str):
        data = data.encode()
    return hashlib.md5(data).digest()[:8]


def make_packet(pkt_type: int, seqnum: int, data: bytes = b'') -> bytes:
    """
    Monta um pacote serializado no formato binário.
    
    Args:
        pkt_type (int): Tipo do pacote (0 = DATA, 1 = ACK, 2 = NAK)
        seqnum (int): Número de sequência
        data (bytes): Payload em bytes
    
    Returns:
        bytes: Pacote pronto para envio via socket
    """
    chksum = checksum(data)
    header = struct.pack(HEADER_FMT, pkt_type, seqnum, chksum)
    return header + data


def parse_packet(packet: bytes, return_header: bool = False):
    """
    Desmonta pacote em seus campos individuais.
    
    Args:
        packet (bytes): Pacote completo em bytes
        return_header (bool): Se True, retorna também o header bruto
    
    Returns:
        tuple: (tipo, seqnum, checksum, dados) ou
               (tipo, seqnum, checksum, dados, header_bytes) se return_header=True
    
    Raises:
        ValueError: Se o pacote for muito curto
    """
    if len(packet) < HEADER_SIZE:
        raise ValueError("Pacote muito curto para conter cabeçalho")

    pkt_type, seqnum, chksum = struct.unpack(HEADER_FMT, packet[:HEADER_SIZE])
    data = packet[HEADER_SIZE:]

    if return_header:
        return pkt_type, seqnum, chksum, data, packet[:HEADER_SIZE]
    return pkt_type, seqnum, chksum, data


def packet_header_size() -> int:
    """
    Retorna o tamanho do cabeçalho (em bytes).
    
    Returns:
        int: tamanho do cabeçalho
    """
    return HEADER_SIZE
