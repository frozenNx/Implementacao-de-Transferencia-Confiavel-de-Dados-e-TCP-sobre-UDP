"""
utils/logger.py
-----------------------------------
Sistema de logging para registrar eventos de envio, recepção, perda e corrupção de pacotes.
Todos os logs incluem timestamp para auxiliar na depuração.
"""

import time


def _timestamp() -> str:
    """
    Gera timestamp formatado para logs.

    Returns:
        str: horário atual no formato HH:MM:SS
    """
    return time.strftime("%H:%M:%S", time.localtime())


def log_sent(seqnum: int, pkt_type: int) -> None:
    """
    Registra que um pacote foi enviado.

    Args:
        seqnum (int): número de sequência do pacote
        pkt_type (int): tipo do pacote (0=DATA, 1=ACK)
    """
    print(f"[{_timestamp()}] ENVIADO   - Tipo: {pkt_type}, Seq: {seqnum}")


def log_received(seqnum: int, pkt_type: int) -> None:
    """
    Registra que um pacote foi recebido.

    Args:
        seqnum (int): número de sequência do pacote
        pkt_type (int): tipo do pacote (0=DATA, 1=ACK)
    """
    print(f"[{_timestamp()}] RECEBIDO - Tipo: {pkt_type}, Seq: {seqnum}")


def log_lost(seqnum: int, pkt_type: int | None = None) -> None:
    """
    Registra que um pacote foi perdido.

    Args:
        seqnum (int): número de sequência do pacote
        pkt_type (int | None): tipo do pacote (opcional)
    """
    info = f" Tipo: {pkt_type}" if pkt_type is not None else ""
    print(f"[{_timestamp()}] PERDIDO   - Seq: {seqnum}{info}")


def log_corrupt(seqnum: int, pkt_type: int | None = None) -> None:
    """
    Registra que um pacote foi corrompido.

    Args:
        seqnum (int): número de sequência do pacote
        pkt_type (int | None): tipo do pacote (opcional)
    """
    info = f" Tipo: {pkt_type}" if pkt_type is not None else ""
    print(f"[{_timestamp()}] CORROMPIDO - Seq: {seqnum}{info}")
    
def log_timeout(seqnum: int) -> None:
    """
    Registra que ocorreu um timeout para o pacote especificado.

    Args:
        seqnum (int): número de sequência do pacote que expirou o timer
    """
    print(f"[{_timestamp()}] TIMEOUT   - Seq: {seqnum}")