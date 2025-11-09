"""
utils/logger.py
-----------------------------------
Sistema de logging para registrar eventos de envio, recepção, perda e corrupção de pacotes.
Usa timestamp em milissegundos para alta precisão.
"""

import time
import sys
from typing import Optional

# --- Constantes (Tipos de Pacote) ---
PKT_TYPES = {
    0: "DATA",
    1: "ACK",
    2: "NAK",
}

# --- Variável Global para Controlar a Saída ---
LOG_FILE = None 

def _output(message: str, stream=sys.stdout) -> None:
    """Função interna para direcionar a saída do log."""
    print(message, file=stream)
    if LOG_FILE:
        with open(LOG_FILE, 'a') as f:
            print(message, file=f)

# CORREÇÃO CRÍTICA APLICADA AQUI
def _timestamp() -> str:
    """Gera timestamp formatado com milissegundos (HH:MM:SS.ms)."""
    now = time.time()
    # Pega o tempo formatado sem a fração de segundo
    base_time = time.strftime("%H:%M:%S", time.localtime(now))
    # Calcula os milissegundos (parte fracionária * 1000)
    ms = int(now * 1000) % 1000
    return f"{base_time}.{ms:03d}"

def _get_pkt_type_str(pkt_type: Optional[int]) -> str:
    """Converte o inteiro do tipo de pacote para string simbólica."""
    if pkt_type is None:
        return "N/A"
    return PKT_TYPES.get(pkt_type, f"TYPE:{pkt_type}")


# =================================================================
# FUNÇÕES DE LOG PRINCIPAIS
# =================================================================

def log_sent(seqnum: int, pkt_type: int) -> None:
    """Registra que um pacote foi enviado."""
    pkt_str = _get_pkt_type_str(pkt_type)
    _output(f"[{_timestamp()}] [SND] ENVIADO    | Tipo: {pkt_str:4s} | Seq: {seqnum}")

def log_received(seqnum: int, pkt_type: int) -> None:
    """Registra que um pacote foi recebido (no socket)."""
    pkt_str = _get_pkt_type_str(pkt_type)
    _output(f"[{_timestamp()}] [RCV] RECEBIDO   | Tipo: {pkt_str:4s} | Seq: {seqnum}")

def log_delivered(msg_index: int) -> None:
    """Registra que o pacote DATA foi entregue à aplicação."""
    _output(f"[{_timestamp()}] [APP] ENTREGUE   | Mensagem Index: {msg_index}")

def log_lost(seqnum: int, pkt_type: Optional[int] = None) -> None:
    """Registra que um pacote foi perdido pelo simulador ou canal."""
    pkt_str = _get_pkt_type_str(pkt_type)
    _output(f"[{_timestamp()}] [CHAN] PERDIDO   | Tipo: {pkt_str:4s} | Seq: {seqnum}")

def log_corrupt(seqnum: Optional[int], pkt_type: Optional[int] = None) -> None:
    """Registra que um pacote foi corrompido e detectado pelo checksum."""
    pkt_str = _get_pkt_type_str(pkt_type)
    _output(f"[{_timestamp()}] [CHK] CORROMPIDO | Tipo: {pkt_str:4s} | Seq: {seqnum}")

def log_timeout(seqnum: int) -> None:
    """Registra que ocorreu um timeout para o pacote especificado."""
    _output(f"[{_timestamp()}] [TIMER] TIMEOUT  | Seq: {seqnum} | AÇÃO: Retransmitir")

def log_retransmit(seqnum: int, reason: str = "Timeout/NAK") -> None:
    """Registra que o Remetente está retransmitindo um pacote."""
    _output(f"[{_timestamp()}] [SND] RETRANSMIT | Seq: {seqnum} | Motivo: {reason}")


# =================================================================
# FUNÇÕES AUXILIARES E DE CONTROLE
# =================================================================

def info(message: str) -> None:
    """Log de informação geral."""
    _output(f"[{_timestamp()}] [INFO] {message}")
    
def error(message: str) -> None:
    """Log de erro crítico."""
    _output(f"[{_timestamp()}] [ERRO] {message}", stream=sys.stderr)

def header(title: str) -> None:
    """Gera um cabeçalho visual para separar seções."""
    separator = "=" * len(title)
    _output(f"\n{separator}\n{title}\n{separator}")
    
def debug(message: str) -> None:
    """Log de debug de baixo nível."""
    _output(f"[{_timestamp()}] [DEBUG] {message}")

def config(**kwargs) -> None:
    """Loga as configurações atuais do ambiente de teste."""
    header("CONFIGURAÇÃO DE TESTE")
    for key, value in kwargs.items():
        _output(f"| {key:<20s}: {value}")
    _output("-" * 30)
    
def success(message: str) -> None:
    """Loga uma mensagem de sucesso."""
    _output(f"\n[SUCESSO] {message}\n")
    
def clear_log() -> None:
    """Limpa o arquivo de log se estiver configurado."""
    if LOG_FILE:
        with open(LOG_FILE, 'w') as f:
            f.write(f"Log iniciado em: {time.ctime()}\n")
            
def log_duplicate_ack(seq, count):
    """Loga quando um ACK duplicado é recebido."""
    _output(f"[{_timestamp()}] [DUP ACK] Seq: {seq} | Contagem: {count}")

def log_fast_retransmit(base):
    """Loga quando a Retransmissão Rápida é acionada."""
    _output(f"[{_timestamp()}] [FAST RTR] Base: {base} | AÇÃO: Retransmitir Rápido")