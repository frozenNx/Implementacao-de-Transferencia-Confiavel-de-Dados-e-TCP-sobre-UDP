"""
===========================================================
Módulo: logger.py
===========================================================

Implementa o sistema de logging utilizado por todos os
módulos do projeto (RDT, TCP-Sobre-UDP, Canal, etc).

O logger registra:
    - Envio e recebimento de pacotes
    - Eventos de perda/corrupção
    - Timeouts e retransmissões
    - ACKs e updates de janela
    - Mensagens gerais do sistema

Características:
    - Thread-safe via Lock
    - Impressão no console
    - Escrita em arquivo no diretório /logs
    - Arquivos nomeados automaticamente com timestamp
===========================================================
"""

import os
import datetime
import threading


class Logger:
    """
    Logger thread-safe para registrar eventos dos protocolos RDT
    e TCP simplificado.

    Características:
        - Permite múltiplos componentes (origin)
        - Escrita protegida com Lock
        - Log simultâneo no console e arquivo .log
        - Criação automática do diretório de logs
        - Arquivos nomeados: <prefix>_YYYYMMDD_HHMMSS.log

    Args:
        log_dir (str): Diretório onde os arquivos .log serão armazenados.
        prefix (str): Prefixo do arquivo de log. Ex.: "tcp", "rdt20".
        origin (str): Nome do módulo emissor ("TCP", "RDT", "CHANNEL"...)
    """

    def __init__(self, log_dir="logs", prefix="log", origin=None):
        self.log_dir = log_dir
        self.prefix = prefix
        self.origin = origin or "SYSTEM"

        self.lock = threading.Lock()  # Thread-safe

        os.makedirs(self.log_dir, exist_ok=True)

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.prefix}_{timestamp}.log"

        self.file_path = os.path.join(self.log_dir, filename)
        self.file = open(self.file_path, "a", encoding="utf-8")

    # ============================================================ #
    # Método central
    # ============================================================ #
    def log(self, event_type: str, message: str):
        """
        Registra uma linha de log formatada.

        Formato:
            [HH:MM:SS.mmm] [ORIGEM   ] [TIPO   ] mensagem

        Args:
            event_type (str): Categoria do evento (SEND, ACK, RECV…)
            message (str): Texto do evento.
        """
        if self.file.closed:
            return

        now = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
        formatted = (
            f"[{now}] "
            f"[{self.origin:9}] "
            f"[{event_type.upper():7}] "
            f"{message}"
        )

        with self.lock:
            print(formatted)
            self.file.write(formatted + "\n")
            self.file.flush()

    # ============================================================ #
    # Categorias de log
    # ============================================================ #
    def info(self, message):    self.log("INFO", message)
    def send(self, message):    self.log("SEND", message)
    def recv(self, message):    self.log("RECV", message)
    def loss(self, message):    self.log("LOSS", message)
    def corrupt(self, message): self.log("CORRUPT", message)
    def timeout(self, message): self.log("TIMEOUT", message)
    def drop(self, message):    self.log("DROP", message)
    def deliver(self, message): self.log("DELIVER", message)

    # Categorias específicas TCP/RDT
    def ack(self, message):     self.log("ACK", message)
    def window(self, message):  self.log("WINDOW", message)
    def retry(self, message):   self.log("RETRY", message)
    def syn(self, message):     self.log("SYN", message)

    # Compatibilidade com testadores antigos
    def log_recv_ack(self, seq):
        """
        Registro compatível com versões antigas dos testes
        da Fase 2.
        """
        self.ack(f"ACK {seq} recebido")

    # ============================================================ #
    # Fechamento
    # ============================================================ #
    def close(self):
        """Fecha o arquivo de log com segurança."""
        try:
            self.file.close()
        except Exception:
            pass

    def __del__(self):
        """Garante fechamento automático ao destruir o objeto."""
        self.close()
