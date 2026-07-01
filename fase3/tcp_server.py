"""
===========================================================
Módulo: fase3/tcp_server.py
===========================================================

Aplicação de exemplo: servidor que utiliza TCPSocket (TCP
simplificado sobre UDP) para receber conexões e ecoar
mensagens recebidas do cliente definido em fase3/tcp_client.py.

Fluxo:
    - Coloca o socket em modo de escuta (listen)
    - Aceita a conexão entrante (accept), completando o handshake
    - Recebe mensagens e responde com eco ("OK: <mensagem>")
    - Encerra ao receber "sair" ou ao atingir timeout

Execução:
    python -m fase3.tcp_server
===========================================================
"""

from __future__ import annotations

from fase3.tcp_socket import TCPSocket


def main():
    """Aceita uma conexão e ecoa mensagens recebidas até o cliente sair."""
    srv = TCPSocket(local_addr=("127.0.0.1", 12345))
    print("Servidor aguardando conexão em 127.0.0.1:12345 ...")

    # IMPORTANTE: chamar listen() antes de accept()
    srv.listen()

    srv.accept()
    print("Conexão estabelecida!")

    try:
        while True:
            data = srv.recv(timeout=10.0)
            if not data:
                print("[SERVER] Conexão encerrada pelo cliente ou timeout.")
                break

            text = data.decode(errors="replace")
            print("Recebido:", text)

            if text.strip().lower() == "sair":
                print("[SERVER] Cliente pediu para sair.")
                break

            srv.send(b"OK: " + data)

    finally:
        srv.close()
        print("Servidor encerrado.")


if __name__ == "__main__":
    main()
