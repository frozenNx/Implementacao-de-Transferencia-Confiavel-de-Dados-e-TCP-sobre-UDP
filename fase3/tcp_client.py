"""
===========================================================
Módulo: fase3/tcp_client.py
===========================================================

Aplicação de exemplo: cliente que utiliza TCPSocket (TCP
simplificado sobre UDP) para trocar mensagens com o servidor
definido em fase3/tcp_server.py.

Fluxo:
    - Conecta via three-way handshake (connect)
    - Envia uma sequência de mensagens de teste
    - Lê a resposta do servidor para cada mensagem
    - Encerra a conexão via four-way handshake (close)

Execução:
    python -m fase3.tcp_client
===========================================================
"""

from __future__ import annotations

import time

from fase3.tcp_socket import TCPSocket


def main():
    """Conecta ao servidor, envia mensagens de teste e encerra a conexão."""
    client = TCPSocket()  # Porta local aleatória
    client.connect(("127.0.0.1", 12345))
    print("Conectado ao servidor.")

    msgs = ["Olá", "Teste de transferência", "sair"]

    for m in msgs:
        print("Enviando:", m)
        client.send(m.encode())

        resp = client.recv(timeout=5.0)
        if resp:
            print("Resposta:", resp.decode(errors="replace"))
        else:
            print("Resposta: <vazio>")

        time.sleep(1)

    client.close()
    print("Cliente finalizado.")


if __name__ == "__main__":
    main()