"""
Exemplo de servidor usando TCPSocket
"""

from tcp_socket import TCPSocket


def main():
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
