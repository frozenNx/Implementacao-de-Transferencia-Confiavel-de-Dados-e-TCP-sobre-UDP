"""
Exemplo de cliente usando TCPSocket
"""

from tcp_socket import TCPSocket
import time


def main():
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