"""
===========================================================
Módulo: testes/test_fase3.py
===========================================================

Descrição:
    Teste completo da Fase 3 (TCP simplificado sobre UDP).

    Testa:
        - Handshake (three-way)
        - Transferência de dados (10KB e 1MB)
        - Controle de fluxo (janela de recepção reduzida)
        - Retransmissão simulada por perda de pacotes
        - Encerramento (four-way)
        - Gráfico de desempenho (throughput e RTT estimado pelo cliente)

    Observação:
        Usa a implementação TCPSocket presente em fase3/tcp_socket.py.

Execução:
    python -m testes.test_fase3
===========================================================
"""

import threading
import time
import sys
import os
import random
import matplotlib.pyplot as plt

from utils.logger import Logger

# Diretório único para armazenar logs e gráficos
OUT_DIR = "logs"
os.makedirs(OUT_DIR, exist_ok=True)

# Permitir import dos módulos da fase3
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'fase3')))
from fase3.tcp_socket import TCPSocket

logger = Logger(prefix="fase3", origin="TEST")

# --------------------------- Helpers ---------------------------

def _temporary_drop_sendto(probability):
    import socket
    orig = socket.socket.sendto

    def patched_sendto(self, data, addr):
        if random.random() < probability:
            # simulate drop
            return len(data)
        return orig(self, data, addr)

    socket.socket.sendto = patched_sendto
    return orig


def mbps(bytes_sent, seconds):
    return (bytes_sent / 1e6) / max(seconds, 1e-9)


# --------------------------- Test routines ---------------------------

def run_handshake_test(server_port=8000, client_port=9000, timeout=5.0):
    result = {"server_connected": False, "client_connected": False}

    def server_task():
        srv = TCPSocket(local_addr=("127.0.0.1", server_port))
        srv.accept()
        result["server_connected"] = srv.connected
        server_task.srv = srv

    def client_task():
        cli = TCPSocket(local_addr=("127.0.0.1", client_port))
        cli.connect(("127.0.0.1", server_port))
        result["client_connected"] = cli.connected
        client_task.cli = cli

    t_srv = threading.Thread(target=server_task, daemon=True)
    t_srv.start()
    time.sleep(0.05)
    t_cli = threading.Thread(target=client_task, daemon=True)
    t_cli.start()

    start = time.time()
    while time.time() - start < timeout:
        if result["server_connected"] and result["client_connected"]:
            break
        time.sleep(0.01)

    ok = result["server_connected"] and result["client_connected"]

    try:
        if hasattr(client_task, "cli"):
            client_task.cli.close()
    except Exception:
        pass
    try:
        if hasattr(server_task, "srv"):
            server_task.srv.close()
    except Exception:
        pass

    return ok


def run_transfer_test(data_bytes, server_port=8000, client_port=9000, drop_prob=0.0):
    orig_sendto = None
    if drop_prob > 0:
        orig_sendto = _temporary_drop_sendto(drop_prob)

    server = TCPSocket(local_addr=("127.0.0.1", server_port))
    server_ready = threading.Event()

    def server_thread():
        try:
            server.accept()
            server_ready.set()
            received = bytearray()
            while True:
                chunk = server.recv(timeout=2.0)
                if not chunk:
                    # break on timeout or close
                    if len(received) >= data_bytes:
                        break
                    # if no data, keep waiting a bit
                    time.sleep(0.01)
                    continue
                received.extend(chunk)
                if len(received) >= data_bytes:
                    break
            server.received = bytes(received)
        finally:
            try:
                server.close()
            except Exception:
                pass

    t_srv = threading.Thread(target=server_thread, daemon=True)
    t_srv.start()

    client = TCPSocket(local_addr=("127.0.0.1", client_port))
    time.sleep(0.05)
    client.connect(("127.0.0.1", server_port))

    server_ready.wait(timeout=2.0)

    chunk_size = client.recv_window if client.recv_window > 0 else 1024
    t0 = time.time()
    bytes_sent = 0
    while bytes_sent < data_bytes:
        block = b"x" * min(chunk_size, data_bytes - bytes_sent)
        client.send(block)
        bytes_sent += len(block)

    try:
        client.close()
    except Exception:
        pass

    # captura os contadores depois do close,é durante o close() que o TCPSocket espera o send_buffer esvaziar, e é nessa espera que a maior parte das retransmissões acontece (thread do timer aindarodando)
    retransmissions = getattr(client, "total_retransmissions", 0)
    retransmitted_bytes = getattr(client, "total_retransmitted_bytes", 0)

    t_srv.join(timeout=30.0)

    t1 = time.time()
    elapsed = t1 - t0
    throughput_val = mbps(bytes_sent, elapsed)
    client_rtt = getattr(client, "estimated_rtt", None)
    received_len = len(getattr(server, "received", b""))
    success = (received_len == data_bytes)
    overhead_pct = (retransmitted_bytes / bytes_sent * 100) if bytes_sent else 0.0

    try:
        server.close()
    except Exception:
        pass

    if orig_sendto is not None:
        import socket
        socket.socket.sendto = orig_sendto

    return {
        "success": success,
        "elapsed": elapsed,
        "throughput_MBs": throughput_val,
        "client_rtt": client_rtt,
        "bytes_sent": bytes_sent,
        "bytes_received": received_len,
        "drop_prob": drop_prob,
        "retransmissions": retransmissions,
        "retransmitted_bytes": retransmitted_bytes,
        "overhead_pct": overhead_pct,
    }


def run_flow_control_test(total_bytes=10240, recv_window=1024, server_port=8000, client_port=9000):
    server = TCPSocket(local_addr=("127.0.0.1", server_port), recv_window=recv_window)

    def server_thread():
        server.accept()
        received = bytearray()
        while len(received) < total_bytes:
            chunk = server.recv(timeout=2.0)
            if not chunk:
                continue
            received.extend(chunk)
        server.received = bytes(received)

    t_srv = threading.Thread(target=server_thread, daemon=True)
    t_srv.start()

    client = TCPSocket(local_addr=("127.0.0.1", client_port))
    time.sleep(0.05)
    client.connect(("127.0.0.1", server_port))

    t0 = time.time()
    sent = 0
    chunk_size = 2048
    while sent < total_bytes:
        block = b"x" * min(chunk_size, total_bytes - sent)
        client.send(block)
        sent += len(block)

    time.sleep(1.0 + total_bytes / 1e6)
    t1 = time.time()
    elapsed = t1 - t0
    throughput_val = mbps(sent, elapsed)
    received_len = len(getattr(server, "received", b""))

    client.close()
    server.close()

    retransmissions = getattr(client, "total_retransmissions", 0)
    retransmitted_bytes = getattr(client, "total_retransmitted_bytes", 0)
    overhead_pct = (retransmitted_bytes / sent * 100) if sent else 0.0

    return {
        "sent": sent,
        "received": received_len,
        "elapsed": elapsed,
        "throughput_MBs": throughput_val,
        "recv_window": recv_window,
        "retransmissions": retransmissions,
        "retransmitted_bytes": retransmitted_bytes,
        "overhead_pct": overhead_pct,
    }


# --------------------------- Main runner ---------------------------

def main():
    print("=== Iniciando testes da Fase 3 (TCP simplificado) ===")
    results = {}

    print("\n[TEST 1] Handshake (3-way)")
    ok = run_handshake_test()
    print("Handshake OK?", ok)
    results["handshake_ok"] = ok
    logger.info(f"RESUMO: teste=handshake | OK={ok}")

    print("\n[TEST 2] Transferência 10KB")
    res_10kb = run_transfer_test(10 * 1024)
    print(res_10kb)
    results["10KB"] = res_10kb
    logger.info(
        f"RESUMO: teste=10KB | OK={res_10kb['success']} | "
        f"bytes_enviados={res_10kb['bytes_sent']} | bytes_recebidos={res_10kb['bytes_received']} | "
        f"elapsed={res_10kb['elapsed']:.3f}s | throughput={res_10kb['throughput_MBs']:.4f}MB/s | "
        f"RTT={(res_10kb['client_rtt'] or 0) * 1000:.1f}ms | Retransmissões={res_10kb['retransmissions']}"
    )

    print("\n[TEST 3] Controle de fluxo (recv_window=1KB)")
    flow_res = run_flow_control_test(total_bytes=10 * 1024, recv_window=1024)
    print(flow_res)
    results["flow"] = flow_res
    logger.info(
        f"RESUMO: teste=flow_1KB | OK={flow_res['sent'] == flow_res['received']} | "
        f"enviado={flow_res['sent']} | recebido={flow_res['received']} | "
        f"elapsed={flow_res['elapsed']:.3f}s | throughput={flow_res['throughput_MBs']:.4f}MB/s | "
        f"Retransmissões={flow_res['retransmissions']}"
    )

    print("\n[TEST 4] Retransmissão com perda simulada (20%)")
    res_loss = run_transfer_test(50 * 1024, drop_prob=0.20)
    print(res_loss)
    results["loss_20"] = res_loss
    logger.info(
        f"RESUMO: teste=loss20 | OK={res_loss['success']} | "
        f"bytes_enviados={res_loss['bytes_sent']} | bytes_recebidos={res_loss['bytes_received']} | "
        f"elapsed={res_loss['elapsed']:.3f}s | throughput={res_loss['throughput_MBs']:.4f}MB/s | "
        f"RTT={(res_loss['client_rtt'] or 0) * 1000:.1f}ms | Retransmissões={res_loss['retransmissions']}"
    )

    print("\n[TEST 5] Desempenho - transfer 1MB")
    res_1mb = run_transfer_test(1024 * 1024)
    print(res_1mb)
    results["1MB"] = res_1mb
    logger.info(
        f"RESUMO: teste=1MB | OK={res_1mb['success']} | "
        f"bytes_enviados={res_1mb['bytes_sent']} | bytes_recebidos={res_1mb['bytes_received']} | "
        f"elapsed={res_1mb['elapsed']:.3f}s | throughput={res_1mb['throughput_MBs']:.4f}MB/s | "
        f"RTT={(res_1mb['client_rtt'] or 0) * 1000:.1f}ms | Retransmissões={res_1mb['retransmissions']}"
    )

    labels = ["10KB", "50KB_loss20%", "1MB"]
    throughputs = [
        results["10KB"]["throughput_MBs"],
        results["loss_20"]["throughput_MBs"],
        results["1MB"]["throughput_MBs"],
    ]
    rtts = [
        results["10KB"]["client_rtt"] or 0,
        results["loss_20"]["client_rtt"] or 0,
        results["1MB"]["client_rtt"] or 0,
    ]

    plt.figure(figsize=(10, 4))
    plt.subplot(1, 2, 1)
    plt.bar(labels, throughputs)
    plt.ylabel("Throughput (MBps)")
    plt.title("Throughput em testes selecionados")

    plt.subplot(1, 2, 2)
    plt.bar(labels, rtts)
    plt.ylabel("Estimated RTT (s)")
    plt.title("RTT estimado pelo TCPSocket")

    plt.tight_layout()

    out_png = os.path.join(OUT_DIR, "fase3_desempenho.png")
    plt.savefig(out_png, dpi=150)
    print("\nGráfico salvo em:", out_png)

    print("\n=== Resultados resumidos ===")
    for k, v in results.items():
        print(k, v)

    # Resumo simples em .txt, mesmo padrão usado na Fase 2
    # (transfer1mb_summary.txt, loss10_summary.txt, throughput_sweep_summary.txt)
    summary_path = os.path.join(OUT_DIR, "fase3_resultados_summary.txt")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("Fase 3 - Resultados Resumidos\n")
        f.write(f"Handshake: OK={ok}\n\n")

        f.write(
            f"10KB: OK={res_10kb['success']} bytes_enviados={res_10kb['bytes_sent']} "
            f"bytes_recebidos={res_10kb['bytes_received']} elapsed={res_10kb['elapsed']:.3f}s "
            f"throughput={res_10kb['throughput_MBs']:.4f}MB/s "
            f"RTT={(res_10kb['client_rtt'] or 0) * 1000:.1f}ms "
            f"retransmissoes={res_10kb['retransmissions']}\n"
        )
        f.write(
            f"Fluxo(1KB win): OK={flow_res['sent'] == flow_res['received']} "
            f"enviado={flow_res['sent']} recebido={flow_res['received']} "
            f"elapsed={flow_res['elapsed']:.3f}s throughput={flow_res['throughput_MBs']:.4f}MB/s "
            f"retransmissoes={flow_res['retransmissions']}\n"
        )
        f.write(
            f"Loss20%: OK={res_loss['success']} bytes_enviados={res_loss['bytes_sent']} "
            f"bytes_recebidos={res_loss['bytes_received']} elapsed={res_loss['elapsed']:.3f}s "
            f"throughput={res_loss['throughput_MBs']:.4f}MB/s "
            f"RTT={(res_loss['client_rtt'] or 0) * 1000:.1f}ms "
            f"retransmissoes={res_loss['retransmissions']}\n"
        )
        f.write(
            f"1MB: OK={res_1mb['success']} bytes_enviados={res_1mb['bytes_sent']} "
            f"bytes_recebidos={res_1mb['bytes_received']} elapsed={res_1mb['elapsed']:.3f}s "
            f"throughput={res_1mb['throughput_MBs']:.4f}MB/s "
            f"RTT={(res_1mb['client_rtt'] or 0) * 1000:.1f}ms "
            f"retransmissoes={res_1mb['retransmissions']}\n"
        )

    print("[Resumo salvo em]", summary_path)
    print("\n✅ Testes da Fase 3 finalizados.")


if __name__ == "__main__":
    main()
