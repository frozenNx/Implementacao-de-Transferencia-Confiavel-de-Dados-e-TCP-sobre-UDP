"""
# testes/test_fase2.py

Testes comparativos de protocolos: rdt3.0 (Stop-and-Wait) e Go-Back-N.

Objetivos:
- Teste 1: Eficiência (1 MB, perda 0%) - Compara RDT3 com GBN (N=5).
- Teste 2: Robustez com perdas (10 pacotes, perda = 10%) - Avalia a penalidade da perda.
- Teste 3: Throughput x Tamanho da Janela (16 KB, perda 0%, N=1,5,10,20) - Demonstra o pipelining.

"""

import time
import threading
import matplotlib.pyplot as plt

from utils.simulator import UnreliableChannel
from fase1.rdt30 import RDT30Sender, RDT30Receiver
from fase2.gbn import GBNSender, GBNReceiver


# =====================================================================
# Funções auxiliares (mantidas como estavam)
# =====================================================================
def try_call_ctor(cls, /, *pos_args, **kw_args):
    try:
        return cls(*pos_args, **kw_args)
    except TypeError:
        fallback_kw = dict(kw_args)
        for k in list(fallback_kw.keys()):
            tmp = dict(fallback_kw)
            tmp.pop(k, None)
            try:
                return cls(*pos_args, **tmp)
            except TypeError:
                continue
        return cls(*pos_args)

def start_receiver_loop(receiver_obj):
    for method in ("receive", "start", "run", "_recv_loop", "recv_loop"):
        fn = getattr(receiver_obj, method, None)
        if callable(fn):
            t = threading.Thread(target=fn, daemon=True)
            t.start()
            return t
    return None

def instantiate_receiver(receiver_cls, channel, deliver_callback, window_size: int, verbose: bool):
    tries = [
        {"local_addr": ("localhost", 0), "deliver_callback": deliver_callback, "channel": channel, "window_size": window_size, "verbose": verbose},
        {"local_addr": ("localhost", 0), "deliver_callback": deliver_callback, "channel": channel, "verbose": verbose},
        {"local_addr": ("localhost", 0), "deliver_callback": deliver_callback, "window_size": window_size},
        {"local_addr": ("localhost", 0), "channel": channel, "window_size": window_size},
        {"channel": channel, "window_size": window_size},
        {"channel": channel},
        {}
    ]
    last_exc = None
    for kw in tries:
        try:
            inst = try_call_ctor(receiver_cls, **kw)
            port = None
            sock = getattr(inst, "sock", None)
            if sock:
                try:
                    port = sock.getsockname()[1]
                except Exception:
                    pass
            return inst, port
        except Exception as e:
            last_exc = e
            continue
    raise last_exc

def instantiate_sender(sender_cls, dest_addr, channel, window_size: int, timeout: float, verbose: bool):
    tries = [
        {"local_addr": ("localhost", 0), "dest_addr": dest_addr, "channel": channel, "N": window_size, "window_size": window_size, "timeout": timeout, "verbose": verbose},
        {"local_addr": ("localhost", 0), "dest_addr": dest_addr, "channel": channel, "window_size": window_size, "timeout": timeout, "verbose": verbose},
        {"local_addr": ("localhost", 0), "dest_addr": dest_addr, "channel": channel, "timeout": timeout, "verbose": verbose},
        {"local_addr": ("localhost", 0), "dest_addr": dest_addr, "channel": channel},
        {"channel": channel, "window_size": window_size, "timeout": timeout},
        {"channel": channel},
        {}
    ]
    last_exc = None
    for kw in tries:
        try:
            inst = try_call_ctor(sender_cls, **kw)
            if dest_addr:
                if hasattr(inst, "dest"): inst.dest = dest_addr
                elif hasattr(inst, "dest_addr"): inst.dest_addr = dest_addr
            return inst
        except Exception as e:
            last_exc = e
            continue
    raise last_exc

def display_results_table(results):
    """Imprime os resultados em um formato de tabela limpo e formatado."""

    # 1. Agrupamento por teste para melhor visualização
    # Teste 1: 1MB, Perda 0%
    t1 = [r for r in results if r['received'] == 1024 and r['retransmissions'] == 0]
    # Teste 1 (Anomalia): 1MB, GBN com Retransmissões
    t1_anomaly = [r for r in results if r['received'] == 1024 and r['retransmissions'] > 0]
    # Teste 2: 10 pacotes, Perda 10%
    t2 = [r for r in results if r['received'] == 10 and r['retransmissions'] > 0]
    # Teste 3: 16KB, GBN x Janela
    t3 = [r for r in results if r['received'] == 128]

    all_tests = [
        ("Teste 1: Eficiência (1MB, Perda 0%)", t1 + t1_anomaly, False),
        ("Teste 2: Robustez (10 Pacotes, Perda 10%)", t2, True),
        ("Teste 3: Throughput x Janela (16KB, Análise GBN)", t3, False)
    ]

    print("\n" + "="*75)
    print(f"{' '*25} | RESUMO DOS TESTES DE PROTOCOLO |")
    print("="*75)

    for title, data, is_loss_test in all_tests:
        if not data:
            continue

        print(f"\n## {title}")
        print("-" * 75)
        
        # Colunas e títulos
        headers = ["Protocolo", "Janela (N)", "Tempo (s)", "Throughput (MB/s)", "Retransmissões"]
        print(f"{headers[0]:<12}{headers[1]:<12}{headers[2]:<15}{headers[3]:<22}{headers[4]:<12}")
        print("-" * 75)

        for r in data:
            protocol = r['protocol']
            window = r['window']
            time_val = f"{r['time']:.2f}"
            # Formatação para 6 casas decimais, ou notação científica se o valor for muito pequeno
            throughput_val = f"{r['throughput']:.6f}" if r['throughput'] >= 1e-4 else f"{r['throughput']:.2e}"
            retransmissions = r['retransmissions']
            
            # Adicionar um marcador se for uma retransmissão anômala no teste sem perda
            if not is_loss_test and retransmissions > 0:
                 retrans_str = f"**{retransmissions} (ANOMALIA)**"
            else:
                 retrans_str = str(retransmissions)

            print(f"{protocol:<12}{window:<12}{time_val:<15}{throughput_val:<22}{retrans_str:<12}")
        print("-" * 75)

# =====================================================================
# Função principal de execução de teste
# =====================================================================
def run_protocol_test(name, sender_cls, receiver_cls,
                      total_chunks, chunk_size,
                      window_size, loss_rate, delay_range=(0.01, 0.05), verbose=False):

    channel = UnreliableChannel(loss_rate=loss_rate, corrupt_rate=0.0, delay_range=delay_range)
    received = []

    def deliver(*args):
        if len(args) == 2:
            seq, data = args
        else:
            data = args[0]
        received.append(data)

    receiver, port = instantiate_receiver(receiver_cls, channel, deliver, window_size, verbose)
    start_receiver_loop(receiver)

    if port is None:
        port = 13001 if name == "RDT3" else 14001

    SENDER_TIMEOUT = 3.0
    sender = instantiate_sender(sender_cls, ('localhost', port), channel, window_size, SENDER_TIMEOUT, verbose)

    chunks = [bytes([i % 256]) * chunk_size for i in range(total_chunks)]

    start = time.time()

    for c in chunks:
        try: sender.send(c)
        except: sender.send(c)

    if hasattr(sender, "finish_sending"):
        sender.finish_sending()
    if hasattr(receiver, "wait_for_completion"):
        # Bloqueia o thread de teste até que todos os 'total_chunks' sejam entregues.
        receiver.wait_for_completion(total_chunks)
    else:
        # Fallback para protocolos sem wait_for_completion (Ex: RDT3.0 simples, se não foi modificado)
        time.sleep(1.0)
    
    # 3. Fecha os componentes após a conclusão garantida
    if hasattr(sender, "close"):
        sender.close()
    if hasattr(receiver, "close"):
        receiver.close()

    elapsed = max(time.time() - start, 1e-6)
    # 1e6 = 1 Megabyte
    throughput = (total_chunks * chunk_size) / elapsed / 1e6
    retransmissions = getattr(sender, "retransmissions", 0)
    delivered = len(received)

    return {
        "protocol": name,
        "window": window_size,
        "time": elapsed,
        "throughput": throughput,
        "retransmissions": retransmissions,
        "received": delivered
    }


# =====================================================================
# MAIN reestruturado conforme o enunciado e com ajustes
# =====================================================================
def main():

    delay = (0.01, 0.05)
    verbose = False
    results = []

    # --- TESTE 1: Eficiência (1MB, perda = 0%) ---
    # Objetivo: Comparar RDT 3.0 (SW) com GBN (Pipelining) para ver o ganho
    print("\n=== TESTE 1: Eficiência (1MB, perda = 0%) ===")
    rdt3 = run_protocol_test("RDT3", RDT30Sender, RDT30Receiver,
                             total_chunks=1024, chunk_size=1024, # 1MB total
                             window_size=1, loss_rate=0.0, delay_range=delay, verbose=verbose)
    # GBN com N=5 para demonstrar o ganho do pipelining vs. SW
    gbn5_eff = run_protocol_test("GBN", GBNSender, GBNReceiver,
                                 total_chunks=1024, chunk_size=1024, # 1MB total
                                 window_size=5, loss_rate=0.0, delay_range=delay, verbose=verbose)
    results += [rdt3, gbn5_eff]

    # --- TESTE 2: Robustez com Perdas (10 pacotes, perda = 10%) ---
    # Objetivo: Contar retransmissões e tempo em cenário de perdas
    print("\n=== TESTE 2: Robustez com Perdas (10 pacotes, perda = 10%) ===")
    rdt3_loss = run_protocol_test("RDT3", RDT30Sender, RDT30Receiver,
                                  total_chunks=10, chunk_size=128, # Amostra pequena
                                  window_size=1, loss_rate=0.1, delay_range=delay, verbose=verbose)
    gbn5_loss = run_protocol_test("GBN", GBNSender, GBNReceiver,
                                  total_chunks=10, chunk_size=128, # Amostra pequena
                                  window_size=5, loss_rate=0.1, delay_range=delay, verbose=verbose)
    results += [rdt3_loss, gbn5_loss]

    # --- TESTE 3: Throughput × Janela (16 KB, perda = 0%) ---
    # Objetivo: Plotar Throughput vs. N. Usando 16KB (128 * 128)
    print("\n=== TESTE 3: Throughput × Janela (16 KB, perda = 0%) ===")
    for N in [1, 5, 10, 20]:
        r = run_protocol_test("GBN", GBNSender, GBNReceiver,
                              total_chunks=128, chunk_size=128, # 16KB total
                              window_size=N, loss_rate=0.0, delay_range=delay, verbose=verbose)
        results.append(r)

    print("\n=== RESULTADOS ===")
    
    display_results_table(results)

    # --- Plotagem do Gráfico ---
    plt.figure()
    # Filtra apenas os resultados do Teste 3 (GBN com 0% perda e 16KB) para o gráfico principal
    gbn_throughput = [r for r in results if r["protocol"] == "GBN" and r["received"] == 128]
    
    # Ordena para garantir que o plot está correto (1, 5, 10, 20)
    gbn_throughput.sort(key=lambda x: x["window"]) 

    plt.plot([r["window"] for r in gbn_throughput], [r["throughput"] for r in gbn_throughput], marker='o')
    plt.xlabel("Janela (N)")
    plt.ylabel("Throughput (MB/s)")
    plt.title("GBN: Throughput × Tamanho da Janela")
    plt.grid(True)
    plt.savefig("throughput_vs_janela.png")
    print("\nGráfico salvo: throughput_vs_janela.png")
    plt.show()

if __name__ == "__main__":
    main()