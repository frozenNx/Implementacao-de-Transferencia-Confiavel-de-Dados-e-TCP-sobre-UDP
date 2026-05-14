# 🌐 EFC 02 – Implementação de Transferência Confiável de Dados (RDT, GBN e TCP Simplificado)

Este projeto de **Redes de Computadores** implementa protocolos de comunicação confiável sobre a camada não confiável do **UDP** (User Datagram Protocol). O trabalho evolui através das fases dos protocolos RDT (**Reliable Data Transfer**) clássicos (2.0, 2.1 e 3.0), o protocolo de janela deslizante **Go-Back-N (GBN)**, e culmina em uma implementação funcional e simplificada do **TCP (Transmission Control Protocol)**.

O objetivo é demonstrar a construção de confiabilidade em uma rede propensa a erros, perdas e atrasos.

## ✨ Funcionalidades Principais (TCP Simplificado)

A implementação do TCP (`fase3/tcp_socket.py`) abrange elementos cruciais para a comunicação confiável:

  * **Controle de Conexão:** Implementação completa do processo de **3-way Handshake** (`SYN`, `SYN/ACK`, `ACK`).
  * **Retransmissões:** Mecanismo de **timeout** e retransmissão para pacotes perdidos.
  * **Reconhecimentos (ACKs):** Utilização de **ACKs cumulativos** para reconhecer múltiplos pacotes com uma única mensagem.
  * **Buffers:** Gerenciamento de buffers circulares de envio e recepção.
  * **Fechamento Ordenado:** Implementação do processo de **FIN/ACK** para encerramento limpo da conexão.

-----

## 👥 Integrantes

| RA | Nome |
| :--- | :--- |
| 23001396 | **Enzo de Almeida** |
| 23007553 | **Rafael Celeste** |
| 23014024 | **Tiago Mello** |
| 23013133 | **Thomas Fidelis** |
| 23011585 | **Vinicius Nunes** |

-----

## 📁 Estrutura do Projeto

A estrutura de diretórios organiza as implementações por fases de complexidade e separa módulos de suporte (utilitários, testes, logs).

```
projeto_redes/
├── fase1/                  # Implementações do RDT (2.0, 2.1, 3.0)
├── fase2/                  # Implementação do Go-Back-N (GBN)
├── fase3/                  # Implementação do TCP Simplificado
├── testes/                 # Scripts de Testes (fase1, fase2, fase3)
├── utils/                  # Módulos de suporte (packet, simulator, logger)
├── logs/                   # Logs detalhados e resultados de performance
├── relatorio/              # Relatório final
└── README.md
```

### Detalhamento da Estrutura

| Caminho | Descrição |
| :--- | :--- |
| `fase1/rdt*.py` | **RDT 2.0, 2.1 e 3.0** (Stop-and-Wait). |
| `fase2/gbn.py` | Implementação completa do **Go-Back-N** com janela deslizante. |
| `fase3/tcp_socket.py` | Implementação principal do **TCP simplificado** (conexão, retransmissões, buffers). |
| `fase3/tcp_server.py` | Exemplo de **servidor** usando a API TCP. |
| `fase3/tcp_client.py` | Exemplo de **cliente** usando a API TCP. |
| `utils/simulator.py` | **Canal não confiável** que simula perda, duplicação e atraso de pacotes. |
| `utils/packet.py` | Estrutura de pacotes (`seq`, `ack`, `flags`, `checksum`). |
| `logs/*.png` | **Gráficos** de desempenho (ex: `throughput_vs_window.png`). |

-----

## 📌 Requisitos

Para executar o projeto e os testes, você precisará:

  * **Python 3.10+**
  * **Biblioteca:** `matplotlib` (necessária para a Fase 2 - GBN).
    ```bash
    python -m pip install matplotlib
    ```
  * Permissão para **sockets UDP** no localhost.

O código é compatível com **Windows, Linux e macOS**.

-----

## ▶️ Execução dos Testes

Os scripts na pasta `testes/` são a forma principal de interagir com as implementações e gerar os logs de análise.

### Fase 1 — RDT 2.0, 2.1 e 3.0

O teste verifica as transições de estado e o tratamento de erros nos protocolos RDT.

```bash
python -m testes.test_fase1
```

**Saída em Logs:** Geração de logs com `ACKs/NAKs`, detecção de corrupção, retransmissões e `Timeouts`.

### Fase 2 — Go-Back-N (GBN)

Testes de transferência de 1MB e análises de desempenho sob diferentes condições de perda e tamanho de janela.

```bash
python -m testes.test_fase2
```

**Saída em Logs:**

  * `transfer_1mb_summary.txt`: Comparação de desempenho **GBN vs RDT 3.0**.
  * `loss10_summary.txt`: Análise detalhada com **10% de perda** de pacotes.
  * `throughput_vs_window.png`: **Gráfico** da relação entre tamanho da janela e **throughput**.
  * Logs completos de timeouts, retransmissões e estatísticas de janela.

### Fase 3 — TCP Simplificado

Teste da implementação principal do TCP, incluindo o controle de conexão e fechamento ordenado.

```bash
python -m testes.test_fase3
```

**Saída em Logs:** Testes abrangentes de:

  * **3-way handshake**
  * Retransmissões e timeout
  * **Buffers circulares**
  * Controle básico de fluxo (`rwnd`)
  * Fechamento ordenado (`FIN/ACK`)

-----

## 📓 Observações Importantes

  * **Logging:** Todos os testes geram logs detalhados com **timestamps** para rastrear eventos importantes.
  * **Design:** A estrutura de comunicação utiliza canais independentes para **DATA** e **ACK**, uma abordagem que ajuda a mitigar problemas comuns em sistemas operacionais como o `WinError 10054`.
  * **Referência Acadêmica:** O projeto segue fielmente os modelos de implementação dos protocolos descritos no **Capítulo 3** do livro **Kurose & Ross**.

-----

## 📚 Referências

  * KUROSE, James F.; ROSS, Keith W. **Computer Networking: A Top-Down Approach**. 8ª ed. Pearson, 2021.
  * RFC 793 — Transmission Control Protocol.
  * RFC 5681 — TCP Congestion Control.
