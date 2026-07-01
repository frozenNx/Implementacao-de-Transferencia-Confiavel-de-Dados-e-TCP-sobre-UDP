"""
===========================================================
Módulo: fase3/tcp_socket.py
===========================================================

TCP simplificado sobre UDP usado nos testes da Fase 3.

Características:
    - Handshake 3-way (SYN, SYN-ACK, ACK)
    - Controle de fluxo por janela (window update)
    - Buffer fora-de-ordem (reordenação)
    - RTT estimado e timeout dinâmico
    - Retransmissão com backoff exponencial
    - Fast retransmit por 3 ACKs duplicados

Observação:
    Este módulo assume que utils.packet.Packet e utils.logger.Logger
    expõem as interfaces usadas (make_tcp, from_bytes, flags, window
    etc.).

Classes:
    TCPSocket - Socket TCP simplificado sobre UDP
===========================================================
"""

from __future__ import annotations

import random
import socket
import threading
import time
from typing import Dict, Optional, Tuple

from utils.logger import Logger
from utils.packet import Packet

logger = Logger(prefix="tcp", origin="TCP")

SEQ_MOD = 2 ** 32
MAX_PAYLOAD_DEFAULT = 1000


class TCPSocket:
    """Socket TCP simplificado sobre um socket UDP.

    Este objeto implementa um subconjunto do comportamento TCP necessário
    para os testes da Fase 3: estabelecimento de conexão, envio/recebimento
    com controle de fluxo, retransmissões e fechamento.

    Parameters
    ----------
    local_addr:
        Tupla (host, port) para bind local. Padrão: ("127.0.0.1", 0)
    recv_window:
        Tamanho inicial da janela de recepção anunciada ao par.
    """

    def __init__(
        self,
        local_addr: Tuple[str, int] = ("127.0.0.1", 0),
        recv_window: int = 4096,
    ) -> None:
        # Socket UDP subjacente
        self.udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp.bind(local_addr)

        # Endereço remoto (preenchido em connect/accept)
        self.remote_addr: Optional[Tuple[str, int]] = None

        # Sequência / ACK
        self.seq: int = random.randint(0, SEQ_MOD - 1)
        self.ack: int = 0

        # Janelas
        self.recv_window: int = int(recv_window)
        self.peer_window: int = int(recv_window)

        # Buffers TX/RX
        # send_buffer: seq -> dict(segment: bytes, time: float, data: bytes, retrans: int)
        self.send_buffer: Dict[int, Dict] = {}
        self.recv_buffer: bytearray = bytearray()

        # RTT / timeout
        self.estimated_rtt: float = 0.3
        self.dev_rtt: float = 0.15
        self.timeout_interval: float = self._calc_timeout()

        # Estado da conexão
        self.running: bool = False
        self.connected: bool = False
        self.fin_sent: bool = False
        self.fin_received: bool = False
        self.fin_acked: bool = False

        # Concorrência
        self._lock = threading.Lock()
        self._recv_thread: Optional[threading.Thread] = None

        # Timer/retransmit
        self._timer_running: bool = False
        self._timer_lock = threading.Lock()
        self._timer_thread: Optional[threading.Thread] = None

        # Limites
        self.max_payload: int = min(MAX_PAYLOAD_DEFAULT, max(256, self.recv_window))

        # Out-of-order buffer: seq -> bytes
        self._ooo_buffer: Dict[int, bytes] = {}

        # Duplicate ACK tracking para fast retransmit
        self._dup_acks: Dict[int, int] = {}
        self._last_ack: Optional[int] = None

        # Pequeno sleep para evitar busy-loop quando janela cheia
        self._send_wait_sleep: float = 0.001

    # ---------------------------------------------------------------------
    # Helpers (comparação circular de sequência)
    # ---------------------------------------------------------------------
    def _seq_lt(self, a: int, b: int) -> bool:
        """Retorna True se a < b em aritmética modular 32-bit.

        A comparação considera que distâncias menores que 2**31 significam
        ordem normal (evita problemas com wrap-around).
        """
        return ((b - a) & 0xFFFFFFFF) < (1 << 31)

    def _seq_le(self, a: int, b: int) -> bool:
        """Retorna True se a <= b em aritmética modular 32-bit."""
        return a == b or self._seq_lt(a, b)

    # ---------------------------------------------------------------------
    # Conexão (3-way handshake) - lado cliente
    # ---------------------------------------------------------------------
    def connect(self, remote_addr: Tuple[str, int], timeout: float = 10.0) -> None:
        """Estabelece conexão (SYN -> SYN+ACK -> ACK).

        Parameters
        ----------
        remote_addr:
            Tupla (host, port) do servidor.
        timeout:
            Tempo máximo (s) para tentar completar o handshake.

        Raises
        ------
        TimeoutError
            Se não receber SYN-ACK dentro do tempo limite.
        RuntimeError
            Se o socket já estiver conectado.
        """
        if self.connected:
            raise RuntimeError("Already connected")

        self.remote_addr = remote_addr
        local_port = self.udp.getsockname()[1]

        # Monta SYN usando seq atual
        syn_pkt = Packet.make_tcp(
            src_port=local_port,
            dst_port=remote_addr[1],
            seq=self.seq,
            ack=0,
            flags=Packet.TCP_FLAG_SYN,
            window=self.recv_window,
            data=b"",
        ).to_bytes()

        # Consome o número de sequência usado pelo SYN (1)
        self.seq = (self.seq + 1) % SEQ_MOD

        start = time.time()
        attempts = 0

        while True:
            try:
                self.udp.sendto(syn_pkt, remote_addr)
                logger.syn(f"SEND SYN seq={self.seq} -> {remote_addr}")
            except Exception:
                # não falhar; tentar novamente
                pass

            try:
                self.udp.settimeout(1.0)
                raw, addr = self.udp.recvfrom(65536)
            except socket.timeout:
                attempts += 1
                if attempts >= 8 or (time.time() - start) > timeout:
                    raise TimeoutError("Timeout during connect (no SYN-ACK)")
                continue

            if addr != remote_addr:
                # pacote de outro fluxo; ignora
                continue

            try:
                pkt = Packet.from_bytes(raw)
            except Exception:
                continue

            if pkt.mode != Packet.MODE_TCP or pkt.is_corrupt():
                continue

            # queremos SYN+ACK do par correto
            if not ((pkt.flags & Packet.TCP_FLAG_SYN) and (pkt.flags & Packet.TCP_FLAG_ACK)):
                continue

            if pkt.dst_port != local_port or pkt.src_port != remote_addr[1]:
                continue

            self.remote_addr = addr
            self.ack = (pkt.seq_num + 1) % SEQ_MOD
            try:
                self.peer_window = int(pkt.window)
            except Exception:
                # se não for int válido, ignora
                pass

            logger.syn(
                f"RCV SYN-ACK seq={pkt.seq_num} ack={pkt.ack_num} win={pkt.window} from={addr}"
            )
            break

        # envia ACK final do handshake
        ack_pkt = Packet.make_tcp(
            src_port=local_port,
            dst_port=self.remote_addr[1],
            seq=self.seq,
            ack=self.ack,
            flags=Packet.TCP_FLAG_ACK,
            window=self.recv_window,
            data=b"",
        ).to_bytes()

        try:
            self.udp.sendto(ack_pkt, self.remote_addr)
            logger.syn(f"SEND ACK seq={self.seq} ack={self.ack} -> {self.remote_addr}")
        except Exception:
            pass

        # marca estado e inicia thread de recepção
        self.connected = True
        self.running = True
        self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
        self._recv_thread.start()
        # pequena pausa para garantir que thread suba antes de retornar
        time.sleep(0.01)

    # ---------------------------------------------------------------------
    # Accept / listen - lado servidor
    # ---------------------------------------------------------------------
    def listen(self) -> None:
        """Compatibilidade com API socket: placeholder sem ação."""
        return None

    def accept(self, timeout: Optional[float] = None) -> "TCPSocket":
        """Aguarda e realiza handshake do lado servidor.

        Retorna a própria instância com conexão estabelecida.
        """
        start = time.time()
        local_port = self.udp.getsockname()[1]

        while True:
            try:
                self.udp.settimeout(1.0)
                raw, addr = self.udp.recvfrom(65536)
            except socket.timeout:
                if timeout and (time.time() - start) >= timeout:
                    raise TimeoutError("Timeout waiting for incoming SYN")
                continue

            try:
                pkt = Packet.from_bytes(raw)
            except Exception:
                continue

            if pkt.mode != Packet.MODE_TCP or pkt.is_corrupt():
                continue

            # SYN recebido para esta porta local
            if (pkt.flags & Packet.TCP_FLAG_SYN) and pkt.dst_port == local_port:
                self.remote_addr = addr
                self.ack = (pkt.seq_num + 1) % SEQ_MOD
                try:
                    self.peer_window = int(pkt.window)
                except Exception:
                    pass

                logger.syn(f"[HS-RCV] SYN from {addr} seq={pkt.seq_num} win={pkt.window}")

                synack_pkt = Packet.make_tcp(
                    src_port=local_port,
                    dst_port=addr[1],
                    seq=self.seq,
                    ack=self.ack,
                    flags=Packet.TCP_FLAG_SYN | Packet.TCP_FLAG_ACK,
                    window=self.recv_window,
                    data=b"",
                ).to_bytes()

                try:
                    self.udp.sendto(synack_pkt, addr)
                    logger.syn(f"[HS-SEND] SYN-ACK seq={self.seq} ack={self.ack} -> {addr}")
                except Exception:
                    pass

                # consumes one seq number for SYN-ACK
                self.seq = (self.seq + 1) % SEQ_MOD

                try:
                    self.udp.settimeout(3.0)
                    raw2, addr2 = self.udp.recvfrom(65536)
                except socket.timeout:
                    # espera por ACK final; se não vier, volta a loop (retransmissões do cliente)
                    continue

                if addr2 != addr:
                    continue

                try:
                    pkt2 = Packet.from_bytes(raw2)
                except Exception:
                    continue

                if pkt2.mode != Packet.MODE_TCP or pkt2.is_corrupt():
                    continue

                if (pkt2.flags & Packet.TCP_FLAG_ACK) and pkt2.dst_port == local_port:
                    self.connected = True
                    self.running = True
                    self._recv_thread = threading.Thread(target=self._recv_loop, daemon=True)
                    self._recv_thread.start()
                    logger.info(f"[HS] Connection established with {addr}")
                    return self

    # ---------------------------------------------------------------------
    # Envio (fragmentação + controle de janela)
    # ---------------------------------------------------------------------
    def send(self, data: bytes) -> None:
        """Envia os bytes, fragmentando por max_payload e respeitando a janela do peer.

        O método usa send_buffer para rastrear segmentos não confirmados e
        inicia o timer de retransmissão quando necessário.
        """
        if not self.connected:
            raise ConnectionError("Socket not connected")

        offset = 0
        total = len(data)
        local_port = self.udp.getsockname()[1]

        while offset < total:
            with self._lock:
                bytes_in_flight = sum(len(entry["data"]) for entry in self.send_buffer.values())
                available = max(0, self.peer_window - bytes_in_flight)

            if available <= 0:
                logger.window(
                    f"SEND-WAIT offset={offset} total={total} peer_window={self.peer_window} in_flight={bytes_in_flight}"
                )
                time.sleep(self._send_wait_sleep)
                continue

            send_len = min(self.max_payload, available, total - offset)
            chunk = data[offset : offset + send_len]

            with self._lock:
                pkt_seq = self.seq
                pkt = Packet.make_tcp(
                    src_port=local_port,
                    dst_port=self.remote_addr[1],
                    seq=pkt_seq,
                    ack=self.ack,
                    flags=Packet.TCP_FLAG_PSH,
                    window=self.recv_window,
                    data=chunk,
                ).to_bytes()

                try:
                    self.udp.sendto(pkt, self.remote_addr)
                except Exception:
                    # envia falhou temporariamente; segmento ainda entra no buffer
                    pass

                self.send_buffer[pkt_seq] = {
                    "segment": pkt,
                    "time": time.time(),
                    "data": chunk,
                    "retrans": 0,
                }
                # avança a sequência local pelo tamanho do chunk
                self.seq = (self.seq + len(chunk)) % SEQ_MOD

                logger.send(
                    f"seq={pkt_seq} len={len(chunk)} in_flight={len(self.send_buffer)} peer_window={self.peer_window}"
                )

            # inicia timer caso não esteja rodando
            self._start_timer()
            offset += len(chunk)
            # leve sleep para evitar bursts excessivos
            time.sleep(0.0005)

    # ---------------------------------------------------------------------
    # Recebimento (retorna até bufsize bytes)
    # ---------------------------------------------------------------------
    def recv(self, bufsize: int = 4096, timeout: Optional[float] = 5.0) -> bytes:
        """Retorna até `bufsize` bytes; devolve b'' em timeout."""

        start = time.time()
        while True:
            with self._lock:
                if len(self.recv_buffer) > 0:
                    piece = bytes(self.recv_buffer[:bufsize])
                    # remove os bytes entregues do buffer
                    del self.recv_buffer[: len(piece)]
                    # atualiza janela anunciada
                    self.recv_window = min(65535, self.recv_window + len(piece))

                    # envia WINDOW-UPDATE (ACK sem payload)
                    if self.connected and self.remote_addr:
                        ack_pkt = Packet.make_tcp(
                            src_port=self.udp.getsockname()[1],
                            dst_port=self.remote_addr[1],
                            seq=self.seq,
                            ack=self.ack,
                            flags=Packet.TCP_FLAG_ACK,
                            window=self.recv_window,
                            data=b"",
                        ).to_bytes()
                        try:
                            self.udp.sendto(ack_pkt, self.remote_addr)
                            logger.window(f"WINDOW-UPDATE recv_window={self.recv_window} -> {self.remote_addr}")
                        except Exception:
                            pass

                    return piece

            if timeout is not None and (time.time() - start) >= timeout:
                return b""

            time.sleep(0.01)

    # ---------------------------------------------------------------------
    # Fechamento (envia FIN e aguarda ACK)
    # ---------------------------------------------------------------------
    def close(self, timeout: float = 5.0) -> None:
        """Fecha conexão enviando FIN e aguardando confirmação."""

        # se já desconectado, apenas fecha o socket
        if not self.connected:
            try:
                self.udp.close()
            except Exception:
                pass
            self.running = False
            self.connected = False
            return

        # espera briefly que send_buffer esvazie (até um limite)
        wait_start = time.time()
        while self.send_buffer and (time.time() - wait_start) < 10.0:
            time.sleep(0.02)

        local_port = self.udp.getsockname()[1]

        if not self.fin_sent:
            fin_pkt = Packet.make_tcp(
                src_port=local_port,
                dst_port=self.remote_addr[1],
                seq=self.seq,
                ack=self.ack,
                flags=Packet.TCP_FLAG_FIN,
                window=self.recv_window,
                data=b"",
            ).to_bytes()
            try:
                self.udp.sendto(fin_pkt, self.remote_addr)
            except Exception:
                pass

            # consome 1 número de sequência pelo FIN
            self.seq = (self.seq + 1) % SEQ_MOD

            self.fin_sent = True
            logger.info(f"FIN sent seq={self.seq}")

        # aguarda ACK do FIN
        t0 = time.time()
        while time.time() - t0 < timeout:
            if self.fin_acked:
                break
            time.sleep(0.02)

        # se recebemos FIN do peer e ainda não enviamos ACK final, envia
        if self.fin_received and not self.fin_acked:
            ack_pkt = Packet.make_tcp(
                src_port=local_port,
                dst_port=self.remote_addr[1],
                seq=self.seq,
                ack=self.ack,
                flags=Packet.TCP_FLAG_ACK,
                window=self.recv_window,
                data=b"",
            ).to_bytes()
            try:
                self.udp.sendto(ack_pkt, self.remote_addr)
            except Exception:
                pass
            self.fin_acked = True

        # finaliza estado e threads
        self.running = False
        if self._recv_thread and self._recv_thread.is_alive():
            try:
                self._recv_thread.join(timeout=0.1)
            except Exception:
                pass

        try:
            self.udp.close()
        except Exception:
            pass
        self.connected = False

    # ---------------------------------------------------------------------
    # Loop de recepção (thread)
    # ---------------------------------------------------------------------
    def _recv_loop(self) -> None:
        """Thread que processa pacotes recebidos do socket UDP.

        Lida com:
        - pacotes de dados (PSH): entrega, reordenação (OOO) e ACK cumulativo
        - ACKs: limpeza de send_buffer, cálculo de RTT e fast-retransmit
        - FIN: reconhecimento e fechamento
        """
        local_port = self.udp.getsockname()[1]

        while self.running:
            try:
                raw, addr = self.udp.recvfrom(65536)
            except Exception:
                # socket fechado ou erro: encerra thread
                return

            try:
                pkt = Packet.from_bytes(raw)
            except Exception:
                # formato inválido: ignora
                continue

            if pkt.mode != Packet.MODE_TCP or pkt.is_corrupt():
                # descarta pacotes inválidos
                continue

            if pkt.dst_port != local_port:
                # pacote não dirigido a esta instância
                continue

            # atualiza remote_addr se ainda não conhecido
            if self.remote_addr is None:
                self.remote_addr = addr

            try:
                self.peer_window = int(pkt.window)
            except Exception:
                pass

            flags = pkt.flags

            # ----------------------------
            # DATA (PSH)
            # ----------------------------
            if flags & Packet.TCP_FLAG_PSH:
                with self._lock:
                    seq = pkt.seq_num
                    data_len = len(pkt.data)

                    # caso esperado (cumulativo)
                    if seq == self.ack:
                        self.recv_buffer.extend(pkt.data)
                        self.ack = (self.ack + data_len) % SEQ_MOD
                        logger.recv(f"PSH seq={seq} len={data_len} ack->{self.ack}")

                        # desembaraça buffer fora-de-ordem em cadeia
                        while self.ack in self._ooo_buffer:
                            next_chunk = self._ooo_buffer.pop(self.ack)
                            self.recv_buffer.extend(next_chunk)
                            self.ack = (self.ack + len(next_chunk)) % SEQ_MOD
                        logger.recv(f"[RCV-PSH] deliver OOO seq now ack-> {self.ack}")

                    else:
                        # fora de ordem: armazena se ainda não existir
                        if seq not in self._ooo_buffer:
                            self._ooo_buffer[seq] = pkt.data
                            logger.info(f"OOO seq={seq} expected={self.ack} stored={len(self._ooo_buffer)}")
                        else:
                            logger.info(f"[RCV-PSH] duplicate OOO seq={seq} ignored")

                    # recalcula janela local (tamanho máximo do buffer)
                    MAX_BUF = 65535
                    self.recv_window = max(0, min(65535, MAX_BUF - len(self.recv_buffer)))

                    # sempre envia ACK cumulativo (fora do lock)
                    ack_pkt = Packet.make_tcp(
                        src_port=local_port,
                        dst_port=addr[1],
                        seq=self.seq,
                        ack=self.ack,
                        flags=Packet.TCP_FLAG_ACK,
                        window=self.recv_window,
                        data=b"",
                    ).to_bytes()

                # envio do ACK fora do lock
                try:
                    self.udp.sendto(ack_pkt, addr)
                    logger.ack(f"ACK ack={self.ack} -> {addr}")
                except Exception:
                    pass

                continue

            # ----------------------------
            # ACK
            # ----------------------------
            if flags & Packet.TCP_FLAG_ACK:
                ack_num = pkt.ack_num
                now = time.time()
                to_remove = []

                logger.ack(f"RCV ACK from={addr} ack_num={ack_num} buffer_keys={list(self.send_buffer.keys())}")

                with self._lock:
                    # remove segmentos já confirmados e atualiza RTT
                    for s, entry in list(self.send_buffer.items()):
                        seg_len = len(entry["data"])
                        seg_end = (s + seg_len) % SEQ_MOD

                        if self._seq_le(seg_end, ack_num):
                            sample = now - entry["time"]
                            # só ajusta RTT com amostras não retransmitidas
                            if entry.get("retrans", 0) == 0:
                                self._update_rtt(sample)
                            to_remove.append(s)

                    if to_remove:
                        logger.ack(f"ACK clears segments={to_remove}")
                    else:
                        logger.ack(f"ACK no segments removed (ack_num={ack_num})")

                    for s in to_remove:
                        self.send_buffer.pop(s, None)

                    # fast retransmit: contar ACKs duplicados
                    if self._last_ack is None or self._seq_lt(self._last_ack, ack_num):
                        # ACK avançou -> reset duplicados
                        self._dup_acks.clear()
                        self._last_ack = ack_num
                    elif ack_num == self._last_ack:
                        # duplicado
                        self._dup_acks[ack_num] = self._dup_acks.get(ack_num, 0) + 1
                        if self._dup_acks[ack_num] == 3:
                            # retransmitir o segmento mais antigo em outstanding
                            if self.send_buffer:
                                oldest_seq = min(self.send_buffer.keys(), key=lambda k: k)
                                entry = self.send_buffer.get(oldest_seq)
                                if entry:
                                    try:
                                        logger.retry(f"[FAST-RETX] dupacks={self._dup_acks[ack_num]} retransmit seq={oldest_seq}")
                                        self.udp.sendto(entry["segment"], self.remote_addr)
                                        # marca como retransmitido (atualiza timestamp e contador)
                                        self.send_buffer[oldest_seq]["time"] = time.time()
                                        self.send_buffer[oldest_seq]["retrans"] = entry.get("retrans", 0) + 1
                                    except Exception:
                                        pass

                    # FIN handling: se FIN enviado e ACK confirma posição
                    if self.fin_sent:
                        if self._seq_le(self.seq, ack_num):
                            self.fin_acked = True

                continue

            # ----------------------------
            # FIN
            # ----------------------------
            if flags & Packet.TCP_FLAG_FIN:
                with self._lock:
                    self.fin_received = True
                    # ACK para o FIN do peer
                    self.ack = (pkt.seq_num + 1) % SEQ_MOD

                    ack_pkt = Packet.make_tcp(
                        src_port=local_port,
                        dst_port=addr[1],
                        seq=self.seq,
                        ack=self.ack,
                        flags=Packet.TCP_FLAG_ACK,
                        window=self.recv_window,
                        data=b"",
                    ).to_bytes()

                try:
                    self.udp.sendto(ack_pkt, addr)
                    logger.info(f"RCV FIN from={addr} ack={self.ack}")
                except Exception:
                    pass

                # Se ainda não mandamos FIN, enviaremos agora
                if not self.fin_sent:
                    fin_pkt = Packet.make_tcp(
                        src_port=local_port,
                        dst_port=addr[1],
                        seq=self.seq,
                        ack=self.ack,
                        flags=Packet.TCP_FLAG_FIN,
                        window=self.recv_window,
                        data=b"",
                    ).to_bytes()
                    try:
                        self.udp.sendto(fin_pkt, addr)
                        logger.send(f"[SEND] FIN seq={self.seq} -> {addr}")
                    except Exception:
                        pass

                    # FIN consome 1 número de sequência
                    self.seq = (self.seq + 1) % SEQ_MOD
                    self.fin_sent = True

                continue

        # encerra thread
        self.running = False

    # ---------------------------------------------------------------------
    # Timer de retransmissão (thread)
    # ---------------------------------------------------------------------
    def _start_timer(self) -> None:
        """Inicia thread de timer que monitora send_buffer e retransmite."""

        with self._timer_lock:
            if self._timer_running:
                return
            self._timer_running = True

        def timer_func() -> None:
            while self.running and self.connected and self._timer_running:
                now = time.time()
                resend = []

                with self._lock:
                    # checa todos segmentos outstanding e adiciona vencidos em `resend`
                    if self.send_buffer:
                        for seq_key, entry in list(self.send_buffer.items()):
                            # backoff exponencial por retransmissões anteriores
                            rto = self.timeout_interval * (2 ** entry.get("retrans", 0))
                            # limita rto efetivo para evitar valores extremos (0.05..1.0)
                            effective = min(max(0.05, rto), 1.0)
                            if now - entry.get("time", 0) > effective:
                                resend.append((seq_key, entry))

                # retransmite fora do lock
                for seq_key, entry in resend:
                    logger.retry(f"Retransmit seq={seq_key} count={entry.get('retrans', 0)}")
                    try:
                        self.udp.sendto(entry["segment"], self.remote_addr)
                    except Exception:
                        pass

                    with self._lock:
                        if seq_key in self.send_buffer:
                            self.send_buffer[seq_key]["time"] = time.time()
                            self.send_buffer[seq_key]["retrans"] = entry.get("retrans", 0) + 1

                with self._lock:
                    if not self.send_buffer:
                        # nada a retransmitir: encerra o timer
                        break

                time.sleep(0.02)

            with self._timer_lock:
                self._timer_running = False

        # inicia thread de timer
        self._timer_thread = threading.Thread(target=timer_func, daemon=True)
        self._timer_thread.start()

    # ---------------------------------------------------------------------
    # RTT / timeout helpers
    # ---------------------------------------------------------------------
    def _calc_timeout(self) -> float:
        """Calcula timeout atual baseado em estimated_rtt e dev_rtt."""
        return max(0.01, self.estimated_rtt + 4 * self.dev_rtt)

    def _update_rtt(self, sample: float) -> None:
        """Atualiza estimativas de RTT usando algoritmo de Jacobson/Karels."""
        if sample <= 0:
            return

        est = self.estimated_rtt
        dev = self.dev_rtt

        est_new = 0.875 * est + 0.125 * sample
        dev_new = 0.75 * dev + 0.25 * abs(sample - est)

        self.estimated_rtt = est_new
        self.dev_rtt = dev_new
        self.timeout_interval = self._calc_timeout()

    # ---------------------------------------------------------------------
    # Utilitários
    # ---------------------------------------------------------------------
    def fileno(self) -> int:
        """Retorna o file descriptor do socket UDP subjacente (compatibilidade)."""
        return self.udp.fileno()


def tcp_socket(local_addr: Tuple[str, int] = ("127.0.0.1", 0), recv_window: int = 4096) -> TCPSocket:
    """Factory helper para compatibilidade com testes antigos."""
    return TCPSocket(local_addr=local_addr, recv_window=recv_window)


__all__ = ["TCPSocket", "tcp_socket"]
