"""
fase3/tcp_socket.py
SimpleTCPSocket - TCP simplificado sobre UDP (ou UnreliableChannel).
- Usa UDP sockets por baixo quando channel is None.
- Se channel (UnreliableChannel) for passado, o envio usa channel.send(packet, dest=udp_socket, dest_addr=(host,port))
- Cabeçalho TCP simplificado:
  src_port(2), dst_port(2), seq(4), ack(4), hdr_len(1), flags(1), window(2), checksum(2), urg_ptr(2)
Flags usados: SYN=0x02, ACK=0x10, FIN=0x01
"""
import socket
import threading
import struct
import time
import random
import hashlib
from collections import OrderedDict
from typing import Optional, Tuple

# logger (simplificado) está em utils/packet.py
from utils.packet import logger
from utils.simulator import UnreliableChannel

# Flags
FLAG_FIN = 0x01
FLAG_SYN = 0x02
FLAG_ACK = 0x10

TCP_HDR_FMT = '!HHIIBBHHH'   # src_port, dst_port, seq, ack, hdr_len, flags, window, checksum, urg_ptr
TCP_HDR_SIZE = struct.calcsize(TCP_HDR_FMT)
MSS = 1024  # bytes payload máximo por segmento


def _checksum(data: bytes) -> int:
    """Checksum simples: 16-bit do MD5 truncado (compatível com a prática didática)."""
    h = hashlib.md5(data).digest()
    return struct.unpack('!H', h[:2])[0]


class SimpleTCPSocket:
    def __init__(self, port: int = 0, verbose: bool = True, channel: Optional[UnreliableChannel] = None):
        """
        port: porta local para bind (0 -> SO escolhe)
        verbose: logs
        channel: instância de UnreliableChannel (opcional). Se None, usa UDP real.
        """
        self.channel = channel
        self.verbose = verbose

        # UDP subjacente (sempre criado para receber)
        self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_socket.bind(('0.0.0.0', port))
        self.port = self.udp_socket.getsockname()[1]

        # Estado da conexão
        self.state = 'CLOSED'  # CLOSED, LISTEN, SYN_SENT, SYN_RCVD, ESTABLISHED, FIN_WAIT_1, FIN_WAIT_2, LAST_ACK, TIME_WAIT

        # Números de sequência/ACK
        self.isn = random.randint(0, 2**31 - 1)
        self.next_seq = self.isn          # next sequence number to use for sending (byte-based)
        self.ack_num = 0                  # next expected byte from peer (cumulative)

        # Buffers
        # send buffer: seq -> (payload_bytes, send_time, retrans_count)
        self._unacked = OrderedDict()
        # receive buffer: seq -> payload
        self.recv_buffer = {}
        self.recv_read_ptr = 0  # next expected absolute byte

        # Flow control (recv window advertised)
        self.recv_window = 4096
        self._peer_window = 4096

        # RTT estimator
        self.estimated_rtt = 1.0
        self.dev_rtt = 0.5

        # Peer info
        self.peer_host: Optional[str] = None
        self.peer_port: Optional[int] = None

        # Concurrency helpers
        self._lock = threading.Lock()
        self._running = True

        # Start receiver and retransmission threads
        self._recv_thread = threading.Thread(target=self._receive_loop, daemon=True)
        self._recv_thread.start()
        self._retrans_thread = threading.Thread(target=self._retransmit_loop, daemon=True)
        self._retrans_thread.start()

        if self.verbose:
            logger.info(f"SimpleTCPSocket bound on port {self.port}")

    # ------------------ Public API ------------------
    def listen(self):
        """Coloca socket em modo LISTEN (servidor)."""
        if self.state != 'CLOSED':
            raise RuntimeError("Socket must be CLOSED to listen")
        self.state = 'LISTEN'
        if self.verbose:
            logger.info(f"[STATE] LISTEN (port {self.port})")

    def accept(self, timeout: float = 10.0) -> Tuple['SimpleTCPSocket', Tuple[str, int]]:
        """Bloqueia até handshake completar no server-side."""
        start = time.time()
        while time.time() - start < timeout:
            if self.state == 'ESTABLISHED':
                return (self, (self.peer_host, self.peer_port))
            time.sleep(0.01)
        raise TimeoutError("accept timed out")

    def connect(self, dest: Tuple[str, int], timeout: float = 5.0):
        """Three-way handshake (client)."""
        if self.state != 'CLOSED':
            raise RuntimeError("Socket must be CLOSED to connect")
        self.peer_host, self.peer_port = dest
        self.state = 'SYN_SENT'
        # choose ISN for this side and use next_seq = ISN
        self.isn = random.randint(0, 2**31 - 1)
        self.next_seq = self.isn
        syn_pkt = self._make_segment(flags=FLAG_SYN, seq=self.next_seq, ack=0, window=self.recv_window, payload=b'')
        self._send_raw(syn_pkt, (self.peer_host, self.peer_port))
        if self.verbose:
            logger.info(f"[SND] SYN seq={self.next_seq} -> {dest}")
        start = time.time()
        while time.time() - start < timeout:
            if self.state == 'ESTABLISHED':
                return
            time.sleep(0.01)
        raise TimeoutError("connect timed out")

    def send(self, data: bytes):
        """Envia dados fragmentando em MSS; bloqueia até tudo confirmado."""
        if self.state != 'ESTABLISHED':
            raise RuntimeError("Connection not established")
        offset = 0
        while offset < len(data):
            with self._lock:
                unacked_bytes = sum(len(v[0]) for v in self._unacked.values())
                allowed = max(0, min(self._peer_window - unacked_bytes, MSS))
                if allowed == 0:
                    # janela cheia -> esperar ACKs
                    pass
                else:
                    chunk = data[offset: offset + allowed]
                    seq = self.next_seq
                    pkt = self._make_segment(flags=0, seq=seq, ack=self.ack_num, window=self.recv_window, payload=chunk)
                    self._send_raw(pkt, (self.peer_host, self.peer_port))
                    self._unacked[seq] = (chunk, time.time(), 0)
                    if self.verbose:
                        logger.log_sent(seq, 'DATA')
                    self.next_seq += len(chunk)
                    offset += len(chunk)
            time.sleep(0.001)
        # aguardar confirmação de todos os segmentos
        while True:
            with self._lock:
                if not self._unacked:
                    break
            time.sleep(0.01)

    def recv(self, buffer_size: int = 4096, timeout: Optional[float] = None) -> bytes:
        """Retorna próximo bloco contíguo disponível a partir do recv_read_ptr (bloqueante)."""
        start = time.time()
        while True:
            with self._lock:
                if self.recv_read_ptr in self.recv_buffer:
                    data = self.recv_buffer.pop(self.recv_read_ptr)
                    self.recv_read_ptr += len(data)
                    return data
            if timeout is not None and time.time() - start > timeout:
                return b''
            time.sleep(0.01)

    def close(self, timeout: float = 5.0):
        """Four-way close iniciado por este endpoint (active close)."""
        if self.state != 'ESTABLISHED':
            # se já fechado ou não conectado, shutdown local
            self._shutdown()
            return
        fin_seq = self.next_seq
        fin_pkt = self._make_segment(flags=FLAG_FIN, seq=fin_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
        self._send_raw(fin_pkt, (self.peer_host, self.peer_port))
        if self.verbose:
            logger.info(f"[SND] FIN seq={fin_seq}")
        self.state = 'FIN_WAIT_1'
        start = time.time()
        while time.time() - start < timeout:
            if self.state == 'CLOSED':
                self._shutdown()
                return
            time.sleep(0.01)
        # força shutdown
        self._shutdown()

    # ------------------ Internals ------------------
    def _make_segment(self, flags: int = 0, seq: int = 0, ack: int = 0, window: int = 4096, payload: bytes = b'') -> bytes:
        """Cria header e retorna header+payload com checksum."""
        src_port = self.port
        dst_port = self.peer_port if self.peer_port is not None else 0
        hdr_len = TCP_HDR_SIZE
        urg_ptr = 0
        header_wo_ck = struct.pack(TCP_HDR_FMT, src_port, dst_port, seq, ack, hdr_len, flags, window, 0, urg_ptr)
        chksum = _checksum(header_wo_ck + payload)
        header = struct.pack(TCP_HDR_FMT, src_port, dst_port, seq, ack, hdr_len, flags, window, chksum, urg_ptr)
        return header + payload

    def _parse_segment(self, packet: bytes):
        """Desempacota e valida checksum; retorna dict ou None se inválido."""
        if len(packet) < TCP_HDR_SIZE:
            return None
        try:
            hdr = packet[:TCP_HDR_SIZE]
            src_port, dst_port, seq, ack, hdr_len, flags, window, chksum_rcv, urg_ptr = struct.unpack(TCP_HDR_FMT, hdr)
            payload = packet[TCP_HDR_SIZE:]
        except struct.error:
            return None
        header_wo_ck = struct.pack(TCP_HDR_FMT, src_port, dst_port, seq, ack, hdr_len, flags, window, 0, urg_ptr)
        if _checksum(header_wo_ck + payload) != chksum_rcv:
            return None
        return {
            'src_port': src_port,
            'dst_port': dst_port,
            'seq': seq,
            'ack': ack,
            'flags': flags,
            'window': window,
            'payload': payload
        }

    def _send_raw(self, packet: bytes, addr: Tuple[str, int]):
        """Envia via channel (se fornecido) ou via UDP socket."""
        if self.channel is not None:
            # UnreliableChannel.send(packet, dest_socket, dest_addr)
            # nosso dest_socket é self.udp_socket (para que simulate envie usando sendto para o UDP receptor)
            self.channel.send(packet, dest=self.udp_socket, dest_addr=addr)
        else:
            self.udp_socket.sendto(packet, addr)

    def _receive_loop(self):
        """Thread que recebe datagramas UDP e processa segmentos TCP (handshake, dados, acks, fin)."""
        while self._running:
            try:
                packet, addr = self.udp_socket.recvfrom(65535)
            except Exception:
                break
            info = self._parse_segment(packet)
            if info is None:
                if self.verbose:
                    logger.info("[RCV] pacote inválido ou corrompido")
                continue

            flags = info['flags']
            seq = info['seq']
            ack = info['ack']
            payload = info['payload']
            src_host = addr[0]
            src_port = info['src_port']

            # set peer when server in LISTEN or client in SYN_SENT
            if self.peer_host is None and self.state in ('LISTEN', 'SYN_SENT'):
                self.peer_host = src_host
                self.peer_port = src_port
                if self.verbose:
                    logger.info(f"[INFO] peer set to {(self.peer_host, self.peer_port)}")

            # --- SYN (server) ---
            if flags & FLAG_SYN:
                if self.state == 'LISTEN':
                    # recebi SYN -> enviar SYN-ACK
                    self.ack_num = seq + 1
                    self.isn = random.randint(0, 2**31 - 1)
                    self.next_seq = self.isn
                    synack = self._make_segment(flags=FLAG_SYN | FLAG_ACK, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                    self._send_raw(synack, (self.peer_host, self.peer_port))
                    if self.verbose:
                        logger.info(f"[SND] SYN-ACK seq={self.next_seq} ack={self.ack_num}")
                    self.state = 'SYN_RCVD'
                elif self.state == 'SYN_SENT':
                    # simultaneous open: reply with SYN-ACK
                    self.ack_num = seq + 1
                    synack = self._make_segment(flags=FLAG_SYN | FLAG_ACK, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                    self._send_raw(synack, (self.peer_host, self.peer_port))
                    if self.verbose:
                        logger.info("[SND] SYN-ACK (simultaneous)")
                continue

            # --- SYN-ACK (client side) ---
            if (flags & FLAG_SYN) and (flags & FLAG_ACK) and self.state == 'SYN_SENT':
                self.ack_num = info['seq'] + 1
                # our sequence for ACK is ISN+1 (SYN consumed one byte)
                self.next_seq = self.isn + 1
                ack_pkt = self._make_segment(flags=FLAG_ACK, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                self._send_raw(ack_pkt, (self.peer_host, self.peer_port))
                if self.verbose:
                    logger.info(f"[SND] ACK seq={self.next_seq} ack={self.ack_num}")
                self.state = 'ESTABLISHED'
                continue

            # --- ACK processing (cumulative) ---
            if flags & FLAG_ACK:
                with self._lock:
                    to_remove = []
                    for s, (pl, sent_t, rcount) in list(self._unacked.items()):
                        if s + len(pl) <= ack:
                            sample_rtt = time.time() - sent_t
                            # update RTT estimator
                            self._update_rtt(sample_rtt)
                            to_remove.append(s)
                    for k in to_remove:
                        del self._unacked[k]
                        if self.verbose:
                            logger.log_received(k, 'ACK')
                    # update peer window
                    self._peer_window = info['window']
                # handshake completion server-side
                if self.state == 'SYN_RCVD':
                    self.state = 'ESTABLISHED'
                    if self.verbose:
                        logger.info("[STATE] ESTABLISHED (server)")
                if self.state == 'FIN_WAIT_1':
                    self.state = 'FIN_WAIT_2'
                if self.state == 'LAST_ACK':
                    self.state = 'CLOSED'
                continue

            # --- FIN received ---
            if flags & FLAG_FIN:
                # reply ACK for FIN
                self.ack_num = seq + 1
                ack_pkt = self._make_segment(flags=FLAG_ACK, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                self._send_raw(ack_pkt, (self.peer_host, self.peer_port))
                if self.verbose:
                    logger.info(f"[SND] ACK(for FIN) ack={self.ack_num}")
                if self.state == 'ESTABLISHED':
                    # passive close: send own FIN
                    fin_pkt = self._make_segment(flags=FLAG_FIN, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                    self._send_raw(fin_pkt, (self.peer_host, self.peer_port))
                    if self.verbose:
                        logger.info("[SND] FIN (passive)")
                    self.state = 'LAST_ACK'
                elif self.state == 'FIN_WAIT_2':
                    # peer closed after we already sent FIN and got ACK
                    self.state = 'CLOSED'
                continue

            # --- DATA segment ---
            if len(payload) > 0:
                with self._lock:
                    expected = self.recv_read_ptr
                    if seq == expected:
                        # in-order
                        self.recv_buffer[seq] = payload
                        self.recv_read_ptr += len(payload)
                    else:
                        # out-of-order: buffer
                        if seq not in self.recv_buffer:
                            self.recv_buffer[seq] = payload
                    # recompute cumulative ack (largest contiguous)
                    ack_cand = self.recv_read_ptr
                    while ack_cand in self.recv_buffer:
                        ack_cand += len(self.recv_buffer[ack_cand])
                    self.ack_num = ack_cand
                # send cumulative ACK
                ack_pkt = self._make_segment(flags=FLAG_ACK, seq=self.next_seq, ack=self.ack_num, window=self.recv_window, payload=b'')
                self._send_raw(ack_pkt, (self.peer_host, self.peer_port))
                if self.verbose:
                    logger.info(f"[SND] ACK ack={self.ack_num}")
                continue

    def _retransmit_loop(self):
        """Verifica timeouts e retransmite segmentos não ACKed (simples)."""
        while self._running:
            now = time.time()
            timeout = self._calculate_timeout()
            to_retx = []
            with self._lock:
                # iterate copy to allow deletion during loop
                for seq, (payload, sent_t, rcount) in list(self._unacked.items()):
                    if now - sent_t > timeout:
                        to_retx.append((seq, payload, rcount))
            for seq, payload, rcount in to_retx:
                with self._lock:
                    pkt = self._make_segment(flags=0, seq=seq, ack=self.ack_num, window=self.recv_window, payload=payload)
                    try:
                        self._send_raw(pkt, (self.peer_host, self.peer_port))
                        # update send time and count
                        self._unacked[seq] = (payload, time.time(), rcount + 1)
                        if self.verbose:
                            logger.info(f"[RTR] Retransmit seq={seq} (count={rcount+1})")
                    except Exception:
                        pass
            time.sleep(0.01)

    def _calculate_timeout(self) -> float:
        return self.estimated_rtt + 4.0 * self.dev_rtt

    def _update_rtt(self, sample_rtt: float):
        self.estimated_rtt = 0.875 * self.estimated_rtt + 0.125 * sample_rtt
        self.dev_rtt = 0.75 * self.dev_rtt + 0.25 * abs(sample_rtt - self.estimated_rtt)

    def _shutdown(self):
        self._running = False
        try:
            self.udp_socket.close()
        except Exception:
            pass
        if self.verbose:
            logger.info("[STATE] socket shutdown")
