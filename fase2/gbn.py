"""
# fase2/gbn.py

Implementação do Go-Back-N (GBN) baseada em Sockets UDP.

Esta versão usa threads de recebimento independentes para o Remetente (ACKs)
e o Receptor (DATA), comunicando-se através de sockets UDP reais.
O UnreliableChannel atua como um proxy para simular perda/atraso.
"""

import socket
import threading
import time
from utils.packet import make_packet, parse_packet, TYPE_DATA, TYPE_ACK
from utils import logger

class GBNSender:
    """
    Implementa o Remetente GBN (Go-Back-N) Clássico.
    
    Gerencia a janela deslizante, buffer de retransmissão e um timer único.
    O Remetente depende unicamente de timeouts para detectar e recuperar perdas.
    """
    
    def __init__(self, local_addr=('localhost', 0), dest_addr=('localhost', 14000),
                  channel=None, N=5, timeout=2.0, verbose=True):
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)
        self.sock.settimeout(0.5)
        self.dest = dest_addr
        self.channel = channel
        self.N = N
        self.timeout = timeout
        self.base_protegida = 0
        self.SENDER_TIMEOUT = timeout
        self.base = 0
        self.nextseq = 0
        self.lock = threading.RLock()
        self.cond_janela_nao_cheia = threading.Condition(self.lock)
        self.timer = None
        self.buffer = {} # buffer[seq_abs] = pkt_bytes
        self.running = True
        self.verbose = verbose
        self.retransmissions = 0
        
        threading.Thread(target=self._recv_loop, daemon=True).start()

    def send(self, data: bytes):
        """
        Envia um pacote de dados. Bloqueia se a janela estiver cheia.
        """
        with self.lock:
            # Espera até que a janela tenha espaço
            while self.nextseq >= self.base + self.N:
                if self.verbose:
                    logger.info(f'[GBN SENDER] Janela cheia, esperando... base={self.base}')
                self.cond_janela_nao_cheia.wait()
                
            seq_abs = self.nextseq
            seq_field = seq_abs % 256
            pkt = make_packet(TYPE_DATA, seq_field, data)
            self.buffer[seq_abs] = pkt

            self._send_packet_bytes(pkt)
            
            logger.log_sent(seq_abs, TYPE_DATA)

            if self.base == self.nextseq:
                self._start_timer() # Inicia timer quando primeiro pacote pendente surge

            self.nextseq += 1

    def _send_packet_bytes(self, pkt):
        if self.channel:
            self.channel.send(pkt, self.sock, self.dest)
        else:
            self.sock.sendto(pkt, self.dest)

    def _stop_timer(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None

    def _start_timer(self):
        self._stop_timer()
        self.base_protegida_pelo_timer = self.base
        self.timer = threading.Timer(self.timeout, self._handle_timeout)
        self.timer.start()
        logger.info(f'[TIMER] INICIADO | Protegendo Base: {self.base_protegida_pelo_timer} | Timeout: {self.timeout}s')

    def _retransmit_all(self):
        """Retransmite todos os pacotes na janela de envio (self.base até self.nextseq - 1)."""
        
        for seq_num in range(self.base, self.nextseq):
            # 1. Obter o pacote: Se você armazena pacotes inteiros no buffer:
            packet = self.buffer.get(seq_num) # Ou self.buffer[seq_num], dependendo da sua estrutura
            
            # 2. Reenviar o pacote (usando a mesma lógica de envio de pacotes data)
            if packet:
                # Use a função interna de envio sem a lógica de janela/buffer
                self._send_packet_bytes(packet)
                # Opcional: Contar retransmissões
                self.retransmissions += 1
            else:
                # Isso não deve acontecer se a lógica do buffer estiver correta
                print(f"[ERROR] Pacote {seq_num} não encontrado no buffer para retransmissão.")

    def _handle_timeout(self):
        """Callback do timer: retransmite toda a janela."""
        with self.lock:
            if not self.running:
                return
                
            # 1. VERIFICAÇÃO DE OBSOLESCÊNCIA
            # A variável correta no seu código é self.base_protegida_pelo_timer
            if self.base != self.base_protegida_pelo_timer:
                logger.info(f'[GBN SENDER] Timer obsoleto disparado. Base={self.base}, Último Início={self.base_protegida_pelo_timer}. Abortando retransmissão.')
                return # Aborta o timer obsoleto
            
            logger.log_timeout(self.base)
            
            # 2. Retransmite a janela inteira (Reutiliza _retransmit_all, que é limpo)
            # 🛑 Remova o loop for seq in range(self.base, self.nextseq): que estava aqui antes!
            self._retransmit_all()

            # 3. Contabiliza (apenas 1 evento de timeout, ou a contagem interna de _retransmit_all)
            # Como _retransmit_all já conta por pacote, remova a contagem redundante aqui.
            # self.retransmissions += (self.nextseq - self.base) # <- REMOVER ISSO SE _retransmit_all CONTA

            # 4. Reinicia o timer para proteger a BASE (que acabou de ser retransmitida)
            if self.base < self.nextseq:
                self._start_timer()

    def _recv_loop(self):
        """Thread que escuta ACKs e avança a base cumulativamente."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65536)
            except socket.timeout:
                continue
            except OSError:
                break
                
            info = parse_packet(data) 
            if info is None:
                continue

            if info['type'] == TYPE_ACK and not info['corrupt']:
                ack_mod = info['seq'] # ACK modular (próximo esperado pelo Receptor)
                
                with self.lock:
                    max_new_base = self.base
                    
                    # 1. Tenta calcular o valor absoluto do novo 'base' (Avanço de Janela)
                    for i in range(self.base, self.nextseq):
                        if ((i + 1) % 256) == ack_mod:
                            max_new_base = i + 1
                            
                    new_base = max_new_base

                    logger.info(f'[DEBUG] ACK Mod Recebido: {ack_mod} | Base Atual: {self.base} | New Base Calculada: {new_base}')
                    
                    # 2. Lógica de Avanço de Janela (ACK Cumulativo)
                    if new_base > self.base:
                        logger.log_received(new_base - 1, TYPE_ACK)
                        
                        self.base = new_base 
                        
                        # Limpa o buffer
                        for s in list(self.buffer.keys()):
                            if s < self.base:
                                del self.buffer[s]
                                
                        self.cond_janela_nao_cheia.notify_all()

                        # Gerencia o Timer (Reinicia se houver pacotes pendentes)
                        if self.base == self.nextseq:
                            logger.info(f'[GBN SENDER] Transferência concluída! Base={self.base}')
                            self._stop_timer()
                        else:
                            self._start_timer() 
                            
                    # 3. ACK não avança a base (Duplicado, Fora de Ordem ou Redundante)
                    elif new_base == self.base:
                        if self.base == self.nextseq:
                            logger.info(f'[GBN SENDER] ACK {ack_mod} redundante após conclusão. Base={self.base}')
                        else:
                            logger.info(f'[GBN SENDER] ACK {ack_mod} duplicado/fora de ordem ignorado. Base={self.base}')
                        continue
                            
    def wait_for_completion(self):
        """
        Bloqueia o thread principal até que todos os pacotes tenham sido ACKados.
        Isso é o que acontece no final da transferência no Sender.
        """
        with self.lock:
            while self.base < self.nextseq and self.running:
                logger.info(f'[INFO] Aguardando ACKs finais... Base={self.base}, NextSeq={self.nextseq}')
                
                self.cond_janela_nao_cheia.wait(timeout=self.SENDER_TIMEOUT * 2) # Espera, liberando o lock.

                # Check for spurious wakeups
                if self.base < self.nextseq and self.timer is None:
                    # Reinicia o timer, pois pode ter havido uma perda
                    self._start_timer()
                            
                            
    def close(self):
        """Espera a conclusão e fecha o socket."""
        while self.base < self.nextseq and self.running:
            logger.info(f"Aguardando ACKs finais... Base={self.base}, NextSeq={self.nextseq}")
            time.sleep(0.1) 
            
        self.running = False
        self._stop_timer()
        time.sleep(0.5)
        try:
            self.sock.close()
        except Exception:
            pass


class GBNReceiver:
    """
    Implementa o Receptor GBN (Baseado em Socket).
    """
    def __init__(self, local_addr=('localhost', 14000), deliver_callback=None,
                  channel=None, verbose=True):
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)
        self.channel = channel
        self.running = True
        self.verbose = verbose
        self.expected = 0
        threading.Thread(target=self._recv_loop, daemon=True).start()
        self.lock = threading.RLock()
        self.total_bytes_expected = None
        self.cond_conclusao = threading.Condition(self.lock)
        self.deliver_callback = deliver_callback or (lambda b: None)

    def _send_ack(self, seq_field, addr):
        """Função auxiliar para enviar ACKs."""
        pkt = make_packet(TYPE_ACK, seq_field, b'')
        if self.channel:
            self.channel.send(pkt, self.sock, addr)
        else:
            self.sock.sendto(pkt, addr)
            
        logger.log_sent(seq_field, TYPE_ACK)

    def _recv_loop(self):
        """Thread que escuta pacotes DATA."""
        while self.running:
            try:
                data, addr = self.sock.recvfrom(65536)
            except OSError:
                break
                
            info = parse_packet(data)
            if info is None:
                continue

            if info['type'] == TYPE_DATA:
                
                # 1. Tratar corrupção FORA do bloco principal, mas com a chance de enviar ACK
                if info['corrupt']:
                    # Ação GBN: Ignorar o pacote corrompido e enviar o ACK para o último pacote aceito.
                    with self.lock:
                        if self.verbose:
                            logger.log_corrupt(info['seq'], TYPE_DATA)
                        expected_ack = self.expected % 256
                        self._send_ack(expected_ack, addr)
                    continue # Volta para o início do loop (correto)

                # 2. Lógica principal (dentro do lock, pois manipula self.expected)
                with self.lock:
                    recv_seq = info['seq'] # 0..255
                    expected_mod = self.expected % 256
                    
                    # Pacote esperado chegou (in-order)
                    if recv_seq == expected_mod:
                        if self.verbose:
                            logger.log_received(self.expected, TYPE_DATA)
                        
                        # 3. Entrega do dado e avanço do ponteiro
                        self.deliver_callback(info['payload'])
                        self.expected += 1 
                        
                        # 4. Notificação de Conclusão (para o thread 'wait_for_completion')
                        self.cond_conclusao.notify_all()
                        
                        # 5. Envio do novo ACK cumulativo
                        new_ack = self.expected % 256
                        self._send_ack(new_ack, addr) 
                    
                    # Pacote fora de ordem (duplicado/posterior)
                    else:
                        ack_faltando = self.expected % 256 
                        if self.verbose:
                            logger.info(f'[GBN RECV] Fora de ordem seq={recv_seq}, esperado={expected_mod}. Re-ACK {ack_faltando}')
                        
                        # 6. Reenvia o ACK para o pacote esperado (cumulativo)
                        self._send_ack(ack_faltando, addr)
                        
    def wait_for_completion(self, total_packets_expected):
        """Bloqueia até que o número total de pacotes esperados tenha sido entregue."""
        # Se você está contando pacotes no Sender (sender.nextseq), use total_packets_expected.
        self.total_packets_expected = total_packets_expected 
        
        with self.lock:
            # Loop de espera
            while self.expected < self.total_packets_expected:
                if self.verbose:
                    logger.info(f'[GBN RECV] Esperando a conclusão. Pacotes Entregues: {self.expected}/{self.total_packets_expected}')
                self.cond_conclusao.wait()
                
        if self.verbose:
            logger.info(f'[GBN RECV] Conclusão alcançada. Total de pacotes: {self.expected}')

    def close(self):
        """Fecha o socket do receptor."""
        self.running = False
        try:
            # Acorda o thread que está esperando no wait_for_completion, caso esteja bloqueado
            with self.lock:
                 self.cond_conclusao.notify_all()
            self.sock.close()
        except Exception:
            pass