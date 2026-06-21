"""
===========================================================
Módulo: packet.py
===========================================================

Implementa pacotes usados em todas as fases do projeto de
Transferência Confiável de Dados.

Modos suportados:
    - MODE_RDT : usado nas fases 1 e 2 (RDT 2.0, 2.1, 3.0)
    - MODE_TCP : segmento TCP simplificado usado na fase 3

Principais funcionalidades:
    - Serialização/desserialização automática
    - Cálculo de checksums (RDT e TCP)
    - Criação de pacotes DATA, ACK, SYN, FIN
    - Cabeçalhos e campos específicos de cada modo
"""
from __future__ import annotations

import struct
from typing import Optional


class Packet:
    """
    Representa um pacote RDT (fases 1-2) ou um segmento TCP
    simplificado (fase 3).

    Atributos do modo RDT:
        seq_num (int): número de sequência
        ack_num (int): número de ACK
        flags (int): flags de controle (DATA, ACK, etc.)
        checksum (int): soma de verificação de integridade
        data (bytes): payload

    Atributos do modo TCP:
        src_port (int): porta de origem
        dst_port (int): porta de destino
        seq_num (int): número de sequência
        ack_num (int): número de ACK
        hlen (int): tamanho do cabeçalho
        flags (int): flags TCP (SYN, ACK, FIN, etc.)
        window (int): tamanho da janela anunciada
        urgent (int): ponteiro urgente
        checksum (int): checksum
        data (bytes): payload
    """

    # ---------------------- MODOS ------------------------------- #
    MODE_RDT = 1
    MODE_TCP = 2

    # Flags RDT
    FLAG_DATA = 1 << 0
    FLAG_ACK = 1 << 1
    FLAG_SYN = 1 << 2
    FLAG_FIN = 1 << 3

    # Structs
    RDT_HEADER_FORMAT = ">I I B H"
    RDT_HEADER_SIZE = struct.calcsize(RDT_HEADER_FORMAT)

    TCP_HEADER_FORMAT = ">H H I I B B H H H"
    TCP_HEADER_SIZE = struct.calcsize(TCP_HEADER_FORMAT)

    # Flags TCP
    TCP_FLAG_FIN = 0x01
    TCP_FLAG_SYN = 0x02
    TCP_FLAG_RST = 0x04
    TCP_FLAG_PSH = 0x08
    TCP_FLAG_ACK = 0x10

    def __init__(
        self,
        mode: int = MODE_RDT,
        seq_num: int = 0,
        ack_num: int = 0,
        flags: int = 0,
        data: bytes | str = b"",
        checksum: Optional[int] = None,
        src_port: Optional[int] = None,
        dst_port: Optional[int] = None,
        window: int = 4096,
        urgent: int = 0,
    ) -> None:
        """
        Inicializa um pacote RDT ou TCP.

        Args:
            mode (int): tipo do pacote (MODE_RDT ou MODE_TCP)
            seq_num (int): número de sequência
            ack_num (int): número de ACK
            flags (int): flags de controle
            data (bytes|str): payload do pacote
            checksum (int, opcional): checksum explícito
            src_port (int): porta de origem (TCP)
            dst_port (int): porta de destino (TCP)
            window (int): janela anunciada (TCP)
            urgent (int): ponteiro urgente (TCP)
        """
        self.mode = mode

        if isinstance(data, str):
            data = data.encode()
        self.data: bytes = data

        if mode == self.MODE_RDT:
            self.seq_num = int(seq_num)
            self.ack_num = int(ack_num)
            self.flags = int(flags)
            # se checksum for passado, usa; caso contrário calcula
            self.checksum = checksum if checksum is not None else self._calc_rdt_checksum()
        else:
            self.src_port = int(src_port) if src_port is not None else 0
            self.dst_port = int(dst_port) if dst_port is not None else 0
            self.seq_num = int(seq_num)
            self.ack_num = int(ack_num)
            self.flags = int(flags)
            self.window = int(window)
            self.urgent = int(urgent)
            # header length fixo no nosso formato simplificado
            self.hlen = 20
            self.checksum = checksum if checksum is not None else self._calc_tcp_checksum()

    # ---------------------- RDT METHODS ------------------------- #

    def _calc_rdt_checksum(self) -> int:
        """Calcula o checksum para pacotes RDT (16 bits)."""
        pseudo = (
            self.seq_num.to_bytes(4, "big")
            + self.ack_num.to_bytes(4, "big")
            + self.flags.to_bytes(1, "big")
            + self.data
        )
        return sum(pseudo) & 0xFFFF

    def _to_rdt_bytes(self) -> bytes:
        """Serializa um pacote RDT para bytes."""
        header = struct.pack(
            self.RDT_HEADER_FORMAT,
            int(self.seq_num),
            int(self.ack_num),
            int(self.flags),
            int(self.checksum),
        )
        return header + self.data

    @classmethod
    def _from_rdt_bytes(cls, raw: bytes) -> "Packet":
        """Constrói um pacote RDT a partir de bytes."""
        if len(raw) < cls.RDT_HEADER_SIZE:
            raise ValueError("Raw too small for RDT header")
        header = raw[: cls.RDT_HEADER_SIZE]
        seq_num, ack_num, flags, checksum = struct.unpack(cls.RDT_HEADER_FORMAT, header)
        data = raw[cls.RDT_HEADER_SIZE :]
        return cls(
            mode=cls.MODE_RDT,
            seq_num=seq_num,
            ack_num=ack_num,
            flags=flags,
            data=data,
            checksum=checksum,
        )

    # ---------------------- TCP METHODS ------------------------- #

    def _tcp_pseudo_header(self) -> bytes:
        """Retorna o pseudo-cabeçalho para cálculo do checksum TCP."""
        return (
            self.src_port.to_bytes(2, "big")
            + self.dst_port.to_bytes(2, "big")
            + self.seq_num.to_bytes(4, "big")
            + self.ack_num.to_bytes(4, "big")
            + self.hlen.to_bytes(1, "big")
            + self.flags.to_bytes(1, "big")
            + self.window.to_bytes(2, "big")
            + self.urgent.to_bytes(2, "big")
            + self.data
        )

    def _calc_tcp_checksum(self) -> int:
        """Calcula o checksum para segmento TCP simplificado (16 bits)."""
        return sum(self._tcp_pseudo_header()) & 0xFFFF

    def _to_tcp_bytes(self) -> bytes:
        """Serializa um segmento TCP simplificado."""
        header = struct.pack(
            self.TCP_HEADER_FORMAT,
            int(self.src_port),
            int(self.dst_port),
            int(self.seq_num),
            int(self.ack_num),
            int(self.hlen),
            int(self.flags),
            int(self.window),
            int(self.checksum),
            int(self.urgent),
        )
        return header + self.data

    @classmethod
    def _from_tcp_bytes(cls, raw: bytes) -> "Packet":
        """Constrói um segmento TCP simplificado a partir de bytes."""
        if len(raw) < cls.TCP_HEADER_SIZE:
            raise ValueError("Raw too small for TCP header")
        header = raw[: cls.TCP_HEADER_SIZE]
        parsed = struct.unpack(cls.TCP_HEADER_FORMAT, header)
        src_port, dst_port, seq_num, ack_num, hlen, flags, window, checksum, urgent = parsed
        data = raw[cls.TCP_HEADER_SIZE :]
        pkt = cls(
            mode=cls.MODE_TCP,
            src_port=src_port,
            dst_port=dst_port,
            seq_num=seq_num,
            ack_num=ack_num,
            flags=flags,
            window=window,
            urgent=urgent,
            data=data,
            checksum=checksum,
        )
        # hlen informado no header pode não corresponder ao que criamos; ajustar para visibilidade
        pkt.hlen = int(hlen)
        return pkt

    # ---------------------- PUBLIC API -------------------------- #

    def to_bytes(self) -> bytes:
        """Serializa o pacote automaticamente conforme o modo."""
        return self._to_rdt_bytes() if self.mode == self.MODE_RDT else self._to_tcp_bytes()

    @classmethod
    def from_bytes(cls, raw: bytes) -> "Packet":
        """
        Desserializa automaticamente detectando RDT ou TCP.

        Estratégia:
            - Se `raw` tem tamanho >= TCP_HEADER_SIZE, tentamos parsear como TCP.
            - Depois de parsed as TCP, validamos `hlen` e checksum TCP. Se ambos válidos,
              retornamos o TCP.
            - Caso contrário (checksum inválido ou hlen inesperado), fazemos fallback para RDT.
            - Isso evita interpretar um grande pacote RDT (com payload) como TCP inválido.
        """
        # Tentar interpretar como TCP (quando possível)
        if len(raw) >= cls.TCP_HEADER_SIZE:
            try:
                pkt_tcp = cls._from_tcp_bytes(raw)
                # hlen plausível? (no nosso formato simplificado esperamos 20)
                if getattr(pkt_tcp, "hlen", None) == 20:
                    # se checksum TCP casa, é TCP legítimo
                    if not pkt_tcp.is_corrupt():
                        return pkt_tcp
                    # caso checksum não case, continuar e tentar RDT
                # se hlen não bate, caímos para RDT
            except Exception:
                # parsing TCP falhou: fallback para RDT
                pass

        # fallback para RDT
        return cls._from_rdt_bytes(raw)

    def is_corrupt(self) -> bool:
        """Retorna True se o checksum não bater."""
        expected = self._calc_rdt_checksum() if self.mode == self.MODE_RDT else self._calc_tcp_checksum()
        return int(self.checksum) != int(expected)

    # ------------------- PACOTES AUXILIARES -------------------- #

    @classmethod
    def make_data(cls, seq_num: int, data: bytes | str) -> "Packet":
        """Cria um pacote RDT de dados."""
        return cls(mode=cls.MODE_RDT, seq_num=seq_num, flags=cls.FLAG_DATA, data=data)

    @classmethod
    def make_ack(cls, ack_num: int) -> "Packet":
        """Cria um pacote RDT de ACK."""
        return cls(mode=cls.MODE_RDT, ack_num=ack_num, flags=cls.FLAG_ACK, data=b"")

    @classmethod
    def make_nak(cls, seq_num: int) -> "Packet":
        """Cria um pacote RDT de NAK (reconhecimento negativo).

        Usado pelo RDT 2.0 para solicitar retransmissão quando um pacote
        corrompido é recebido. O campo ``seq_num`` indica o número de
        sequência esperado (que *não* foi entregue corretamente).
        """
        return cls(mode=cls.MODE_RDT, seq_num=seq_num, flags=0, data=b"")

    @classmethod
    def make_tcp(
        cls, src_port: int, dst_port: int, seq: int, ack: int, flags: int, window: int = 4096, data: bytes | str = b""
    ) -> "Packet":
        """Cria um segmento TCP simplificado."""
        return cls(
            mode=cls.MODE_TCP,
            src_port=src_port,
            dst_port=dst_port,
            seq_num=seq,
            ack_num=ack,
            flags=flags,
            window=window,
            data=data,
        )
        
    # ---------------------- DESCRIÇÃO HUMANA ------------------- #
    
    def describe(self) -> str:
        """Retorna uma descrição legível do pacote para logs/debug."""
        if self.mode == self.MODE_RDT:
            flag_names = []
            if self.flags & self.FLAG_DATA: flag_names.append("DATA")
            if self.flags & self.FLAG_ACK:  flag_names.append("ACK")
            if self.flags & self.FLAG_SYN:  flag_names.append("SYN")
            if self.flags & self.FLAG_FIN:  flag_names.append("FIN")

            flags_str = "|".join(flag_names) if flag_names else "NONE"
            return f"RDT[{flags_str}] seq={self.seq_num} ack={self.ack_num} len={len(self.data)}"

        # TCP
        flag_names = []
        if self.flags & self.TCP_FLAG_SYN: flag_names.append("SYN")
        if self.flags & self.TCP_FLAG_ACK: flag_names.append("ACK")
        if self.flags & self.TCP_FLAG_FIN: flag_names.append("FIN")
        if self.flags & self.TCP_FLAG_PSH: flag_names.append("PSH")
        if self.flags & self.TCP_FLAG_RST: flag_names.append("RST")

        flags_str = "|".join(flag_names) if flag_names else "NONE"
        return (
            f"TCP[{flags_str}] src={self.src_port} dst={self.dst_port} "
            f"seq={self.seq_num} ack={self.ack_num} win={self.window} len={len(self.data)}"
        )


    # --------------------- REPRESENTAÇÃO ----------------------- #

    def __repr__(self):
        if self.mode == self.MODE_RDT:
            return f"Packet[RDT]({self.describe()}, checksum={self.checksum}, data_len={len(self.data)})"

        return (
            "Packet[TCP]({desc}, checksum={cs})"
        ).format(
            desc=self.describe(),
            cs=self.checksum,
        )