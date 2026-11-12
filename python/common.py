import json
import os
import time
from datetime import datetime

import msgpack


class Clock:
    """Relógio lógico (Lamport, conforme a aula) + relógio físico ajustável.

    Lógico:  tick() antes de cada envio; update(recebido) = max(local, recebido) a cada recebimento.
    Físico:  o relógio do SO não é alterado; mantemos um deslocamento (offset) que é corrigido na
             sincronização com o coordenador (Berkeley). CLOCK_SKEW_MS simula um relógio com defeito
             (só para demonstrar a sincronização; o padrão é 0).
    """

    def __init__(self):
        self.counter = 0
        self.offset_ms = 0
        self.skew_ms = int(os.environ.get("CLOCK_SKEW_MS", "0"))

    def tick(self):
        self.counter += 1
        return self.counter

    def update(self, received):
        if isinstance(received, int) and not isinstance(received, bool):
            self.counter = max(self.counter, received)

    def raw_ms(self):
        """Relógio local sem correção (inclui o defeito simulado)."""
        return int(time.time() * 1000) + self.skew_ms

    def now_ms(self):
        """Relógio local corrigido: usado em todos os timestamps de envio."""
        return self.raw_ms() + self.offset_ms

    def adjust(self, delta_ms):
        """Soma um ajuste ao deslocamento (usado pelo coordenador ao adotar a média de Berkeley)."""
        self.offset_ms += delta_ms
        return self.offset_ms

    def sync(self, ref_time, t0, t1):
        """Ajusta o offset com a hora correta recebida do coordenador, compensando metade do RTT."""
        estimate = ref_time + (t1 - t0) // 2
        self.offset_ms = estimate - t1
        return self.offset_ms


clock = Clock()


def now_ms():
    """Timestamp de envio: milissegundos desde a epoch (inteiro), pelo relógio corrigido."""
    return clock.now_ms()


def fmt_time(ms):
    """Converte timestamp em ms para HH:MM:SS.mmm (apenas para exibição)."""
    return datetime.fromtimestamp(ms / 1000).strftime("%H:%M:%S.%f")[:-3]


def pack(obj):
    return msgpack.packb(obj, use_bin_type=True)


def unpack(raw):
    return msgpack.unpackb(raw, raw=False)


def log(who, direction, obj):
    """Exibe o conteúdo completo da mensagem enviada/recebida (dict ou texto já formatado)."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    text = obj if isinstance(obj, str) else json.dumps(obj, ensure_ascii=False)
    print(f"[{ts}] {who:<12} {direction:<9} {text}", flush=True)
