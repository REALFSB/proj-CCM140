import json
import time
from datetime import datetime

import msgpack


def now_ms():
    """Timestamp de envio: milissegundos desde a epoch (inteiro)."""
    return int(time.time() * 1000)


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
