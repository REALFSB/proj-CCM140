import json
import time
from datetime import datetime

import msgpack


def now_ms():
    """Timestamp de envio: milissegundos desde a epoch (inteiro)."""
    return int(time.time() * 1000)


def pack(obj):
    return msgpack.packb(obj, use_bin_type=True)


def unpack(raw):
    return msgpack.unpackb(raw, raw=False)


def log(who, direction, obj):
    """Exibe o conteúdo completo da mensagem enviada/recebida."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"[{ts}] {who:<12} {direction:<9} {json.dumps(obj, ensure_ascii=False)}", flush=True)
