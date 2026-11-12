import json
import os
import re
import time

import zmq

from common import clock, log, now_ms, pack, unpack

NAME = os.environ.get("SERVER_NAME", "server_py")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PROXY_XSUB = os.environ.get("PROXY_XSUB", "tcp://proxy:5557")
REFERENCE = os.environ.get("REFERENCE_ADDR", "tcp://reference:5559")
DATA_DIR = os.environ.get("DATA_DIR", "/data")

VALID_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
RESERVED_CHANNELS = {"servers"}  # tópico usado na eleição (Parte 4)
MAX_MESSAGE_LEN = 500
HEARTBEAT_EVERY = 10   # mensagens de clientes recebidas entre heartbeats
REF_TIMEOUT_MS = 3000

ctx = zmq.Context()


class Storage:
    """Persistência em JSON Lines: uma linha por evento (login, canal ou mensagem)."""

    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.channels = []
        self.messages = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if rec.get("type") == "channel" and rec["name"] not in self.channels:
                        self.channels.append(rec["name"])
                    elif rec.get("type") == "message":
                        self.messages.append({k: rec.get(k) for k in ("channel", "user", "message", "timestamp", "clock")})
        self.file = open(path, "a", encoding="utf-8")

    def append(self, rec):
        self.file.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def add_message(self, msg, origin_timestamp):
        self.messages.append(msg)
        self.append({"type": "message", **msg, "origin_timestamp": origin_timestamp})


def reply(service, status, description="", **extra):
    data = {"status": status, "description": description, "timestamp": now_ms(),
            "clock": clock.tick(), "server": NAME}
    data.update(extra)
    return {"service": service, "data": data}


def valid_channel(name):
    return isinstance(name, str) and VALID_NAME.match(name) is not None


def handle(req, store, pub):
    if not isinstance(req, dict):
        return reply("desconhecido", "erro", "mensagem inválida")
    service = req.get("service")
    data = req.get("data") or {}
    clock.update(data.get("clock"))  # relógio lógico: max(local, recebido)

    if service == "login":
        user = data.get("user")
        if not isinstance(user, str) or not VALID_NAME.match(user):
            return reply(service, "erro", "nome de usuário inválido (use 1-32 letras, números, _ ou -)")
        store.append({"type": "login", "user": user, "timestamp": data.get("timestamp"), "received_at": now_ms()})
        return reply(service, "sucesso", "login realizado")

    if service == "channel":
        name = data.get("channel")
        if not valid_channel(name):
            return reply(service, "erro", "nome de canal inválido (use 1-32 letras, números, _ ou -)")
        if name in RESERVED_CHANNELS:
            return reply(service, "erro", "nome de canal reservado")
        if name in store.channels:
            return reply(service, "erro", "canal já existe")
        store.channels.append(name)
        store.append({"type": "channel", "name": name, "user": data.get("user"),
                      "timestamp": data.get("timestamp"), "received_at": now_ms()})
        return reply(service, "sucesso", "canal criado")

    if service == "channels":
        return reply(service, "sucesso", "", channels=list(store.channels))

    if service == "publish":
        user, channel, message = data.get("user"), data.get("channel"), data.get("message")
        if not isinstance(user, str) or not VALID_NAME.match(user):
            return reply(service, "erro", "usuário inválido")
        if not valid_channel(channel) or channel in RESERVED_CHANNELS:
            return reply(service, "erro", "canal inválido")
        if not isinstance(message, str) or not message.strip() or len(message) > MAX_MESSAGE_LEN:
            return reply(service, "erro", f"mensagem vazia ou maior que {MAX_MESSAGE_LEN} caracteres")
        msg = {"channel": channel, "user": user, "message": message,
               "timestamp": now_ms(), "clock": clock.tick()}  # tick: a publicação também é um envio
        store.add_message(msg, data.get("timestamp"))  # grava antes de publicar
        payload = {"service": "publication", "data": {**msg, "server": NAME}}
        pub.send_multipart([channel.encode(), pack(payload)])  # tópico = nome do canal
        log(NAME, "PUBLICOU", {"topic": channel, **payload})
        return reply(service, "sucesso", "mensagem publicada")

    if service == "history":
        channel, since = data.get("channel"), data.get("since")
        if not valid_channel(channel):
            return reply(service, "erro", "canal inválido")
        if since is not None and not isinstance(since, int):
            return reply(service, "erro", "'since' deve ser um timestamp inteiro")
        msgs = [m for m in store.messages
                if m["channel"] == channel and (since is None or m["timestamp"] > since)]
        return reply(service, "sucesso", "", channel=channel, messages=msgs)

    return reply(str(service), "erro", "serviço desconhecido")


# ---------- comunicação com o serviço de referência ----------
def ref_call(service):
    """REQ ao serviço de referência. Devolve (resposta, t0, t1) ou None se não responder."""
    msg = {"service": service, "data": {"name": NAME, "timestamp": now_ms(), "clock": clock.tick()}}
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.setsockopt(zmq.RCVTIMEO, REF_TIMEOUT_MS)
    sock.connect(REFERENCE)
    try:
        log(NAME, "ENV->REF", msg)
        t0 = clock.raw_ms()
        sock.send(pack(msg))
        rep = unpack(sock.recv())
        t1 = clock.raw_ms()
        clock.update(rep["data"].get("clock"))
        log(NAME, "REC<-REF", rep)
        return rep, t0, t1
    except zmq.Again:
        log(NAME, "AVISO", {"description": "serviço de referência não respondeu"})
        return None
    finally:
        sock.close()


def register():
    """Pede o rank ao iniciar; repete até o serviço de referência responder."""
    while True:
        res = ref_call("rank")
        if res and res[0]["data"].get("status") == "sucesso":
            return res[0]["data"]["rank"]
        time.sleep(2)


def heartbeat():
    """Avisa que está vivo, ajusta o relógio físico com a hora recebida e atualiza a lista de servidores."""
    res = ref_call("heartbeat")
    if res and res[0]["data"].get("status") == "sucesso":
        rep, t0, t1 = res
        offset = clock.sync(rep["data"]["time"], t0, t1)
        log(NAME, "RELOGIO", f"relógio físico ajustado: offset {offset:+d} ms (rtt {t1 - t0} ms)")
    res = ref_call("list")
    if res and res[0]["data"].get("status") == "sucesso":
        servers = ", ".join(f"{s['name']} (rank {s['rank']})" for s in res[0]["data"]["servers"])
        log(NAME, "SERVIDORES", servers or "nenhum")


def main():
    store = Storage(os.path.join(DATA_DIR, f"{NAME}.jsonl"))
    pub = ctx.socket(zmq.PUB)
    pub.connect(PROXY_XSUB)

    rank = register()
    log(NAME, "INFO", {"description": f"broker {BROKER} | proxy {PROXY_XSUB} | referência {REFERENCE}",
                       "rank": rank, "canais_carregados": len(store.channels),
                       "mensagens_carregadas": len(store.messages)})
    heartbeat()  # sincroniza o relógio antes de atender clientes

    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)  # só entra no balanceamento depois de registrado e sincronizado
    handled = 0
    while True:
        raw = sock.recv()
        try:
            req = unpack(raw)
            log(NAME, "RECEBIDO", req)
            rep = handle(req, store, pub)
        except Exception as exc:  # sempre responde, para não travar o REP
            rep = reply("desconhecido", "erro", f"falha ao processar: {exc}")
        log(NAME, "ENVIADO", rep)
        sock.send(pack(rep))
        handled += 1
        if handled % HEARTBEAT_EVERY == 0:
            heartbeat()


if __name__ == "__main__":
    main()
