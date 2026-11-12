import json
import os
import re

import zmq

from common import log, now_ms, pack, unpack

NAME = os.environ.get("SERVER_NAME", "server_py")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PROXY_XSUB = os.environ.get("PROXY_XSUB", "tcp://proxy:5557")
DATA_DIR = os.environ.get("DATA_DIR", "/data")

VALID_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
RESERVED_CHANNELS = {"servers"}  # tópico usado na eleição (Parte 4)
MAX_MESSAGE_LEN = 500


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
                        self.messages.append({k: rec[k] for k in ("channel", "user", "message", "timestamp")})
        self.file = open(path, "a", encoding="utf-8")

    def append(self, rec):
        self.file.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())

    def add_message(self, msg, origin_timestamp):
        self.messages.append(msg)
        self.append({"type": "message", **msg, "origin_timestamp": origin_timestamp})


def reply(service, status, description="", **extra):
    data = {"status": status, "description": description, "timestamp": now_ms(), "server": NAME}
    data.update(extra)
    return {"service": service, "data": data}


def valid_channel(name):
    return isinstance(name, str) and VALID_NAME.match(name) is not None


def handle(req, store, pub):
    if not isinstance(req, dict):
        return reply("desconhecido", "erro", "mensagem inválida")
    service = req.get("service")
    data = req.get("data") or {}

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
        ts = now_ms()
        msg = {"channel": channel, "user": user, "message": message, "timestamp": ts}
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


def main():
    store = Storage(os.path.join(DATA_DIR, f"{NAME}.jsonl"))
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)
    pub = ctx.socket(zmq.PUB)
    pub.connect(PROXY_XSUB)
    log(NAME, "INFO", {"description": f"broker {BROKER} | proxy {PROXY_XSUB}",
                       "canais_carregados": len(store.channels), "mensagens_carregadas": len(store.messages)})

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


if __name__ == "__main__":
    main()
