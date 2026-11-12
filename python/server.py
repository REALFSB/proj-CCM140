import json
import os
import re

import zmq

from common import log, now_ms, pack, unpack

NAME = os.environ.get("SERVER_NAME", "server_py")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
DATA_DIR = os.environ.get("DATA_DIR", "/data")

VALID_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
RESERVED_CHANNELS = {"servers"}  # tópico usado na eleição (Parte 4)


class Storage:
    """Persistência em JSON Lines: uma linha por evento (login ou canal)."""

    def __init__(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.channels = []
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if rec.get("type") == "channel" and rec["name"] not in self.channels:
                        self.channels.append(rec["name"])
        self.file = open(path, "a", encoding="utf-8")

    def append(self, rec):
        self.file.write(json.dumps(rec, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())


def reply(service, status, description="", **extra):
    data = {"status": status, "description": description, "timestamp": now_ms(), "server": NAME}
    data.update(extra)
    return {"service": service, "data": data}


def handle(req, store):
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
        if not isinstance(name, str) or not VALID_NAME.match(name):
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

    return reply(str(service), "erro", "serviço desconhecido")


def main():
    store = Storage(os.path.join(DATA_DIR, f"{NAME}.jsonl"))
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)
    log(NAME, "INFO", {"description": f"conectado a {BROKER}", "canais_carregados": len(store.channels)})

    while True:
        raw = sock.recv()
        try:
            req = unpack(raw)
            log(NAME, "RECEBIDO", req)
            rep = handle(req, store)
        except Exception as exc:  # sempre responde, para não travar o REP
            rep = reply("desconhecido", "erro", f"falha ao processar: {exc}")
        log(NAME, "ENVIADO", rep)
        sock.send(pack(rep))


if __name__ == "__main__":
    main()
