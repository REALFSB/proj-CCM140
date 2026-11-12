"""Serviço de referência: rank, lista de servidores, heartbeat e hora correta.

Só conversa com servidores (REP em :5559). Mantém o rank por ordem de cadastro, sem repetir
nomes, e remove da lista o servidor que ficar sem enviar heartbeat por HEARTBEAT_TIMEOUT segundos.
"""
import os
import re
import time

import zmq

from common import clock, log, now_ms, pack, unpack

NAME = "reference"
BIND = os.environ.get("REFERENCE_BIND", "tcp://*:5559")
TIMEOUT_S = float(os.environ.get("HEARTBEAT_TIMEOUT", "30"))
VALID_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")

ranks = {}      # nome -> rank (mantido mesmo após remoção: quem volta recupera o mesmo rank)
last_seen = {}  # nome -> instante (monotonic) do último heartbeat; só os servidores disponíveis
next_rank = 1


def reply(service, status, description="", **extra):
    data = {"status": status, "description": description, "timestamp": now_ms(), "clock": clock.tick()}
    data.update(extra)
    return {"service": service, "data": data}


def register(name):
    global next_rank
    if name not in ranks:
        ranks[name] = next_rank
        next_rank += 1
    last_seen[name] = time.monotonic()
    return ranks[name]


def prune():
    now = time.monotonic()
    for name in [n for n, t in last_seen.items() if now - t > TIMEOUT_S]:
        del last_seen[name]
        log(NAME, "REMOVIDO", {"server": name, "rank": ranks[name],
                               "description": f"sem heartbeat há mais de {TIMEOUT_S:g}s"})


def handle(req):
    if not isinstance(req, dict):
        return reply("desconhecido", "erro", "mensagem inválida")
    service = req.get("service")
    data = req.get("data") or {}
    clock.update(data.get("clock"))

    if service in ("rank", "heartbeat"):
        name = data.get("name")
        if not isinstance(name, str) or not VALID_NAME.match(name):
            return reply(service, "erro", "nome de servidor inválido")
        rank = register(name)  # heartbeat de servidor desconhecido/removido também o cadastra
        if service == "rank":
            return reply(service, "sucesso", "", rank=rank)
        return reply(service, "sucesso", "OK", time=now_ms())  # hora correta, para o ajuste do relógio

    if service == "list":
        prune()
        servers = sorted(({"name": n, "rank": ranks[n]} for n in last_seen), key=lambda s: s["rank"])
        return reply(service, "sucesso", "", servers=servers)

    return reply(str(service), "erro", "serviço desconhecido")


def main():
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REP)
    sock.bind(BIND)
    log(NAME, "INFO", {"description": f"escutando em {BIND}", "heartbeat_timeout_s": TIMEOUT_S})
    while True:
        prune()
        if not sock.poll(1000):
            continue
        raw = sock.recv()
        try:
            req = unpack(raw)
            log(NAME, "RECEBIDO", req)
            rep = handle(req)
        except Exception as exc:
            rep = reply("desconhecido", "erro", f"falha ao processar: {exc}")
        log(NAME, "ENVIADO", rep)
        sock.send(pack(rep))


if __name__ == "__main__":
    main()
