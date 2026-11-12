import os
import random
import time

import zmq

from common import fmt_time, log, now_ms, pack, unpack

NAME = os.environ.get("BOT_NAME", "bot_py")
BROKER = os.environ.get("BROKER_FRONTEND", "tcp://broker:5555")
PROXY_XPUB = os.environ.get("PROXY_XPUB", "tcp://proxy:5558")
FETCH_HISTORY = os.environ.get("FETCH_HISTORY", "0") == "1"  # opcional: pede o histórico ao se inscrever
TIMEOUT_MS = 5000
MIN_CHANNELS = 5
MIN_SUBSCRIPTIONS = 3
MESSAGES_PER_ROUND = 10
INTERVAL_S = 1.0
PHRASES = ["olá pessoal", "alguém aí?", "bom dia", "tudo bem?", "testando o canal",
           "mensagem aleatória", "sistemas distribuídos é legal", "ZeroMQ funcionando"]

ctx = zmq.Context()
sub = ctx.socket(zmq.SUB)
sub.connect(PROXY_XPUB)
subscribed = set()
poller = zmq.Poller()
poller.register(sub, zmq.POLLIN)
counter = 0


def drain(timeout_ms=0):
    """Exibe as publicações recebidas. Espera até timeout_ms pela primeira, depois só as já disponíveis."""
    while dict(poller.poll(timeout_ms)).get(sub):
        topic, raw = sub.recv_multipart()
        received = now_ms()
        channel = topic.decode()
        timeout_ms = 0
        if channel not in subscribed:  # o filtro do SUB é por prefixo: descarta falsos positivos
            continue
        d = unpack(raw)["data"]
        log(NAME, "MENSAGEM", f"#{channel} {d['user']}: {d['message']}  "
                              f"(enviado {fmt_time(d['timestamp'])} | recebido {fmt_time(received)})")


def pause(seconds):
    """Dorme recebendo publicações (um único thread cuida do socket SUB)."""
    end = time.monotonic() + seconds
    while (remaining := end - time.monotonic()) > 0:
        drain(int(remaining * 1000))


def request(service, data):
    """REQ com timeout: se não houver resposta, recria o socket e reenvia."""
    msg = {"service": service, "data": {**data, "timestamp": now_ms()}}
    payload = pack(msg)
    while True:
        sock = ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.LINGER, 0)
        sock.setsockopt(zmq.RCVTIMEO, TIMEOUT_MS)
        sock.connect(BROKER)
        try:
            log(NAME, "ENVIADO", msg)
            sock.send(payload)
            rep = unpack(sock.recv())
            log(NAME, "RECEBIDO", rep)
            return rep
        except zmq.Again:
            log(NAME, "AVISO", {"description": "sem resposta, tentando novamente"})
        finally:
            sock.close()


def login():
    user = NAME
    while True:
        rep = request("login", {"user": user})
        if rep["data"]["status"] == "sucesso":
            return user
        user = f"{NAME}_{random.randint(1000, 9999)}"  # erro: repete o login com outro nome
        time.sleep(1)


def list_channels(user):
    return request("channels", {"user": user})["data"].get("channels", [])


def subscribe(user, channel):
    sub.setsockopt_string(zmq.SUBSCRIBE, channel)
    subscribed.add(channel)
    log(NAME, "INSCRITO", f"#{channel}")
    if FETCH_HISTORY:
        request("history", {"user": user, "channel": channel})


def random_message():
    global counter
    counter += 1
    return f"{random.choice(PHRASES)} (#{counter})"


def main():
    user = login()

    channels = list_channels(user)
    if len(channels) < MIN_CHANNELS:
        request("channel", {"user": user, "channel": f"canal_{random.randint(1000, 9999)}"})
        channels = list_channels(user)

    while True:
        channels = list_channels(user)
        if not channels:
            pause(1)
            continue
        # a cada rodada, se houver menos de 3 inscrições, assina mais um canal
        # (repetir é necessário: sem replicação, a lista pode vir de um servidor que não conhece os canais)
        if len(subscribed) < MIN_SUBSCRIPTIONS:
            candidates = [c for c in channels if c not in subscribed]
            if candidates:
                subscribe(user, random.choice(candidates))
        channel = random.choice(channels)
        for _ in range(MESSAGES_PER_ROUND):
            request("publish", {"user": user, "channel": channel, "message": random_message()})
            pause(INTERVAL_S)


if __name__ == "__main__":
    main()
