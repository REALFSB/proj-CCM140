import os
import random
import time

import zmq

from common import log, now_ms, pack, unpack

NAME = os.environ.get("BOT_NAME", "bot_py")
BROKER = os.environ.get("BROKER_FRONTEND", "tcp://broker:5555")
TIMEOUT_MS = 5000
MIN_CHANNELS = 5

ctx = zmq.Context()


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


def main():
    user = login()
    while True:
        channels = request("channels", {"user": user})["data"]["channels"]
        if len(channels) < MIN_CHANNELS:
            request("channel", {"user": user, "channel": f"canal_{random.randint(1000, 9999)}"})
        time.sleep(3)


if __name__ == "__main__":
    main()
