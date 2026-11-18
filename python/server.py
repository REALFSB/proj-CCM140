import json
import os
import random
import re
import time
import types

import zmq

from common import clock, log, now_ms, pack, unpack

NAME = os.environ.get("SERVER_NAME", "server_py")
BROKER = os.environ.get("BROKER_BACKEND", "tcp://broker:5556")
PROXY_XSUB = os.environ.get("PROXY_XSUB", "tcp://proxy:5557")
PROXY_XPUB = os.environ.get("PROXY_XPUB", "tcp://proxy:5558")
REFERENCE = os.environ.get("REFERENCE_ADDR", "tcp://reference:5559")
PEER_BIND = os.environ.get("PEER_BIND", "tcp://*:6000")  # REQ/REP entre servidores (relógio e eleição)
# Endereço dos outros servidores: tcp://<nome>:6000 na rede do Compose (PEER_MAP permite sobrescrever)
PEER_MAP = dict(kv.split("=", 1) for kv in os.environ.get("PEER_MAP", "").split(",") if kv)
DATA_DIR = os.environ.get("DATA_DIR", "/data")

VALID_NAME = re.compile(r"^[A-Za-z0-9_-]{1,32}$")
RESERVED_CHANNELS = {"servers"}  # tópico usado na eleição (Parte 4)
MAX_MESSAGE_LEN = 500
HEARTBEAT_EVERY = 10    # mensagens de clientes recebidas entre heartbeats
CLOCK_SYNC_EVERY = 15   # mensagens de clientes recebidas entre sincronizações do relógio
REF_TIMEOUT_MS = 3000
PEER_TIMEOUT_MS = 2000
STALE_MS = 60000        # relógio reportado há mais que isso não entra na média de Berkeley
TOPIC = b"servers"      # tópico Pub/Sub do anúncio do coordenador
# Replicação (Parte 5). O "$" não é permitido em nomes de canal: nenhum canal colide com este tópico
# nem recebe este tráfego por casamento de prefixo.
REPL_TOPIC = b"$replication"
ANTI_ENTROPY_S = float(os.environ.get("ANTI_ENTROPY_S", "10"))  # intervalo da sincronização periódica
GAP_SYNC_S = 3          # intervalo mínimo entre sincronizações quando há lacunas conhecidas
SYNC_LIMIT = 500        # eventos por resposta de sincronização
EVENT_TYPES = ("login", "channel", "message")

ctx = zmq.Context()
# estado da eleição: S.coordinator é a variável com o nome do coordenador eleito
S = types.SimpleNamespace(rank=None, coordinator=None, coordinator_rank=None, servers=[],
                          election_requested=False, last_sync=0.0, sync_idx=-1)
reports = {}  # só no coordenador: nome -> (hora reportada, relógio local bruto no momento)
net = types.SimpleNamespace(pub=None, peer=None, sub=None, store=None)


def event_key(ev):
    """Ordem total dos eventos em todos os servidores: relógio lógico, depois origem e sequência."""
    return (ev["clock"], ev["origin"], ev["seq"])


class Storage:
    """Log de eventos replicado, persistido em JSON Lines (uma linha por evento).

    Todo evento (login, canal ou mensagem) tem um ID único (origem, seq): `origin` é o servidor que o
    criou e `seq` um contador desse servidor. Receber o mesmo evento duas vezes não tem efeito.
    `vv` (vetor de versões) guarda, por origem, o maior seq sem lacunas já recebido.
    """

    def __init__(self, path, name):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.name = name
        self.events = {}     # origem -> {seq: evento}
        self.vv = {}         # origem -> maior seq contíguo recebido
        self.max_seq = {}    # origem -> maior seq recebido (maior que vv => há lacuna)
        self.channels = {}   # nome do canal -> evento de criação (prevalece o menor event_key)
        self.messages = []   # eventos de mensagem
        self.own_seq = 0
        legacy = 0
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except ValueError:
                        continue
                    if not isinstance(rec, dict) or rec.get("type") not in EVENT_TYPES:
                        continue
                    if "origin" not in rec or "seq" not in rec:  # registro de uma versão sem replicação
                        legacy += 1
                        rec = {**rec, "origin": name, "seq": legacy}
                    rec["clock"] = rec.get("clock") or 0
                    rec["timestamp"] = rec.get("timestamp") or 0
                    self._apply(rec)
        self.file = open(path, "a", encoding="utf-8")

    def _apply(self, ev):
        origin, seq = ev["origin"], ev["seq"]
        evs = self.events.setdefault(origin, {})
        if seq in evs:
            return False  # duplicado
        evs[seq] = ev
        self.max_seq[origin] = max(self.max_seq.get(origin, 0), seq)
        v = self.vv.get(origin, 0)
        while v + 1 in evs:
            v += 1
        self.vv[origin] = v
        if origin == self.name:
            self.own_seq = max(self.own_seq, seq)
        if ev["type"] == "channel":
            current = self.channels.get(ev["name"])
            if current is None or event_key(ev) < event_key(current):
                self.channels[ev["name"]] = ev  # mesmo nome = mesmo canal; o evento mais antigo prevalece
        elif ev["type"] == "message":
            self.messages.append(ev)
        return True

    def add(self, ev):
        """Aplica um evento (próprio ou replicado) e o grava em disco. False se já existia."""
        if not self._apply(ev):
            return False
        self.file.write(json.dumps(ev, ensure_ascii=False) + "\n")
        self.file.flush()
        os.fsync(self.file.fileno())
        return True

    def has_gaps(self):
        return any(self.max_seq[o] > self.vv.get(o, 0) for o in self.max_seq)

    def events_after(self, vv, limit):
        """Eventos que o solicitante (com vetor de versões `vv`) ainda não tem, até `limit`."""
        out = []
        for origin, evs in self.events.items():
            known = vv.get(origin, 0)
            for seq in sorted(q for q in evs if q > known):
                if len(out) >= limit:
                    return out, True
                out.append(evs[seq])
        return out, False

    def channel_names(self):
        return [e["name"] for e in sorted(self.channels.values(), key=event_key)]

    def channel_messages(self, channel, since=None):
        sel = [m for m in self.messages
               if m["channel"] == channel and (since is None or m["timestamp"] > since)]
        return [{"channel": m["channel"], "user": m["user"], "message": m["message"],
                 "timestamp": m["timestamp"], "clock": m["clock"], "origin": m["origin"]}
                for m in sorted(sel, key=event_key)]

    def total(self):
        return sum(len(e) for e in self.events.values())


def valid_event(ev):
    """Valida um evento recebido de outro servidor."""
    if not isinstance(ev, dict):
        return False
    ints = all(isinstance(ev.get(k), int) and not isinstance(ev.get(k), bool) for k in ("seq", "clock", "timestamp"))
    if not (isinstance(ev.get("origin"), str) and ints and ev["seq"] >= 1):
        return False
    t = ev.get("type")
    if t == "login":
        return isinstance(ev.get("user"), str)
    if t == "channel":
        return isinstance(ev.get("name"), str) and isinstance(ev.get("user"), str)
    if t == "message":
        return all(isinstance(ev.get(k), str) for k in ("channel", "user", "message"))
    return False


def compact(msg):
    """Para o log: não imprime as listas grandes de eventos das respostas de sincronização."""
    d = msg.get("data") if isinstance(msg, dict) else None
    if isinstance(d, dict) and isinstance(d.get("events"), list):
        return {**msg, "data": {**d, "events": f"<{len(d['events'])} eventos>"}}
    return msg


def emit(store, pub, etype, **fields):
    """Cria um evento próprio, grava em disco e o replica para os outros servidores."""
    ev = {"origin": NAME, "seq": store.own_seq + 1, "type": etype,
          "clock": clock.tick(), "timestamp": now_ms(), **fields}
    store.add(ev)
    payload = {"service": "replicate",
               "data": {"event": ev, "timestamp": now_ms(), "clock": clock.tick(), "server": NAME}}
    pub.send_multipart([REPL_TOPIC, pack(payload)])
    log(NAME, "REPLICA->", payload)
    return ev


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
        emit(store, pub, "login", user=user, origin_timestamp=data.get("timestamp"))
        return reply(service, "sucesso", "login realizado")

    if service == "channel":
        name = data.get("channel")
        if not valid_channel(name):
            return reply(service, "erro", "nome de canal inválido (use 1-32 letras, números, _ ou -)")
        if name in RESERVED_CHANNELS:
            return reply(service, "erro", "nome de canal reservado")
        if name in store.channels:
            return reply(service, "erro", "canal já existe")
        emit(store, pub, "channel", name=name, user=data.get("user") or "", origin_timestamp=data.get("timestamp"))
        return reply(service, "sucesso", "canal criado")

    if service == "channels":
        return reply(service, "sucesso", "", channels=store.channel_names())

    if service == "publish":
        user, channel, message = data.get("user"), data.get("channel"), data.get("message")
        if not isinstance(user, str) or not VALID_NAME.match(user):
            return reply(service, "erro", "usuário inválido")
        if not valid_channel(channel) or channel in RESERVED_CHANNELS:
            return reply(service, "erro", "canal inválido")
        if not isinstance(message, str) or not message.strip() or len(message) > MAX_MESSAGE_LEN:
            return reply(service, "erro", f"mensagem vazia ou maior que {MAX_MESSAGE_LEN} caracteres")
        # grava e replica o evento antes de publicar; só o servidor que recebeu o pedido publica no canal
        ev = emit(store, pub, "message", channel=channel, user=user, message=message,
                  origin_timestamp=data.get("timestamp"))
        payload = {"service": "publication",
                   "data": {"channel": channel, "user": user, "message": message,
                            "timestamp": ev["timestamp"], "clock": clock.tick(), "server": NAME}}
        pub.send_multipart([channel.encode(), pack(payload)])  # tópico = nome do canal
        log(NAME, "PUBLICOU", {"topic": channel, **payload})
        return reply(service, "sucesso", "mensagem publicada")

    if service == "history":
        channel, since = data.get("channel"), data.get("since")
        if not valid_channel(channel):
            return reply(service, "erro", "canal inválido")
        if since is not None and not isinstance(since, int):
            return reply(service, "erro", "'since' deve ser um timestamp inteiro")
        return reply(service, "sucesso", "", channel=channel, messages=store.channel_messages(channel, since))

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


def reference_servers():
    """Lista de servidores disponíveis segundo a referência (usa a última lista se ela não responder)."""
    res = ref_call("list")
    if res and res[0]["data"].get("status") == "sucesso":
        S.servers = res[0]["data"]["servers"]
    return S.servers


def heartbeat():
    """Avisa que está vivo (a resposta é só OK) e atualiza a lista de servidores."""
    ref_call("heartbeat")
    servers = ", ".join(f"{s['name']} (rank {s['rank']})" for s in reference_servers())
    log(NAME, "SERVIDORES", servers or "nenhum")


# ---------- comunicação entre servidores ----------
def peer_call(name, service, **extra):
    """REQ a outro servidor. Devolve (resposta, t0, t1) ou None se não responder.

    Enquanto espera a resposta, continua atendendo os pedidos que outros servidores fazem a este
    (senão dois servidores que se consultam ao mesmo tempo ficariam esperando um ao outro até o timeout).
    """
    msg = {"service": service, "data": {"name": NAME, "rank": S.rank, "timestamp": now_ms(),
                                        "clock": clock.tick(), **extra}}
    sock = ctx.socket(zmq.REQ)
    sock.setsockopt(zmq.LINGER, 0)
    sock.connect(PEER_MAP.get(name, f"tcp://{name}:6000"))
    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)
    if net.peer is not None:
        poller.register(net.peer, zmq.POLLIN)
    try:
        log(NAME, "ENV->SRV", compact({"para": name, **msg}))
        t0 = clock.raw_ms()
        sock.send(pack(msg))
        deadline = time.monotonic() + PEER_TIMEOUT_MS / 1000
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                log(NAME, "AVISO", {"description": f"{name} não respondeu"})
                return None
            events = dict(poller.poll(int(remaining * 1000) + 1))
            if net.peer in events:
                serve_peer()
            if sock in events:
                break
        rep = unpack(sock.recv())
        t1 = clock.raw_ms()
        clock.update(rep["data"].get("clock"))
        log(NAME, "REC<-SRV", compact({"de": name, **rep}))
        return rep, t0, t1
    finally:
        sock.close()


def set_coordinator(name, rank, reason=""):
    if S.coordinator != name:
        reports.clear()  # a tabela de Berkeley só vale para quem é coordenador
        log(NAME, "COORD", f"coordenador = {name} (rank {rank})" + (f" - {reason}" if reason else ""))
    S.coordinator, S.coordinator_rank = name, rank


def announce():
    """Avisa todos os servidores, publicando no tópico `servers`, que este servidor é o coordenador."""
    payload = {"service": "coordinator", "data": {"coordinator": NAME, "rank": S.rank,
                                                  "timestamp": now_ms(), "clock": clock.tick()}}
    net.pub.send_multipart([TOPIC, pack(payload)])
    log(NAME, "ANUNCIOU", {"topic": "servers", **payload})


def elect():
    """Eleição (Bully): pede eleição aos servidores de rank maior; se nenhum responder OK, vence."""
    higher = sorted((s for s in reference_servers() if s["rank"] > S.rank), key=lambda s: -s["rank"])
    log(NAME, "ELEICAO", f"iniciada (rank {S.rank}); ranks maiores: {[s['name'] for s in higher] or 'nenhum'}")
    alive = None
    for s in higher:
        res = peer_call(s["name"], "election")
        if res and res[0]["data"].get("status") == "OK" and alive is None:
            alive = s  # o de maior rank que respondeu: ele assume e anuncia
    if alive:
        set_coordinator(alive["name"], alive["rank"], "provisório: servidor de rank maior respondeu OK")
    else:
        set_coordinator(NAME, S.rank, "venci a eleição")
        announce()


def berkeley(name, reported):
    """Berkeley no coordenador: média dos relógios conhecidos (o seu e os reportados), que ele também adota."""
    now, raw = clock.now_ms(), clock.raw_ms()
    reports[name] = (reported, raw)
    times = [now]
    for other, (t, at) in list(reports.items()):
        age = raw - at
        if other != name and age > STALE_MS:
            del reports[other]
            continue
        times.append(t + age)  # estima o relógio atual do servidor pelo tempo decorrido desde o relato
    avg = round(sum(times) / len(times))
    delta = avg - now
    clock.adjust(delta)
    reports[name] = (avg, raw)  # após a resposta, o servidor adota a média
    log(NAME, "BERKELEY", f"média de {len(times)} relógios = {avg}; ajuste do coordenador {delta:+d} ms "
                          f"(offset total {clock.offset_ms:+d} ms)")
    return avg


def sync_clock():
    """Pede a hora correta ao coordenador; se ele não responder, pede eleição e tenta de novo."""
    if S.coordinator is None:
        elect()
    for _ in range(2):
        if S.coordinator == NAME:
            return  # o coordenador ajusta o próprio relógio ao atender os outros
        res = peer_call(S.coordinator, "clock", time=clock.now_ms())
        if res is None:  # coordenador indisponível
            log(NAME, "AVISO", {"description": f"coordenador {S.coordinator} indisponível: pedindo eleição"})
            elect()
            continue
        rep, t0, t1 = res
        if rep["data"].get("status") == "sucesso":
            offset = clock.sync(rep["data"]["time"], t0, t1)
            log(NAME, "RELOGIO", f"relógio ajustado pelo coordenador {S.coordinator}: "
                                 f"offset total {offset:+d} ms (rtt {t1 - t0} ms)")
            return
        # o servidor consultado não é o coordenador: adota o que ele indicou
        indicated = rep["data"].get("coordinator")
        if indicated:
            set_coordinator(indicated, None, "indicado por outro servidor")
        else:
            S.coordinator = None
            elect()


def handle_peer(req):
    """Requisições de outros servidores. Devolve (resposta, ação a executar depois de responder)."""
    if not isinstance(req, dict):
        return reply("desconhecido", "erro", "mensagem inválida"), None
    service = req.get("service")
    data = req.get("data") or {}
    clock.update(data.get("clock"))

    if service == "election":
        # resposta OK; quem tem rank maior que o solicitante assume e inicia a própria eleição
        lower = isinstance(data.get("rank"), int) and data["rank"] < S.rank
        return reply(service, "OK", "", coordinator=S.coordinator), (elect if lower else None)

    if service == "clock":
        if S.coordinator != NAME:
            return reply(service, "erro", "não sou o coordenador", coordinator=S.coordinator), None
        if not isinstance(data.get("time"), int) or not isinstance(data.get("name"), str):
            return reply(service, "erro", "requisição inválida"), None
        return reply(service, "sucesso", "", time=berkeley(data["name"], data["time"])), None

    if service == "sync":
        vv, limit = data.get("vv"), data.get("limit")
        if not isinstance(vv, dict) or not isinstance(limit, int) or limit < 1:
            return reply(service, "erro", "requisição inválida"), None
        events, more = net.store.events_after(vv, min(limit, 1000))
        return reply(service, "sucesso", "", events=events, more=more), None

    return reply(str(service), "erro", "serviço desconhecido"), None


def serve_peer():
    raw = net.peer.recv()
    after = None
    try:
        req = unpack(raw)
        log(NAME, "REQ<-SRV", compact(req))
        rep, after = handle_peer(req)
    except Exception as exc:
        rep = reply("desconhecido", "erro", f"falha ao processar: {exc}")
    log(NAME, "REP->SRV", compact(rep))
    net.peer.send(pack(rep))
    if after:
        S.election_requested = True  # a eleição roda no laço principal, depois de responder


def on_announcement(raw):
    msg = unpack(raw)
    data = msg["data"]
    clock.update(data.get("clock"))
    log(NAME, "ANUNCIO", msg)
    name, rank = data.get("coordinator"), data.get("rank")
    if name == NAME:
        return
    if isinstance(rank, int) and rank < S.rank:
        elect()  # um servidor de rank menor se anunciou: o de rank maior assume
    else:
        set_coordinator(name, rank, "anunciado no tópico servers")


# ---------- replicação ----------
def on_replication(raw):
    msg = unpack(raw)
    data = msg["data"]
    clock.update(data.get("clock"))
    if data.get("server") == NAME:
        return  # eco do próprio evento
    ev = data.get("event")
    if not valid_event(ev):
        log(NAME, "AVISO", {"description": "evento replicado inválido", "evento": ev})
        return
    new = net.store.add(ev)
    log(NAME, "REPLICA<-", {**msg, "novo": new})


def sync_with(name):
    """Anti-entropia: pede a `name` os eventos que este servidor ainda não tem (envia seu vetor de versões)."""
    total_new = 0
    while True:
        res = peer_call(name, "sync", vv=dict(net.store.vv), limit=SYNC_LIMIT)
        if not res or res[0]["data"].get("status") != "sucesso":
            break
        data = res[0]["data"]
        new = sum(1 for ev in data.get("events", []) if valid_event(ev) and net.store.add(ev))
        total_new += new
        if not data.get("more") or new == 0:  # sem progresso: evita repetir a mesma página
            break
    if total_new:
        log(NAME, "SYNC", f"{total_new} eventos novos recebidos de {name} (total local: {net.store.total()})")
    return total_new


def sync_all():
    for s in reference_servers():
        if s["name"] != NAME:
            sync_with(s["name"])


def maybe_anti_entropy():
    """Sincronização periódica com um servidor (ou com todos, se houver lacunas), mesmo sem tráfego."""
    now = time.monotonic()
    gaps = net.store.has_gaps()
    if now - S.last_sync < (GAP_SYNC_S if gaps else ANTI_ENTROPY_S):
        return
    S.last_sync = now
    peers = [s["name"] for s in S.servers if s["name"] != NAME]
    if not peers:
        return
    if gaps:
        targets = peers
    else:
        S.sync_idx = (S.sync_idx + 1) % len(peers)
        targets = [peers[S.sync_idx]]
    for target in targets:
        sync_with(target)


def run_pending():
    if S.election_requested:
        S.election_requested = False
        elect()


def serve_peer_events(events):
    """Atende o tópico `servers` e as requisições de outros servidores que estiverem prontos."""
    if net.sub in events:
        while net.sub.poll(0):
            topic, raw = net.sub.recv_multipart()
            if topic in (TOPIC, REPL_TOPIC):  # o filtro do SUB é por prefixo: exige o tópico exato
                try:
                    on_announcement(raw) if topic == TOPIC else on_replication(raw)
                except Exception as exc:
                    log(NAME, "AVISO", {"description": f"mensagem inválida em {topic.decode()}: {exc}"})
    if net.peer in events:
        serve_peer()


def main():
    store = Storage(os.path.join(DATA_DIR, f"{NAME}.jsonl"), NAME)
    net.store = store
    net.pub = ctx.socket(zmq.PUB)
    net.pub.connect(PROXY_XSUB)

    S.rank = register()
    log(NAME, "INFO", {"description": f"broker {BROKER} | proxy {PROXY_XSUB}/{PROXY_XPUB} | referência {REFERENCE}",
                       "rank": S.rank, "eventos_carregados": store.total(),
                       "canais_carregados": len(store.channels), "mensagens_carregadas": len(store.messages)})
    heartbeat()

    # canal entre servidores (relógio/eleição) e assinatura do tópico `servers`
    net.peer = ctx.socket(zmq.REP)
    net.peer.bind(PEER_BIND)
    net.sub = ctx.socket(zmq.SUB)
    net.sub.connect(PROXY_XPUB)
    net.sub.setsockopt(zmq.SUBSCRIBE, TOPIC)
    net.sub.setsockopt(zmq.SUBSCRIBE, REPL_TOPIC)
    peer_poller = zmq.Poller()
    peer_poller.register(net.peer, zmq.POLLIN)
    peer_poller.register(net.sub, zmq.POLLIN)

    end = time.monotonic() + 1.0  # dá tempo para a assinatura chegar ao proxy, atendendo os outros servidores
    while time.monotonic() < end:
        serve_peer_events(dict(peer_poller.poll(100)))
    elect()       # eleição ao iniciar: o de maior rank vence
    run_pending()
    sync_all()    # replicação: obtém dos outros servidores tudo o que perdeu enquanto esteve fora
    sync_clock()  # primeira sincronização do relógio, antes de atender clientes
    S.last_sync = time.monotonic() - random.uniform(0, ANTI_ENTROPY_S)  # escalona a anti-entropia entre servidores
    log(NAME, "INFO", {"description": "pronto para atender clientes", "eventos": store.total(),
                       "canais": len(store.channels), "mensagens": len(store.messages)})

    sock = ctx.socket(zmq.REP)
    sock.connect(BROKER)  # só entra no balanceamento depois de registrado e sincronizado
    poller = zmq.Poller()
    for s in (sock, net.peer, net.sub):
        poller.register(s, zmq.POLLIN)

    handled = 0
    while True:
        events = dict(poller.poll(1000))
        serve_peer_events(events)
        run_pending()
        maybe_anti_entropy()
        if sock not in events:
            continue
        raw = sock.recv()
        try:
            req = unpack(raw)
            log(NAME, "RECEBIDO", req)
            rep = handle(req, store, net.pub)
        except Exception as exc:  # sempre responde, para não travar o REP
            rep = reply("desconhecido", "erro", f"falha ao processar: {exc}")
        log(NAME, "ENVIADO", rep)
        sock.send(pack(rep))
        handled += 1
        if handled % HEARTBEAT_EVERY == 0:
            heartbeat()
        if handled % CLOCK_SYNC_EVERY == 0:
            sync_clock()


if __name__ == "__main__":
    main()
