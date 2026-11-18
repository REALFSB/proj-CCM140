const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode, clock, nowMs, log } = require("./common");

const NAME = process.env.SERVER_NAME || "server_js";
const BROKER = process.env.BROKER_BACKEND || "tcp://broker:5556";
const PROXY_XSUB = process.env.PROXY_XSUB || "tcp://proxy:5557";
const PROXY_XPUB = process.env.PROXY_XPUB || "tcp://proxy:5558";
const REFERENCE = process.env.REFERENCE_ADDR || "tcp://reference:5559";
const PEER_BIND = process.env.PEER_BIND || "tcp://0.0.0.0:6000"; // REQ/REP entre servidores (relógio e eleição)
// Endereço dos outros servidores: tcp://<nome>:6000 na rede do Compose (PEER_MAP permite sobrescrever)
const PEER_MAP = Object.fromEntries(
  (process.env.PEER_MAP || "").split(",").filter(Boolean).map((kv) => [kv.slice(0, kv.indexOf("=")), kv.slice(kv.indexOf("=") + 1)]));
const DATA_DIR = process.env.DATA_DIR || "/data";

const VALID_NAME = /^[A-Za-z0-9_-]{1,32}$/;
const RESERVED_CHANNELS = new Set(["servers"]); // tópico usado na eleição (Parte 4)
const MAX_MESSAGE_LEN = 500;
const HEARTBEAT_EVERY = 10;  // mensagens de clientes recebidas entre heartbeats
const CLOCK_SYNC_EVERY = 15; // mensagens de clientes recebidas entre sincronizações do relógio
const REF_TIMEOUT_MS = 3000;
const PEER_TIMEOUT_MS = 2000;
const STALE_MS = 60000;      // relógio reportado há mais que isso não entra na média de Berkeley
const TOPIC = "servers";     // tópico Pub/Sub do anúncio do coordenador
// Replicação (Parte 5). O "$" não é permitido em nomes de canal: nenhum canal colide com este tópico
// nem recebe este tráfego por casamento de prefixo.
const REPL_TOPIC = "$replication";
const ANTI_ENTROPY_S = Number(process.env.ANTI_ENTROPY_S || 10); // intervalo da sincronização periódica
const GAP_SYNC_S = 3;        // intervalo mínimo entre sincronizações quando há lacunas conhecidas
const SYNC_LIMIT = 500;      // eventos por resposta de sincronização
const EVENT_TYPES = ["login", "channel", "message"];

// estado da eleição: S.coordinator é a variável com o nome do coordenador eleito
const S = { rank: null, coordinator: null, coordinatorRank: null, servers: [], lastSync: 0, syncIdx: -1 };
const net = { store: null };
const reports = new Map(); // só no coordenador: nome -> { time, at } (hora reportada, relógio local bruto)
let electing = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Ordem total dos eventos em todos os servidores: relógio lógico, depois origem e sequência
const cmpEvents = (a, b) =>
  (a.clock - b.clock) || (a.origin < b.origin ? -1 : a.origin > b.origin ? 1 : 0) || (a.seq - b.seq);

/*
 * Log de eventos replicado, persistido em JSON Lines (uma linha por evento).
 * Todo evento (login, canal ou mensagem) tem um ID único (origin, seq): `origin` é o servidor que o
 * criou e `seq` um contador desse servidor. Receber o mesmo evento duas vezes não tem efeito.
 * `vv` (vetor de versões) guarda, por origem, o maior seq sem lacunas já recebido.
 */
class Storage {
  constructor(file, name) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.name = name;
    this.events = {};          // origem -> { seq: evento }
    this.vv = {};              // origem -> maior seq contíguo recebido
    this.maxSeq = {};          // origem -> maior seq recebido (maior que vv => há lacuna)
    this.channels = new Map(); // nome do canal -> evento de criação (prevalece o menor)
    this.messages = [];        // eventos de mensagem
    this.ownSeq = 0;
    let legacy = 0;
    if (fs.existsSync(file)) {
      for (const line of fs.readFileSync(file, "utf-8").split("\n")) {
        let rec;
        try { rec = JSON.parse(line); } catch (_) { continue; }
        if (rec === null || typeof rec !== "object" || !EVENT_TYPES.includes(rec.type)) continue;
        if (rec.origin === undefined || rec.seq === undefined) { // registro de uma versão sem replicação
          legacy += 1;
          rec = { ...rec, origin: name, seq: legacy };
        }
        rec.clock = rec.clock || 0;
        rec.timestamp = rec.timestamp || 0;
        this._apply(rec);
      }
    }
    this.fd = fs.openSync(file, "a");
  }

  _apply(ev) {
    const { origin, seq } = ev;
    const evs = (this.events[origin] = this.events[origin] || {});
    if (evs[seq]) return false; // duplicado
    evs[seq] = ev;
    this.maxSeq[origin] = Math.max(this.maxSeq[origin] || 0, seq);
    let v = this.vv[origin] || 0;
    while (evs[v + 1]) v += 1;
    this.vv[origin] = v;
    if (origin === this.name) this.ownSeq = Math.max(this.ownSeq, seq);
    if (ev.type === "channel") {
      const cur = this.channels.get(ev.name);
      if (!cur || cmpEvents(ev, cur) < 0) this.channels.set(ev.name, ev); // mesmo nome = mesmo canal; o mais antigo prevalece
    } else if (ev.type === "message") {
      this.messages.push(ev);
    }
    return true;
  }

  // Aplica um evento (próprio ou replicado) e o grava em disco. false se já existia
  add(ev) {
    if (!this._apply(ev)) return false;
    fs.writeSync(this.fd, JSON.stringify(ev) + "\n");
    fs.fsyncSync(this.fd);
    return true;
  }

  hasGaps() {
    return Object.keys(this.maxSeq).some((o) => this.maxSeq[o] > (this.vv[o] || 0));
  }

  // Eventos que o solicitante (com vetor de versões `vv`) ainda não tem, até `limit`
  eventsAfter(vv, limit) {
    const out = [];
    for (const origin of Object.keys(this.events)) {
      const known = Number.isInteger(vv[origin]) ? vv[origin] : 0;
      const evs = this.events[origin];
      for (const seq of Object.keys(evs).map(Number).filter((q) => q > known).sort((a, b) => a - b)) {
        if (out.length >= limit) return { events: out, more: true };
        out.push(evs[seq]);
      }
    }
    return { events: out, more: false };
  }

  channelNames() {
    return [...this.channels.values()].sort(cmpEvents).map((e) => e.name);
  }

  channelMessages(channel, since) {
    return this.messages
      .filter((m) => m.channel === channel && (since == null || m.timestamp > since))
      .sort(cmpEvents)
      .map((m) => ({ channel: m.channel, user: m.user, message: m.message, timestamp: m.timestamp, clock: m.clock, origin: m.origin }));
  }

  total() {
    return Object.values(this.events).reduce((n, evs) => n + Object.keys(evs).length, 0);
  }
}

// Valida um evento recebido de outro servidor
function validEvent(ev) {
  if (ev === null || typeof ev !== "object") return false;
  if (typeof ev.origin !== "string" || !["seq", "clock", "timestamp"].every((k) => Number.isInteger(ev[k])) || ev.seq < 1) return false;
  const str = (...ks) => ks.every((k) => typeof ev[k] === "string");
  if (ev.type === "login") return str("user");
  if (ev.type === "channel") return str("name", "user");
  if (ev.type === "message") return str("channel", "user", "message");
  return false;
}

// Para o log: não imprime as listas grandes de eventos das respostas de sincronização
function compact(msg) {
  const d = msg && msg.data;
  if (d && Array.isArray(d.events)) return { ...msg, data: { ...d, events: `<${d.events.length} eventos>` } };
  return msg;
}

// Cria um evento próprio, grava em disco e o replica para os outros servidores
async function emit(store, pub, type, fields) {
  const ev = { origin: NAME, seq: store.ownSeq + 1, type, clock: clock.tick(), timestamp: nowMs(), ...fields };
  store.add(ev); // síncrono: ninguém mais pega este seq
  const payload = { service: "replicate", data: { event: ev, timestamp: nowMs(), clock: clock.tick(), server: NAME } };
  await pub.send([Buffer.from(REPL_TOPIC), encode(payload)]);
  log(NAME, "REPLICA->", payload);
  return ev;
}

function reply(service, status, description = "", extra = {}) {
  return { service, data: { status, description, timestamp: nowMs(), clock: clock.tick(), server: NAME, ...extra } };
}

const validChannel = (n) => typeof n === "string" && VALID_NAME.test(n);

async function handle(req, store, pub) {
  if (req === null || typeof req !== "object") return reply("desconhecido", "erro", "mensagem inválida");
  const service = req.service;
  const data = req.data || {};
  clock.update(data.clock); // relógio lógico: max(local, recebido)

  if (service === "login") {
    const user = data.user;
    if (typeof user !== "string" || !VALID_NAME.test(user))
      return reply(service, "erro", "nome de usuário inválido (use 1-32 letras, números, _ ou -)");
    await emit(store, pub, "login", { user, origin_timestamp: data.timestamp });
    return reply(service, "sucesso", "login realizado");
  }

  if (service === "channel") {
    const name = data.channel;
    if (!validChannel(name)) return reply(service, "erro", "nome de canal inválido (use 1-32 letras, números, _ ou -)");
    if (RESERVED_CHANNELS.has(name)) return reply(service, "erro", "nome de canal reservado");
    if (store.channels.has(name)) return reply(service, "erro", "canal já existe");
    await emit(store, pub, "channel", { name, user: data.user || "", origin_timestamp: data.timestamp });
    return reply(service, "sucesso", "canal criado");
  }

  if (service === "channels") return reply(service, "sucesso", "", { channels: store.channelNames() });

  if (service === "publish") {
    const { user, channel, message } = data;
    if (typeof user !== "string" || !VALID_NAME.test(user)) return reply(service, "erro", "usuário inválido");
    if (!validChannel(channel) || RESERVED_CHANNELS.has(channel)) return reply(service, "erro", "canal inválido");
    if (typeof message !== "string" || !message.trim() || message.length > MAX_MESSAGE_LEN)
      return reply(service, "erro", `mensagem vazia ou maior que ${MAX_MESSAGE_LEN} caracteres`);
    // grava e replica o evento antes de publicar; só o servidor que recebeu o pedido publica no canal
    const ev = await emit(store, pub, "message", { channel, user, message, origin_timestamp: data.timestamp });
    const payload = {
      service: "publication",
      data: { channel, user, message, timestamp: ev.timestamp, clock: clock.tick(), server: NAME },
    };
    await pub.send([Buffer.from(channel), encode(payload)]); // tópico = nome do canal
    log(NAME, "PUBLICOU", { topic: channel, ...payload });
    return reply(service, "sucesso", "mensagem publicada");
  }

  if (service === "history") {
    const { channel, since } = data;
    if (!validChannel(channel)) return reply(service, "erro", "canal inválido");
    if (since != null && !Number.isInteger(since)) return reply(service, "erro", "'since' deve ser um timestamp inteiro");
    return reply(service, "sucesso", "", { channel, messages: store.channelMessages(channel, since) });
  }

  return reply(String(service), "erro", "serviço desconhecido");
}

// ---------- comunicação com o serviço de referência ----------
// REQ ao serviço de referência. Devolve { rep, t0, t1 } ou null se não responder.
async function refCall(service) {
  const msg = { service, data: { name: NAME, timestamp: nowMs(), clock: clock.tick() } };
  const sock = new zmq.Request({ linger: 0, receiveTimeout: REF_TIMEOUT_MS });
  sock.connect(REFERENCE);
  try {
    log(NAME, "ENV->REF", msg);
    const t0 = clock.rawMs();
    await sock.send(encode(msg));
    const [raw] = await sock.receive();
    const t1 = clock.rawMs();
    const rep = decode(raw);
    clock.update(rep.data.clock);
    log(NAME, "REC<-REF", rep);
    return { rep, t0, t1 };
  } catch (err) {
    log(NAME, "AVISO", { description: "serviço de referência não respondeu", erro: err.code || err.message });
    return null;
  } finally {
    sock.close();
  }
}

// Pede o rank ao iniciar; repete até o serviço de referência responder
async function register() {
  for (;;) {
    const res = await refCall("rank");
    if (res && res.rep.data.status === "sucesso") return res.rep.data.rank;
    await sleep(2000);
  }
}

// Lista de servidores disponíveis segundo a referência (usa a última lista se ela não responder)
async function referenceServers() {
  const res = await refCall("list");
  if (res && res.rep.data.status === "sucesso") S.servers = res.rep.data.servers;
  return S.servers;
}

// Avisa que está vivo (a resposta é só OK) e atualiza a lista de servidores
async function heartbeat() {
  await refCall("heartbeat");
  const servers = (await referenceServers()).map((x) => `${x.name} (rank ${x.rank})`).join(", ");
  log(NAME, "SERVIDORES", servers || "nenhum");
}

// ---------- comunicação entre servidores ----------
// REQ a outro servidor. Devolve { rep, t0, t1 } ou null se não responder
async function peerCall(name, service, extra = {}) {
  const msg = { service, data: { name: NAME, rank: S.rank, timestamp: nowMs(), clock: clock.tick(), ...extra } };
  const sock = new zmq.Request({ linger: 0, receiveTimeout: PEER_TIMEOUT_MS });
  sock.connect(PEER_MAP[name] || `tcp://${name}:6000`);
  try {
    log(NAME, "ENV->SRV", compact({ para: name, ...msg }));
    const t0 = clock.rawMs();
    await sock.send(encode(msg));
    const [raw] = await sock.receive();
    const t1 = clock.rawMs();
    const rep = decode(raw);
    clock.update(rep.data.clock);
    log(NAME, "REC<-SRV", compact({ de: name, ...rep }));
    return { rep, t0, t1 };
  } catch (err) {
    log(NAME, "AVISO", { description: `${name} não respondeu`, erro: err.code || err.message });
    return null;
  } finally {
    sock.close();
  }
}

function setCoordinator(name, rank, reason = "") {
  if (S.coordinator !== name) {
    reports.clear(); // a tabela de Berkeley só vale para quem é coordenador
    log(NAME, "COORD", `coordenador = ${name} (rank ${rank})` + (reason ? ` - ${reason}` : ""));
  }
  S.coordinator = name;
  S.coordinatorRank = rank;
}

// Avisa todos os servidores, publicando no tópico `servers`, que este servidor é o coordenador
async function announce(pub) {
  const payload = { service: "coordinator", data: { coordinator: NAME, rank: S.rank, timestamp: nowMs(), clock: clock.tick() } };
  await pub.send([Buffer.from(TOPIC), encode(payload)]);
  log(NAME, "ANUNCIOU", { topic: TOPIC, ...payload });
}

// Eleição (Bully): pede eleição aos servidores de rank maior; se nenhum responder OK, vence
async function elect(pub) {
  if (electing) return;
  electing = true;
  try {
    const higher = (await referenceServers()).filter((x) => x.rank > S.rank).sort((a, b) => b.rank - a.rank);
    log(NAME, "ELEICAO", `iniciada (rank ${S.rank}); ranks maiores: ${higher.length ? higher.map((x) => x.name).join(", ") : "nenhum"}`);
    let alive = null;
    for (const x of higher) {
      const res = await peerCall(x.name, "election");
      if (res && res.rep.data.status === "OK" && alive === null) alive = x; // o de maior rank que respondeu assume e anuncia
    }
    if (alive) {
      setCoordinator(alive.name, alive.rank, "provisório: servidor de rank maior respondeu OK");
    } else {
      setCoordinator(NAME, S.rank, "venci a eleição");
      await announce(pub);
    }
  } finally {
    electing = false;
  }
}

// Berkeley no coordenador: média dos relógios conhecidos (o seu e os reportados), que ele também adota
function berkeley(name, reported) {
  const now = clock.nowMs();
  const raw = clock.rawMs();
  reports.set(name, { time: reported, at: raw });
  const times = [now];
  for (const [other, r] of [...reports]) {
    const age = raw - r.at;
    if (other !== name && age > STALE_MS) { reports.delete(other); continue; }
    times.push(r.time + age); // estima o relógio atual do servidor pelo tempo decorrido desde o relato
  }
  const avg = Math.round(times.reduce((a, b) => a + b, 0) / times.length);
  const delta = avg - now;
  clock.adjust(delta);
  reports.set(name, { time: avg, at: raw }); // após a resposta, o servidor adota a média
  const sign = (n) => (n >= 0 ? "+" : "") + n;
  log(NAME, "BERKELEY", `média de ${times.length} relógios = ${avg}; ajuste do coordenador ${sign(delta)} ms (offset total ${sign(clock.offsetMs)} ms)`);
  return avg;
}

// Pede a hora correta ao coordenador; se ele não responder, pede eleição e tenta de novo
async function syncClock(pub) {
  if (S.coordinator === null) await elect(pub);
  for (let attempt = 0; attempt < 2; attempt++) {
    if (S.coordinator === NAME || S.coordinator === null) return; // o coordenador ajusta o próprio relógio ao atender os outros
    const target = S.coordinator; // guarda o alvo: o coordenador pode mudar durante a espera
    const res = await peerCall(target, "clock", { time: nowMs() });
    if (!res) { // coordenador indisponível
      log(NAME, "AVISO", { description: `coordenador ${target} indisponível: pedindo eleição` });
      await elect(pub);
      continue;
    }
    const d = res.rep.data;
    if (d.status === "sucesso") {
      const offset = clock.sync(d.time, res.t0, res.t1);
      log(NAME, "RELOGIO", `relógio ajustado pelo coordenador ${target}: offset total ${offset >= 0 ? "+" : ""}${offset} ms (rtt ${res.t1 - res.t0} ms)`);
      return;
    }
    // o servidor consultado não é o coordenador: adota o que ele indicou
    if (d.coordinator) setCoordinator(d.coordinator, null, "indicado por outro servidor");
    else { S.coordinator = null; await elect(pub); }
  }
}

// Requisições de outros servidores. Devolve { rep, after } (after roda depois de responder)
function handlePeer(req, pub) {
  if (req === null || typeof req !== "object") return { rep: reply("desconhecido", "erro", "mensagem inválida") };
  const service = req.service;
  const data = req.data || {};
  clock.update(data.clock);

  if (service === "election") {
    // resposta OK; quem tem rank maior que o solicitante assume e inicia a própria eleição
    const lower = Number.isInteger(data.rank) && data.rank < S.rank;
    return { rep: reply(service, "OK", "", { coordinator: S.coordinator }), after: lower ? () => elect(pub) : null };
  }

  if (service === "clock") {
    if (S.coordinator !== NAME) return { rep: reply(service, "erro", "não sou o coordenador", { coordinator: S.coordinator }) };
    if (!Number.isInteger(data.time) || typeof data.name !== "string") return { rep: reply(service, "erro", "requisição inválida") };
    return { rep: reply(service, "sucesso", "", { time: berkeley(data.name, data.time) }) };
  }

  if (service === "sync") {
    const { vv, limit } = data;
    if (vv === null || typeof vv !== "object" || Array.isArray(vv) || !Number.isInteger(limit) || limit < 1)
      return { rep: reply(service, "erro", "requisição inválida") };
    const { events, more } = net.store.eventsAfter(vv, Math.min(limit, 1000));
    return { rep: reply(service, "sucesso", "", { events, more }) };
  }

  return { rep: reply(String(service), "erro", "serviço desconhecido") };
}

async function peerLoop(peer, pub) {
  for await (const [raw] of peer) {
    let rep, after = null;
    try {
      const req = decode(raw);
      log(NAME, "REQ<-SRV", compact(req));
      ({ rep, after } = handlePeer(req, pub));
    } catch (err) {
      rep = reply("desconhecido", "erro", `falha ao processar: ${err.message}`);
    }
    log(NAME, "REP->SRV", compact(rep));
    await peer.send(encode(rep));
    if (after) after().catch((e) => log(NAME, "AVISO", { description: `falha na eleição: ${e.message}` }));
  }
}

function onAnnouncement(raw, pub) {
  const msg = decode(raw);
  const d = msg.data;
  clock.update(d.clock);
  log(NAME, "ANUNCIO", msg);
  if (d.coordinator === NAME) return;
  if (Number.isInteger(d.rank) && d.rank < S.rank) {
    elect(pub).catch(() => {}); // um servidor de rank menor se anunciou: o de rank maior assume
  } else {
    setCoordinator(d.coordinator, d.rank, "anunciado no tópico servers");
  }
}

// ---------- replicação ----------
function onReplication(raw) {
  const msg = decode(raw);
  const d = msg.data;
  clock.update(d.clock);
  if (d.server === NAME) return; // eco do próprio evento
  if (!validEvent(d.event)) { log(NAME, "AVISO", { description: "evento replicado inválido", evento: d.event }); return; }
  const novo = net.store.add(d.event);
  log(NAME, "REPLICA<-", { ...msg, novo });
}

// Anti-entropia: pede a `name` os eventos que este servidor ainda não tem (envia seu vetor de versões)
async function syncWith(name) {
  let totalNew = 0;
  for (;;) {
    const res = await peerCall(name, "sync", { vv: { ...net.store.vv }, limit: SYNC_LIMIT });
    if (!res || res.rep.data.status !== "sucesso") break;
    const data = res.rep.data;
    let added = 0;
    for (const ev of data.events || []) if (validEvent(ev) && net.store.add(ev)) added += 1;
    totalNew += added;
    if (!data.more || added === 0) break; // sem progresso: evita repetir a mesma página
  }
  if (totalNew) log(NAME, "SYNC", `${totalNew} eventos novos recebidos de ${name} (total local: ${net.store.total()})`);
  return totalNew;
}

async function syncAll() {
  for (const x of await referenceServers()) if (x.name !== NAME) await syncWith(x.name);
}

// Sincronização periódica com um servidor (ou com todos, se houver lacunas), mesmo sem tráfego
let syncing = false;
async function maybeAntiEntropy() {
  if (syncing) return;
  const gaps = net.store.hasGaps();
  if (Date.now() - S.lastSync < (gaps ? GAP_SYNC_S : ANTI_ENTROPY_S) * 1000) return;
  S.lastSync = Date.now();
  const peers = S.servers.filter((x) => x.name !== NAME).map((x) => x.name);
  if (!peers.length) return;
  syncing = true;
  try {
    let targets = peers;
    if (!gaps) { S.syncIdx = (S.syncIdx + 1) % peers.length; targets = [peers[S.syncIdx]]; }
    for (const t of targets) await syncWith(t);
  } finally {
    syncing = false;
  }
}

async function subLoop(sub, pub) {
  for await (const [topic, raw] of sub) {
    const t = topic.toString();
    if (t !== TOPIC && t !== REPL_TOPIC) continue; // o filtro do SUB é por prefixo: exige o tópico exato
    try {
      if (t === TOPIC) onAnnouncement(raw, pub); else onReplication(raw);
    } catch (err) { log(NAME, "AVISO", { description: `mensagem inválida em ${t}: ${err.message}` }); }
  }
}

async function main() {
  const store = new Storage(path.join(DATA_DIR, `${NAME}.jsonl`), NAME);
  net.store = store;
  const pub = new zmq.Publisher();
  pub.connect(PROXY_XSUB);

  S.rank = await register();
  log(NAME, "INFO", {
    description: `broker ${BROKER} | proxy ${PROXY_XSUB}/${PROXY_XPUB} | referência ${REFERENCE}`,
    rank: S.rank,
    eventos_carregados: store.total(),
    canais_carregados: store.channels.size,
    mensagens_carregadas: store.messages.length,
  });
  await heartbeat();

  // canal entre servidores (relógio/eleição) e assinatura do tópico `servers`
  const peer = new zmq.Reply();
  await peer.bind(PEER_BIND);
  const sub = new zmq.Subscriber();
  sub.connect(PROXY_XPUB);
  sub.subscribe(TOPIC);
  sub.subscribe(REPL_TOPIC);
  peerLoop(peer, pub);
  subLoop(sub, pub);

  await sleep(1000); // dá tempo para a assinatura chegar ao proxy
  await elect(pub);  // eleição ao iniciar: o de maior rank vence
  await syncAll();       // replicação: obtém dos outros servidores tudo o que perdeu enquanto esteve fora
  await syncClock(pub);  // primeira sincronização do relógio, antes de atender clientes
  S.lastSync = Date.now() - Math.random() * ANTI_ENTROPY_S * 1000; // escalona a anti-entropia entre servidores
  setInterval(() => maybeAntiEntropy().catch((e) => log(NAME, "AVISO", { description: `falha na sincronização: ${e.message}` })), 1000);
  log(NAME, "INFO", { description: "pronto para atender clientes", eventos: store.total(), canais: store.channels.size, mensagens: store.messages.length });

  const sock = new zmq.Reply();
  sock.connect(BROKER); // só entra no balanceamento depois de registrado e sincronizado
  let handled = 0;
  for await (const [raw] of sock) {
    let rep;
    try {
      const req = decode(raw);
      log(NAME, "RECEBIDO", req);
      rep = await handle(req, store, pub);
    } catch (err) { // sempre responde, para não travar o Reply
      rep = reply("desconhecido", "erro", `falha ao processar: ${err.message}`);
    }
    log(NAME, "ENVIADO", rep);
    await sock.send(encode(rep));
    handled += 1;
    if (handled % HEARTBEAT_EVERY === 0) await heartbeat();
    if (handled % CLOCK_SYNC_EVERY === 0) await syncClock(pub);
  }
}

main();
