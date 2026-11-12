const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode, clock, nowMs, log } = require("./common");

const NAME = process.env.SERVER_NAME || "server_js";
const BROKER = process.env.BROKER_BACKEND || "tcp://broker:5556";
const PROXY_XSUB = process.env.PROXY_XSUB || "tcp://proxy:5557";
const REFERENCE = process.env.REFERENCE_ADDR || "tcp://reference:5559";
const DATA_DIR = process.env.DATA_DIR || "/data";

const VALID_NAME = /^[A-Za-z0-9_-]{1,32}$/;
const RESERVED_CHANNELS = new Set(["servers"]); // tópico usado na eleição (Parte 4)
const MAX_MESSAGE_LEN = 500;
const HEARTBEAT_EVERY = 10; // mensagens de clientes recebidas entre heartbeats
const REF_TIMEOUT_MS = 3000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Persistência em JSON Lines: uma linha por evento (login, canal ou mensagem)
class Storage {
  constructor(file) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.channels = [];
    this.messages = [];
    if (fs.existsSync(file)) {
      for (const line of fs.readFileSync(file, "utf-8").split("\n")) {
        try {
          const rec = JSON.parse(line);
          if (rec.type === "channel" && !this.channels.includes(rec.name)) this.channels.push(rec.name);
          else if (rec.type === "message") {
            const { channel, user, message, timestamp, clock } = rec;
            this.messages.push({ channel, user, message, timestamp, clock });
          }
        } catch (_) { /* linha vazia/inválida */ }
      }
    }
    this.fd = fs.openSync(file, "a");
  }

  append(rec) {
    fs.writeSync(this.fd, JSON.stringify(rec) + "\n");
    fs.fsyncSync(this.fd);
  }

  addMessage(msg, originTimestamp) {
    this.messages.push(msg);
    this.append({ type: "message", ...msg, origin_timestamp: originTimestamp });
  }
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
    store.append({ type: "login", user, timestamp: data.timestamp, received_at: nowMs() });
    return reply(service, "sucesso", "login realizado");
  }

  if (service === "channel") {
    const name = data.channel;
    if (!validChannel(name)) return reply(service, "erro", "nome de canal inválido (use 1-32 letras, números, _ ou -)");
    if (RESERVED_CHANNELS.has(name)) return reply(service, "erro", "nome de canal reservado");
    if (store.channels.includes(name)) return reply(service, "erro", "canal já existe");
    store.channels.push(name);
    store.append({ type: "channel", name, user: data.user, timestamp: data.timestamp, received_at: nowMs() });
    return reply(service, "sucesso", "canal criado");
  }

  if (service === "channels") return reply(service, "sucesso", "", { channels: [...store.channels] });

  if (service === "publish") {
    const { user, channel, message } = data;
    if (typeof user !== "string" || !VALID_NAME.test(user)) return reply(service, "erro", "usuário inválido");
    if (!validChannel(channel) || RESERVED_CHANNELS.has(channel)) return reply(service, "erro", "canal inválido");
    if (typeof message !== "string" || !message.trim() || message.length > MAX_MESSAGE_LEN)
      return reply(service, "erro", `mensagem vazia ou maior que ${MAX_MESSAGE_LEN} caracteres`);
    // tick: a publicação também é um envio
    const msg = { channel, user, message, timestamp: nowMs(), clock: clock.tick() };
    store.addMessage(msg, data.timestamp); // grava antes de publicar
    const payload = { service: "publication", data: { ...msg, server: NAME } };
    await pub.send([Buffer.from(channel), encode(payload)]); // tópico = nome do canal
    log(NAME, "PUBLICOU", { topic: channel, ...payload });
    return reply(service, "sucesso", "mensagem publicada");
  }

  if (service === "history") {
    const { channel, since } = data;
    if (!validChannel(channel)) return reply(service, "erro", "canal inválido");
    if (since != null && !Number.isInteger(since)) return reply(service, "erro", "'since' deve ser um timestamp inteiro");
    const messages = store.messages.filter((m) => m.channel === channel && (since == null || m.timestamp > since));
    return reply(service, "sucesso", "", { channel, messages });
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

// Avisa que está vivo, ajusta o relógio físico com a hora recebida e atualiza a lista de servidores
async function heartbeat() {
  let res = await refCall("heartbeat");
  if (res && res.rep.data.status === "sucesso") {
    const offset = clock.sync(res.rep.data.time, res.t0, res.t1);
    log(NAME, "RELOGIO", `relógio físico ajustado: offset ${offset >= 0 ? "+" : ""}${offset} ms (rtt ${res.t1 - res.t0} ms)`);
  }
  res = await refCall("list");
  if (res && res.rep.data.status === "sucesso") {
    const servers = res.rep.data.servers.map((s) => `${s.name} (rank ${s.rank})`).join(", ");
    log(NAME, "SERVIDORES", servers || "nenhum");
  }
}

async function main() {
  const store = new Storage(path.join(DATA_DIR, `${NAME}.jsonl`));
  const pub = new zmq.Publisher();
  pub.connect(PROXY_XSUB);

  const rank = await register();
  log(NAME, "INFO", {
    description: `broker ${BROKER} | proxy ${PROXY_XSUB} | referência ${REFERENCE}`,
    rank,
    canais_carregados: store.channels.length,
    mensagens_carregadas: store.messages.length,
  });
  await heartbeat(); // sincroniza o relógio antes de atender clientes

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
  }
}

main();
