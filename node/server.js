const fs = require("fs");
const path = require("path");
const zmq = require("zeromq");
const { encode, decode, nowMs, log } = require("./common");

const NAME = process.env.SERVER_NAME || "server_js";
const BROKER = process.env.BROKER_BACKEND || "tcp://broker:5556";
const DATA_DIR = process.env.DATA_DIR || "/data";

const VALID_NAME = /^[A-Za-z0-9_-]{1,32}$/;
const RESERVED_CHANNELS = new Set(["servers"]); // tópico usado na eleição (Parte 4)

// Persistência em JSON Lines: uma linha por evento (login ou canal)
class Storage {
  constructor(file) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.channels = [];
    if (fs.existsSync(file)) {
      for (const line of fs.readFileSync(file, "utf-8").split("\n")) {
        try {
          const rec = JSON.parse(line);
          if (rec.type === "channel" && !this.channels.includes(rec.name)) this.channels.push(rec.name);
        } catch (_) { /* linha vazia/inválida */ }
      }
    }
    this.fd = fs.openSync(file, "a");
  }

  append(rec) {
    fs.writeSync(this.fd, JSON.stringify(rec) + "\n");
    fs.fsyncSync(this.fd);
  }
}

function reply(service, status, description = "", extra = {}) {
  return { service, data: { status, description, timestamp: nowMs(), server: NAME, ...extra } };
}

function handle(req, store) {
  if (req === null || typeof req !== "object") return reply("desconhecido", "erro", "mensagem inválida");
  const service = req.service;
  const data = req.data || {};

  if (service === "login") {
    const user = data.user;
    if (typeof user !== "string" || !VALID_NAME.test(user))
      return reply(service, "erro", "nome de usuário inválido (use 1-32 letras, números, _ ou -)");
    store.append({ type: "login", user, timestamp: data.timestamp, received_at: nowMs() });
    return reply(service, "sucesso", "login realizado");
  }

  if (service === "channel") {
    const name = data.channel;
    if (typeof name !== "string" || !VALID_NAME.test(name))
      return reply(service, "erro", "nome de canal inválido (use 1-32 letras, números, _ ou -)");
    if (RESERVED_CHANNELS.has(name)) return reply(service, "erro", "nome de canal reservado");
    if (store.channels.includes(name)) return reply(service, "erro", "canal já existe");
    store.channels.push(name);
    store.append({ type: "channel", name, user: data.user, timestamp: data.timestamp, received_at: nowMs() });
    return reply(service, "sucesso", "canal criado");
  }

  if (service === "channels") return reply(service, "sucesso", "", { channels: [...store.channels] });

  return reply(String(service), "erro", "serviço desconhecido");
}

async function main() {
  const store = new Storage(path.join(DATA_DIR, `${NAME}.jsonl`));
  const sock = new zmq.Reply();
  sock.connect(BROKER);
  log(NAME, "INFO", { description: `conectado a ${BROKER}`, canais_carregados: store.channels.length });

  for await (const [raw] of sock) {
    let rep;
    try {
      const req = decode(raw);
      log(NAME, "RECEBIDO", req);
      rep = handle(req, store);
    } catch (err) { // sempre responde, para não travar o Reply
      rep = reply("desconhecido", "erro", `falha ao processar: ${err.message}`);
    }
    log(NAME, "ENVIADO", rep);
    await sock.send(encode(rep));
  }
}

main();
