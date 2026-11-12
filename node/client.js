const zmq = require("zeromq");
const { encode, decode, clock, nowMs, fmtTime, log } = require("./common");

const NAME = process.env.BOT_NAME || "bot_js";
const BROKER = process.env.BROKER_FRONTEND || "tcp://broker:5555";
const PROXY_XPUB = process.env.PROXY_XPUB || "tcp://proxy:5558";
const FETCH_HISTORY = process.env.FETCH_HISTORY === "1"; // opcional: pede o histórico ao se inscrever
const TIMEOUT_MS = 5000;
const MIN_CHANNELS = 5;
const MIN_SUBSCRIPTIONS = 3;
const MESSAGES_PER_ROUND = 10;
const INTERVAL_MS = 1000;
const PHRASES = ["olá pessoal", "alguém aí?", "bom dia", "tudo bem?", "testando o canal",
  "mensagem aleatória", "sistemas distribuídos é legal", "ZeroMQ funcionando"];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = () => Math.floor(1000 + Math.random() * 9000);
const pick = (arr) => arr[Math.floor(Math.random() * arr.length)];

const sub = new zmq.Subscriber();
sub.connect(PROXY_XPUB);
const subscribed = new Set();
let counter = 0;

// Exibe cada publicação recebida (roda em paralelo ao restante do bot)
(async () => {
  for await (const [topic, raw] of sub) {
    const received = nowMs();
    const channel = topic.toString();
    if (!subscribed.has(channel)) continue; // o filtro do SUB é por prefixo: descarta falsos positivos
    const d = decode(raw).data;
    clock.update(d.clock); // relógio lógico: max(local, recebido)
    log(NAME, "MENSAGEM", `#${channel} ${d.user}: ${d.message}  ` +
      `(enviado ${fmtTime(d.timestamp)} | recebido ${fmtTime(received)} | relógio ${d.clock})`);
  }
})();

// REQ com timeout: se não houver resposta, recria o socket e reenvia
async function request(service, data) {
  for (;;) {
    const msg = { service, data: { ...data, timestamp: nowMs(), clock: clock.tick() } };
    const payload = encode(msg);
    const sock = new zmq.Request({ linger: 0, receiveTimeout: TIMEOUT_MS });
    sock.connect(BROKER);
    try {
      log(NAME, "ENVIADO", msg);
      await sock.send(payload);
      const [raw] = await sock.receive();
      const rep = decode(raw);
      clock.update(rep.data.clock); // relógio lógico: max(local, recebido)
      log(NAME, "RECEBIDO", rep);
      return rep;
    } catch (err) {
      log(NAME, "AVISO", { description: "sem resposta, tentando novamente", erro: err.code || err.message });
    } finally {
      sock.close();
    }
  }
}

async function login() {
  let user = NAME;
  for (;;) {
    const rep = await request("login", { user });
    if (rep.data.status === "sucesso") return user;
    user = `${NAME}_${rand()}`; // erro: repete o login com outro nome
    await sleep(1000);
  }
}

async function listChannels(user) {
  return (await request("channels", { user })).data.channels || [];
}

async function subscribe(user, channel) {
  sub.subscribe(channel);
  subscribed.add(channel);
  log(NAME, "INSCRITO", `#${channel}`);
  if (FETCH_HISTORY) await request("history", { user, channel });
}

function randomMessage() {
  counter += 1;
  return `${pick(PHRASES)} (#${counter})`;
}

async function main() {
  const user = await login();

  let channels = await listChannels(user);
  if (channels.length < MIN_CHANNELS) {
    await request("channel", { user, channel: `canal_${rand()}` });
    channels = await listChannels(user);
  }

  for (;;) {
    channels = await listChannels(user);
    if (!channels.length) { await sleep(1000); continue; }
    // a cada rodada, se houver menos de 3 inscrições, assina mais um canal
    // (repetir é necessário: sem replicação, a lista pode vir de um servidor que não conhece os canais)
    if (subscribed.size < MIN_SUBSCRIPTIONS) {
      const candidates = channels.filter((c) => !subscribed.has(c));
      if (candidates.length) await subscribe(user, pick(candidates));
    }
    const channel = pick(channels);
    for (let i = 0; i < MESSAGES_PER_ROUND; i++) {
      await request("publish", { user, channel, message: randomMessage() });
      await sleep(INTERVAL_MS);
    }
  }
}

main();
