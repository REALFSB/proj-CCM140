const zmq = require("zeromq");
const { encode, decode, nowMs, log } = require("./common");

const NAME = process.env.BOT_NAME || "bot_js";
const BROKER = process.env.BROKER_FRONTEND || "tcp://broker:5555";
const TIMEOUT_MS = 5000;
const MIN_CHANNELS = 5;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rand = () => Math.floor(1000 + Math.random() * 9000);

// REQ com timeout: se não houver resposta, recria o socket e reenvia
async function request(service, data) {
  const msg = { service, data: { ...data, timestamp: nowMs() } };
  const payload = encode(msg);
  for (;;) {
    const sock = new zmq.Request({ linger: 0, receiveTimeout: TIMEOUT_MS });
    sock.connect(BROKER);
    try {
      log(NAME, "ENVIADO", msg);
      await sock.send(payload);
      const [raw] = await sock.receive();
      const rep = decode(raw);
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

async function main() {
  const user = await login();
  for (;;) {
    const rep = await request("channels", { user });
    if (rep.data.channels.length < MIN_CHANNELS) {
      await request("channel", { user, channel: `canal_${rand()}` });
    }
    await sleep(3000);
  }
}

main();
