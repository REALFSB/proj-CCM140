const { encode, decode } = require("@msgpack/msgpack");

// Timestamp de envio: milissegundos desde a epoch (inteiro)
const nowMs = () => Date.now();

// Exibe o conteúdo completo da mensagem enviada/recebida
function log(who, direction, obj) {
  const d = new Date();
  const ts = d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0");
  console.log(`[${ts}] ${who.padEnd(12)} ${direction.padEnd(9)} ${JSON.stringify(obj)}`);
}

module.exports = { encode, decode, nowMs, log };
