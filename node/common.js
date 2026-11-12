const { encode, decode } = require("@msgpack/msgpack");

// Timestamp de envio: milissegundos desde a epoch (inteiro)
const nowMs = () => Date.now();

// Converte timestamp em ms para HH:MM:SS.mmm (apenas para exibição)
function fmtTime(ms) {
  const d = new Date(ms);
  return d.toTimeString().slice(0, 8) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

// Exibe o conteúdo completo da mensagem enviada/recebida (objeto ou texto já formatado)
function log(who, direction, obj) {
  const text = typeof obj === "string" ? obj : JSON.stringify(obj);
  console.log(`[${fmtTime(Date.now())}] ${who.padEnd(12)} ${direction.padEnd(9)} ${text}`);
}

module.exports = { encode, decode, nowMs, fmtTime, log };
