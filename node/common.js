const { encode, decode } = require("@msgpack/msgpack");

/*
 * Relógio lógico (Lamport, conforme a aula) + relógio físico ajustável.
 * Lógico: tick() antes de cada envio; update(recebido) = max(local, recebido) a cada recebimento.
 * Físico: o relógio do SO não é alterado; mantemos um offset corrigido na sincronização com o
 *         coordenador (Berkeley). CLOCK_SKEW_MS simula um relógio com defeito (padrão 0).
 */
class Clock {
  constructor() {
    this.counter = 0;
    this.offsetMs = 0;
    this.skewMs = Number(process.env.CLOCK_SKEW_MS || 0);
  }
  tick() { return ++this.counter; }
  update(received) {
    if (Number.isInteger(received)) this.counter = Math.max(this.counter, received);
  }
  rawMs() { return Date.now() + this.skewMs; }          // relógio local sem correção
  nowMs() { return this.rawMs() + this.offsetMs; }      // relógio local corrigido
  adjust(deltaMs) {                                     // usado pelo coordenador ao adotar a média
    this.offsetMs += deltaMs;
    return this.offsetMs;
  }
  sync(refTime, t0, t1) {                               // compensa metade do RTT
    const estimate = refTime + Math.floor((t1 - t0) / 2);
    this.offsetMs = estimate - t1;
    return this.offsetMs;
  }
}
const clock = new Clock();

// Timestamp de envio: milissegundos desde a epoch (inteiro), pelo relógio corrigido
const nowMs = () => clock.nowMs();

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

module.exports = { encode, decode, clock, nowMs, fmtTime, log };
