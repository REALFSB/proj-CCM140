# Sistema de troca de mensagens instantâneas (estilo BBS/IRC)

Projeto de Sistemas Distribuídos. Bots (clientes) conversam com servidores usando **ZeroMQ**,
tudo executado em containers e orquestrado com Docker Compose. Nenhuma interação humana é necessária.

> **Estado atual: Parte 1** — login, listagem e criação de canais, com persistência em disco.

## Como executar

```bash
docker compose up --build
```

Sobe 1 broker, 4 servidores e 4 bots. Cada processo imprime no terminal todo o conteúdo das
mensagens que envia/recebe (`[hora] processo ENVIADO|RECEBIDO {conteúdo}`).

## Arquitetura

```
 bots (REQ) ──► broker :5555 (ROUTER) ──round-robin──► :5556 (DEALER) ◄── servidores (REP)
```

| Serviço | Linguagem | Quantidade |
|---|---|---|
| `broker` | Python | 1 |
| `server_py_1`, `server_py_2` | Python | 2 |
| `server_js_1`, `server_js_2` | JavaScript (Node.js) | 2 |
| `bot_py_1`, `bot_py_2` | Python | 2 |
| `bot_js_1`, `bot_js_2` | JavaScript (Node.js) | 2 |

Cada integrante implementou cliente e servidor na sua linguagem. Como o broker distribui
as requisições em round-robin, qualquer bot pode ser atendido por qualquer servidor,
em qualquer linguagem.

## Escolhas

**Linguagens:** Python (`pyzmq`) e JavaScript/Node.js (`zeromq` v6), ambas com bindings maduros de ZeroMQ.

**Serialização:** [MessagePack](https://msgpack.org) (`msgpack` / `@msgpack/msgpack`). É binário,
compacto e tem biblioteca nas duas linguagens. Nenhuma mensagem trafega em JSON ou texto.

**Persistência:** cada servidor grava em seu próprio arquivo `/data/<nome_do_servidor>.jsonl`
(JSON Lines, um evento por linha, com `fsync`). Cada servidor tem um volume Docker nomeado próprio,
então os dados sobrevivem à remoção do container e nunca são compartilhados entre servidores.
Ao iniciar, o servidor relê o arquivo e reconstrói a lista de canais. O formato JSON Lines é usado
só em disco, não na troca de mensagens.

Eventos gravados:
```json
{"type": "login",   "user": "bot_py_1", "timestamp": 1794443197558, "received_at": 1794443197559}
{"type": "channel", "name": "canal_6644", "user": "bot_py_1", "timestamp": 1794443197561, "received_at": 1794443197562}
```

## Formato das mensagens

Toda mensagem é um mapa MessagePack `{service, data}`, e `data.timestamp` é o instante do envio
(milissegundos desde a epoch, inteiro). Esse formato será mantido nas próximas partes.

| `service` | Requisição (`data`) | Resposta (`data`) |
|---|---|---|
| `login` | `user`, `timestamp` | `status`, `description`, `timestamp`, `server` |
| `channels` | `user`, `timestamp` | acima + `channels` (lista de nomes) |
| `channel` | `user`, `channel`, `timestamp` | `status`, `description`, `timestamp`, `server` |

`status` é `"sucesso"` ou `"erro"`; em caso de erro, `description` explica o motivo.

## Tratamento de erros

- **Login:** nome com 1 a 32 caracteres `A-Za-z0-9_-`. Se o servidor responder erro, o bot repete o
  login com um novo nome (`<nome>_<número aleatório>`). Relogar com o mesmo nome é permitido
  (o bot pode reiniciar) e cada login é registrado.
- **Canais:** mesma regra de caracteres. São recusados nomes inválidos, duplicados e o nome
  reservado `servers` (será o tópico de eleição na Parte 4).
- **Sem resposta:** o bot espera 5 s; se não houver resposta, recria o socket e reenvia.
  Isso também cobre a ordem de inicialização dos containers.

## Comportamento dos bots (Parte 1)

Login → listar canais → se houver menos de 5 canais, criar um com nome aleatório → repetir a cada 3 s.

## Limitações conhecidas

Sem replicação (Parte 5), cada servidor conhece apenas os canais criados por ele. Como o broker
usa round-robin, `channels` pode devolver listas diferentes conforme o servidor que atende, e um
nome de canal pode ser criado uma vez em cada servidor. Isso é esperado até a replicação.
