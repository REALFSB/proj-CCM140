# Sistema de troca de mensagens instantâneas (estilo BBS/IRC)

Projeto de Sistemas Distribuídos. Bots (clientes) conversam com servidores usando **ZeroMQ**,
tudo executado em containers e orquestrado com Docker Compose. Nenhuma interação humana é necessária.

> **Estado atual: Parte 2** — login, canais, **publicação e inscrição em canais (Pub/Sub)**,
> persistência das mensagens e consulta de histórico.

## Como executar

```bash
docker compose up --build
```

Sobe 1 broker, 1 proxy, 4 servidores e 4 bots. Cada processo imprime no terminal o conteúdo das
mensagens que envia/recebe (`[hora] processo TIPO conteúdo`). Os bots imprimem as publicações
recebidas assim:

```
[12:00:01.104] bot_js_1  MENSAGEM  #canal_9198 bot_py_2: bom dia (#4)  (enviado 12:00:01.103 | recebido 12:00:01.104)
```

## Arquitetura

```
REQ/REP:  bots (REQ) ──► broker :5555 (ROUTER) ──round-robin──► :5556 (DEALER) ◄── servidores (REP)
PUB/SUB:  servidores (PUB) ──► proxy :5557 (XSUB) ──► :5558 (XPUB) ──► bots (SUB)
```

| Serviço | Linguagem | Quantidade |
|---|---|---|
| `broker` (REQ/REP) | Python | 1 |
| `proxy` (Pub/Sub) | Python | 1 |
| `server_py_1`, `server_py_2` | Python | 2 |
| `server_js_1`, `server_js_2` | JavaScript (Node.js) | 2 |
| `bot_py_1`, `bot_py_2` | Python | 2 |
| `bot_js_1`, `bot_js_2` | JavaScript (Node.js) | 2 |

Cada integrante implementou cliente e servidor na sua linguagem. Como o broker distribui as
requisições em round-robin, qualquer bot pode ser atendido por qualquer servidor, em qualquer
linguagem, e as publicações chegam a bots de ambas as linguagens.

O proxy Pub/Sub é separado do broker REQ/REP, como pedido no enunciado: os servidores se conectam
ao XSUB (5557) para publicar e os bots ao XPUB (5558) para assinar.

## Escolhas

**Linguagens:** Python (`pyzmq`) e JavaScript/Node.js (`zeromq` v6), ambas com bindings maduros de ZeroMQ.

**Serialização:** [MessagePack](https://msgpack.org) (`msgpack` / `@msgpack/msgpack`). É binário,
compacto e tem biblioteca nas duas linguagens. Nenhuma mensagem trafega em JSON ou texto,
nem as requisições/respostas, nem as publicações.

**Persistência:** cada servidor grava em seu próprio arquivo `/data/<nome_do_servidor>.jsonl`
(JSON Lines, um evento por linha, com `fsync`). Cada servidor tem um volume Docker nomeado próprio,
então os dados sobrevivem à remoção do container e nunca são compartilhados entre servidores.
Ao iniciar, o servidor relê o arquivo e reconstrói canais e mensagens. O JSON Lines é usado só em
disco, não na troca de mensagens. Cada linha contém todos os campos da mensagem, o que permite
recuperar o histórico no futuro.

```json
{"type": "login",   "user": "bot_py_1", "timestamp": 1794443197558, "received_at": 1794443197559}
{"type": "channel", "name": "canal_6644", "user": "bot_py_1", "timestamp": 1794443197561, "received_at": 1794443197562}
{"type": "message", "channel": "canal_6644", "user": "bot_js_1", "message": "bom dia (#1)", "timestamp": 1794445000554, "origin_timestamp": 1794445000553}
```

Em `message`, `timestamp` é o instante da publicação pelo servidor e `origin_timestamp` é o instante
em que o bot enviou a requisição. A mensagem é gravada **antes** de ser publicada, para que nada seja
publicado sem estar registrado.

## Formato das mensagens

Toda mensagem é um mapa MessagePack `{service, data}`, e `data.timestamp` é o instante do envio
(milissegundos desde a epoch, inteiro).

### Requisição/resposta (broker)

| `service` | Requisição (`data`) | Resposta (`data`) |
|---|---|---|
| `login` | `user`, `timestamp` | `status`, `description`, `timestamp`, `server` |
| `channels` | `user`, `timestamp` | acima + `channels` (lista de nomes) |
| `channel` | `user`, `channel`, `timestamp` | `status`, `description`, `timestamp`, `server` |
| `publish` | `user`, `channel`, `message`, `timestamp` | `status`, `description`, `timestamp`, `server` |
| `history` | `user`, `channel`, `since` (opcional), `timestamp` | acima + `channel`, `messages` |

`status` é `"sucesso"` ou `"erro"`; em caso de erro, `description` explica o motivo.

**`history`:** devolve as mensagens do canal que este servidor possui, em ordem de publicação.
Se `since` (timestamp em ms) for informado, devolve apenas as posteriores a ele. Cada item tem
`channel`, `user`, `message` e `timestamp`.

### Publicação (proxy)

O servidor publica um quadro de duas partes `[tópico, corpo]`, em que o **tópico é o nome do canal**
e o corpo é MessagePack:

```
{service: "publication", data: {channel, user, message, timestamp, server}}
```

`timestamp` é o instante da publicação. O bot calcula o instante de recebimento ao receber o quadro
e exibe os dois.

Fluxo de uma publicação (conforme o enunciado):

```
bot ──REQ publish──► servidor ──PUB (tópico = canal)──► proxy ──► bots inscritos
bot ◄──REP sucesso/erro── servidor
```

## Tratamento de erros

- **Login:** nome com 1 a 32 caracteres `A-Za-z0-9_-`. Se o servidor responder erro, o bot repete o
  login com um novo nome (`<nome>_<número aleatório>`). Relogar com o mesmo nome é permitido
  (o bot pode reiniciar) e cada login é registrado.
- **Canais:** mesma regra de caracteres. São recusados nomes inválidos, duplicados e o nome
  reservado `servers` (será o tópico de eleição na Parte 4).
- **Publicação:** erro se o usuário ou o canal forem inválidos (incluindo `servers`), ou se a
  mensagem for vazia ou maior que 500 caracteres.
- **Sem resposta:** o bot espera 5 s; se não houver resposta, recria o socket e reenvia.
  Isso também cobre a ordem de inicialização dos containers.

## Comportamento dos bots (padronizado)

Ao conectar, o bot faz login e então:

1. Se existirem menos de 5 canais, cria um novo (uma vez, com nome aleatório).
2. Entra em um loop infinito. A cada rodada:
   1. lista os canais disponíveis;
   2. se estiver inscrito em menos de 3 canais, inscreve-se em mais um, escolhido aleatoriamente
      entre os que ainda não assina;
   3. escolhe um canal aleatório da lista e envia 10 mensagens aleatórias, com 1 s de intervalo.

Os bots recebem as publicações enquanto esperam entre os envios e exibem canal, remetente,
mensagem e os timestamps de envio e de recebimento.

O passo 2.2 é repetido a cada rodada, e não só na conexão, porque sem replicação a lista de canais
pode vir de um servidor que ainda não conhece nenhum canal. Assim cada bot chega a 3 inscrições
em poucas rodadas.

**Inscrição por prefixo:** o filtro do ZeroMQ SUB funciona por prefixo (assinar `geral` também
receberia `geral2`). Por isso o bot descarta qualquer publicação cujo tópico não seja exatamente
um dos canais em que se inscreveu.

**Teste do histórico:** com a variável `FETCH_HISTORY=1`, o bot pede o `history` do canal logo
depois de se inscrever (a resposta aparece no log). Fica desligado por padrão para não alterar o
fluxo padronizado dos bots.

## Limitações conhecidas (resolvidas na Parte 5)

- Sem replicação, cada servidor conhece apenas os canais criados por ele e só guarda as mensagens
  que publicou. Como o broker usa round-robin, `channels` e `history` podem devolver resultados
  parciais ou diferentes conforme o servidor que atende.
- Pelo mesmo motivo, o servidor **não exige que o canal exista localmente** para publicar: ele
  valida apenas o formato do nome. Se exigisse, a maioria das publicações seria recusada por
  servidores que não conhecem o canal. Com a replicação, essa verificação poderá ser reforçada.
