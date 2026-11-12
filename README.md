# Sistema de troca de mensagens instantâneas (estilo BBS/IRC)

Projeto de Sistemas Distribuídos. Bots (clientes) conversam com servidores usando **ZeroMQ**,
tudo executado em containers e orquestrado com Docker Compose. Nenhuma interação humana é necessária.

> **Estado atual: Parte 4** — login, canais, publicação/inscrição (Pub/Sub), persistência,
> histórico, relógio lógico, serviço de referência (rank, lista, heartbeat) e, **nesta parte,
> eleição de coordenador (Bully) e sincronização do relógio físico pelo algoritmo de Berkeley**.

## Como executar

```bash
docker compose up --build
```

Sobe 1 broker, 1 proxy, 1 serviço de referência, 4 servidores e 4 bots. Cada processo imprime no
terminal o conteúdo das mensagens que envia/recebe (`[hora] processo TIPO conteúdo`). Os bots
imprimem as publicações recebidas assim:

```
[12:00:01.104] bot_js_1  MENSAGEM  #canal_9198 bot_py_2: bom dia (#4)  (enviado 12:00:01.103 | recebido 12:00:01.104 | relógio 87)
```

## Arquitetura

```
REQ/REP:  bots (REQ) ──► broker :5555 (ROUTER) ──round-robin──► :5556 (DEALER) ◄── servidores (REP)
PUB/SUB:  servidores (PUB) ──► proxy :5557 (XSUB) ──► :5558 (XPUB) ──► bots (SUB)
REQ/REP:  servidores (REQ) ──► serviço de referência :5559 (REP)      (os bots não falam com ele)
REQ/REP:  servidor (REQ) ◄──► outro servidor :6000 (REP)                (relógio e eleição; porta interna)
PUB/SUB:  coordenador eleito (PUB) ──► proxy ──► todos os servidores (SUB, tópico `servers`)
```

| Serviço | Linguagem | Quantidade |
|---|---|---|
| `broker` (REQ/REP) | Python | 1 |
| `proxy` (Pub/Sub) | Python | 1 |
| `reference` (rank, lista, heartbeat, hora) | Python | 1 |
| `server_py_1`, `server_py_2` | Python | 2 |
| `server_js_1`, `server_js_2` | JavaScript (Node.js) | 2 |
| `bot_py_1`, `bot_py_2` | Python | 2 |
| `bot_js_1`, `bot_js_2` | JavaScript (Node.js) | 2 |

Cada integrante implementou cliente e servidor na sua linguagem. Como o broker distribui as
requisições em round-robin, qualquer bot pode ser atendido por qualquer servidor, em qualquer
linguagem, e as publicações chegam a bots de ambas as linguagens.

## Escolhas

**Linguagens:** Python (`pyzmq`) e JavaScript/Node.js (`zeromq` v6), ambas com bindings maduros de ZeroMQ.
O broker, o proxy e o serviço de referência são em Python.

**Serialização:** [MessagePack](https://msgpack.org) (`msgpack` / `@msgpack/msgpack`). É binário,
compacto e tem biblioteca nas duas linguagens. Nenhuma mensagem trafega em JSON ou texto, nem as
requisições/respostas, nem as publicações, nem as mensagens ao serviço de referência.

**Persistência:** cada servidor grava em seu próprio arquivo `/data/<nome_do_servidor>.jsonl`
(JSON Lines, um evento por linha, com `fsync`). Cada servidor tem um volume Docker nomeado próprio,
então os dados sobrevivem à remoção do container e nunca são compartilhados entre servidores.
Ao iniciar, o servidor relê o arquivo e reconstrói canais e mensagens. O JSON Lines é usado só em
disco, não na troca de mensagens.

```json
{"type": "login",   "user": "bot_py_1", "timestamp": 1794443197558, "received_at": 1794443197559}
{"type": "channel", "name": "canal_6644", "user": "bot_py_1", "timestamp": 1794443197561, "received_at": 1794443197562}
{"type": "message", "channel": "canal_6644", "user": "bot_js_1", "message": "bom dia (#1)", "timestamp": 1794445000554, "clock": 412, "origin_timestamp": 1794445000553}
```

Em `message`, `timestamp` e `clock` são o instante físico e o contador lógico da publicação pelo
servidor; `origin_timestamp` é o instante em que o bot enviou a requisição. A mensagem é gravada
**antes** de ser publicada.

## Formato das mensagens

Toda mensagem é um mapa MessagePack `{service, data}`. Em `data`, **todas** as mensagens (de bots,
servidores, publicações e do serviço de referência) trazem:

- `timestamp`: instante do envio (milissegundos desde a epoch, inteiro);
- `clock`: valor do relógio lógico de quem envia (inteiro).

### Requisição/resposta (bot → broker → servidor)

| `service` | Requisição (`data`) | Resposta (`data`) |
|---|---|---|
| `login` | `user` | `status`, `description`, `server` |
| `channels` | `user` | acima + `channels` (lista de nomes) |
| `channel` | `user`, `channel` | `status`, `description`, `server` |
| `publish` | `user`, `channel`, `message` | `status`, `description`, `server` |
| `history` | `user`, `channel`, `since` (opcional) | acima + `channel`, `messages` |

(além de `timestamp` e `clock`, presentes em todas). `status` é `"sucesso"` ou `"erro"`; em caso de
erro, `description` explica o motivo. `history` devolve as mensagens do canal que o servidor possui,
em ordem de publicação; se `since` for informado, só as posteriores a ele.

### Publicação (servidor → proxy → bots)

O servidor publica um quadro de duas partes `[tópico, corpo]`; o **tópico é o nome do canal** e o
corpo é `{service: "publication", data: {channel, user, message, timestamp, clock, server}}`.

```
bot ──REQ publish──► servidor ──PUB (tópico = canal)──► proxy ──► bots inscritos
bot ◄──REP sucesso/erro── servidor
```

### Serviço de referência (servidor ↔ referência)

Todas as requisições levam `data.name` (nome do servidor).

| `service` | Resposta (`data`) | Função |
|---|---|---|
| `rank` | `rank` | cadastra o servidor (sem repetir nomes) e devolve seu rank |
| `list` | `servers`: `[{name, rank}, ...]` | lista os servidores disponíveis |
| `heartbeat` | `description: "OK"` | marca o servidor como vivo (a resposta não traz mais a hora) |

## Relógio lógico

Implementado em todos os processos (bots, servidores e serviço de referência) como na aula:

1. o contador começa em 0 junto com o processo e é **incrementado antes de cada envio**; o valor
   vai na mensagem (`data.clock`);
2. ao **receber** uma mensagem, o processo passa a usar `max(valor local, valor recebido)`.

Isso vale para toda troca: requisição e resposta, publicação (o servidor incrementa ao publicar e
de novo ao responder) e mensagens ao serviço de referência. O bot também atualiza o contador ao
receber uma publicação. Se uma requisição precisar ser reenviada por falta de resposta, o reenvio é
um novo envio e incrementa o contador.

## Serviço de referência e heartbeat

- **Rank:** atribuído por ordem de cadastro (1, 2, 3, ...). O nome vem da variável `SERVER_NAME`.
  Um servidor que volta com o mesmo nome recupera o mesmo rank.
- **Lista:** só inclui os servidores com heartbeat recente. Se um servidor ficar mais de
  `HEARTBEAT_TIMEOUT` segundos (30 s no Compose) sem enviar heartbeat, ele é removido e o serviço
  registra `REMOVIDO` no log. Um heartbeat de servidor removido o cadastra de novo.
- **Heartbeat:** cada servidor o envia **a cada 10 mensagens de clientes recebidas** (e mais um
  logo ao iniciar). Em seguida pede a lista de servidores e a exibe no log (`SERVIDORES`).
- **Início do servidor:** ele pede o rank (repetindo até o serviço responder), faz o primeiro
  heartbeat, abre o canal entre servidores, realiza a eleição e a primeira sincronização do relógio,
  e só então se conecta ao broker, para não receber requisições antes de estar sincronizado.

Como o heartbeat é por contagem de mensagens, um servidor sem tráfego por mais que o timeout seria
removido da lista até o próximo heartbeat. Com 4 bots enviando cerca de 1 mensagem por segundo cada,
cada servidor envia um heartbeat a cada ~10 s, bem abaixo dos 30 s.

## Eleição de coordenador (algoritmo Bully)

Cada servidor mantém a variável `coordinator` com o **nome do servidor eleito**. O servidor de
**maior rank** vence (o rank vem do serviço de referência, por ordem de cadastro).

Mensagens (todas com `timestamp` e `clock`):

| Mensagem | Padrão | Conteúdo |
|---|---|---|
| `election` | `REQ` servidor A → servidor B, `REP: OK` | A envia `name` e `rank`; B responde `status: "OK"` e o `coordinator` que conhece |
| `clock` | `REQ` servidor → coordenador, `REP: hora correta` | pedido com `name` e `time`; resposta com `time` (hora correta) |
| anúncio | `PUB` no tópico `servers` | `[servers, {coordinator, rank, timestamp, clock}]`: o eleito avisa todos os servidores |

Funcionamento:

1. **Ao iniciar:** o servidor pede a lista à referência e envia `election` a **todos os servidores de
   rank maior**. Se nenhum responder `OK` (timeout de 2 s), ele vence e publica seu nome em `servers`.
   Se algum responder, esse servidor assume a eleição, e o de maior rank que respondeu fica como
   coordenador provisório até o anúncio chegar.
2. **Quem recebe `election` de um rank menor** responde `OK` e inicia a própria eleição.
3. **Coordenador indisponível:** a cada sincronização do relógio (abaixo), se o coordenador não
   responder, o servidor pede eleição aos de rank maior e tenta de novo com o novo coordenador.
4. **Servidor de rank maior que volta** (mesmo nome, mesmo rank) vence a eleição ao iniciar e reassume.
5. **Anúncio de um rank menor** faz o servidor de rank maior iniciar eleição, para que o maior sempre vença.
6. **Recuperação:** se o servidor consultado para o relógio não for o coordenador, ele responde
   `erro` indicando quem é, e o solicitante passa a usar essa indicação. Isso cobre anúncios
   perdidos (o Pub/Sub não garante entrega a quem acabou de assinar).

Os servidores se enxergam em `tcp://<nome>:6000` pela rede do Compose (porta interna, não publicada).
O tópico `servers` é verificado por igualdade exata, pois o filtro do SUB é por prefixo.

## Sincronização do relógio físico (algoritmo de Berkeley)

A cada **15 mensagens de clientes recebidas**, o servidor pede a hora ao coordenador. O relógio do
sistema não é alterado: cada servidor mantém um deslocamento (offset) somado ao relógio local, e todo
`timestamp` enviado usa o relógio corrigido. O serviço de referência **não responde mais a hora**.

No Berkeley não há uma fonte de tempo externa: o coordenador calcula a média dos relógios do grupo.

1. O servidor envia `clock` com o próprio relógio (`time`).
2. O coordenador guarda o último relógio reportado por cada servidor e estima o valor atual de cada
   um somando o tempo decorrido desde o relato (relatos com mais de 60 s são ignorados).
3. Ele calcula a **média** desses relógios e do seu próprio, **adota essa média** e a devolve como
   hora correta (`time`).
4. O servidor ajusta o offset para a hora recebida, compensando metade do RTT medido no pedido.

Assim, com o tempo, todos os relógios convergem para um valor comum (a média do grupo, e não a hora
do host). O coordenador não envia pedidos de relógio a si mesmo: ele se ajusta ao atender os outros.
Ao mudar o coordenador, a tabela de relatos é descartada e recomeça no novo.

**Como conferir:** os containers compartilham o relógio do host, então o Compose define
`CLOCK_SKEW_MS` em dois servidores para simular relógios com defeito (`server_py_2` com +3000 ms e
`server_js_2` com −2000 ms). O log mostra `offset total` em cada sincronização (`RELOGIO` nos
servidores e `BERKELEY` no coordenador). Somando o defeito simulado ao offset de cada servidor, o
resultado deve ser praticamente o mesmo nos quatro (nos testes, diferenças de poucos milissegundos).
Para desligar a simulação, remova a variável (o padrão é 0).

## Tratamento de erros

- **Login:** nome com 1 a 32 caracteres `A-Za-z0-9_-`. Se o servidor responder erro, o bot repete o
  login com um novo nome (`<nome>_<número aleatório>`). Relogar com o mesmo nome é permitido
  (o bot pode reiniciar) e cada login é registrado.
- **Canais:** mesma regra de caracteres. São recusados nomes inválidos, duplicados e o nome
  reservado `servers` (será o tópico de eleição na Parte 4).
- **Publicação:** erro se o usuário ou o canal forem inválidos (incluindo `servers`), ou se a
  mensagem for vazia ou maior que 500 caracteres.
- **Servidores entre si:** esperam 2 s por outro servidor; sem resposta, ele é tratado como
  indisponível (disparando a eleição quando é o coordenador).
- **Sem resposta:** o bot espera 5 s; se não houver resposta, recria o socket e reenvia. Os servidores
  esperam 3 s pelo serviço de referência e, se ele não responder, seguem atendendo clientes e tentam
  de novo no próximo heartbeat.

## Comportamento dos bots (padronizado)

Ao conectar, o bot faz login e então:

1. Se existirem menos de 5 canais, cria um novo (uma vez, com nome aleatório).
2. Entra em um loop infinito. A cada rodada:
   1. lista os canais disponíveis;
   2. se estiver inscrito em menos de 3 canais, inscreve-se em mais um, escolhido aleatoriamente
      entre os que ainda não assina;
   3. escolhe um canal aleatório da lista e envia 10 mensagens aleatórias, com 1 s de intervalo.

O passo 2.2 é repetido a cada rodada porque, sem replicação, a lista de canais pode vir de um
servidor que ainda não conhece nenhum canal. O filtro do ZeroMQ SUB funciona por prefixo; por isso
o bot descarta publicações cujo tópico não seja exatamente um dos canais em que se inscreveu.
Com `FETCH_HISTORY=1`, o bot pede o `history` do canal ao se inscrever (desligado por padrão).

## Limitações conhecidas (resolvidas na Parte 5)

- Sem replicação, cada servidor conhece apenas os canais criados por ele e só guarda as mensagens
  que publicou. Como o broker usa round-robin, `channels` e `history` podem devolver resultados
  parciais ou diferentes conforme o servidor que atende.
- Pelo mesmo motivo, o servidor **não exige que o canal exista localmente** para publicar: ele
  valida apenas o formato do nome.
