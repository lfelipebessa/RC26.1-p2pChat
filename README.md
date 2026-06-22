# Chat P2P — Trabalho Final de Redes de Computadores

Cliente de chat peer-to-peer em Python. Cada instância se registra em um servidor
**Rendezvous**, descobre outros peers automaticamente, abre **conexões TCP diretas
e persistentes** com eles e troca mensagens em tempo real (unicast e difusão),
com keep-alive (PING/PONG), reconexão automática e encerramento limpo.

> Não há relay/multi-hop: apenas **conexões diretas** entre peers. O `ttl` das
> mensagens peer-to-peer é fixo em `1` e não é decrementado.

## Requisitos

- **Python 3.11+**
- **Apenas biblioteca padrão** (`asyncio`, `json`, `logging`, `uuid`, `argparse`, …).
  Nenhuma dependência externa para rodar o cliente.
- (Opcional, só para testes) `pytest` — veja a seção [Testes](#testes).

## Estrutura do projeto

```
src/
  main.py                   # entrada: parse de args/config, setup de logging, asyncio.run
  config.py                 # carregamento de config (defaults < config.json < CLI) + validação
  framing.py                # JSON-lines: encode/decode (limite 32 KiB) e validadores de campos
  rendezvous_connection.py  # REGISTER / DISCOVER / UNREGISTER (conexões TCP curtas)
  peer_connection.py        # servidor de escuta + conexões outbound, handshake, read-loop
  message_router.py         # SEND/ACK, PUB, BYE/BYE_OK; deduplicação por msg_id
  keep_alive.py             # PING/PONG periódico e cálculo de RTT por peer
  peer_table.py             # estados dos peers, backoff exponencial (camada de política)
  state.py                  # visões de leitura para a CLI (/conn, /rtt, /peers)
  cli.py                    # leitura assíncrona de stdin, parser e despacho de comandos
  p2p_client.py             # orquestrador: registra, descobre, reconcilia, integra a CLI
config.json                 # configuração padrão (sobreposta por argumentos de CLI)
conftest.py                 # coloca src/ no sys.path para rodar os testes com pytest
tests/                      # testes (unittest); local_rendezvous.py = rendezvous de teste
```

## Como rodar

Identidade de um peer é `name@namespace` (ex.: `alice@CIC`).

```bash
# Peer 1
python3 src/main.py --name alice --namespace CIC --listen-port 4000

# Peer 2 (outro terminal)
python3 src/main.py --name bob --namespace CIC --listen-port 4001
```

Por padrão o cliente usa o servidor Rendezvous público do professor
(`pyp2p.mfcaetano.cc:8080`, configurado no `config.json`). Ao subir, o peer:

1. faz **REGISTER** no rendezvous (e renova periodicamente antes do TTL expirar);
2. faz **DISCOVER** recorrente para encontrar peers do mesmo namespace;
3. abre conexões TCP diretas com os peers novos (handshake HELLO/HELLO_OK);
4. mantém keep-alive (PING/PONG) e reconecta com backoff se uma conexão cai.

> ⚠️ Para testar **dois peers na mesma máquina**, veja
> [Testando 2 peers localmente](#testando-2-peers-localmente) — há uma pegadinha
> de rede (IP público) que exige um rendezvous local.

## Comandos da CLI

| Comando | Função |
|---|---|
| `/help` | Mostra a ajuda |
| `/peers [* \| #namespace]` | Descobre e lista peers (DISCOVER) |
| `/msg <peer_id> <mensagem>` | Mensagem direta (SEND, com confirmação ACK) |
| `/pub * <msg>` | Broadcast para todos os peers conectados |
| `/pub #<ns> <msg>` | Broadcast para os peers de um namespace |
| `/conn` | Lista as conexões ativas (inbound/outbound) |
| `/rtt` | RTT médio por peer |
| `/reconnect` | Força reconciliação e religação dos peers `STALE` |
| `/log <nível>` | Ajusta o nível de log do console (DEBUG/INFO/…) |
| `/quit` | Encerra limpo: BYE para todos + UNREGISTER + sai |

## Configuração

Os valores vêm de **defaults** (em `src/config.py`), sobrepostos pelo
**`config.json`**, sobrepostos pelos **argumentos de linha de comando**.

| Campo (config.json) | Default | Argumento CLI | Descrição |
|---|---|---|---|
| `rendezvous_host` | `pyp2p.mfcaetano.cc` | `--rendezvous-host` | Host do servidor Rendezvous |
| `rendezvous_port` | `8080` | `--rendezvous-port` | Porta do Rendezvous |
| `namespace` | — (obrigatório) | `--namespace` | Namespace do peer (1–64 chars) |
| `name` | — (obrigatório) | `--name` | Nome do peer (1–64 chars) |
| `listen_port` | — (obrigatório) | `--listen-port` | Porta TCP de escuta deste peer |
| `register_ttl` | `3600` | — | TTL (s) do registro no rendezvous; renovado em metade do TTL |
| `ping_interval` | `30` | — | Intervalo (s) entre PINGs de keep-alive |
| `discover_interval` | `20` | — | Intervalo (s) entre DISCOVERs automáticos |
| `max_reconnect_attempts` | `5` | — | Máximo de tentativas de reconexão antes de desistir |
| `reconnect_base_delay` | `1` | — | Base (s) do backoff exponencial (1, 2, 4, 8…) |
| `ack_timeout` | `5` | — | Tempo (s) de espera por ACK antes de avisar timeout |
| `log_level` | `INFO` | `--log-level` | Nível de log no console |
| `log_file` | `null` | `--log-file` | Arquivo de log (em DEBUG); `null` = só console |

> O rendezvous público aplica **rate limit de 50 requisições/min por IP**
> (estourar resulta em ban de 1 minuto). Por isso `discover_interval` não deve
> ser muito curto.

## Arquitetura

O cliente roda em um **único event loop `asyncio`** (sem threads para a lógica de
rede), com várias **tarefas concorrentes** criadas em `p2p_client.py:run()`:

- `_register_loop` — REGISTER inicial e renovação periódica;
- `_discovery_loop` — DISCOVER recorrente + reconciliação;
- `_maintenance_loop` — reconcilia periodicamente e quando acordado (`/reconnect`);
- `cli.run` — lê e despacha os comandos do usuário;
- além do `PeerServer` (escuta) e do `KeepAlive` (PING/PONG), com seus próprios loops.

**Cadeia de handlers das conexões.** Cada conexão entre peers roda um *read-loop*
(`peer_connection.py`) que entrega cada mensagem recebida a um handler. O handler
é encadeado: `KeepAlive.handle` trata `PING`/`PONG` e delega o resto para
`MessageRouter.handle`, que trata `SEND`/`ACK`/`PUB`/`BYE`. Esse encadeamento é
montado em `p2p_client.py` (`server.message_handler = keep_alive.handle`;
`keep_alive.next_handler = router.handle`).

**Uma fonte de verdade para "estou conectado?".** A `PeerTable`
(`peer_table.py`) é a **camada de política**: guarda o estado de cada peer
(`DISCOVERED → CONNECTING → CONNECTED → STALE`, e `CLOSED` ao encerrar), o
backoff e o endereço. Quem é a verdade sobre conexões vivas é o
`PeerServer.connections`; a `reconcile()` (em `p2p_client.py`) sincroniza os dois
a cada ciclo, sob um `asyncio.Lock` (porque três caminhos a chamam: discovery,
manutenção e `/reconnect`).

## Protocolos

### Transporte e codificação (rendezvous e peers)

- TCP; mensagens são **JSON UTF-8, uma por linha, delimitadas por `\n`**
  (line-delimited JSON), implementado em `framing.py`.
- TCP é um *stream* de bytes sem fronteiras de mensagem; o `\n` é o que delimita
  cada mensagem. Limite de **32 KiB por linha**, validado nos dois sentidos.
- Os campos são **validados no cliente** antes de enviar e ao receber
  (`namespace`/`name` 1–64 chars; `port` 1–65535; `ttl` 1–86400).

### Rendezvous

Cada operação é uma **conexão TCP curta** (abre → 1 requisição → 1 resposta →
fecha), em `rendezvous_connection.py`:

- **REGISTER** — anuncia/renova o peer; resposta traz o IP público visto pelo servidor.
- **DISCOVER** — lista peers (opcionalmente filtrando por namespace).
- **UNREGISTER** — remove o registro (chamado no `/quit`).

### Entre peers (conexão TCP persistente)

`HELLO`/`HELLO_OK` (handshake), `PING`/`PONG` (keep-alive + RTT),
`SEND`/`ACK` (unicast confiável), `PUB` (difusão), `BYE`/`BYE_OK` (encerramento).
A conexão só é considerada **ativa após o handshake**.

## Decisões de projeto

- **Assíncrono com um event loop (`asyncio`), não threads.** Toda a rede é I/O
  não-bloqueante; tarefas concorrentes coordenam servidor, discovery, keep-alive
  e CLI sem locks espalhados (só a `reconcile` usa um `Lock`).
- **Framing por linha com limite de 32 KiB.** Solução simples e robusta para
  delimitar mensagens sobre o stream TCP; o limite evita linhas gigantes/abuso.
- **Validação dupla dos campos.** Validamos antes de enviar e ao receber, sem
  depender só do erro do servidor — entrada inválida na CLI é recusada com mensagem clara.
- **Handshake obrigatório.** Uma conexão só conta como ativa depois de HELLO/HELLO_OK;
  mensagens antes disso são rejeitadas.
- **Evitar conexão duplicada com desempate determinístico.** Quando os dois peers
  se descobrem ao mesmo tempo, só o de **menor `peer_id` inicia a conexão
  outbound**; o outro espera a inbound (`peer_connection.py:connect_to`). Assim
  nunca há duas conexões para o mesmo peer.
- **Entrega confiável na camada de aplicação (SEND/ACK).** O TCP já garante
  entrega no transporte, mas o `ACK` confirma que o *peer* processou a mensagem;
  sem ACK em `ack_timeout` segundos, registramos um WARNING.
- **Deduplicação por `msg_id`.** Mensagens repetidas (mesmo `msg_id`) são ignoradas.
- **`PeerTable` é política; liveness vem das conexões.** Evita duas "verdades"
  divergentes sobre quem está conectado.
- **Backoff exponencial com limite.** Não martelamos um peer morto: as tentativas
  espaçam (1, 2, 4, 8…) até `max_reconnect_attempts`; depois o peer fica `STALE`
  até um `/reconnect`. Também ajuda a respeitar o rate limit do rendezvous.
- **Discovery recorrente respeitando 50 req/min.** O intervalo é configurável.
- **`ttl` fixo em 1, sem relay/multi-hop.** O escopo do trabalho é apenas
  conexões diretas.
- **CLI sem bloquear o loop.** A leitura de `stdin` usa
  `loop.run_in_executor(None, input)` para não travar o event loop.
- **Encerramento limpo.** O `/quit` cancela as tarefas, envia `BYE` (e aguarda
  `BYE_OK`) a todas as conexões e faz `UNREGISTER` no rendezvous.

## Testes

Os testes são escritos em **`unittest`** (biblioteca padrão). Há duas formas de rodar:

```bash
# Com pytest (recomendado): o conftest.py coloca src/ no path automaticamente
pytest

# Só com a biblioteca padrão (precisa apontar o PYTHONPATH para src/)
PYTHONPATH=src python3 -m unittest discover -s tests -p "test_*.py"
```

## Testando 2 peers localmente

O servidor Rendezvous responde com o **IP público** que ele enxerga (o IP do seu
roteador na internet). Se você roda dois peers na **mesma máquina/rede**, ambos
registram o mesmo IP público, e tentar conectar nesse IP de volta para a própria
rede depende de *NAT hairpinning*, que roteadores domésticos normalmente não
fazem — então os dois peers se **descobrem** mas não conseguem **conectar**.

Há duas saídas:

1. **Rodar os peers em redes diferentes** (dois computadores, ou um via celular):
   aí cada IP público é roteável e a conexão direta funciona normalmente.

2. **Usar o rendezvous local de teste** (mesma máquina). Como ele enxerga as
   conexões vindas de `127.0.0.1`, devolve `127.0.0.1` no DISCOVER — endereço
   alcançável localmente:

```bash
# Terminal 1 — rendezvous local de teste (escuta em 127.0.0.1:8090)
PYTHONPATH=src python3 tests/local_rendezvous.py

# Terminal 2 — alice apontando para o rendezvous local
python3 src/main.py --name alice --namespace CIC --listen-port 4000 \
  --rendezvous-host 127.0.0.1 --rendezvous-port 8090

# Terminal 3 — bob apontando para o rendezvous local
python3 src/main.py --name bob --namespace CIC --listen-port 4001 \
  --rendezvous-host 127.0.0.1 --rendezvous-port 8090
```

`tests/local_rendezvous.py` é uma implementação mínima e própria do protocolo
(REGISTER/DISCOVER/UNREGISTER) só para esse cenário de teste local; o fluxo do
cliente é exatamente o mesmo do servidor público.
