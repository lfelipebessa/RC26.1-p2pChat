# Backlog — Chat P2P (Redes de Computadores)

Quadro de tarefas do projeto. Como usar:

- Ao **pegar uma tarefa**, escreva seu nome entre parênteses no início dela (ex.: `**F2.1** (Luiz) ...`) e suba o commit do backlog, para o outro saber que ela está em andamento.
- Ao **concluir**, marque `[x]` e referencie a tarefa na mensagem de commit (ex.: `F2.1: implementa REGISTER no rendezvous`).

---

## Fase 1 — Fundação
- [x] **F1.1** (Luiz, Luciano) Esqueleto do projeto (`src/`, `tests/`, `main.py` com argparse) e utilitário de framing JSON-lines: serializar/parsear mensagens JSON delimitadas por `\n`, com validação do limite de 32 KiB. *(esqueleto/`main.py` inicial do Luciano; framing e reescrita do Luiz)*
- [x] **F1.2** (Luiz, Luciano) `config.json` + carregador de configuração (defaults, override por argumentos de CLI) e setup de logging (formato `[Modulo] mensagem` com timestamp; INFO no console, DEBUG em arquivo opcional). *(setup de logging inicial do Luciano; config e reescrita do Luiz)*
- [x] **F1.3** (Luiz) Validação de campos das mensagens (item da rubrica): `namespace`/`name` strings 1–64 chars não-vazias, `port` inteiro 1–65535, `ttl` inteiro 1–86400 — validar no cliente antes de enviar e ao receber; entrada inválida na CLI ⇒ recusar com erro claro.

## Fase 2 — Servidor Rendezvous
- [x] **F2.1** (Luiz, Luciano) `rendezvous_connection.py`: funções REGISTER, DISCOVER e UNREGISTER (conexão TCP curta: abre → 1 requisição → 1 resposta → fecha), com tratamento dos erros do servidor (`bad_name`, `peer_not_registered`, rate limit etc.). *(versão inicial do Luciano; reescrita async do Luiz)*
- [x] **F2.2** (Luiz) Teste manual contra o servidor público (pyp2p.mfcaetano.cc:8080): registrar, descobrir a si mesmo, desregistrar. Documentar a saída.

## Fase 3 — Conexões entre peers
- [x] **F3.1** (Luiz, Luciano) `peer_connection.py`: servidor TCP de escuta (aceita conexões inbound) e abertura de conexões outbound. *(conexão outbound inicial do Luciano; servidor/inbound e reescrita async do Luiz)*
- [x] **F3.2** (Luiz) Handshake HELLO/HELLO_OK nos dois sentidos; conexão só vira "ativa" após o handshake; rejeitar mensagens antes dele.
- [x] **F3.3** (Luiz) Evitar conexão duplicada (item da rubrica): índice de conexões ativas por `peer_id` (inbound + outbound); se já existe conexão, não abrir outra; desempate determinístico (menor `peer_id` mantém a outbound). Não conectar em si mesmo.

## Fase 4 — Mensageria
- [x] **F4.1** (Luiz, Luciano) `message_router.py`: SEND com `require_ack`, resposta ACK, timeout de 5s sem ACK gera WARNING no log; deduplicação por `msg_id`. *(stub inicial do Luciano; reescrita async do Luiz)*
- [x] **F4.2** (Luiz) PUB para `#namespace` (só peers do namespace) e `*` (todos os conectados).

## Fase 5 — Keep-alive
- [x] **F5.1** (Luiz, Luciano) `keep_alive.py`: loop de PING a cada 30s (configurável), resposta PONG imediata, cálculo de RTT por peer. *(PING/PONG inicial do Luciano com threading; reescrita async + RTT do Luiz)*
- [x] **F5.2** (Luiz) RTT médio por peer acumulado + log periódico (ex.: `[KeepAlive] Sent 2 PINGs | Average RTT = 43.2 ms`).

## Fase 6 — PeerTable e reconexão
- [ ] **F6.1** `peer_table.py`: estados (`DISCOVERED`, `CONNECTING`, `CONNECTED`, `STALE`, `CLOSED`), backoff exponencial e limite `max_reconnect_attempts` do config.
- [ ] **F6.2** Loops automáticos no `p2p_client.py`: DISCOVER recorrente (respeitando rate limit de 50 req/min do servidor), renovação do REGISTER antes do TTL expirar, reconciliação da PeerTable e tentativa de conexão a peers novos. Diferenciar peers já conhecidos dos novos (diff com a `PeerTable`) e marcar `STALE` os que sumiram da lista.

## Fase 7 — CLI
- [ ] **F7.1** `cli.py`: leitura assíncrona do stdin e parser de comandos.
- [ ] **F7.2** Comandos: `/peers`, `/msg`, `/pub`, `/conn`, `/rtt`, `/reconnect`, `/log`, `/quit`.

## Fase 8 — Encerramento limpo
- [ ] **F8.1** BYE/BYE_OK em todas as conexões ativas no `/quit`, seguido de UNREGISTER no rendezvous e cancelamento das tasks assíncronas sem traceback. Ao **receber** BYE: responder BYE_OK, marcar a conexão como `CLOSED` e fechar o socket.

## Fase 9 — Testes e entrega
- [ ] **F9.1** Executar os 6 cenários mínimos da especificação e registrar evidências (prints/logs).
- [ ] **F9.2** README com instruções de execução, arquitetura e decisões de projeto.
- [ ] **F9.3** Entrega administrativa: cabeçalho de identificação em cada arquivo `.py` (número do grupo, nomes completos e matrículas) e preencher o número do grupo (lista oficial da UnB) no formulário/relatório de avaliação.

---

## Ordem e dependências

```
F1 ──► F2 (rendezvous) ──► F5 ──► F6.1 ──► F7
  └──► F3 (peers)      ──► F4 ──► F6.2 ──► F8 ──► F9
```

Depois da Fase 1, as duas trilhas (2→5 e 3→4) podem andar **em paralelo** — são módulos diferentes, então não geram conflito de merge. Ponto de atenção: no início da Fase 3, combinar a interface da conexão (como enviar/receber mensagem de um peer), pois o keep-alive (F5) vai usá-la.
