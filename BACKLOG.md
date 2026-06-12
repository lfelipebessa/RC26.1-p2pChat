# Backlog — Chat P2P (Redes de Computadores)

Quadro de tarefas do projeto. Como usar:

- Ao **pegar uma tarefa**, escreva seu nome entre parênteses no início dela (ex.: `**F2.1** (Luiz) ...`) e suba o commit do backlog, para o outro saber que ela está em andamento.
- Ao **concluir**, marque `[x]` e referencie a tarefa na mensagem de commit (ex.: `F2.1: implementa REGISTER no rendezvous`).

---

## Fase 1 — Fundação
- [ ] **F1.1** Esqueleto do projeto (`src/`, `tests/`, `main.py` com argparse) e utilitário de framing JSON-lines: serializar/parsear mensagens JSON delimitadas por `\n`, com validação do limite de 32 KiB.
- [ ] **F1.2** `config.json` + carregador de configuração (defaults, override por argumentos de CLI) e setup de logging (formato `[Modulo] mensagem` com timestamp; INFO no console, DEBUG em arquivo opcional).

## Fase 2 — Servidor Rendezvous
- [ ] **F2.1** `rendezvous_connection.py`: funções REGISTER, DISCOVER e UNREGISTER (conexão TCP curta: abre → 1 requisição → 1 resposta → fecha), com tratamento dos erros do servidor (`bad_name`, `peer_not_registered`, rate limit etc.).
- [ ] **F2.2** Teste manual contra o servidor público (pyp2p.mfcaetano.cc:8080): registrar, descobrir a si mesmo, desregistrar. Documentar a saída.

## Fase 3 — Conexões entre peers
- [ ] **F3.1** `peer_connection.py`: servidor TCP de escuta (aceita conexões inbound) e abertura de conexões outbound.
- [ ] **F3.2** Handshake HELLO/HELLO_OK nos dois sentidos; conexão só vira "ativa" após o handshake; rejeitar mensagens antes dele.

## Fase 4 — Mensageria
- [ ] **F4.1** `message_router.py`: SEND com `require_ack`, resposta ACK, timeout de 5s sem ACK gera WARNING no log; deduplicação por `msg_id`.
- [ ] **F4.2** PUB para `#namespace` (só peers do namespace) e `*` (todos os conectados).

## Fase 5 — Keep-alive
- [ ] **F5.1** `keep_alive.py`: loop de PING a cada 30s (configurável), resposta PONG imediata, cálculo de RTT por peer.
- [ ] **F5.2** RTT médio por peer acumulado + log periódico (ex.: `[KeepAlive] Sent 2 PINGs | Average RTT = 43.2 ms`).

## Fase 6 — PeerTable e reconexão
- [ ] **F6.1** `peer_table.py`: estados (`DISCOVERED`, `CONNECTING`, `CONNECTED`, `STALE`), backoff exponencial e limite `max_reconnect_attempts` do config.
- [ ] **F6.2** Loops automáticos no `p2p_client.py`: DISCOVER recorrente (respeitando rate limit de 50 req/min do servidor), renovação do REGISTER antes do TTL expirar, reconciliação da PeerTable e tentativa de conexão a peers novos.

## Fase 7 — CLI
- [ ] **F7.1** `cli.py`: leitura assíncrona do stdin e parser de comandos.
- [ ] **F7.2** Comandos: `/peers`, `/msg`, `/pub`, `/conn`, `/rtt`, `/reconnect`, `/log`, `/quit`.

## Fase 8 — Encerramento limpo
- [ ] **F8.1** BYE/BYE_OK em todas as conexões ativas no `/quit`, seguido de UNREGISTER no rendezvous e cancelamento das tasks assíncronas sem traceback.

## Fase 9 — Testes e entrega
- [ ] **F9.1** Executar os 6 cenários mínimos da especificação e registrar evidências (prints/logs).
- [ ] **F9.2** README com instruções de execução, arquitetura e decisões de projeto.

---

## Ordem e dependências

```
F1 ──► F2 (rendezvous) ──► F5 ──► F6.1 ──► F7
  └──► F3 (peers)      ──► F4 ──► F6.2 ──► F8 ──► F9
```

Depois da Fase 1, as duas trilhas (2→5 e 3→4) podem andar **em paralelo** — são módulos diferentes, então não geram conflito de merge. Ponto de atenção: no início da Fase 3, combinar a interface da conexão (como enviar/receber mensagem de um peer), pois o keep-alive (F5) vai usá-la.
