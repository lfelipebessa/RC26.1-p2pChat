"""CLI (Fase 7): leitura assíncrona de stdin e despacho de comandos.

Lê linhas via run_in_executor (input() bloqueante numa thread do executor),
parseia e despacha para métodos do P2PClient. Esta parte cobre o parser e o
texto do /help; o despacho e o loop de leitura entram nas tasks seguintes.
"""
import logging


class CLI:
    HELP = (
        "Comandos:\n"
        "  /help                      mostra esta ajuda\n"
        "  /peers [* | #ns]           descobre e lista peers (DISCOVER)\n"
        "  /msg <peer_id> <msg>       mensagem direta (SEND, com ACK)\n"
        "  /pub * <msg>               broadcast para todos os conectados\n"
        "  /pub #<ns> <msg>           broadcast para um namespace\n"
        "  /conn                      conexões ativas (inbound/outbound)\n"
        "  /rtt                       RTT médio por peer\n"
        "  /reconnect                 força reconciliação/religação\n"
        "  /log <nível>               ajusta o nível de log (DEBUG/INFO/...)\n"
        "  /quit                      sai limpando tudo (BYE + UNREGISTER)"
    )

    def __init__(self, client):
        self.client = client
        self.logger = logging.getLogger("CLI")

    def parse(self, line):
        line = line.strip()
        if not line:
            return None, ""
        parts = line.split(maxsplit=1)
        cmd = parts[0].lstrip("/").lower()
        rest = parts[1] if len(parts) > 1 else ""
        return cmd, rest

    async def dispatch(self, line):
        cmd, rest = self.parse(line)
        if cmd is None: return
        if cmd == "help":   print(self.HELP); return
        if cmd == "quit":   self.client.request_stop(); return
        if cmd == "conn":   print(self.client.cmd_conn()); return
        if cmd == "rtt":    print(self.client.cmd_rtt()); return
        if cmd == "reconnect": self.client.cmd_reconnect(); return
        if cmd == "peers":  await self.client.cmd_peers(rest.strip() or None); return
        if cmd == "log":    self.client.cmd_log(rest.strip()); return
        if cmd == "msg":
            alvo, _, texto = rest.partition(" ")
            if not alvo or not texto.strip():
                print("uso: /msg <peer_id> <mensagem>"); return
            await self.client.cmd_msg(alvo, texto.strip()); return
        if cmd == "pub":
            escopo, _, texto = rest.partition(" ")
            if escopo not in ("*",) and not escopo.startswith("#"):
                print("uso: /pub * <msg>  ou  /pub #<ns> <msg>"); return
            if not texto.strip():
                print("uso: /pub <escopo> <mensagem>"); return
            await self.client.cmd_pub(escopo, texto.strip()); return
        print(f"comando desconhecido: /{cmd} (use /help)")
