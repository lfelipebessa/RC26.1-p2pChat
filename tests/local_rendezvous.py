# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

"""Rendezvous LOCAL de teste (AUTORAL — não é o servidor do professor).

Implementa o protocolo documentado na especificação (REGISTER / DISCOVER /
UNREGISTER; JSON UTF-8 por linha, delimitado por '\\n') apenas para permitir
testar a conexão peer-a-peer na MESMA máquina. Como os peers conectam de
127.0.0.1, o IP que este servidor enxerga (e devolve) é 127.0.0.1 — alcançável
localmente —, contornando o NAT hairpinning do servidor público.

Cada operação é uma conexão TCP curta: abre -> 1 requisição -> 1 resposta -> fecha.
"""
import asyncio
import logging
import time

from framing import encode, read_message, MAX_LINE_SIZE


def _is_str(s):
    return isinstance(s, str) and 1 <= len(s) <= 64


def _is_port(p):
    return isinstance(p, int) and not isinstance(p, bool) and 1 <= p <= 65535


class LocalRendezvous:
    def __init__(self, host="127.0.0.1", port=8090):
        self.host = host
        self.port = port
        self.peers = {}                 # (namespace, name) -> dict(ip, port, ttl, expires_at)
        self.logger = logging.getLogger("rendez")

    def register(self, req, src_ip):
        ns, name, port = req.get("namespace"), req.get("name"), req.get("port")
        if not _is_str(name):      return {"status": "bad_name"}
        if not _is_str(ns):        return {"status": "bad_namespace"}
        if not _is_port(port):     return {"status": "bad_port"}
        ttl = req.get("ttl", 7200)
        if not isinstance(ttl, int) or isinstance(ttl, bool):
            return {"status": "bad_ttl"}
        ttl = max(1, min(86400, ttl))                       # clamp [1, 86400]
        self.peers[(ns, name)] = {"ip": src_ip, "port": port, "ttl": ttl,
                                  "expires_at": time.monotonic() + ttl}
        self.logger.info("REGISTER %s@%s -> %s:%s (ttl=%s)", name, ns, src_ip, port, ttl)
        return {"status": "OK", "ttl": ttl, "ip": src_ip, "port": port}

    def discover(self, req):
        ns = req.get("namespace")
        if ns is not None and not _is_str(ns):
            return {"status": "bad_namespace"}
        now = time.monotonic()
        for k in [k for k, v in self.peers.items() if v["expires_at"] <= now]:
            del self.peers[k]                               # expira registros vencidos
        peers = [
            {"ip": v["ip"], "port": v["port"], "name": pname, "namespace": pns,
             "ttl": v["ttl"], "expires_in": int(v["expires_at"] - now)}
            for (pns, pname), v in self.peers.items()
            if ns is None or pns == ns
        ]
        self.logger.info("DISCOVER ns=%s -> %d peer(s)", ns, len(peers))
        return {"status": "OK", "peers": peers}

    def unregister(self, req):
        ns, name, port = req.get("namespace"), req.get("name"), req.get("port")
        if not _is_str(ns):        return {"status": "namespace_required"}
        if not _is_port(port):     return {"status": "bad_port"}
        rec = self.peers.get((ns, name))
        if rec is None:            return {"status": "peer_not_registered"}
        if rec["port"] != port:    return {"status": "peer_credentials_do_not_match"}
        del self.peers[(ns, name)]
        self.logger.info("UNREGISTER %s@%s", name, ns)
        return {"status": "OK"}

    async def _on_client(self, reader, writer):
        src_ip = writer.get_extra_info("peername")[0]
        try:
            req = await read_message(reader)
            if req is None:
                return
            t = req.get("type")
            if   t == "REGISTER":   resp = self.register(req, src_ip)
            elif t == "DISCOVER":   resp = self.discover(req)
            elif t == "UNREGISTER": resp = self.unregister(req)
            else:                   resp = {"status": "bad_request"}
            writer.write(encode(resp))
            await writer.drain()
        except Exception:
            self.logger.exception("erro tratando cliente")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass

    async def serve(self):
        server = await asyncio.start_server(self._on_client, self.host, self.port,
                                            limit=MAX_LINE_SIZE)
        self.logger.info("Rendezvous LOCAL escutando em %s:%s", self.host, self.port)
        async with server:
            await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    try:
        asyncio.run(LocalRendezvous().serve())
    except KeyboardInterrupt:
        pass
