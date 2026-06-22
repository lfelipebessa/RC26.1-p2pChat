"""Roteamento de mensagens entre peers: SEND/ACK (entrega confiável na camada
de aplicação, com timeout de 5s) e PUB (difusão para um namespace ou para
todos). Trata também o encerramento de sessão BYE/BYE_OK. Deduplica mensagens
repetidas por msg_id.
"""
import asyncio
import logging
import uuid
from collections import deque
from datetime import datetime, timezone

MAX_SEEN = 1000        # janela de dedup: ao estourar, descarta o msg_id mais antigo (LRU)
ACK_TIMEOUT = 5.0


class MessageRouter:
    def __init__(self, my_peer_id, server, on_chat=None, on_close=None):
        self.my_peer_id = my_peer_id
        self.server = server                 # PeerServer (acessa connections no PUB)
        self.on_chat = on_chat               # callback(src, payload, kind) p/ a CLI
        self.on_close = on_close             # callback(peer_id) ao receber BYE (Fase 8)
        self._seen = set()                   # dedup de msg_id
        self._seen_order = deque()
        self._pending_acks = {}              # msg_id -> Future
        self._pending_byes = {}              # msg_id -> Future (espera de BYE_OK)
        self.logger = logging.getLogger("Router")

    def _new_id(self):
        return uuid.uuid4().hex

    def _now(self):
        return datetime.now(timezone.utc).isoformat()

    def _mark_seen(self, msg_id):
        self._seen.add(msg_id)
        self._seen_order.append(msg_id)
        if len(self._seen_order) > MAX_SEEN:
            self._seen.discard(self._seen_order.popleft())

    async def handle(self, conn, msg):
        """Despacha uma mensagem recebida (chamado pelo read-loop da conexão)."""
        mtype = msg.get("type")
        if mtype == "ACK":
            self._on_ack(msg)
            return
        if mtype == "BYE_OK":
            self._on_bye_ok(msg)
            return
        msg_id = msg.get("msg_id")
        if msg_id is not None:
            if msg_id in self._seen:
                self.logger.debug("msg_id repetido %s ignorado", msg_id)
                return
            self._mark_seen(msg_id)
        if mtype == "SEND":
            await self._on_send(conn, msg)
        elif mtype == "PUB":
            self._on_pub(msg)
        elif mtype == "BYE":
            await self._on_bye(conn, msg)

    async def _on_send(self, conn, msg):
        src = msg.get("src")
        payload = msg.get("payload")
        self.logger.info("SEND de %s: %s", src, payload)
        if self.on_chat:
            self.on_chat(src, payload, "msg")
        if msg.get("require_ack"):
            await conn.send({"type": "ACK", "msg_id": msg.get("msg_id"),
                             "timestamp": self._now(), "ttl": 1})

    def _on_pub(self, msg):
        src = msg.get("src")
        payload = msg.get("payload")
        scope = msg.get("dst")
        self.logger.info("PUB de %s (%s): %s", src, scope, payload)
        if self.on_chat:
            self.on_chat(src, payload, "pub")

    def _on_ack(self, msg):
        fut = self._pending_acks.get(msg.get("msg_id"))
        if fut and not fut.done():
            fut.set_result(True)

    async def _on_bye(self, conn, msg):
        """Recebeu BYE: responde BYE_OK, marca a conexão CLOSED, avisa o cliente e fecha."""
        self.logger.info("BYE de %s (motivo=%s)", msg.get("src"), msg.get("reason"))
        try:
            await conn.send({"type": "BYE_OK", "msg_id": msg.get("msg_id"),
                             "timestamp": self._now(), "ttl": 1})
        except OSError:
            pass
        conn.state = "CLOSED"
        if self.on_close and conn.peer_id:
            self.on_close(conn.peer_id)
        await conn.close()

    def _on_bye_ok(self, msg):
        fut = self._pending_byes.get(msg.get("msg_id"))
        if fut and not fut.done():
            fut.set_result(True)

    async def send(self, dst, payload, timeout=ACK_TIMEOUT):
        """Envia SEND (unicast) e espera ACK por `timeout` segundos.
        Retorna True se o ACK chegou, False se não há conexão ou deu timeout.
        Pode levantar OSError se a conexão quebrar no meio do envio (a Fase 6
        trata marcando o peer como STALE)."""
        conn = self.server.connections.get(dst)
        if conn is None:
            self.logger.warning("Sem conexão ativa com %s", dst)
            return False
        msg_id = self._new_id()
        fut = asyncio.get_running_loop().create_future()
        self._pending_acks[msg_id] = fut
        try:
            await conn.send({"type": "SEND", "msg_id": msg_id, "src": self.my_peer_id,
                             "dst": dst, "payload": payload, "require_ack": True, "ttl": 1})
            await asyncio.wait_for(fut, timeout)
            return True
        except asyncio.TimeoutError:
            self.logger.warning("Sem ACK de %s em %ss (msg_id=%s)", dst, timeout, msg_id)
            return False
        finally:
            self._pending_acks.pop(msg_id, None)

    async def send_bye(self, conn, reason="quit", timeout=2.0):
        """Envia BYE e espera o BYE_OK por `timeout` segundos. Retorna True se
        confirmado, False se deu timeout ou a conexão quebrou."""
        msg_id = self._new_id()
        fut = asyncio.get_running_loop().create_future()
        self._pending_byes[msg_id] = fut
        try:
            await conn.send({"type": "BYE", "msg_id": msg_id, "src": self.my_peer_id,
                             "dst": conn.peer_id, "reason": reason, "ttl": 1})
            await asyncio.wait_for(fut, timeout)
            return True
        except (asyncio.TimeoutError, OSError):
            return False
        finally:
            self._pending_byes.pop(msg_id, None)

    async def publish(self, scope, payload):
        """Envia PUB para o escopo: '*' (todos) ou '#namespace'. Retorna nº de envios."""
        msg = {"type": "PUB", "msg_id": self._new_id(), "src": self.my_peer_id,
               "dst": scope, "payload": payload, "require_ack": False, "ttl": 1}
        targets = self._pub_targets(scope)
        for conn in targets:
            await conn.send(msg)
        self.logger.info("PUB %s para %d peer(s)", scope, len(targets))
        return len(targets)

    def _pub_targets(self, scope):
        conns = list(self.server.connections.values())
        if scope == "*":
            return conns
        if scope.startswith("#"):
            ns = scope[1:]
            return [c for c in conns if c.peer_id and c.peer_id.rsplit("@", 1)[-1] == ns]
        return []
