"""Keep-alive entre peers: PING/PONG periódico para detectar conexões mortas
e medir o RTT (tempo de ida e volta) por peer.

Encaixa-se como handler das conexões: trata PING (responde PONG) e PONG
(calcula o RTT), e delega os demais tipos (SEND/ACK/PUB) para o próximo
handler (o MessageRouter).
"""
import asyncio
import logging
import time
import uuid
from datetime import datetime, timezone


class KeepAlive:
    def __init__(self, my_peer_id, server, interval=30.0, next_handler=None):
        self.my_peer_id = my_peer_id
        self.server = server
        self.interval = interval
        self.next_handler = next_handler          # handler para os tipos não-keepalive
        self._pending = {}                        # msg_id -> (peer_id, t0 monotonic)
        self._rtt_sum = {}                        # peer_id -> soma de RTTs (ms)
        self._rtt_count = {}                      # peer_id -> nº de RTTs
        self._pings_since_log = 0
        self._task = None
        self.logger = logging.getLogger("KeepAlive")

    def _now(self):
        # timestamp ISO-8601 vai na mensagem (rede); o RTT usa time.monotonic (mede intervalo)
        return datetime.now(timezone.utc).isoformat()

    async def handle(self, conn, msg):
        """Handler das conexões: trata PING/PONG; delega o resto."""
        mtype = msg.get("type")
        if mtype == "PING":
            await conn.send({"type": "PONG", "msg_id": msg.get("msg_id"),
                             "timestamp": self._now(), "ttl": 1})
        elif mtype == "PONG":
            self._on_pong(msg)
        elif self.next_handler is not None:
            await self.next_handler(conn, msg)

    def _on_pong(self, msg):
        info = self._pending.pop(msg.get("msg_id"), None)
        if info is None:
            return
        peer_id, t0 = info
        rtt_ms = (time.monotonic() - t0) * 1000.0
        self._rtt_sum[peer_id] = self._rtt_sum.get(peer_id, 0.0) + rtt_ms
        self._rtt_count[peer_id] = self._rtt_count.get(peer_id, 0) + 1
        self.logger.debug("PONG de %s: RTT = %.1f ms", peer_id, rtt_ms)

    def _evict_stale(self):
        """Descarta PINGs antigos que nunca receberam PONG (peer mudo/morto),
        para o _pending não crescer sem limite."""
        limite = time.monotonic() - self.interval * 2
        for mid in [m for m, (_, t0) in self._pending.items() if t0 < limite]:
            self._pending.pop(mid, None)

    def clear_peer(self, peer_id):
        """Esquece um peer (a PeerTable chama isto ao marcar STALE, na Fase 6)."""
        for mid in [m for m, (pid, _) in self._pending.items() if pid == peer_id]:
            self._pending.pop(mid, None)
        self._rtt_sum.pop(peer_id, None)
        self._rtt_count.pop(peer_id, None)

    async def send_pings(self):
        """Envia um PING para cada conexão ativa. Retorna o nº de PINGs enviados."""
        self._evict_stale()
        sent = 0
        for conn in list(self.server.connections.values()):
            msg_id = uuid.uuid4().hex
            self._pending[msg_id] = (conn.peer_id, time.monotonic())
            try:
                await conn.send({"type": "PING", "msg_id": msg_id,
                                 "timestamp": self._now(), "ttl": 1})
                sent += 1
            except OSError:
                self._pending.pop(msg_id, None)   # conexão caiu; ignora
        self._pings_since_log += sent
        return sent

    def average_rtt(self, peer_id):
        c = self._rtt_count.get(peer_id, 0)
        return (self._rtt_sum[peer_id] / c) if c else None

    def overall_average(self):
        total = sum(self._rtt_sum.values())
        count = sum(self._rtt_count.values())
        return (total / count) if count else None

    async def run(self):
        """Loop periódico: a cada `interval`, envia PINGs e loga a média de RTT."""
        while True:
            await asyncio.sleep(self.interval)
            await self.send_pings()
            avg = self.overall_average()
            if avg is not None:
                self.logger.info("Sent %d PINGs | Average RTT = %.1f ms",
                                 self._pings_since_log, avg)
            self._pings_since_log = 0

    def start(self):
        self._task = asyncio.create_task(self.run())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
