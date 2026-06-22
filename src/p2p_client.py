"""P2PClient (Fase 6): orquestrador. Instancia os módulos, cola a cadeia de
handlers e roda as tarefas concorrentes (registro, discovery, manutenção,
keep-alive, CLI). reconcile() sincroniza a PeerTable com o servidor e disca
nos peers elegíveis, sob asyncio.Lock (3 chamadores: discovery, manutenção, /reconnect).
"""
import asyncio
import logging

from rendezvous_connection import RendezvousConnection, RendezvousError
from peer_connection import PeerServer
from message_router import MessageRouter
from keep_alive import KeepAlive
from peer_table import PeerTable, DISCOVERED, CONNECTING, CONNECTED, STALE, CLOSED
from state import AppState
from cli import CLI


class P2PClient:
    def __init__(self, config):
        self.config = config
        self.my_peer_id = f"{config.name}@{config.namespace}"
        self.logger = logging.getLogger("P2PClient")
        self.rendezvous = RendezvousConnection(config.rendezvous_host, config.rendezvous_port)
        self.server = PeerServer(self.my_peer_id, "0.0.0.0", config.listen_port)
        self.router = MessageRouter(self.my_peer_id, self.server,
                                    on_chat=self._on_chat, on_close=self._on_peer_closed)
        self.keep_alive = KeepAlive(self.my_peer_id, self.server,
                                    interval=config.ping_interval,
                                    next_handler=self.router.handle)
        self.server.message_handler = self.keep_alive.handle   # cadeia: keepalive -> router
        self.table = PeerTable(self.my_peer_id, config.reconnect_base_delay,
                               config.max_reconnect_attempts)
        self.state = AppState(self.my_peer_id, config)
        self.cli = CLI(self)
        self._tasks = []
        self._stop = None              # asyncio.Event (criado no run, dentro do loop)
        self._wake_reconcile = None
        self._reconcile_lock = None

    def _on_chat(self, src, payload, kind):
        prefixo = "[PUB]" if kind == "pub" else "[MSG]"
        print(f"\n{prefixo} {src}: {payload}\n> ", end="", flush=True)

    def _on_peer_closed(self, peer_id):
        self.table.mark(peer_id, CLOSED)
        self.keep_alive.clear_peer(peer_id)

    async def _shutdown(self):
        self.logger.info("Encerrando %s...", self.my_peer_id)
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        conns = list(self.server.connections.values())
        await asyncio.gather(*(self.router.send_bye(c) for c in conns),
                             return_exceptions=True)
        try:
            await self.rendezvous.unregister(self.config.namespace, self.config.name,
                                             self.config.listen_port)
        except RendezvousError as e:
            self.logger.warning("UNREGISTER falhou: %s", e)
        await self.keep_alive.stop()
        await self.server.stop()
        self.logger.info("Cliente encerrado.")

    async def reconcile(self):
        async with self._reconcile_lock:
            connected = set(self.server.connections)
            for pid in connected:                          # 1. sync: vivos -> CONNECTED
                self.table.mark_connected(pid)
            for info in list(self.table.peers.values()):   # 2. caiu -> STALE + backoff
                if info.state == CONNECTED and info.peer_id not in connected:
                    self.table.record_failure(info.peer_id, "conexão caiu")
                    self.keep_alive.clear_peer(info.peer_id)
            for info in list(self.table.peers.values()):   # 3. disca nos elegíveis
                if self.table.eligible_to_dial(info, connected):
                    await self._try_connect(info)

    async def _try_connect(self, info):
        self.table.mark(info.peer_id, CONNECTING)
        try:
            conn = await self.server.connect_to(info.ip, info.port, info.peer_id)
        except OSError as e:
            self.table.record_failure(info.peer_id, e)
            return
        if conn is None:                          # self / desempate / duplicata: não é falha
            self.table.mark(info.peer_id, DISCOVERED)
        else:
            self.table.mark_connected(info.peer_id)

    async def _discovery_once(self):
        try:
            peers = await self.rendezvous.discover(self.config.namespace)
        except RendezvousError as e:
            self.logger.warning("DISCOVER falhou: %s", e)
            return
        present = set()
        for p in peers:
            pid = f"{p['name']}@{p['namespace']}"
            if pid == self.my_peer_id:
                continue                                  # não conecta em si mesmo
            self.table.upsert_from_discover(p)
            present.add(pid)
        connected = set(self.server.connections)
        for info in list(self.table.peers.values()):      # sumiu e não conectado -> STALE
            if (info.peer_id not in present and info.peer_id not in connected
                    and info.state not in (STALE, CLOSED)):
                self.table.mark(info.peer_id, STALE)
        await self.reconcile()

    async def _register_loop(self):
        try:
            while True:
                try:
                    resp = await self.rendezvous.register(
                        self.config.namespace, self.config.name,
                        self.config.listen_port, self.config.register_ttl)
                    self.state.public_ip = resp.get("ip")
                except RendezvousError as e:
                    self.logger.warning("REGISTER falhou: %s", e)
                await asyncio.sleep(max(1, self.config.register_ttl // 2))
        except asyncio.CancelledError:
            raise

    async def _discovery_loop(self):
        try:
            while True:
                await self._discovery_once()
                await asyncio.sleep(self.config.discover_interval)
        except asyncio.CancelledError:
            raise

    async def _maintenance_loop(self):
        try:
            while True:
                try:
                    await asyncio.wait_for(self._wake_reconcile.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    pass
                self._wake_reconcile.clear()
                await self.reconcile()
        except asyncio.CancelledError:
            raise

    def wake_reconcile(self):
        if self._wake_reconcile:
            self._wake_reconcile.set()

    async def run(self):
        self._stop = asyncio.Event()
        self._wake_reconcile = asyncio.Event()
        self._reconcile_lock = asyncio.Lock()
        await self.server.start()
        self.keep_alive.start()
        self._tasks = [
            asyncio.create_task(self._register_loop()),
            asyncio.create_task(self._discovery_loop()),
            asyncio.create_task(self._maintenance_loop()),
            asyncio.create_task(self.cli.run()),
        ]
        self.logger.info("Cliente %s no ar (escuta %s).", self.my_peer_id, self.config.listen_port)
        await self._stop.wait()
        await self._shutdown()           # implementado numa fase futura

    def cmd_conn(self):   return self.state.render_conns(self.server, self.table)
    def cmd_rtt(self):    return self.state.render_rtt(self.keep_alive, self.server)
    def is_stopping(self): return self._stop is not None and self._stop.is_set()
    def request_stop(self):
        if self._stop: self._stop.set()

    def cmd_reconnect(self):
        n = self.table.reset_stale()
        self.wake_reconcile()
        print(f"reconciliação forçada ({n} peer(s) reativados)")

    def cmd_log(self, nivel):
        import logging as _lg
        lvl = getattr(_lg, nivel.upper(), None)
        if lvl is None:
            print(f"nível inválido: {nivel}"); return
        for h in _lg.getLogger().handlers:
            if isinstance(h, _lg.StreamHandler) and not getattr(h, "baseFilename", None):
                h.setLevel(lvl)
        print(f"nível de console -> {nivel.upper()}")

    async def cmd_peers(self, arg):
        ns = self.config.namespace if (arg in (None, "*")) else arg.lstrip("#")
        try:
            peers = await self.rendezvous.discover(ns)
        except RendezvousError as e:
            print(f"DISCOVER falhou: {e}"); return
        for p in peers:
            if f"{p['name']}@{p['namespace']}" == self.my_peer_id: continue
            self.table.upsert_from_discover(p)
        self.wake_reconcile()
        print(self.state.render_peers(self.table))

    async def cmd_msg(self, peer_id, texto):
        try:
            ok = await self.router.send(peer_id, texto)
        except OSError:
            ok = False; self.table.record_failure(peer_id, "envio falhou")
        print("ACK ok" if ok else "sem ACK (timeout ou sem conexão)")

    async def cmd_pub(self, escopo, texto):
        n = await self.router.publish(escopo, texto)
        print(f"PUB enviado a {n} peer(s)")
