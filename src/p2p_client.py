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
        self.router = MessageRouter(self.my_peer_id, self.server, on_chat=self._on_chat)
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
