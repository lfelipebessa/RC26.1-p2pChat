# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

"""PeerTable (Fase 6): registro de peers conhecidos + política de reconexão.

CAMADA DE POLÍTICA: guarda estado/backoff/endereço de cada peer, mas NÃO segura
socket — a liveness real ("estou conectado?") é lida de PeerServer.connections
(decisão A1). O backoff é exponencial; o desempate ("menor id disca") reaproveita
a regra já existente no PeerServer.connect_to.
"""
import logging
import time
from dataclasses import dataclass

DISCOVERED = "DISCOVERED"
CONNECTING = "CONNECTING"
CONNECTED = "CONNECTED"
STALE = "STALE"
CLOSED = "CLOSED"


@dataclass
class PeerInfo:
    peer_id: str
    ip: str
    port: int
    namespace: str
    name: str
    state: str = DISCOVERED
    attempts: int = 0
    next_retry_at: float = 0.0
    last_error: str | None = None


class PeerTable:
    def __init__(self, my_peer_id, base_delay=1, max_attempts=5):
        self.my_peer_id = my_peer_id
        self.base_delay = base_delay
        self.max_attempts = max_attempts
        self.peers = {}                       # peer_id -> PeerInfo
        self.logger = logging.getLogger("PeerTable")

    def _now(self):
        return time.monotonic()

    def upsert_from_discover(self, peer):
        peer_id = f"{peer['name']}@{peer['namespace']}"
        info = self.peers.get(peer_id)
        if info is None:
            info = PeerInfo(peer_id, peer["ip"], int(peer["port"]),
                            peer["namespace"], peer["name"])
            self.peers[peer_id] = info
            self.logger.info("Novo peer descoberto: %s", peer_id)
            return info, True
        info.ip, info.port = peer["ip"], int(peer["port"])
        return info, False

    def mark(self, peer_id, state):
        info = self.peers.get(peer_id)
        if info and info.state != state:
            info.state = state
            self.logger.debug("Peer %s -> %s", peer_id, state)

    def mark_connected(self, peer_id):
        info = self.peers.get(peer_id)
        if info:
            info.state = CONNECTED
            info.attempts = 0
            info.last_error = None
            info.next_retry_at = 0.0

    def record_failure(self, peer_id, error=None):
        info = self.peers.get(peer_id)
        if info is None:
            return
        info.attempts += 1
        info.state = STALE
        info.last_error = str(error) if error else None
        delay = self.base_delay * (2 ** (info.attempts - 1))
        info.next_retry_at = self._now() + delay
        self.logger.info("Peer %s STALE (tentativa %d/%d, retry em %ds)",
                         peer_id, info.attempts, self.max_attempts, delay)

    def reset_stale(self):
        n = 0
        for info in self.peers.values():
            if info.state == STALE:
                info.attempts = 0
                info.next_retry_at = 0.0
                info.state = DISCOVERED
                n += 1
        return n

    def eligible_to_dial(self, info, connected_ids):
        return (
            info.peer_id not in connected_ids
            and info.state in (DISCOVERED, STALE)
            and info.attempts < self.max_attempts
            and self.my_peer_id < info.peer_id          # desempate: só o menor disca
            and self._now() >= info.next_retry_at
        )
