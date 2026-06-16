"""Conexões diretas entre peers: servidor de escuta (inbound), conexões
outbound, handshake HELLO/HELLO_OK e registro de conexões (sem duplicar).

Cada peer é cliente E servidor: aceita conexões e também inicia as suas.
Para não abrir duas conexões com o mesmo peer, o desempate é determinístico:
só o peer de MENOR peer_id inicia a outbound; o de maior espera a inbound.
"""
import asyncio
import logging

from framing import encode, read_message, MAX_LINE_SIZE

VERSION = "1.0"
FEATURES = ["ack", "metrics"]


class PeerConnection:
    """Uma conexão TCP com OUTRO peer: faz o handshake e envia/recebe mensagens."""

    def __init__(self, reader, writer, my_peer_id, direction):
        self.reader = reader
        self.writer = writer
        self.my_peer_id = my_peer_id
        self.direction = direction          # "inbound" ou "outbound"
        self.peer_id = None                 # id do outro lado (após handshake)
        self.state = "CONNECTING"
        self._write_lock = asyncio.Lock()
        self.logger = logging.getLogger("PeerConn")

    def _hello(self, msg_type):
        return {"type": msg_type, "peer_id": self.my_peer_id,
                "version": VERSION, "features": FEATURES, "ttl": 1}

    async def send(self, obj):
        async with self._write_lock:        # escrita serializada (não intercala mensagens)
            self.writer.write(encode(obj))
            await self.writer.drain()

    async def handshake_outbound(self) -> bool:
        """Nós conectamos: enviamos HELLO e esperamos HELLO_OK."""
        await self.send(self._hello("HELLO"))
        msg = await read_message(self.reader)
        if not msg or msg.get("type") != "HELLO_OK" or not msg.get("peer_id"):
            return False
        self.peer_id = msg["peer_id"]
        self.state = "CONNECTED"
        return True

    async def handshake_inbound(self) -> bool:
        """Alguém conectou: esperamos HELLO e respondemos HELLO_OK."""
        msg = await read_message(self.reader)
        if not msg or msg.get("type") != "HELLO" or not msg.get("peer_id"):
            return False
        self.peer_id = msg["peer_id"]
        await self.send(self._hello("HELLO_OK"))
        self.state = "CONNECTED"
        return True

    async def close(self):
        self.state = "CLOSED"
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except OSError:
            pass


class PeerServer:
    """Escuta conexões inbound, abre conexões outbound e mantém o registro de
    conexões ativas (uma por peer_id)."""

    def __init__(self, my_peer_id, host, port):
        self.my_peer_id = my_peer_id
        self.host = host
        self.port = port
        self.connections = {}               # peer_id -> PeerConnection
        self.server = None
        self.logger = logging.getLogger("PeerServer")

    async def start(self):
        self.server = await asyncio.start_server(
            self._on_inbound, self.host, self.port, limit=MAX_LINE_SIZE
        )
        self.logger.info("Escutando em %s:%s", self.host, self.port)

    async def _on_inbound(self, reader, writer):
        conn = PeerConnection(reader, writer, self.my_peer_id, "inbound")
        if not await conn.handshake_inbound():
            self.logger.warning("Handshake inbound falhou; fechando")
            await conn.close()
            return
        if self._register(conn) is None:
            self.logger.info("Conexão duplicada de %s; fechando", conn.peer_id)
            await conn.close()
            return
        self.logger.info("Inbound connected: %s", conn.peer_id)

    async def connect_to(self, host, port, peer_id):
        """Abre uma conexão outbound com um peer (o DISCOVER nos deu o peer_id).

        Levanta OSError se a conexão TCP falhar (peer fora do ar); quem chama é
        responsável por capturar e aplicar backoff (PeerTable, Fase 6).
        """
        if peer_id == self.my_peer_id:
            return None                              # não conecta em si mesmo
        if peer_id in self.connections:
            return self.connections[peer_id]         # já conectado: não abre outra
        if self.my_peer_id > peer_id:
            # desempate: o de MENOR id inicia; eu (maior) espero a inbound dele
            self.logger.debug("Aguardo inbound de %s (desempate)", peer_id)
            return None
        reader, writer = await asyncio.open_connection(host, port, limit=MAX_LINE_SIZE)
        conn = PeerConnection(reader, writer, self.my_peer_id, "outbound")
        if not await conn.handshake_outbound():
            self.logger.warning("Handshake outbound com %s falhou", peer_id)
            await conn.close()
            return None
        if conn.peer_id != peer_id:
            # o outro lado se identificou diferente do que o DISCOVER prometeu
            self.logger.warning(
                "peer_id inesperado: esperava %s, recebi %s; fechando", peer_id, conn.peer_id
            )
            await conn.close()
            return None
        if self._register(conn) is None:
            await conn.close()
            return self.connections.get(conn.peer_id)
        self.logger.info("Outbound connected: %s", conn.peer_id)
        return conn

    def _register(self, conn):
        """Registra se ainda não há conexão com esse peer_id. Devolve a conexão
        registrada, ou None se já existia uma (duplicata a ser fechada)."""
        if conn.peer_id in self.connections:
            return None
        self.connections[conn.peer_id] = conn
        return conn

    async def stop(self):
        if self.server:                 # para de aceitar novas conexões primeiro
            self.server.close()
            await self.server.wait_closed()
        for conn in list(self.connections.values()):
            await conn.close()
        self.connections.clear()
