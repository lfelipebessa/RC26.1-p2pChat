import asyncio
import logging
import unittest

from framing import encode, read_message, ValidationError
from rendezvous_connection import RendezvousConnection, RendezvousError


class FakeRendezvous:
    """Servidor rendezvous falso: responde cada requisição com uma resposta
    pré-definida e grava as requisições recebidas (para asserções)."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []
        self.server = None

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        req = await read_message(reader)
        self.requests.append(req)
        resp = self.responses.pop(0)
        writer.write(encode(resp))
        await writer.drain()
        writer.close()

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()


class TestRendezvous(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # silencia os WARNINGs esperados (erro/timeout) para não poluir a saída
        logging.disable(logging.CRITICAL)

    def tearDown(self):
        logging.disable(logging.NOTSET)

    async def test_register_envia_campos_corretos(self):
        fake = FakeRendezvous([{"status": "OK", "ttl": 3600, "ip": "1.2.3.4", "port": 4000}])
        port = await fake.start()
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            resp = await rdv.register("CIC", "alice", 4000, ttl=3600)
            self.assertEqual(resp["ip"], "1.2.3.4")
            self.assertEqual(
                fake.requests[0],
                {"type": "REGISTER", "namespace": "CIC", "name": "alice", "port": 4000, "ttl": 3600},
            )
        finally:
            await fake.stop()

    async def test_discover_extrai_lista_de_peers(self):
        peers = [{"ip": "1.2.3.4", "port": 4000, "name": "alice",
                  "namespace": "CIC", "ttl": 3600, "expires_in": 3500}]
        fake = FakeRendezvous([{"status": "OK", "peers": peers}])
        port = await fake.start()
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            result = await rdv.discover("CIC")
            self.assertEqual(result, peers)
            self.assertEqual(fake.requests[0], {"type": "DISCOVER", "namespace": "CIC"})
        finally:
            await fake.stop()

    async def test_discover_sem_namespace_omite_campo(self):
        fake = FakeRendezvous([{"status": "OK", "peers": []}])
        port = await fake.start()
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            result = await rdv.discover()
            self.assertEqual(result, [])
            self.assertEqual(fake.requests[0], {"type": "DISCOVER"})
        finally:
            await fake.stop()

    async def test_unregister_envia_campos(self):
        fake = FakeRendezvous([{"status": "OK"}])
        port = await fake.start()
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            await rdv.unregister("CIC", "alice", 4000)
            self.assertEqual(
                fake.requests[0],
                {"type": "UNREGISTER", "namespace": "CIC", "name": "alice", "port": 4000},
            )
        finally:
            await fake.stop()

    async def test_status_erro_levanta_rendezvous_error(self):
        fake = FakeRendezvous([{"status": "bad_name"}])
        port = await fake.start()
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            with self.assertRaises(RendezvousError):
                await rdv.register("CIC", "alice", 4000)
        finally:
            await fake.stop()

    async def test_valida_antes_de_enviar(self):
        # port inválido deve levantar ValidationError ANTES de abrir conexão
        rdv = RendezvousConnection("127.0.0.1", 9)  # porta não usada
        with self.assertRaises(ValidationError):
            await rdv.register("CIC", "alice", 70000)

    async def test_timeout_levanta_rendezvous_error(self):
        async def silent_handle(reader, writer):
            await asyncio.sleep(5)  # nunca responde a tempo
            writer.close()

        server = await asyncio.start_server(silent_handle, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            with self.assertRaises(RendezvousError) as ctx:
                await rdv.register("CIC", "alice", 4000, timeout=0.1)
            self.assertEqual(ctx.exception.status, "timeout")
        finally:
            server.close()
            await server.wait_closed()

    async def test_no_response_levanta_rendezvous_error(self):
        async def le_e_fecha(reader, writer):
            await read_message(reader)  # consome a requisição e fecha sem responder
            writer.close()

        server = await asyncio.start_server(le_e_fecha, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            rdv = RendezvousConnection("127.0.0.1", port)
            with self.assertRaises(RendezvousError) as ctx:
                await rdv.discover("CIC")
            self.assertEqual(ctx.exception.status, "no_response")
        finally:
            server.close()
            await server.wait_closed()

    async def test_conexao_recusada_vira_connection_error(self):
        # abre e fecha um servidor para obter uma porta garantidamente livre,
        # então conectar nela é recusado (OSError) -> connection_error
        server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        server.close()
        await server.wait_closed()
        rdv = RendezvousConnection("127.0.0.1", port)
        with self.assertRaises(RendezvousError) as ctx:
            await rdv.discover("CIC")
        self.assertEqual(ctx.exception.status, "connection_error")


if __name__ == "__main__":
    unittest.main()
