# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

import asyncio
import unittest

from framing import encode, read_message
from peer_connection import PeerServer, PeerConnection


class TestPeerHandshake(unittest.IsolatedAsyncioTestCase):
    async def test_handshake_outbound_e_inbound(self):
        bob = PeerServer("bob@CIC", "127.0.0.1", 0)
        await bob.start()
        bport = bob.server.sockets[0].getsockname()[1]
        alice = PeerServer("alice@CIC", "127.0.0.1", 0)
        await alice.start()
        try:
            # alice@CIC < bob@CIC, então alice inicia a outbound
            conn = await alice.connect_to("127.0.0.1", bport, "bob@CIC")
            self.assertIsNotNone(conn)
            self.assertEqual(conn.state, "CONNECTED")
            self.assertEqual(conn.peer_id, "bob@CIC")
            self.assertIn("bob@CIC", alice.connections)
            await asyncio.sleep(0.05)  # deixa o handler inbound do bob registrar
            self.assertIn("alice@CIC", bob.connections)
        finally:
            await alice.stop()
            await bob.stop()

    async def test_nao_conecta_em_si_mesmo(self):
        alice = PeerServer("alice@CIC", "127.0.0.1", 0)
        await alice.start()
        port = alice.server.sockets[0].getsockname()[1]
        try:
            conn = await alice.connect_to("127.0.0.1", port, "alice@CIC")
            self.assertIsNone(conn)
            self.assertNotIn("alice@CIC", alice.connections)
        finally:
            await alice.stop()

    async def test_maior_id_nao_inicia_outbound(self):
        # bob@CIC > alice@CIC: bob NÃO disca (espera a inbound da alice)
        bob = PeerServer("bob@CIC", "127.0.0.1", 0)
        await bob.start()
        try:
            conn = await bob.connect_to("127.0.0.1", 65000, "alice@CIC")
            self.assertIsNone(conn)
            self.assertEqual(bob.connections, {})
        finally:
            await bob.stop()

    async def test_nao_duplica_conexao(self):
        bob = PeerServer("bob@CIC", "127.0.0.1", 0)
        await bob.start()
        bport = bob.server.sockets[0].getsockname()[1]
        alice = PeerServer("alice@CIC", "127.0.0.1", 0)
        await alice.start()
        try:
            c1 = await alice.connect_to("127.0.0.1", bport, "bob@CIC")
            c2 = await alice.connect_to("127.0.0.1", bport, "bob@CIC")
            self.assertIs(c1, c2)  # devolve a mesma; não abre outra
            self.assertEqual(len(alice.connections), 1)
        finally:
            await alice.stop()
            await bob.stop()

    async def test_rejeita_mensagem_que_nao_e_hello(self):
        bob = PeerServer("bob@CIC", "127.0.0.1", 0)
        await bob.start()
        bport = bob.server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", bport)
        try:
            writer.write(encode({"type": "PING"}))  # não é HELLO
            await writer.drain()
            resp = await read_message(reader)  # servidor fecha sem responder
            self.assertIsNone(resp)
            await asyncio.sleep(0.05)
            self.assertEqual(bob.connections, {})
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            await bob.stop()


if __name__ == "__main__":
    unittest.main()
