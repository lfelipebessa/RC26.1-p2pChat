import asyncio
import unittest

from peer_connection import PeerServer
from message_router import MessageRouter


class Collector:
    def __init__(self):
        self.msgs = []

    def __call__(self, src, payload, kind):
        self.msgs.append((kind, src, payload))


async def make_peer(peer_id):
    server = PeerServer(peer_id, "127.0.0.1", 0)
    coll = Collector()
    router = MessageRouter(peer_id, server, on_chat=coll)
    server.message_handler = router.handle
    await server.start()
    return server, router, coll


def port_of(server):
    return server.server.sockets[0].getsockname()[1]


class TestMessaging(unittest.IsolatedAsyncioTestCase):
    async def test_send_recebe_ack_e_entrega(self):
        bob_s, bob_r, bob_c = await make_peer("bob@CIC")
        alice_s, alice_r, alice_c = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            ok = await alice_r.send("bob@CIC", "oi bob")
            self.assertTrue(ok)  # ACK recebido
            self.assertIn(("msg", "alice@CIC", "oi bob"), bob_c.msgs)
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_send_sem_conexao_retorna_false(self):
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            ok = await alice_r.send("ze@CIC", "ninguem ouve")
            self.assertFalse(ok)
        finally:
            await alice_s.stop()

    async def test_send_sem_ack_da_timeout(self):
        # bob aceita conexão mas o handler IGNORA tudo (não responde ACK)
        bob_s = PeerServer("bob@CIC", "127.0.0.1", 0)

        async def ignora(conn, msg):
            return None

        bob_s.message_handler = ignora
        await bob_s.start()
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            ok = await alice_r.send("bob@CIC", "sem resposta", timeout=0.2)
            self.assertFalse(ok)  # sem ACK -> False (e WARNING no log)
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_pub_broadcast(self):
        bob_s, bob_r, bob_c = await make_peer("bob@CIC")
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            n = await alice_r.publish("*", "ola geral")
            self.assertEqual(n, 1)
            await asyncio.sleep(0.05)
            self.assertIn(("pub", "alice@CIC", "ola geral"), bob_c.msgs)
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_pub_namespace_so_no_escopo(self):
        bob_s, bob_r, bob_c = await make_peer("bob@CIC")
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            n_cic = await alice_r.publish("#CIC", "ola cic")
            n_outro = await alice_r.publish("#OUTRO", "ninguem")
            self.assertEqual(n_cic, 1)
            self.assertEqual(n_outro, 0)
            await asyncio.sleep(0.05)
            self.assertIn(("pub", "alice@CIC", "ola cic"), bob_c.msgs)
            self.assertNotIn(("pub", "alice@CIC", "ninguem"), bob_c.msgs)
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_dedup_ignora_msg_id_repetido(self):
        server, router, coll = await make_peer("bob@CIC")
        try:
            msg = {"type": "PUB", "msg_id": "X", "src": "alice@CIC",
                   "dst": "*", "payload": "oi", "ttl": 1}
            await router.handle(None, msg)
            await router.handle(None, msg)  # repetido -> ignorado
            self.assertEqual(len(coll.msgs), 1)
        finally:
            await server.stop()


class TestBye(unittest.IsolatedAsyncioTestCase):
    async def test_bye_recebe_bye_ok_e_fecha(self):
        closed = []
        bob_s, bob_r, _ = await make_peer("bob@CIC")
        bob_r.on_close = lambda pid: closed.append(pid)
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            conn = alice_s.connections["bob@CIC"]
            ok = await alice_r.send_bye(conn)
            self.assertTrue(ok)                      # BYE_OK recebido
            await asyncio.sleep(0.05)
            self.assertEqual(closed, ["alice@CIC"])  # bob marcou a sessão com alice como fechada
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_send_bye_sem_resposta_da_false(self):
        bob_s = PeerServer("bob@CIC", "127.0.0.1", 0)
        async def ignora(conn, msg): return None
        bob_s.message_handler = ignora
        await bob_s.start()
        alice_s, alice_r, _ = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            conn = alice_s.connections["bob@CIC"]
            ok = await alice_r.send_bye(conn, timeout=0.2)
            self.assertFalse(ok)
        finally:
            await alice_s.stop()
            await bob_s.stop()


if __name__ == "__main__":
    unittest.main()
