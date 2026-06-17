import asyncio
import unittest

from peer_connection import PeerServer
from keep_alive import KeepAlive


class FakeConn:
    def __init__(self, peer_id="x@CIC"):
        self.peer_id = peer_id
        self.sent = []

    async def send(self, obj):
        self.sent.append(obj)


def port_of(server):
    return server.server.sockets[0].getsockname()[1]


async def make_peer(peer_id):
    server = PeerServer(peer_id, "127.0.0.1", 0)
    ka = KeepAlive(peer_id, server, interval=999)  # sem auto-loop nos testes
    server.message_handler = ka.handle
    await server.start()
    return server, ka


class TestKeepAlive(unittest.IsolatedAsyncioTestCase):
    async def test_responde_pong_ao_ping(self):
        ka = KeepAlive("bob@CIC", None)
        conn = FakeConn()
        await ka.handle(conn, {"type": "PING", "msg_id": "abc", "ttl": 1})
        self.assertEqual(conn.sent[0]["type"], "PONG")
        self.assertEqual(conn.sent[0]["msg_id"], "abc")  # mesmo msg_id do PING

    async def test_delega_tipos_nao_keepalive(self):
        recebidos = []

        async def proximo(conn, msg):
            recebidos.append(msg)

        ka = KeepAlive("bob@CIC", None, next_handler=proximo)
        await ka.handle(FakeConn(), {"type": "SEND", "msg_id": "1", "payload": "oi"})
        self.assertEqual(len(recebidos), 1)
        # PING NÃO deve ir para o próximo handler
        await ka.handle(FakeConn(), {"type": "PING", "msg_id": "2"})
        self.assertEqual(len(recebidos), 1)

    async def test_ping_pong_mede_rtt(self):
        bob_s, bob_ka = await make_peer("bob@CIC")
        alice_s, alice_ka = await make_peer("alice@CIC")
        try:
            await alice_s.connect_to("127.0.0.1", port_of(bob_s), "bob@CIC")
            await asyncio.sleep(0.05)
            n = await alice_ka.send_pings()       # alice -> PING -> bob
            self.assertEqual(n, 1)
            await asyncio.sleep(0.05)             # bob responde PONG; alice registra RTT
            avg = alice_ka.average_rtt("bob@CIC")
            self.assertIsNotNone(avg)
            self.assertGreaterEqual(avg, 0.0)
            self.assertIsNotNone(alice_ka.overall_average())
        finally:
            await alice_s.stop()
            await bob_s.stop()

    async def test_pong_orfao_e_ignorado(self):
        # PONG sem PING pendente (ex.: chegou após reconexão): não quebra nem mede RTT
        ka = KeepAlive("bob@CIC", None)
        await ka.handle(FakeConn(), {"type": "PONG", "msg_id": "nao-existe"})
        self.assertIsNone(ka.overall_average())

    async def test_clear_peer_esquece_pendencias_e_rtt(self):
        ka = KeepAlive("alice@CIC", None)
        ka._pending["m1"] = ("bob@CIC", 0.0)
        ka._rtt_sum["bob@CIC"] = 10.0
        ka._rtt_count["bob@CIC"] = 1
        ka.clear_peer("bob@CIC")
        self.assertEqual(ka._pending, {})
        self.assertIsNone(ka.average_rtt("bob@CIC"))


if __name__ == "__main__":
    unittest.main()
