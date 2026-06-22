import unittest
from peer_table import CONNECTED, STALE, DISCOVERED

class FakeConn:
    def __init__(self, peer_id): self.peer_id = peer_id

class FakeServer:
    def __init__(self, my_peer_id):
        self.my_peer_id = my_peer_id
        self.connections = {}
        self.dial_result = "ok"          # "ok" | "fail" | "none"
        self.dialed = []
    async def connect_to(self, host, port, peer_id):
        self.dialed.append(peer_id)
        if self.dial_result == "fail":
            raise OSError("recusou")
        if self.dial_result == "none":
            return None
        conn = FakeConn(peer_id); self.connections[peer_id] = conn
        return conn

def make_client(my_peer_id="alice@CIC"):
    from p2p_client import P2PClient
    from config import Config
    cfg = Config(rendezvous_host="h", rendezvous_port=8080, namespace="CIC",
                 name="alice", listen_port=4000, register_ttl=3600, ping_interval=30,
                 discover_interval=20, max_reconnect_attempts=3, reconnect_base_delay=1,
                 ack_timeout=5, log_level="INFO", log_file=None)
    c = P2PClient(cfg)
    c.server = FakeServer(my_peer_id)            # injeta fakes
    c.keep_alive.server = c.server
    c.router.server = c.server
    import asyncio
    c._reconcile_lock = asyncio.Lock()           # criado no run() real; aqui injetamos
    return c

class TestReconcile(unittest.IsolatedAsyncioTestCase):
    async def test_conecta_peer_novo_elegivel(self):
        c = make_client()
        c.table.upsert_from_discover({"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001})
        await c.reconcile()
        self.assertIn("bob@CIC", c.server.dialed)
        self.assertEqual(c.table.peers["bob@CIC"].state, CONNECTED)

    async def test_falha_marca_stale_com_backoff(self):
        c = make_client(); c.server.dial_result = "fail"
        c.table.upsert_from_discover({"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001})
        await c.reconcile()
        info = c.table.peers["bob@CIC"]
        self.assertEqual(info.state, STALE)
        self.assertEqual(info.attempts, 1)

    async def test_conexao_que_caiu_vira_stale(self):
        c = make_client()
        c.table.upsert_from_discover({"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001})
        c.table.mark_connected("bob@CIC")        # estava conectado...
        # ...mas não está mais em server.connections -> reconcile detecta queda
        await c.reconcile()
        self.assertEqual(c.table.peers["bob@CIC"].state, STALE)

    async def test_nao_disca_a_si_mesmo_nem_duplicado(self):
        c = make_client()
        c.server.connections["bob@CIC"] = FakeConn("bob@CIC")   # já conectado
        c.table.upsert_from_discover({"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001})
        await c.reconcile()
        self.assertNotIn("bob@CIC", c.server.dialed)

if __name__ == "__main__":
    unittest.main()
