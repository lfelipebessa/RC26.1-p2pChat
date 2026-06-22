# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

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
    c._wake_reconcile = asyncio.Event()
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

class FakeRendezvous:
    def __init__(self, peers): self._peers = peers
    async def discover(self, namespace=None): return self._peers

class TestDiscoveryOnce(unittest.IsolatedAsyncioTestCase):
    async def test_discovery_upsert_filtra_self_e_disca(self):
        c = make_client()
        c.rendezvous = FakeRendezvous([
            {"name":"alice","namespace":"CIC","ip":"127.0.0.1","port":4000},  # eu mesmo: filtrar
            {"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001},
        ])
        await c._discovery_once()
        self.assertNotIn("alice@CIC", c.table.peers)     # não me incluo
        self.assertIn("bob@CIC", c.table.peers)
        self.assertIn("bob@CIC", c.server.dialed)        # reconcile disparado

    async def test_peer_que_sumiu_vira_stale(self):
        c = make_client(); c.server.dial_result = "fail"     # sumido = inalcançável
        c.table.upsert_from_discover({"name":"bob","namespace":"CIC","ip":"127.0.0.1","port":4001})
        c.rendezvous = FakeRendezvous([])                # bob sumiu do DISCOVER
        await c._discovery_once()
        # marcado STALE; o reconcile tenta religar, a rediscagem falha -> segue STALE (em backoff)
        self.assertEqual(c.table.peers["bob@CIC"].state, STALE)
        self.assertEqual(c.table.peers["bob@CIC"].attempts, 1)

class TestShutdown(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_cancela_tasks_bye_e_unregister(self):
        import asyncio
        c = make_client()
        c._stop = asyncio.Event()
        async def dorme(): await asyncio.sleep(3600)
        c._tasks = [asyncio.create_task(dorme()) for _ in range(2)]
        c.server.connections = {"bob@CIC": FakeConn("bob@CIC")}
        byes = []
        async def fake_send_bye(conn, *a, **k):
            byes.append(conn.peer_id); return True
        c.router.send_bye = fake_send_bye
        unreg = []
        async def fake_unreg(ns, name, port):
            unreg.append((ns, name, port)); return {"status": "OK"}
        c.rendezvous.unregister = fake_unreg
        async def fake_server_stop(): pass
        c.server.stop = fake_server_stop
        await c._shutdown()
        self.assertEqual(byes, ["bob@CIC"])
        self.assertEqual(len(unreg), 1)
        self.assertTrue(all(t.cancelled() for t in c._tasks))

if __name__ == "__main__":
    unittest.main()
