# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

import unittest
from state import AppState

class C:  # fake conn
    def __init__(self, peer_id, direction, state="CONNECTED"):
        self.peer_id = peer_id; self.direction = direction; self.state = state

class FakeServer:
    def __init__(self, conns): self.connections = conns

class FakeKA:
    def __init__(self, rtts, overall):
        self._rtts = rtts; self._overall = overall
    def average_rtt(self, pid): return self._rtts.get(pid)
    def overall_average(self): return self._overall

class PInfo:
    def __init__(self, state, ip="127.0.0.1", port=4001):
        self.state = state; self.ip = ip; self.port = port

class FakeTable:
    def __init__(self, peers): self.peers = peers

class Cfg: pass

class TestAppState(unittest.TestCase):
    def setUp(self):
        self.s = AppState("alice@CIC", Cfg())

    def test_render_conns_vazio(self):
        self.assertIn("Nenhuma", self.s.render_conns(FakeServer({}), FakeTable({})))

    def test_render_conns_lista_direcao_e_estado(self):
        server = FakeServer({"bob@CIC": C("bob@CIC", "outbound")})
        table = FakeTable({"bob@CIC": PInfo("CONNECTED")})
        out = self.s.render_conns(server, table)
        self.assertIn("bob@CIC", out)
        self.assertIn("outbound", out)
        self.assertIn("CONNECTED", out)

    def test_render_rtt_com_media(self):
        server = FakeServer({"bob@CIC": C("bob@CIC", "outbound")})
        ka = FakeKA({"bob@CIC": 42.3}, 42.3)
        out = self.s.render_rtt(ka, server)
        self.assertIn("42.3", out)
        self.assertIn("geral", out)

    def test_render_rtt_sem_pong_ainda(self):
        server = FakeServer({"bob@CIC": C("bob@CIC", "outbound")})
        ka = FakeKA({}, None)                 # ainda sem RTT
        out = self.s.render_rtt(ka, server)
        self.assertIn("bob@CIC", out)
        self.assertIn("—", out)

    def test_render_peers(self):
        table = FakeTable({"bob@CIC": PInfo("STALE", "10.0.0.1", 4002)})
        out = self.s.render_peers(table)
        self.assertIn("bob@CIC", out)
        self.assertIn("STALE", out)
        self.assertIn("10.0.0.1:4002", out)

if __name__ == "__main__":
    unittest.main()
