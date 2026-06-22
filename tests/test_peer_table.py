# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

import time
import unittest
from peer_table import (PeerTable, DISCOVERED, CONNECTED, STALE)

def disc(name, ns="CIC", ip="127.0.0.1", port=4000):
    return {"name": name, "namespace": ns, "ip": ip, "port": port}

class TestPeerTable(unittest.TestCase):
    def setUp(self):
        # my_peer_id "alice@CIC" é MENOR que "bob@CIC" (alice disca bob)
        self.t = PeerTable("alice@CIC", base_delay=1, max_attempts=3)

    def test_upsert_novo_e_atualiza_endereco(self):
        info, is_new = self.t.upsert_from_discover(disc("bob", port=4001))
        self.assertTrue(is_new)
        self.assertEqual(info.state, DISCOVERED)
        info2, is_new2 = self.t.upsert_from_discover(disc("bob", ip="10.0.0.5", port=4002))
        self.assertFalse(is_new2)
        self.assertEqual((info2.ip, info2.port), ("10.0.0.5", 4002))

    def test_record_failure_backoff_exponencial(self):
        self.t.upsert_from_discover(disc("bob"))
        t0 = time.monotonic()
        self.t.record_failure("bob@CIC", "recusou")
        self.assertEqual(self.t.peers["bob@CIC"].state, STALE)
        self.assertEqual(self.t.peers["bob@CIC"].attempts, 1)
        self.assertAlmostEqual(self.t.peers["bob@CIC"].next_retry_at - t0, 1.0, delta=0.2)
        self.t.record_failure("bob@CIC")   # 2ª falha -> 2s
        self.assertAlmostEqual(self.t.peers["bob@CIC"].next_retry_at - time.monotonic(), 2.0, delta=0.2)

    def test_exaustao_nao_e_elegivel(self):
        self.t.upsert_from_discover(disc("bob"))
        for _ in range(3):                       # max_attempts=3
            self.t.record_failure("bob@CIC")
        info = self.t.peers["bob@CIC"]
        info.next_retry_at = 0.0                 # mesmo com backoff vencido...
        self.assertFalse(self.t.eligible_to_dial(info, set()))   # ...exaurido não disca

    def test_reset_stale_volta_a_ser_elegivel(self):
        self.t.upsert_from_discover(disc("bob"))
        for _ in range(3):
            self.t.record_failure("bob@CIC")
        n = self.t.reset_stale()
        self.assertEqual(n, 1)
        info = self.t.peers["bob@CIC"]
        self.assertEqual(info.state, DISCOVERED)
        self.assertTrue(self.t.eligible_to_dial(info, set()))

    def test_desempate_maior_id_nao_disca(self):
        # eu sou "zeta@CIC" (MAIOR que bob@CIC): espero a inbound, não disco
        t = PeerTable("zeta@CIC", base_delay=1, max_attempts=3)
        info, _ = t.upsert_from_discover(disc("bob"))
        self.assertFalse(t.eligible_to_dial(info, set()))

    def test_conectado_nao_e_elegivel(self):
        info, _ = self.t.upsert_from_discover(disc("bob"))
        self.assertFalse(self.t.eligible_to_dial(info, {"bob@CIC"}))

    def test_mark_connected_zera_attempts(self):
        self.t.upsert_from_discover(disc("bob"))
        self.t.record_failure("bob@CIC")
        self.t.mark_connected("bob@CIC")
        self.assertEqual(self.t.peers["bob@CIC"].state, CONNECTED)
        self.assertEqual(self.t.peers["bob@CIC"].attempts, 0)

if __name__ == "__main__":
    unittest.main()
