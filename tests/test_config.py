# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

import unittest

from config import merge_config, build_config, DEFAULTS, Config


class TestMergeConfig(unittest.TestCase):
    def test_defaults_quando_vazio(self):
        merged = merge_config({}, {})
        self.assertEqual(merged["ping_interval"], DEFAULTS["ping_interval"])
        self.assertEqual(merged["rendezvous_port"], 8080)

    def test_arquivo_sobrescreve_default(self):
        merged = merge_config({"ping_interval": 10}, {})
        self.assertEqual(merged["ping_interval"], 10)

    def test_cli_sobrescreve_arquivo(self):
        merged = merge_config({"name": "alice"}, {"name": "bob"})
        self.assertEqual(merged["name"], "bob")

    def test_cli_none_nao_sobrescreve(self):
        merged = merge_config({"name": "alice"}, {"name": None})
        self.assertEqual(merged["name"], "alice")


class TestBuildConfig(unittest.TestCase):
    def test_constroi_config_valida(self):
        merged = merge_config(
            {}, {"namespace": "CIC", "name": "alice", "listen_port": 4000}
        )
        cfg = build_config(merged)
        self.assertIsInstance(cfg, Config)
        self.assertEqual(cfg.name, "alice")
        self.assertEqual(cfg.listen_port, 4000)

    def test_falta_campo_obrigatorio(self):
        merged = merge_config({}, {"namespace": "CIC", "name": "alice"})  # sem listen_port
        with self.assertRaises(ValueError):
            build_config(merged)

    def test_listen_port_invalido(self):
        merged = merge_config(
            {}, {"namespace": "CIC", "name": "alice", "listen_port": 0}
        )
        with self.assertRaises(ValueError):
            build_config(merged)


if __name__ == "__main__":
    unittest.main()
