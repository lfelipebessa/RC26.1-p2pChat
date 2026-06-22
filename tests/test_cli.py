import unittest
from cli import CLI

class FakeClient: pass

class TestParse(unittest.TestCase):
    def setUp(self):
        self.cli = CLI(FakeClient())

    def test_parse_comando_simples(self):
        self.assertEqual(self.cli.parse("/conn"), ("conn", ""))

    def test_parse_com_argumentos(self):
        self.assertEqual(self.cli.parse("/msg bob@CIC oi tudo bem"), ("msg", "bob@CIC oi tudo bem"))

    def test_parse_vazio(self):
        self.assertEqual(self.cli.parse("   "), (None, ""))

    def test_parse_normaliza_caixa_e_barra(self):
        self.assertEqual(self.cli.parse("/CONN"), ("conn", ""))

    def test_help_lista_todos_os_comandos(self):
        h = CLI.HELP
        for c in ("/help","/peers","/msg","/pub","/conn","/rtt","/reconnect","/log","/quit"):
            self.assertIn(c, h)

if __name__ == "__main__":
    unittest.main()
