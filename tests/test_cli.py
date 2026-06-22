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

class RecClient:
    def __init__(self):
        self.calls = []
    def request_stop(self): self.calls.append(("quit",))
    def cmd_conn(self): self.calls.append(("conn",)); return "conns"
    def cmd_rtt(self): self.calls.append(("rtt",)); return "rtts"
    def cmd_reconnect(self): self.calls.append(("reconnect",))
    def cmd_log(self, nivel): self.calls.append(("log", nivel))
    async def cmd_peers(self, arg): self.calls.append(("peers", arg))
    async def cmd_msg(self, alvo, texto): self.calls.append(("msg", alvo, texto))
    async def cmd_pub(self, escopo, texto): self.calls.append(("pub", escopo, texto))

class TestDispatch(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = RecClient()
        self.cli = CLI(self.client)

    async def test_conn_chama_cmd_conn(self):
        await self.cli.dispatch("/conn")
        self.assertIn(("conn",), self.client.calls)

    async def test_quit_chama_request_stop(self):
        await self.cli.dispatch("/quit")
        self.assertIn(("quit",), self.client.calls)

    async def test_msg_separa_alvo_e_texto(self):
        await self.cli.dispatch("/msg bob@CIC oi tudo")
        self.assertIn(("msg", "bob@CIC", "oi tudo"), self.client.calls)

    async def test_msg_sem_texto_e_recusado(self):
        await self.cli.dispatch("/msg bob@CIC")
        self.assertEqual(self.client.calls, [])

    async def test_pub_namespace(self):
        await self.cli.dispatch("/pub #CIC olá")
        self.assertIn(("pub", "#CIC", "olá"), self.client.calls)

    async def test_pub_escopo_invalido_recusado(self):
        await self.cli.dispatch("/pub bob olá")
        self.assertEqual(self.client.calls, [])

    async def test_peers_sem_arg_passa_none(self):
        await self.cli.dispatch("/peers")
        self.assertIn(("peers", None), self.client.calls)

    async def test_log_passa_nivel(self):
        await self.cli.dispatch("/log DEBUG")
        self.assertIn(("log", "DEBUG"), self.client.calls)

    async def test_desconhecido_nao_quebra(self):
        await self.cli.dispatch("/xyz foo")
        self.assertEqual(self.client.calls, [])

if __name__ == "__main__":
    unittest.main()
