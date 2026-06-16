import logging
import unittest

from main import setup_logging


class TestSetupLogging(unittest.TestCase):
    def tearDown(self):
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)

    def test_console_handler_respeita_nivel(self):
        setup_logging(level="INFO", log_file=None)
        root = logging.getLogger()
        self.assertEqual(root.level, logging.DEBUG)  # raiz captura tudo
        console = root.handlers[0]
        self.assertEqual(console.level, logging.INFO)  # console filtra em INFO

    def test_formato_tem_nome_do_modulo(self):
        setup_logging(level="INFO", log_file=None)
        fmt = logging.getLogger().handlers[0].formatter._fmt
        self.assertIn("%(name)s", fmt)
        self.assertIn("%(asctime)s", fmt)


if __name__ == "__main__":
    unittest.main()
