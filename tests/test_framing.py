# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

import asyncio
import json
import unittest

from framing import encode, read_message, MAX_LINE_SIZE, FramingError
from framing import (
    validate_namespace,
    validate_name,
    validate_port,
    validate_ttl,
    ValidationError,
)


class TestEncode(unittest.TestCase):
    def test_roundtrip(self):
        obj = {"type": "PING", "msg_id": "abc", "ttl": 1}
        data = encode(obj)
        self.assertTrue(data.endswith(b"\n"))
        self.assertEqual(json.loads(data.decode("utf-8")), obj)

    def test_rejects_oversized(self):
        big = {"type": "SEND", "payload": "a" * (MAX_LINE_SIZE + 1)}
        with self.assertRaises(FramingError):
            encode(big)


class TestReadMessage(unittest.IsolatedAsyncioTestCase):
    async def test_reads_messages_in_order(self):
        reader = asyncio.StreamReader()
        reader.feed_data(encode({"type": "PING"}))
        reader.feed_data(encode({"type": "PONG"}))
        reader.feed_eof()
        self.assertEqual(await read_message(reader), {"type": "PING"})
        self.assertEqual(await read_message(reader), {"type": "PONG"})
        self.assertIsNone(await read_message(reader))  # EOF

    async def test_returns_none_on_malformed_json(self):
        reader = asyncio.StreamReader()
        reader.feed_data(b"isto nao e json\n")
        reader.feed_eof()
        self.assertIsNone(await read_message(reader))


class TestValidation(unittest.TestCase):
    def test_namespace_ok(self):
        self.assertEqual(validate_namespace("CIC"), "CIC")
        self.assertEqual(validate_namespace("x" * 64), "x" * 64)

    def test_namespace_invalido(self):
        for bad in ("", "x" * 65, 123, None):
            with self.assertRaises(ValidationError):
                validate_namespace(bad)

    def test_name_invalido(self):
        for bad in ("", "x" * 65, 10):
            with self.assertRaises(ValidationError):
                validate_name(bad)

    def test_port_ok(self):
        self.assertEqual(validate_port(1), 1)
        self.assertEqual(validate_port(65535), 65535)

    def test_port_invalido(self):
        for bad in (0, 65536, "80", True, 4000.0):
            with self.assertRaises(ValidationError):
                validate_port(bad)

    def test_ttl_ok(self):
        self.assertEqual(validate_ttl(1), 1)
        self.assertEqual(validate_ttl(86400), 86400)

    def test_ttl_invalido(self):
        for bad in (0, 86401, "3600", True):
            with self.assertRaises(ValidationError):
                validate_ttl(bad)


if __name__ == "__main__":
    unittest.main()
