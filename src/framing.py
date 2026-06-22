# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

"""Enquadramento (framing) das mensagens do protocolo.

TCP entrega um *fluxo* de bytes, não mensagens delimitadas: um recv pode trazer
meia mensagem ou várias juntas. Por isso cada mensagem é um JSON UTF-8 terminado
por '\n' (line-delimited JSON), com limite de 32 KiB por linha.
"""
import asyncio
import json

MAX_LINE_SIZE = 32768  # 32 KiB — tamanho máximo de uma mensagem (linha)


class FramingError(Exception):
    """Mensagem maior que o limite, ou linha que não pôde ser enquadrada."""


class ValidationError(Exception):
    """Campo de mensagem com tipo ou formato inválido."""


def encode(obj: dict) -> bytes:
    """Serializa um dict como JSON UTF-8 terminado por '\n'. Recusa > 32 KiB."""
    data = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    if len(data) > MAX_LINE_SIZE:
        raise FramingError(
            f"mensagem excede {MAX_LINE_SIZE} bytes (tem {len(data)})"
        )
    return data


async def read_message(reader: asyncio.StreamReader) -> dict | None:
    """Lê UMA mensagem (até '\n') de um StreamReader e devolve o dict.

    Retorna None em fim de conexão, linha grande demais ou JSON inválido —
    isso é o sinal para o chamador fechar a conexão e marcar o peer STALE.
    """
    try:
        line = await reader.readuntil(b"\n")
    except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
        return None
    if not line:
        return None
    try:
        return json.loads(line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _validate_str_1_64(value, campo):
    # bool é subclasse de int, mas aqui exigimos str mesmo
    if not isinstance(value, str) or not (1 <= len(value) <= 64):
        raise ValidationError(f"{campo} deve ser string de 1 a 64 chars: {value!r}")
    return value


def validate_namespace(ns):
    return _validate_str_1_64(ns, "namespace")


def validate_name(name):
    return _validate_str_1_64(name, "name")


def _validate_int_range(value, lo, hi, campo):
    # isinstance(True, int) é True em Python — rejeitamos bool explicitamente
    if isinstance(value, bool) or not isinstance(value, int) or not (lo <= value <= hi):
        raise ValidationError(f"{campo} deve ser inteiro de {lo} a {hi}: {value!r}")
    return value


def validate_port(port):
    return _validate_int_range(port, 1, 65535, "port")


def validate_ttl(ttl):
    return _validate_int_range(ttl, 1, 86400, "ttl")
