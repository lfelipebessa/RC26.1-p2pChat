"""Comunicação com o servidor Rendezvous (registro e descoberta de peers).

Cada operação é uma conexão TCP CURTA: abre -> envia 1 requisição -> lê 1
resposta -> fecha. Mensagens são JSON delimitado por '\n' (módulo framing).
Os campos são validados no cliente antes de enviar.
"""
import asyncio
import logging

from framing import (
    encode,
    read_message,
    MAX_LINE_SIZE,
    validate_namespace,
    validate_name,
    validate_port,
    validate_ttl,
)


class RendezvousError(Exception):
    """O servidor respondeu com status diferente de OK (ex.: bad_name), ou não respondeu."""

    def __init__(self, status, response=None):
        super().__init__(f"rendezvous retornou status {status!r}")
        self.status = status
        self.response = response


class RendezvousConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.logger = logging.getLogger("Rendezvous")

    async def _do_request(self, req: dict) -> dict:
        reader, writer = await asyncio.open_connection(
            self.host, self.port, limit=MAX_LINE_SIZE
        )
        try:
            writer.write(encode(req))
            await writer.drain()
            resp = await read_message(reader)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
        if resp is None:
            raise RendezvousError("no_response")
        return resp

    async def _request(self, req: dict, timeout: float = 10.0) -> dict:
        try:
            resp = await asyncio.wait_for(self._do_request(req), timeout)
        except asyncio.TimeoutError:
            self.logger.warning("Rendezvous sem resposta em %ss", timeout)
            raise RendezvousError("timeout")
        except OSError as exc:
            # Python 3.11: wait_for pode engolir CancelledError quando a corrotina interna
            # termina com OSError simultaneamente ao cancelamento externo. Verificar se a
            # task está sendo cancelada e repropagar CancelledError para não travar o shutdown.
            task = asyncio.current_task()
            if task is not None and task.cancelling() > 0:
                raise asyncio.CancelledError() from exc
            # conexão recusada, DNS falho, rede inacessível: unifica como RendezvousError
            self.logger.warning("Falha de conexão com rendezvous: %s", exc)
            raise RendezvousError("connection_error") from exc
        if resp.get("status") != "OK":
            status = resp.get("status", "unknown")
            self.logger.warning("Rendezvous retornou erro: %s", status)
            raise RendezvousError(status, resp)
        return resp

    async def register(self, namespace, name, port, ttl=None, timeout=10.0) -> dict:
        validate_namespace(namespace)
        validate_name(name)
        validate_port(port)
        req = {"type": "REGISTER", "namespace": namespace, "name": name, "port": port}
        if ttl is not None:
            validate_ttl(ttl)
            req["ttl"] = ttl
        resp = await self._request(req, timeout)
        self.logger.info(
            "REGISTER ok: %s@%s ttl=%s ip=%s", name, namespace, resp.get("ttl"), resp.get("ip")
        )
        return resp

    async def discover(self, namespace=None, timeout=10.0) -> list:
        req = {"type": "DISCOVER"}
        if namespace is not None:
            validate_namespace(namespace)
            req["namespace"] = namespace
        resp = await self._request(req, timeout)
        peers = resp.get("peers", [])
        self.logger.info("DISCOVER ok: %d peer(s)", len(peers))
        return peers

    async def unregister(self, namespace, name, port, timeout=10.0) -> dict:
        validate_namespace(namespace)
        validate_name(name)
        validate_port(port)
        req = {"type": "UNREGISTER", "namespace": namespace, "name": name, "port": port}
        resp = await self._request(req, timeout)
        self.logger.info("UNREGISTER ok: %s@%s", name, namespace)
        return resp
