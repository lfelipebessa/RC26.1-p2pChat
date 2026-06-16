"""Carregamento de configuração: defaults < config.json < argumentos de CLI."""
import argparse
import json
from dataclasses import dataclass

from framing import (
    validate_namespace,
    validate_name,
    validate_port,
    validate_ttl,
    ValidationError,
)

DEFAULTS = {
    "rendezvous_host": "pyp2p.mfcaetano.cc",
    "rendezvous_port": 8080,
    "namespace": None,
    "name": None,
    "listen_port": None,
    "register_ttl": 3600,
    "ping_interval": 30,
    "discover_interval": 20,
    "max_reconnect_attempts": 5,
    "reconnect_base_delay": 1,
    "ack_timeout": 5,
    "log_level": "INFO",
    "log_file": None,
}


@dataclass
class Config:
    rendezvous_host: str
    rendezvous_port: int
    namespace: str
    name: str
    listen_port: int
    register_ttl: int
    ping_interval: int
    discover_interval: int
    max_reconnect_attempts: int
    reconnect_base_delay: int
    ack_timeout: int
    log_level: str
    log_file: str | None


def merge_config(file_values: dict, cli_values: dict) -> dict:
    """Combina defaults < arquivo < CLI. Valores None vindos da CLI são ignorados."""
    values = dict(DEFAULTS)
    values.update(file_values or {})
    values.update({k: v for k, v in (cli_values or {}).items() if v is not None})
    return values


def build_config(values: dict) -> Config:
    """Valida os campos e devolve um Config. Levanta ValueError se algo faltar/for inválido."""
    for req in ("namespace", "name", "listen_port"):
        if values.get(req) is None:
            raise ValueError(f"campo obrigatório ausente: {req}")
    try:
        validate_namespace(values["namespace"])
        validate_name(values["name"])
        validate_port(values["listen_port"])
        validate_port(values["rendezvous_port"])
        validate_ttl(values["register_ttl"])
    except ValidationError as e:
        raise ValueError(str(e)) from e
    return Config(**values)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Chat P2P")
    p.add_argument("--config", default="config.json", help="caminho do config.json")
    p.add_argument("--name")
    p.add_argument("--namespace")
    p.add_argument("--listen-port", type=int, dest="listen_port")
    p.add_argument("--rendezvous-host", dest="rendezvous_host")
    p.add_argument("--rendezvous-port", type=int, dest="rendezvous_port")
    p.add_argument("--log-level", dest="log_level")
    p.add_argument("--log-file", dest="log_file")
    return p


def load_config(argv=None) -> Config:
    """Lê CLI + config.json (se existir) e devolve um Config validado."""
    args = _build_parser().parse_args(argv)
    file_values = {}
    try:
        with open(args.config, encoding="utf-8") as f:
            file_values = json.load(f)
    except FileNotFoundError:
        pass  # config.json é opcional; identidade pode vir só da CLI
    cli_values = {
        "name": args.name,
        "namespace": args.namespace,
        "listen_port": args.listen_port,
        "rendezvous_host": args.rendezvous_host,
        "rendezvous_port": args.rendezvous_port,
        "log_level": args.log_level,
        "log_file": args.log_file,
    }
    return build_config(merge_config(file_values, cli_values))
