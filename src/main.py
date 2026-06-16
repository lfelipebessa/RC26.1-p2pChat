"""Ponto de entrada do Chat P2P: carrega config, configura logging e inicia o cliente."""
import logging

from config import load_config


def setup_logging(level: str = "INFO", log_file: str | None = None) -> None:
    """Configura logging: console no nível pedido, arquivo (se houver) em DEBUG.

    Formato: 'timestamp [Modulo] mensagem'. A raiz captura DEBUG para que o
    arquivo possa registrar tudo, enquanto o console filtra no nível escolhido.
    """
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in list(root.handlers):  # idempotente: limpa handlers anteriores
        root.removeHandler(h)

    fmt = logging.Formatter("%(asctime)s [%(name)s] %(message)s")

    console = logging.StreamHandler()
    console.setLevel(getattr(logging, level.upper(), logging.INFO))
    console.setFormatter(fmt)
    root.addHandler(console)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        root.addHandler(fh)


def main() -> None:
    config = load_config()
    setup_logging(config.log_level, config.log_file)
    log = logging.getLogger("main")
    log.info(
        "Cliente iniciado: %s@%s (escuta na porta %s)",
        config.name, config.namespace, config.listen_port,
    )
    # Próximas fases: instanciar e rodar o P2PClient aqui (asyncio.run(client.run())).


if __name__ == "__main__":
    main()
