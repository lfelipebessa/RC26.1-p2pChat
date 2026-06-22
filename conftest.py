# =============================================================================
# Trabalho Final de Redes de Computadores — Chat P2P
# Grupo 7
#   - Luiz Bessa — matrícula 231011687
#   - Luciano Ferreira — matrícula 221033143
# =============================================================================

"""Configuração de testes: coloca `src/` no sys.path.

Os módulos do projeto vivem em `src/` e os testes importam direto (ex.:
`from cli import CLI`). Este arquivo é carregado automaticamente pelo pytest
antes de coletar os testes, então `pytest` funciona a partir da raiz do
repositório sem precisar exportar `PYTHONPATH=src`.

Os testes continuam escritos em `unittest` (apenas biblioteca padrão); o pytest
serve só como executor. Quem preferir o executor da stdlib pode usar
`PYTHONPATH=src python3 -m unittest discover -s tests`.
"""
import os
import sys

_SRC = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
