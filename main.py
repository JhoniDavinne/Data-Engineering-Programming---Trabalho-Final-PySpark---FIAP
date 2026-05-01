"""Ponto de entrada ao executar ``python main.py`` na raiz do repositório.

Garante que ``src/`` esteja no ``sys.path`` e delega para
:func:`pipeline.cli.run_pipeline`, onde ocorre a composição das dependências
(injeção) e a execução end-to-end do pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    """Garante que ``src/`` esteja em ``sys.path`` ao executar via ``python main.py``."""
    src_dir = Path(__file__).resolve().parent / "src"
    if src_dir.is_dir() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


_ensure_src_on_path()

from pipeline.cli import run_pipeline  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(run_pipeline())
