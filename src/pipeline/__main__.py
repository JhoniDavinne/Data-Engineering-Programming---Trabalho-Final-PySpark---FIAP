"""Permite executar o pipeline via ``python -m pipeline``."""

from __future__ import annotations

import sys

from pipeline.cli import run_pipeline

if __name__ == "__main__":
    sys.exit(run_pipeline())
