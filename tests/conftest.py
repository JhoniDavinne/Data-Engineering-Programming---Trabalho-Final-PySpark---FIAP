"""Configuração global do pytest.

- Adiciona ``src/`` ao ``sys.path`` para que os pacotes sejam importáveis
  sem instalação prévia.
- Fixa ``PYSPARK_PYTHON``/``PYSPARK_DRIVER_PYTHON`` para o interpretador
  atual (evita que o Spark tente resolver ``python`` pelo PATH do sistema
  — importante no Windows, onde o alias da Microsoft Store pode causar
  ``Python was not found`` e travar os workers).
- Expõe uma ``SparkSession`` local como fixture de escopo de sessão.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

_CURRENT_PYTHON = sys.executable
os.environ["PYSPARK_PYTHON"] = _CURRENT_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = _CURRENT_PYTHON

from pyspark.sql import SparkSession  # noqa: E402


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """SparkSession local (modo ``local[2]``) para testes unitários."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pedidos-recusados-legitimos-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
