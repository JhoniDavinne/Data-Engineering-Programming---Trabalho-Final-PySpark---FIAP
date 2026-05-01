"""Configuração global do pytest e fixtures compartilhadas.

- Adiciona ``src/`` ao ``sys.path`` como fallback para quando o pacote
  não está instalado em modo editável (``pip install -e ".[dev]"``).
- Fixa ``PYSPARK_PYTHON``/``PYSPARK_DRIVER_PYTHON`` para o interpretador
  atual (evita que o Spark tente resolver ``python`` pelo PATH do sistema
  — importante no Windows, onde o alias da Microsoft Store pode causar
  ``Python was not found`` e travar os workers).
- Expõe uma ``SparkSession`` local como fixture de escopo de sessão.
- Fornece arquivos gzip de pedidos/pagamentos de exemplo (módulo de testes).
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import pytest

_SESSION_T0: float | None = None
_SESSION_EXITSTATUS: int = 0

ROOT = Path(__file__).resolve().parents[1]
TESTS_DIR = Path(__file__).resolve().parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# Permite ``import fixtures_datasets`` sem empacotar como ``tests.*``
if str(TESTS_DIR) not in sys.path:
    sys.path.append(str(TESTS_DIR))

from pipeline.cli import _configure_stdio_utf8_windows  # noqa: E402

_configure_stdio_utf8_windows()

_CURRENT_PYTHON = sys.executable
if os.name == "nt" and " " in _CURRENT_PYTHON:
    os.environ.pop("PYSPARK_PYTHON", None)
    os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
else:
    os.environ["PYSPARK_PYTHON"] = _CURRENT_PYTHON
    os.environ["PYSPARK_DRIVER_PYTHON"] = _CURRENT_PYTHON

from pyspark.sql import SparkSession  # noqa: E402

from data_io.reader import PagamentosReader, PedidosReader  # noqa: E402
from schemas.pagamentos_schema import PagamentosSchema  # noqa: E402
from schemas.pedidos_schema import PedidosSchema  # noqa: E402

from fixtures_datasets import gravar_pedidos_e_pagamentos_gzip  # noqa: E402


def pytest_sessionstart(session: pytest.Session) -> None:
    """Marca o início da sessão (para tempo total no resumo final)."""
    global _SESSION_T0
    _SESSION_T0 = time.perf_counter()


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Guarda o código de saída para o painel em ``pytest_unconfigure``."""
    global _SESSION_EXITSTATUS
    _SESSION_EXITSTATUS = exitstatus


def pytest_report_header(config: pytest.Config) -> list[str]:
    """Cabeçalho didático no topo da execução (camadas do projeto).

    Texto em ASCII para evitar caracteres corrompidos em consoles Windows (cp1252)
    quando o encoding da sessão não está em UTF-8.
    """
    py = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
    return [
        "",
        " FIAP | Data Engineering Programming - pipeline PySpark (pedidos recusados legitimos)",
        f" interpretador: {sys.executable}",
        f" Python {py} | pytest {pytest.__version__}",
        " camadas: config -> cli/run_pipeline -> schemas/readers -> relatorio -> orchestrator",
        "",
    ]


def pytest_unconfigure(config: pytest.Config) -> None:
    """Último hook do pytest: painel resumo abaixo de durações e linha 'passed'."""
    tr = config.pluginmanager.get_plugin("terminalreporter")
    if tr is None:
        return

    global _SESSION_EXITSTATUS
    exitstatus = _SESSION_EXITSTATUS
    stats = tr.stats
    passed = len(stats.get("passed", []))
    failed = len(stats.get("failed", []))
    errors = len(stats.get("error", []))
    skipped = len(stats.get("skipped", []))
    total = passed + failed + errors + skipped

    elapsed = ""
    global _SESSION_T0
    if _SESSION_T0 is not None:
        elapsed = f"  |  {time.perf_counter() - _SESSION_T0:.2f}s"

    w = 62
    line = "\u2500" * w

    def row(inner: str, **kw: object) -> None:
        inner_stripped = inner.replace("\n", " ")
        if len(inner_stripped) > w:
            inner_stripped = inner_stripped[: w - 1] + "\u2026"
        tr.write_line("\u2502" + inner_stripped.ljust(w) + "\u2502", **kw)

    tr.write_line("")
    tr.write_line("\u256d" + line + "\u256e", bold=True)
    tr.write_line("\u2502" + " FIAP / PySpark  |  RESUMO DA SUITE ".center(w) + "\u2502", bold=True)
    tr.write_line("\u251c" + line + "\u2524", bold=True)
    row(f" total: {total} testes{elapsed}")
    tr.write_line("\u2502" + f" [ok] aprovados: {passed}".ljust(w) + "\u2502", green=True)
    if failed:
        tr.write_line("\u2502" + f" [x]  falhas: {failed}".ljust(w) + "\u2502", red=True)
    if errors:
        tr.write_line("\u2502" + f" [!]  erros: {errors}".ljust(w) + "\u2502", red=True)
    if skipped:
        tr.write_line("\u2502" + f" [-]  ignorados: {skipped}".ljust(w) + "\u2502", yellow=True)

    # Até 62 caracteres (largura do painel) para evitar truncamento com "…".
    ok_msg = " Pipeline OK: config, cli, readers, relatorio, orchestrator."
    bad_msg = " Corrija falhas antes de entregar."
    tr.write_line("\u251c" + line + "\u2524", bold=True)
    row(ok_msg if exitstatus == 0 else bad_msg)
    tr.write_line("\u2570" + line + "\u256f", bold=True)
    tr.write_line("")
    tr.write_line(
        " Nota: linhas 'PID ... finalizado' no Windows sao do encerramento do Spark/JVM;",
        blue=True,
    )
    tr.write_line("       nao fazem parte do resultado do pytest.", blue=True)
    tr.write_line("")


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


@pytest.fixture(scope="session")
def diretorio_fixtures_pedidos_pagamentos(tmp_path_factory) -> Path:
    """Pasta com ``pedidos.csv.gz`` e ``pagamentos.json.gz`` (dados didáticos).

    Escopo ``session`` alinhado ao da :fixture:`spark` para evitar re-leitura
    desnecessária dos gzip a cada módulo de teste.
    """
    base = tmp_path_factory.mktemp("fixtures_pedidos_pagamentos")
    gravar_pedidos_e_pagamentos_gzip(base)
    return base


@pytest.fixture(scope="session")
def dataframe_pedidos_exemplo(spark: SparkSession, diretorio_fixtures_pedidos_pagamentos: Path):
    """:class:`PedidosReader` lendo o CSV gzip de exemplo."""
    reader = PedidosReader(spark=spark, schema=PedidosSchema.get())
    return reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pedidos.csv.gz"))


@pytest.fixture(scope="session")
def dataframe_pagamentos_exemplo(spark: SparkSession, diretorio_fixtures_pedidos_pagamentos: Path):
    """:class:`PagamentosReader` lendo o JSON gzip de exemplo."""
    reader = PagamentosReader(spark=spark, schema=PagamentosSchema.get())
    return reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pagamentos.json.gz"))
