"""Aggregation Root do projeto PySpark.

Este módulo instancia todas as dependências (configuração, SparkSession,
schemas, readers, writer, lógica de negócio e orquestrador) e injeta-as no
``PipelineOrchestrator``, que por sua vez executa o pipeline end-to-end.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


def _ensure_src_on_path() -> None:
    """Garante que ``src/`` esteja em ``sys.path`` ao executar via ``python main.py``."""
    src_dir = Path(__file__).resolve().parent / "src"
    if src_dir.is_dir() and str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def _ensure_pyspark_python() -> None:
    """Garante que o Spark use o mesmo interpretador do driver para os workers.

    No Windows, sem isso, o Spark resolve ``python`` pelo PATH e pode cair no
    alias do Microsoft Store (``Python was not found; run without arguments
    to install from the Microsoft Store``), derrubando os workers.
    """
    current = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", current)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", current)


_ensure_src_on_path()
_ensure_pyspark_python()


from projeto_final.business.relatorio_pedidos import (  # noqa: E402
    RelatorioPedidosRecusadosLegitimos,
)
from projeto_final.config.app_config import AppConfig  # noqa: E402
from projeto_final.io.reader import (  # noqa: E402
    PagamentosReader,
    PedidosReader,
)
from projeto_final.io.writer import ParquetWriter  # noqa: E402
from projeto_final.pipeline.pipeline_orchestrator import (  # noqa: E402
    PipelineOrchestrator,
)
from projeto_final.schemas.pagamentos_schema import PagamentosSchema  # noqa: E402
from projeto_final.schemas.pedidos_schema import PedidosSchema  # noqa: E402
from projeto_final.spark.spark_session_manager import (  # noqa: E402
    SparkSessionManager,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parseia argumentos de linha de comando para sobrescrever configurações."""
    parser = argparse.ArgumentParser(
        description="Executa o pipeline de pedidos recusados e legítimos."
    )
    parser.add_argument(
        "--ano-filtro",
        "--ano",
        dest="ano_filtro",
        type=int,
        help=(
            "Ano usado no filtro de pedidos. "
            "Precedência: CLI > PROJETO_FINAL_ANO_FILTRO > default."
        ),
    )
    return parser.parse_args(argv)


def main() -> int:
    """Ponto de entrada da aplicação."""
    args = _parse_args()
    config = AppConfig()
    if args.ano_filtro is not None:
        config.ano_filtro = args.ano_filtro

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger("main")

    spark_manager = SparkSessionManager(config)
    spark = spark_manager.get_or_create()

    try:
        pedidos_reader = PedidosReader(
            spark=spark, schema=PedidosSchema.get()
        )
        pagamentos_reader = PagamentosReader(
            spark=spark, schema=PagamentosSchema.get()
        )
        writer = ParquetWriter(
            mode=config.output_mode,
            compression=config.output_compression,
        )
        relatorio = RelatorioPedidosRecusadosLegitimos(
            ano_filtro=config.ano_filtro
        )

        orchestrator = PipelineOrchestrator(
            config=config,
            pedidos_reader=pedidos_reader,
            pagamentos_reader=pagamentos_reader,
            relatorio=relatorio,
            writer=writer,
        )
        orchestrator.run()
        return 0
    except Exception as exc:
        logger.exception("Pipeline finalizado com erro: %s", exc)
        return 1
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    raise SystemExit(main())
