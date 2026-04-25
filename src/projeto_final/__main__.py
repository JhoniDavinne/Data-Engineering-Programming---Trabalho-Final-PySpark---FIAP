"""Permite executar o pipeline via ``python -m projeto_final``."""

from __future__ import annotations

import logging
import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from projeto_final.business.relatorio_pedidos import (  # noqa: E402
    RelatorioPedidosRecusadosLegitimos,
)
from projeto_final.config.app_config import AppConfig  # noqa: E402
from projeto_final.io.reader import PagamentosReader, PedidosReader  # noqa: E402
from projeto_final.io.writer import ParquetWriter  # noqa: E402
from projeto_final.pipeline.pipeline_orchestrator import (  # noqa: E402
    PipelineOrchestrator,
)
from projeto_final.schemas.pagamentos_schema import PagamentosSchema  # noqa: E402
from projeto_final.schemas.pedidos_schema import PedidosSchema  # noqa: E402
from projeto_final.spark.spark_session_manager import (  # noqa: E402
    SparkSessionManager,
)


def main() -> int:
    """Ponto de entrada equivalente ao ``main.py`` da raiz."""
    config = AppConfig()

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger("projeto_final")

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
    sys.exit(main())
