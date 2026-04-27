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
    alias da Microsoft Store ou em um ``python.exe`` antigo do ``.venv`` que
    já não existe (por exemplo após recriar o venv). Por isso **sempre**
    sobrescrevemos as variáveis com ``sys.executable``, não ``setdefault``.
    """
    current = sys.executable
    os.environ["PYSPARK_PYTHON"] = current
    os.environ["PYSPARK_DRIVER_PYTHON"] = current


_ensure_src_on_path()
_ensure_pyspark_python()


from business.relatorio_pedidos import (
    RelatorioPedidosRecusadosLegitimos,
)
from config.app_config import AppConfig
from data_io.reader import (
    PagamentosReader,
    PedidosReader,
)
from data_io.writer import ParquetWriter
from pipeline.pipeline_orchestrator import PipelineOrchestrator
from schemas.pagamentos_schema import PagamentosSchema
from schemas.pedidos_schema import PedidosSchema
from spark.spark_session_manager import SparkSessionManager


def _validate_input_datasets(config: AppConfig) -> None:
    """Falha cedo com mensagem clara se faltar clone dos repositórios de dados."""
    pedidos_dir = Path(config.pedidos_input_path)
    pagamentos_dir = Path(config.pagamentos_input_path)
    missing: list[str] = []

    if not pedidos_dir.is_dir():
        missing.append(
            f" Pasta inexistente (pedidos): {pedidos_dir}\n"
            "   -> Faca o clone em data/input conforme a secao Datasets do README."
        )
    elif not list(pedidos_dir.glob("pedidos-*.csv.gz")):
        missing.append(
            f" Nenhum pedidos-*.csv.gz em: {pedidos_dir}\n"
            "   -> git clone https://github.com/infobarbosa/datasets-csv-pedidos "
            "data/input/datasets-csv-pedidos"
        )

    if not pagamentos_dir.is_dir():
        missing.append(
            f" Pasta inexistente (pagamentos): {pagamentos_dir}\n"
            "   -> Faca o clone em data/input conforme a secao Datasets do README."
        )
    elif not list(pagamentos_dir.glob("pagamentos-*.json.gz")):
        missing.append(
            f" Nenhum pagamentos-*.json.gz em: {pagamentos_dir}\n"
            "   -> git clone https://github.com/infobarbosa/dataset-json-pagamentos "
            "data/input/dataset-json-pagamentos"
        )

    if missing:
        msg = "Datasets de entrada ausentes ou vazios:\n" + "\n".join(missing)
        raise FileNotFoundError(msg)


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
    try:
        _validate_input_datasets(config)
        spark = spark_manager.get_or_create()

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
    except FileNotFoundError as exc:
        logger.error("%s", exc.args[0] if exc.args else exc)
        return 2
    except Exception as exc:
        logger.exception("Pipeline finalizado com erro: %s", exc)
        return 1
    finally:
        spark_manager.stop()


if __name__ == "__main__":
    raise SystemExit(main())
