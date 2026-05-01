"""Lógica compartilhada de inicialização e execução do pipeline.

Tanto ``main.py`` (raiz) quanto ``python -m pipeline`` delegam para
:func:`run_pipeline`, ponto único onde as dependências são compostas.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


def _ensure_pyspark_python() -> None:
    """Garante que o Spark use o mesmo interpretador do driver para os workers.

    No Windows, sem isso, o Spark resolve ``python`` pelo PATH e pode cair no
    alias da Microsoft Store ou em um ``python.exe`` antigo do ``.venv`` que
    já não existe (por exemplo após recriar o venv). Por isso **sempre**
    sobrescrevemos as variáveis com ``sys.executable``, não ``setdefault``.
    """
    current = sys.executable
    if os.name == "nt" and " " in current:
        # No Git Bash/Windows, PYSPARK_* sem escaping adequado de espaços faz o
        # launcher do Spark interpretar o caminho errado (ex.: "...Data engineering").
        # Nesses casos, deixar o PySpark resolver o Python evita o falso positivo
        # "Missing Python executable" observado em ambiente real.
        os.environ.pop("PYSPARK_PYTHON", None)
        os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
        return

    os.environ["PYSPARK_PYTHON"] = current
    os.environ["PYSPARK_DRIVER_PYTHON"] = current


def _configure_stdio_utf8_windows() -> None:
    """Garante UTF-8 no console do Windows para logs com acentos.

    O encoding padrão (ex.: cp1252) corrompe caracteres ao emitir
    :class:`logging.StreamHandler` em ``stdout``/``stderr``.
    """
    if sys.platform != "win32":
        return
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8")
        except (OSError, ValueError, TypeError):
            pass


def _validate_input_datasets(config) -> None:
    """Falha cedo com mensagem clara se faltar clone dos repositórios de dados."""
    from config.app_config import resolve_input_directory

    pedidos_dir = resolve_input_directory(config.pedidos_input_path)
    pagamentos_dir = resolve_input_directory(config.pagamentos_input_path)
    missing: list[str] = []

    if not pedidos_dir.is_dir():
        missing.append(
            f" Pasta inexistente (pedidos): {pedidos_dir}\n"
            "   -> Faça o clone em data/input conforme a seção Datasets do README."
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
            "   -> Faça o clone em data/input conforme a seção Datasets do README."
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


def run_pipeline(argv: list[str] | None = None) -> int:
    """Ponto de entrada unificado da aplicação.

    Instancia todas as dependências (configuração, SparkSession, schemas,
    readers, writer, lógica de negócio e orquestrador) e executa o pipeline
    end-to-end.

    Parâmetros
    ----------
    argv : list[str] | None
        Argumentos de linha de comando. ``None`` usa ``sys.argv``.

    Retorna
    -------
    int
        Código de saída: 0 = sucesso, 1 = erro genérico, 2 = dataset ausente.
    """
    _ensure_pyspark_python()

    args = _parse_args(argv)

    from business.relatorio_pedidos import RelatorioPedidosRecusadosLegitimos
    from config.app_config import AppConfig
    from data_io.reader import PagamentosReader, PedidosReader
    from data_io.writer import ParquetWriter
    from pipeline.pipeline_orchestrator import PipelineOrchestrator
    from schemas.pagamentos_schema import PagamentosSchema
    from schemas.pedidos_schema import PedidosSchema
    from spark.spark_session_manager import SparkSessionManager

    config = AppConfig()
    if args.ano_filtro is not None:
        import dataclasses
        config = dataclasses.replace(config, ano_filtro=args.ano_filtro)

    _configure_stdio_utf8_windows()

    logging.basicConfig(
        level=getattr(logging, config.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    logger = logging.getLogger("pipeline")

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
