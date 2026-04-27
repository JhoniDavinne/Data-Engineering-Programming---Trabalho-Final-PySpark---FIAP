"""Testes da classe :class:`PipelineOrchestrator`.

Orquestração com dependências mockadas: ordem read → gerar → write.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from business.relatorio_pedidos import RelatorioPedidosRecusadosLegitimos
from config.app_config import AppConfig
from data_io.reader import PagamentosReader, PedidosReader
from data_io.writer import ParquetWriter
from pipeline.pipeline_orchestrator import PipelineOrchestrator


def test_pipeline_orchestrator_run_chama_read_gerar_write_na_ordem():
    """Contrato do método ``run``: ETL sequencial com paths do config."""
    config = MagicMock(spec=AppConfig)
    config.app_name = "pipeline-teste"
    config.pedidos_glob = "/fake/pedidos-*.csv.gz"
    config.pagamentos_glob = "/fake/pagamentos-*.json.gz"
    config.output_path = "/fake/output/relatorio"

    pedidos_reader = MagicMock(spec=PedidosReader)
    pagamentos_reader = MagicMock(spec=PagamentosReader)
    relatorio = MagicMock(spec=RelatorioPedidosRecusadosLegitimos)
    writer = MagicMock(spec=ParquetWriter)

    df_pedidos = MagicMock(name="df_pedidos")
    df_pagamentos = MagicMock(name="df_pagamentos")
    df_relatorio = MagicMock(name="df_relatorio")
    pedidos_reader.read.return_value = df_pedidos
    pagamentos_reader.read.return_value = df_pagamentos
    relatorio.gerar.return_value = df_relatorio

    orchestrator = PipelineOrchestrator(
        config=config,
        pedidos_reader=pedidos_reader,
        pagamentos_reader=pagamentos_reader,
        relatorio=relatorio,
        writer=writer,
    )
    orchestrator.run()

    pedidos_reader.read.assert_called_once_with("/fake/pedidos-*.csv.gz")
    pagamentos_reader.read.assert_called_once_with("/fake/pagamentos-*.json.gz")
    relatorio.gerar.assert_called_once_with(df_pedidos, df_pagamentos)
    writer.write.assert_called_once_with(df_relatorio, "/fake/output/relatorio")
