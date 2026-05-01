"""Classe de orquestração do pipeline ETL."""

from __future__ import annotations

import logging

from business.relatorio_pedidos import RelatorioPedidosRecusadosLegitimos
from config.app_config import AppConfig
from data_io.reader import PagamentosReader, PedidosReader
from data_io.writer import ParquetWriter


class PipelineOrchestrator:
    """Orquestra as etapas do pipeline: leitura, transformação e escrita.

    Dependências são injetadas via construtor pelo ponto de composição
    (:func:`pipeline.cli.run_pipeline`).
    """

    def __init__(
        self,
        config: AppConfig,
        pedidos_reader: PedidosReader,
        pagamentos_reader: PagamentosReader,
        relatorio: RelatorioPedidosRecusadosLegitimos,
        writer: ParquetWriter,
    ) -> None:
        self._config = config
        self._pedidos_reader = pedidos_reader
        self._pagamentos_reader = pagamentos_reader
        self._relatorio = relatorio
        self._writer = writer
        self._logger = logging.getLogger(self.__class__.__name__)

    def run(self) -> None:
        """Executa o pipeline end-to-end."""
        self._logger.info(
            "Iniciando pipeline '%s'.", self._config.app_name
        )

        self._logger.info(
            "Lendo pedidos de: %s", self._config.pedidos_glob
        )
        pedidos_df = self._pedidos_reader.read(self._config.pedidos_glob)

        self._logger.info(
            "Lendo pagamentos de: %s", self._config.pagamentos_glob
        )
        pagamentos_df = self._pagamentos_reader.read(
            self._config.pagamentos_glob
        )

        relatorio_df = self._relatorio.gerar(pedidos_df, pagamentos_df)

        self._logger.info(
            "Escrevendo relatório em: %s", self._config.output_path
        )
        self._writer.write(relatorio_df, self._config.output_path)

        self._logger.info("Pipeline finalizado com sucesso.")
