"""Classe responsável por gerenciar o ciclo de vida da SparkSession."""

from __future__ import annotations

from typing import Optional

from pyspark.sql import SparkSession

from projeto_final.config.app_config import AppConfig


class SparkSessionManager:
    """Encapsula a criação e o encerramento de uma ``SparkSession``.

    A instância é criada de forma preguiçosa (lazy) via ``get_or_create`` e
    configurada a partir do :class:`AppConfig` injetado no construtor.
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._session: Optional[SparkSession] = None

    def get_or_create(self) -> SparkSession:
        """Cria (ou retorna) a ``SparkSession`` configurada."""
        if self._session is None:
            builder = (
                SparkSession.builder.appName(self._config.app_name)
                .config(
                    "spark.sql.shuffle.partitions",
                    str(self._config.shuffle_partitions),
                )
                .config("spark.sql.session.timeZone", self._config.timezone)
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.ui.showConsoleProgress", "false")
            )
            self._session = builder.getOrCreate()
            self._session.sparkContext.setLogLevel("WARN")
        return self._session

    def stop(self) -> None:
        """Encerra a ``SparkSession`` caso tenha sido criada."""
        if self._session is not None:
            self._session.stop()
            self._session = None
