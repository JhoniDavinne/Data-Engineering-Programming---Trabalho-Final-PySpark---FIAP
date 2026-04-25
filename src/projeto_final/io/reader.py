"""Classes responsáveis pela leitura dos datasets de entrada."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


class PedidosReader:
    """Leitor do dataset de pedidos (CSV gzip com separador ``;``).

    Utiliza schema explícito via ``.schema(...)`` para evitar inferência.
    """

    CSV_OPTIONS = {
        "sep": ";",
        "header": "true",
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
        "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss",
    }

    def __init__(self, spark: SparkSession, schema: StructType) -> None:
        self._spark = spark
        self._schema = schema

    def read(self, path: str) -> DataFrame:
        """Lê um ou mais arquivos CSV (suporta glob) retornando um DataFrame."""
        reader = self._spark.read.schema(self._schema)
        for option, value in self.CSV_OPTIONS.items():
            reader = reader.option(option, value)
        return reader.csv(path)


class PagamentosReader:
    """Leitor do dataset de pagamentos (JSON Lines gzip).

    Utiliza schema explícito com ``avaliacao_fraude`` aninhado.
    """

    JSON_OPTIONS = {
        "mode": "PERMISSIVE",
        "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss",
    }

    def __init__(self, spark: SparkSession, schema: StructType) -> None:
        self._spark = spark
        self._schema = schema

    def read(self, path: str) -> DataFrame:
        """Lê um ou mais arquivos JSON (suporta glob) retornando um DataFrame."""
        reader = self._spark.read.schema(self._schema)
        for option, value in self.JSON_OPTIONS.items():
            reader = reader.option(option, value)
        return reader.json(path)
