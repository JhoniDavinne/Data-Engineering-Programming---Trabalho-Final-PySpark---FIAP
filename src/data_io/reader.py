"""Classes responsáveis pela leitura dos datasets de entrada."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def _parse_data_criacao(col: Column) -> Column:
    """Converte ``data_criacao`` textual em timestamp (vários formatos do CSV real)."""
    c = F.trim(F.regexp_replace(col, "^\uFEFF", ""))
    # Remove sufixo "Z" para JVMs que não aceitam "Z" como offset em XXX.
    no_z = F.regexp_replace(c, "Z$", "")
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
        # no_z: tenta sem o sufixo Z para Spark/JVM que não aceita "Z" como offset.
        F.to_timestamp(no_z, "yyyy-MM-dd'T'HH:mm:ss.SSS"),   # "...T12:00:00.123Z"
        F.to_timestamp(no_z, "yyyy-MM-dd'T'HH:mm:ss"),        # "...T12:00:00Z"
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy-MM-dd"),
        F.to_timestamp(c, "dd/MM/yyyy HH:mm:ss"),
        F.to_timestamp(c, "dd/MM/yyyy"),
    )


class PedidosReader:
    """Leitor do dataset de pedidos (CSV gzip com separador ``;``).

    Utiliza schema explícito via ``.schema(...)`` para evitar inferência.
    """

    CSV_OPTIONS = {
        "sep": ";",
        "header": "true",
        "encoding": "UTF-8",
        "mode": "PERMISSIVE",
    }

    def __init__(self, spark: SparkSession, schema: StructType) -> None:
        self._spark = spark
        self._schema = schema

    def read(self, path: str) -> DataFrame:
        """Lê um ou mais arquivos CSV (suporta glob) retornando um DataFrame."""
        reader = self._spark.read.schema(self._schema)
        for option, value in self.CSV_OPTIONS.items():
            reader = reader.option(option, value)
        raw = reader.csv(path)
        return raw.withColumn("data_criacao", _parse_data_criacao(F.col("data_criacao")))


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
