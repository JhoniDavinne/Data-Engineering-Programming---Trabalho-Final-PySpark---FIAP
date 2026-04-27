"""Classes responsáveis pela leitura dos datasets de entrada."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def _parse_data_criacao(col: Column) -> Column:
    """Converte ``data_criacao`` textual em timestamp (vários formatos do CSV real)."""
    c = F.trim(F.regexp_replace(col, "^\uFEFF", ""))
    # Prefixo estável yyyy-MM-ddTHH:mm:ss (ignora fração e sufixo Z / timezone).
    iso_prefix = F.regexp_extract(c, r"^(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})", 1)
    iso_t = F.regexp_replace(iso_prefix, " ", "T")
    no_z = F.regexp_replace(c, "Z$", "")
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
        F.to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(no_z, "yyyy-MM-dd'T'HH:mm:ss.SSS"),
        F.when(iso_t != "", F.to_timestamp(iso_t, "yyyy-MM-dd'T'HH:mm:ss")),
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
