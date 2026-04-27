"""Classe responsável pela escrita de dados em formato Parquet."""

from __future__ import annotations

from pyspark.sql import DataFrame


class ParquetWriter:
    """Escritor de DataFrames em arquivos Parquet.

    Permite configurar o modo de escrita e a compressão no construtor.
    """

    def __init__(
        self,
        mode: str = "overwrite",
        compression: str = "snappy",
    ) -> None:
        self._mode = mode
        self._compression = compression

    def write(self, df: DataFrame, path: str) -> None:
        """Grava o DataFrame no caminho ``path`` em formato Parquet."""
        (
            df.write.mode(self._mode)
            .option("compression", self._compression)
            .parquet(path)
        )
