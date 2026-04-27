"""Testes da classe :class:`ParquetWriter`.

O encadeamento ``write.mode.option.parquet`` é verificado com mock para não
depender de workers Python em escrita Parquet (fonte de instabilidade no Windows).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from data_io.writer import ParquetWriter


def test_parquet_writer_construtor_guarda_modo_e_compressao():
    """Valores injetados ficam disponíveis para a escrita."""
    w = ParquetWriter(mode="overwrite", compression="snappy")
    assert w._mode == "overwrite"
    assert w._compression == "snappy"


def test_parquet_writer_write_encadeia_mode_opcao_compression_e_parquet():
    """``write`` delega ao DataFrameWriter com modo, compressão e caminho corretos."""
    df = MagicMock()
    chain = MagicMock()
    df.write = chain
    chain.mode.return_value = chain
    chain.option.return_value = chain

    writer = ParquetWriter(mode="overwrite", compression="snappy")
    writer.write(df, "/caminho/saida")

    chain.mode.assert_called_once_with("overwrite")
    chain.option.assert_called_once_with("compression", "snappy")
    chain.parquet.assert_called_once_with("/caminho/saida")
