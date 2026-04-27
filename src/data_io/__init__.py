"""Pacote de leitura e escrita de dados (I/O)."""

from data_io.reader import PagamentosReader, PedidosReader
from data_io.writer import ParquetWriter

__all__ = ["PedidosReader", "PagamentosReader", "ParquetWriter"]
