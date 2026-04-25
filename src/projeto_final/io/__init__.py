"""Pacote de leitura e escrita de dados (I/O)."""

from projeto_final.io.reader import PagamentosReader, PedidosReader
from projeto_final.io.writer import ParquetWriter

__all__ = ["PedidosReader", "PagamentosReader", "ParquetWriter"]
