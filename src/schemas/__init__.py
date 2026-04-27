"""Pacote de schemas explícitos (StructType) dos datasets."""

from schemas.pagamentos_schema import PagamentosSchema
from schemas.pedidos_schema import PedidosSchema

__all__ = ["PedidosSchema", "PagamentosSchema"]
