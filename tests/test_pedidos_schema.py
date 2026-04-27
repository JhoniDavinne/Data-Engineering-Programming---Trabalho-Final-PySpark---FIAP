"""Testes da classe :class:`PedidosSchema`.

Validam o **contrato de tipos** do CSV de pedidos (sem inferência em runtime).
"""

from __future__ import annotations

from pyspark.sql.types import DoubleType, LongType, StringType

from schemas.pedidos_schema import PedidosSchema


def test_pedidos_schema_get_retorna_mesma_estrutura_que_constante():
    """``get()`` deve expor o mesmo ``StructType`` documentado em ``SCHEMA``."""
    assert PedidosSchema.get() is PedidosSchema.SCHEMA


def test_pedidos_schema_contem_colunas_obrigatorias_do_negocio():
    """Campos alinhados ao dataset ``datasets-csv-pedidos``."""
    campos = {f.name: f.dataType for f in PedidosSchema.get().fields}
    assert campos["id_pedido"] == StringType()
    assert campos["produto"] == StringType()
    assert campos["valor_unitario"] == DoubleType()
    assert campos["quantidade"] == LongType()
    assert campos["data_criacao"] == StringType()
    assert campos["uf"] == StringType()
    assert campos["id_cliente"] == LongType()


def test_pedidos_schema_id_pedido_nao_nulo():
    """Chave de negócio: ``id_pedido`` nullable=False no schema explícito."""
    campo = next(f for f in PedidosSchema.get().fields if f.name == "id_pedido")
    assert campo.nullable is False
