"""Testes da classe :class:`PagamentosSchema`.

Garante o struct aninhado ``avaliacao_fraude`` e os tipos do JSON de pagamentos.
"""

from __future__ import annotations

from pyspark.sql.types import BooleanType, DoubleType, StringType, TimestampType

from schemas.pagamentos_schema import PagamentosSchema


def test_pagamentos_schema_get_retorna_schema_documentado():
    """``get()`` expõe o ``StructType`` principal."""
    assert PagamentosSchema.get() is PagamentosSchema.SCHEMA


def test_pagamentos_schema_avaliacao_fraude_aninhada():
    """``avaliacao_fraude`` contém ``fraude`` (bool) e ``score`` (double)."""
    schema = PagamentosSchema.get()
    campo = next(f for f in schema.fields if f.name == "avaliacao_fraude")
    internos = {f.name: f.dataType for f in campo.dataType.fields}
    assert internos["fraude"] == BooleanType()
    assert internos["score"] == DoubleType()


def test_pagamentos_schema_colunas_principais():
    """Tipos das colunas de primeiro nível (JSON Lines)."""
    campos = {f.name: f.dataType for f in PagamentosSchema.get().fields}
    assert campos["id_pedido"] == StringType()
    assert campos["forma_pagamento"] == StringType()
    assert campos["valor_pagamento"] == DoubleType()
    assert campos["status"] == BooleanType()
    assert campos["data_processamento"] == TimestampType()
