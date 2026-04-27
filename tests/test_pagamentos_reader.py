"""Testes da classe :class:`PagamentosReader`.

Valida leitura JSON Lines gzip com schema explícito e struct aninhada.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from data_io.reader import PagamentosReader
from schemas.pagamentos_schema import PagamentosSchema


def test_pagamentos_reader_opcoes_json():
    """Modo PERMISSIVE e formato de timestamp compatível com o dataset."""
    assert PagamentosReader.JSON_OPTIONS["mode"] == "PERMISSIVE"
    assert "timestampFormat" in PagamentosReader.JSON_OPTIONS


def test_pagamentos_reader_read_carrega_avaliacao_fraude_aninhada(
    spark, diretorio_fixtures_pedidos_pagamentos
):
    """Campo ``avaliacao_fraude.fraude`` acessível após a leitura."""
    reader = PagamentosReader(spark=spark, schema=PagamentosSchema.get())
    df = reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pagamentos.json.gz"))

    linha = df.filter(F.col("id_pedido") == "p1").first()
    assert linha is not None
    assert linha["avaliacao_fraude"]["fraude"] is False


def test_pagamentos_reader_read_conta_todas_as_linhas_jsonl(
    spark, diretorio_fixtures_pedidos_pagamentos
):
    """Uma linha JSON por registro de pagamento."""
    reader = PagamentosReader(spark=spark, schema=PagamentosSchema.get())
    df = reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pagamentos.json.gz"))
    assert df.count() == 6
