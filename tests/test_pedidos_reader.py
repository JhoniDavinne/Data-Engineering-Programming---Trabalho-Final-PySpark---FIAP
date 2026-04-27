"""Testes da classe :class:`PedidosReader`.

Foca em opções de leitura CSV e na conversão de ``data_criacao`` para timestamp.
"""

from __future__ import annotations

from pyspark.sql import functions as F
from data_io.reader import PedidosReader
from schemas.pedidos_schema import PedidosSchema


def test_pedidos_reader_opcoes_csv_alinhadas_ao_dataset_real():
    """Separador ``;``, header, UTF-8 e modo PERMISSIVE (igual ao professor)."""
    assert PedidosReader.CSV_OPTIONS["sep"] == ";"
    assert PedidosReader.CSV_OPTIONS["header"] == "true"
    assert PedidosReader.CSV_OPTIONS["encoding"] == "UTF-8"
    assert PedidosReader.CSV_OPTIONS["mode"] == "PERMISSIVE"


def test_pedidos_reader_read_parseia_data_criacao_como_timestamp(
    spark, diretorio_fixtures_pedidos_pagamentos
):
    """Após ``read``, ``data_criacao`` deixa de ser string bruta do CSV."""
    reader = PedidosReader(spark=spark, schema=PedidosSchema.get())
    df = reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pedidos.csv.gz"))

    tipo = dict(df.dtypes)["data_criacao"]
    assert tipo == "timestamp"

    amostra = df.filter(F.col("id_pedido") == "p1").select("data_criacao").first()
    assert amostra is not None
    assert amostra["data_criacao"] is not None


def test_pedidos_reader_read_preserva_todas_as_linhas_do_fixture(
    spark, diretorio_fixtures_pedidos_pagamentos
):
    """Nenhuma linha perdida por formato de data (coalesce de parsers)."""
    reader = PedidosReader(spark=spark, schema=PedidosSchema.get())
    df = reader.read(str(diretorio_fixtures_pedidos_pagamentos / "pedidos.csv.gz"))
    assert df.count() == 6
