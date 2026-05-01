"""Testes da classe :class:`RelatorioPedidosRecusadosLegitimos`.

Cada teste foca em **uma** regra de negócio ou invariante do relatório.
Os DataFrames de entrada vêm dos readers sobre arquivos gzip temporários
(fixtures em :mod:`fixtures_datasets`), alinhado ao pipeline real.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from business.relatorio_pedidos import RelatorioPedidosRecusadosLegitimos


def test_relatorio_define_colunas_de_saida_esperadas():
    """A lista ``COLUNAS_SAIDA`` documenta o contrato do relatório."""
    assert RelatorioPedidosRecusadosLegitimos.COLUNAS_SAIDA == [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_criacao",
    ]


def test_construtor_armazena_ano_filtro():
    """O ano informado no construtor é usado no filtro ``year(data_criacao)``."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2030)
    assert relatorio.ano_filtro == 2030


def test_gerar_inclui_so_recusados_legitimos_do_ano(
    dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
):
    """Somente ``status=false`` + ``fraude=false`` + ano do pedido = ``ano_filtro``."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    resultado = relatorio.gerar(
        dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
    )

    linhas = resultado.collect()
    ids = [row["id_pedido"] for row in linhas]

    # Casos excluídos:
    #   p2: avaliacao_fraude.fraude=true  (fraude confirmada)
    #   p3: status=true                   (pagamento aprovado, não recusado)
    #   p4: data_criacao no ano 2024      (fora do ano_filtro=2025)
    assert set(ids) == {"p5", "p6", "p1"}, "Filtro de recusados+legítimos+ano incorreto"
    # Ordenação: RJ/BOLETO/2025-02-20 → SP/PIX/2025-02-01 → SP/PIX/2025-03-10
    assert ids == ["p5", "p6", "p1"], "Ordenação uf/forma_pagamento/data_criacao incorreta"
    assert set(resultado.columns) == set(
        RelatorioPedidosRecusadosLegitimos.COLUNAS_SAIDA
    )


def test_gerar_ordenacao_uf_forma_pagamento_data_criacao(
    dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
):
    """Ordenação estável: uf ASC, forma_pagamento ASC, data_criacao ASC."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    resultado = relatorio.gerar(
        dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
    )
    chaves = [
        (row["uf"], row["forma_pagamento"], row["data_criacao"])
        for row in resultado.collect()
    ]
    assert chaves == sorted(chaves)


def test_gerar_valor_total_e_produto_unitario_por_quantidade(
    dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
):
    """``valor_total = valor_unitario * quantidade`` (tipos numéricos)."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    linhas = (
        relatorio.gerar(dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo)
        .select("id_pedido", "valor_total")
        .collect()
    )
    mapa = {row["id_pedido"]: row["valor_total"] for row in linhas}

    assert mapa["p1"] == pytest.approx(1500.0 * 2)
    assert mapa["p5"] == pytest.approx(600.0 * 4)
    assert mapa["p6"] == pytest.approx(500.0 * 1)


def test_gerar_data_criacao_saida_formato_string_iso(
    dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
):
    """Saída usa string ISO (evita ``{}`` em export JSON a partir do Parquet)."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    rel = relatorio.gerar(dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo)
    amostra = rel.filter(F.col("id_pedido") == F.lit("p1")).select("data_criacao").first()
    assert amostra is not None
    assert isinstance(amostra["data_criacao"], str)
    assert "T" in amostra["data_criacao"]


def test_gerar_outro_ano_filtro_retorna_apenas_pedidos_daquele_ano(
    dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
):
    """Trocar ``ano_filtro`` restringe os pedidos antes do join."""
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2024)
    linhas = relatorio.gerar(
        dataframe_pedidos_exemplo, dataframe_pagamentos_exemplo
    ).collect()

    assert [row["id_pedido"] for row in linhas] == ["p4"]
    assert linhas[0]["valor_total"] == pytest.approx(4000.0)
