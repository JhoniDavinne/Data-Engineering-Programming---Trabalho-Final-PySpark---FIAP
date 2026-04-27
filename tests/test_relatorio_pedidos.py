"""Testes unitários da classe RelatorioPedidosRecusadosLegitimos.

Os datasets de teste são gravados em arquivos temporários CSV/JSON e lidos
através dos próprios ``PedidosReader`` e ``PagamentosReader``. Isso evita
dependências do ``SparkSession.createDataFrame`` (que exige a execução de
Python workers adicionais — uma conhecida fonte de flakiness no Windows)
e, de quebra, cobre também a camada de I/O em um fluxo end-to-end.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from business.relatorio_pedidos import RelatorioPedidosRecusadosLegitimos
from data_io.reader import PagamentosReader, PedidosReader
from schemas.pagamentos_schema import PagamentosSchema
from schemas.pedidos_schema import PedidosSchema


PEDIDOS_CSV = """\
id_pedido;produto;valor_unitario;quantidade;data_criacao;uf;id_cliente
p1;NOTEBOOK;1500.0;2;2025-03-10T12:00:00;SP;1
p2;CELULAR;1000.0;1;2025-04-15T09:00:00;RJ;2
p3;TABLET;1100.0;3;2025-01-01T00:00:00;MG;3
p4;GELADEIRA;2000.0;1;2024-12-31T23:59:59;SP;4
p5;MONITOR;600.0;4;2025-02-20T08:30:00;RJ;5
p6;HOMETHEATER;500.0;1;2025-02-01T10:00:00;SP;6
"""


PAGAMENTOS_JSONL = [
    {  # p1: recusado + legítimo -> entra
        "id_pedido": "p1",
        "forma_pagamento": "PIX",
        "valor_pagamento": 3000.0,
        "status": False,
        "data_processamento": "2025-03-10T12:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.10},
    },
    {  # p2: recusado + fraude -> NÃO entra
        "id_pedido": "p2",
        "forma_pagamento": "CARTAO_CREDITO",
        "valor_pagamento": 1000.0,
        "status": False,
        "data_processamento": "2025-04-15T09:05:00",
        "avaliacao_fraude": {"fraude": True, "score": 0.95},
    },
    {  # p3: aprovado -> NÃO entra
        "id_pedido": "p3",
        "forma_pagamento": "BOLETO",
        "valor_pagamento": 3300.0,
        "status": True,
        "data_processamento": "2025-01-01T00:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.05},
    },
    {  # p4: recusado + legítimo, mas o pedido é de 2024 -> NÃO entra
        "id_pedido": "p4",
        "forma_pagamento": "PIX",
        "valor_pagamento": 2000.0,
        "status": False,
        "data_processamento": "2024-12-31T23:59:59",
        "avaliacao_fraude": {"fraude": False, "score": 0.20},
    },
    {  # p5: recusado + legítimo -> entra
        "id_pedido": "p5",
        "forma_pagamento": "BOLETO",
        "valor_pagamento": 2400.0,
        "status": False,
        "data_processamento": "2025-02-20T08:35:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.30},
    },
    {  # p6: recusado + legítimo -> entra (mesmo UF/forma que p1, data anterior)
        "id_pedido": "p6",
        "forma_pagamento": "PIX",
        "valor_pagamento": 500.0,
        "status": False,
        "data_processamento": "2025-02-01T10:05:00",
        "avaliacao_fraude": {"fraude": False, "score": 0.15},
    },
]


@pytest.fixture(scope="module")
def fixtures_dir(tmp_path_factory) -> Path:
    """Cria arquivos gzip (CSV de pedidos e JSONL de pagamentos) para o teste."""
    base = tmp_path_factory.mktemp("fixtures")
    pedidos_path = base / "pedidos.csv.gz"
    pagamentos_path = base / "pagamentos.json.gz"

    with gzip.open(pedidos_path, mode="wt", encoding="utf-8") as fp:
        fp.write(PEDIDOS_CSV)

    with gzip.open(pagamentos_path, mode="wt", encoding="utf-8") as fp:
        for obj in PAGAMENTOS_JSONL:
            fp.write(json.dumps(obj))
            fp.write("\n")

    return base


@pytest.fixture(scope="module")
def pedidos_df(spark: SparkSession, fixtures_dir: Path):
    reader = PedidosReader(spark=spark, schema=PedidosSchema.get())
    return reader.read(str(fixtures_dir / "pedidos.csv.gz"))


@pytest.fixture(scope="module")
def pagamentos_df(spark: SparkSession, fixtures_dir: Path):
    reader = PagamentosReader(spark=spark, schema=PagamentosSchema.get())
    return reader.read(str(fixtures_dir / "pagamentos.json.gz"))


def test_gerar_retorna_apenas_pedidos_recusados_legitimos_de_2025(
    pedidos_df, pagamentos_df
):
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    resultado = relatorio.gerar(pedidos_df, pagamentos_df)

    linhas = resultado.collect()
    ids = [row["id_pedido"] for row in linhas]

    # Ordenação esperada: uf ASC, forma_pagamento ASC, data_criacao ASC.
    # - p5: RJ / BOLETO / 2025-02-20
    # - p6: SP / PIX   / 2025-02-01
    # - p1: SP / PIX   / 2025-03-10
    assert ids == ["p5", "p6", "p1"], (
        "Esperava pedidos ordenados por uf, forma_pagamento, data_criacao."
    )
    assert set(resultado.columns) == {
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_criacao",
    }


def test_gerar_calcula_valor_total_como_unitario_vezes_quantidade(
    pedidos_df, pagamentos_df
):
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2025)
    linhas = (
        relatorio.gerar(pedidos_df, pagamentos_df)
        .select("id_pedido", "valor_total")
        .collect()
    )
    mapa = {row["id_pedido"]: row["valor_total"] for row in linhas}

    assert mapa["p1"] == pytest.approx(1500.0 * 2)
    assert mapa["p5"] == pytest.approx(600.0 * 4)
    assert mapa["p6"] == pytest.approx(500.0 * 1)


def test_gerar_permite_reutilizar_pipeline_para_outro_ano(
    pedidos_df, pagamentos_df
):
    relatorio = RelatorioPedidosRecusadosLegitimos(ano_filtro=2024)
    linhas = relatorio.gerar(pedidos_df, pagamentos_df).collect()

    assert [row["id_pedido"] for row in linhas] == ["p4"]
    assert linhas[0]["valor_total"] == pytest.approx(2000.0)
