"""Lógica de negócio do relatório de pedidos recusados e legítimos."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class RelatorioPedidosRecusadosLegitimos:
    """Produz o relatório de pedidos cujos pagamentos foram recusados
    (``status=false``) mas avaliados como legítimos (``fraude=false``).

    Atributos finais do relatório:

    - ``id_pedido``
    - ``uf``
    - ``forma_pagamento``
    - ``valor_total`` (``valor_unitario * quantidade``)
    - ``data_criacao``

    Regras adicionais:

    - filtra somente pedidos do ano informado em ``ano_filtro``;
    - ordena por ``uf``, ``forma_pagamento`` e ``data_criacao``.
    """

    COLUNAS_SAIDA = [
        "id_pedido",
        "uf",
        "forma_pagamento",
        "valor_total",
        "data_criacao",
    ]

    def __init__(self, ano_filtro: int) -> None:
        self._ano_filtro = ano_filtro
        self._logger = logging.getLogger(self.__class__.__name__)

    def gerar(
        self,
        pedidos_df: DataFrame,
        pagamentos_df: DataFrame,
    ) -> DataFrame:
        """Aplica os filtros, cálculos, join e ordenação do relatório."""
        try:
            self._logger.info(
                "Iniciando geração do relatório (ano_filtro=%s).",
                self._ano_filtro,
            )

            pagamentos_filtrados = self._filtrar_pagamentos(pagamentos_df)
            pedidos_filtrados = self._filtrar_pedidos_por_ano(pedidos_df)

            self._logger.info("Aplicando join entre pedidos e pagamentos.")
            relatorio = (
                pedidos_filtrados.alias("ped")
                .join(
                    pagamentos_filtrados.alias("pag"),
                    on="id_pedido",
                    how="inner",
                )
                .withColumn(
                    "valor_total",
                    (
                        F.col("valor_unitario").cast("double")
                        * F.col("quantidade").cast("double")
                    ),
                )
                .select(
                    F.col("id_pedido"),
                    F.col("uf"),
                    F.col("forma_pagamento"),
                    F.col("valor_total"),
                    F.col("data_criacao"),
                )
                .orderBy(
                    F.col("uf").asc(),
                    F.col("forma_pagamento").asc(),
                    F.col("data_criacao").asc(),
                )
            )

            self._logger.info("Relatório gerado com sucesso.")
            return relatorio
        except Exception as exc:
            self._logger.exception(
                "Falha ao gerar o relatório de pedidos recusados e legítimos: %s",
                exc,
            )
            raise

    def _filtrar_pagamentos(self, pagamentos_df: DataFrame) -> DataFrame:
        """Filtra pagamentos recusados e considerados legítimos pela avaliação
        de fraude."""
        self._logger.info(
            "Filtrando pagamentos com status=false e avaliacao_fraude.fraude=false."
        )
        return pagamentos_df.filter(
            (F.col("status") == F.lit(False))
            & (F.col("avaliacao_fraude.fraude") == F.lit(False))
        ).select(
            F.col("id_pedido"),
            F.col("forma_pagamento"),
        )

    def _filtrar_pedidos_por_ano(self, pedidos_df: DataFrame) -> DataFrame:
        """Mantém apenas pedidos cujo ano de criação é igual ao ``ano_filtro``."""
        self._logger.info(
            "Filtrando pedidos do ano %s.", self._ano_filtro
        )
        return pedidos_df.filter(
            F.year(F.col("data_criacao")) == F.lit(self._ano_filtro)
        )
