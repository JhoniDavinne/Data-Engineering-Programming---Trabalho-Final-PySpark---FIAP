"""Lógica de negócio do relatório de pedidos recusados e legítimos."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


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

    #: Contrato documentacional das colunas do relatório final.
    #: **Não** é utilizada programaticamente no ``select()`` de ``gerar()``
    #: (que faz o select explícito com ``date_format``). Serve como referência
    #: para testes e documentação do schema de saída.
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

    @property
    def ano_filtro(self) -> int:
        """Ano usado no filtro de pedidos (somente leitura)."""
        return self._ano_filtro

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

            # Passo 1: computar valor_total com data_criacao ainda como TimestampType.
            # O sort DEVE acontecer sobre o Timestamp (ordenação temporal correta),
            # não sobre a string ISO (que seria lexicográfica e produziria resultados
            # incorretos para timestamps em meses/anos diferentes).
            base = (
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
            )

            # Passo 2: ordenar pelo Timestamp original (antes da conversão para string).
            # Separar em duas etapas garante que o Catalyst optimizer não reorganize
            # o plano de forma a aplicar o date_format antes do sort.
            relatorio = (
                base
                .orderBy(
                    F.col("uf").asc(),
                    F.col("forma_pagamento").asc(),
                    F.col("data_criacao").asc(),
                )
                .select(
                    F.col("id_pedido"),
                    F.col("uf"),
                    F.col("forma_pagamento"),
                    F.col("valor_total"),
                    # String ISO evita `{}` em exportadores JSON que não serializam
                    # TIMESTAMP do Parquet corretamente.
                    F.date_format(
                        F.col("data_criacao"), "yyyy-MM-dd'T'HH:mm:ss"
                    ).alias("data_criacao"),
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
