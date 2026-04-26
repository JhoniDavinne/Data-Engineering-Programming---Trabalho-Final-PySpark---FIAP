"""Schema explícito do dataset de pedidos."""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


class PedidosSchema:
    """Schema explícito (sem inferência) para os arquivos CSV de pedidos.

    Estrutura conforme documentação do repositório
    ``datasets-csv-pedidos`` (separador ``;``, header presente, gzip).

    ``data_criacao`` é lida como string no CSV e convertida para timestamp em
    :class:`PedidosReader`, para aceitar ISO com/sem fração de segundos e
    somente data (``yyyy-MM-dd``), sem perder linhas por formato estrito.
    """

    SCHEMA: StructType = StructType(
        [
            StructField("id_pedido", StringType(), nullable=False),
            StructField("produto", StringType(), nullable=True),
            StructField("valor_unitario", DoubleType(), nullable=True),
            StructField("quantidade", LongType(), nullable=True),
            StructField("data_criacao", StringType(), nullable=True),
            StructField("uf", StringType(), nullable=True),
            StructField("id_cliente", LongType(), nullable=True),
        ]
    )

    @classmethod
    def get(cls) -> StructType:
        """Retorna o ``StructType`` explícito dos pedidos."""
        return cls.SCHEMA
