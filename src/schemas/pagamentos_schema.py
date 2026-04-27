"""Schema explícito do dataset de pagamentos."""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class PagamentosSchema:
    """Schema explícito (sem inferência) para os arquivos JSON de pagamentos.

    O atributo ``avaliacao_fraude`` é um ``StructType`` aninhado, conforme
    documentação do repositório ``dataset-json-pagamentos``.
    """

    AVALIACAO_FRAUDE_SCHEMA: StructType = StructType(
        [
            StructField("fraude", BooleanType(), nullable=True),
            StructField("score", DoubleType(), nullable=True),
        ]
    )

    SCHEMA: StructType = StructType(
        [
            StructField("id_pedido", StringType(), nullable=False),
            StructField("forma_pagamento", StringType(), nullable=True),
            StructField("valor_pagamento", DoubleType(), nullable=True),
            StructField("status", BooleanType(), nullable=True),
            StructField("data_processamento", TimestampType(), nullable=True),
            StructField(
                "avaliacao_fraude",
                AVALIACAO_FRAUDE_SCHEMA,
                nullable=True,
            ),
        ]
    )

    @classmethod
    def get(cls) -> StructType:
        """Retorna o ``StructType`` explícito dos pagamentos."""
        return cls.SCHEMA
