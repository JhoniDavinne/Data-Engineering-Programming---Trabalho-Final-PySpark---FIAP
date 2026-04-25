"""Classe de configuração centralizada do projeto."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _project_root() -> Path:
    """Retorna a raiz do projeto (dois níveis acima de src/projeto_final/config)."""
    return Path(__file__).resolve().parents[3]


@dataclass
class AppConfig:
    """Configurações centralizadas da aplicação.

    Valores padrão apontam para os datasets locais em ``data/input`` e o
    parquet de saída em ``data/output``. Todos os atributos podem ser
    sobrescritos via variáveis de ambiente (prefixo ``PROJETO_FINAL_``).
    """

    app_name: str = field(
        default_factory=lambda: os.getenv(
            "PROJETO_FINAL_APP_NAME",
            "pedidos-recusados-legitimos",
        )
    )
    ano_filtro: int = field(
        default_factory=lambda: int(os.getenv("PROJETO_FINAL_ANO_FILTRO", "2025"))
    )
    log_level: str = field(
        default_factory=lambda: os.getenv("PROJETO_FINAL_LOG_LEVEL", "INFO")
    )
    pedidos_input_path: str = field(
        default_factory=lambda: os.getenv(
            "PROJETO_FINAL_PEDIDOS_PATH",
            str(
                _project_root()
                / "data"
                / "input"
                / "datasets-csv-pedidos"
                / "data"
                / "pedidos"
            ),
        )
    )
    pagamentos_input_path: str = field(
        default_factory=lambda: os.getenv(
            "PROJETO_FINAL_PAGAMENTOS_PATH",
            str(
                _project_root()
                / "data"
                / "input"
                / "dataset-json-pagamentos"
                / "data"
                / "pagamentos"
            ),
        )
    )
    output_path: str = field(
        default_factory=lambda: os.getenv(
            "PROJETO_FINAL_OUTPUT_PATH",
            str(
                _project_root()
                / "data"
                / "output"
                / "relatorio_pedidos_recusados_legitimos"
            ),
        )
    )
    output_compression: str = field(
        default_factory=lambda: os.getenv(
            "PROJETO_FINAL_OUTPUT_COMPRESSION", "snappy"
        )
    )
    output_mode: str = field(
        default_factory=lambda: os.getenv("PROJETO_FINAL_OUTPUT_MODE", "overwrite")
    )
    shuffle_partitions: int = field(
        default_factory=lambda: int(
            os.getenv("PROJETO_FINAL_SHUFFLE_PARTITIONS", "8")
        )
    )
    timezone: str = field(
        default_factory=lambda: os.getenv("PROJETO_FINAL_TIMEZONE", "UTC")
    )

    @property
    def pedidos_glob(self) -> str:
        """Glob pattern que cobre todos os arquivos gzip de pedidos."""
        return str(Path(self.pedidos_input_path) / "pedidos-*.csv.gz")

    @property
    def pagamentos_glob(self) -> str:
        """Glob pattern que cobre todos os arquivos gzip de pagamentos."""
        return str(Path(self.pagamentos_input_path) / "pagamentos-*.json.gz")
