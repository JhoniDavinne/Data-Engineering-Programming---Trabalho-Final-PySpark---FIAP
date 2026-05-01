"""Classe de configuração centralizada do projeto."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _project_root() -> Path:
    """Retorna a raiz do projeto (dois níveis acima de ``src/config``)."""
    return Path(__file__).resolve().parents[2]


def resolve_project_path(path_str: str) -> Path:
    """Resolve caminho absoluto ancorado na raiz do projeto.

    - caminho absoluto: retorna normalizado com ``resolve()``
    - caminho relativo: ancora em ``_project_root()``
    """
    base = Path(path_str)
    if not base.is_absolute():
        base = _project_root() / base
    return base.resolve()


def resolve_input_directory(input_dir: str) -> Path:
    """Resolve diretório de entrada; caminhos relativos são relativos à raiz do projeto.

    Mesma regra usada em :meth:`AppConfig.pedidos_glob` / :meth:`AppConfig.pagamentos_glob`
    para manter validação pré-flight e leitura Spark alinhadas.
    """
    return resolve_project_path(input_dir)


def _safe_int(env_var: str, default: str) -> int:
    """Converte variável de ambiente para ``int`` com mensagem de erro clara."""
    raw = os.getenv(env_var, default)
    try:
        return int(raw)
    except ValueError:
        raise ValueError(
            f"Variável de ambiente {env_var}={raw!r} não é um inteiro válido."
        ) from None


@dataclass(frozen=True)
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
        default_factory=lambda: _safe_int("PROJETO_FINAL_ANO_FILTRO", "2025")
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
        default_factory=lambda: _safe_int("PROJETO_FINAL_SHUFFLE_PARTITIONS", "8")
    )
    timezone: str = field(
        default_factory=lambda: os.getenv("PROJETO_FINAL_TIMEZONE", "UTC")
    )

    def _spark_glob(self, input_dir: str, file_glob: str) -> str:
        """Caminho com glob no estilo POSIX (Spark no Windows lida melhor assim)."""
        return str((resolve_input_directory(input_dir) / file_glob).as_posix())

    def __post_init__(self) -> None:
        # Garante comportamento determinístico quando output_path vier relativo por env.
        object.__setattr__(
            self,
            "output_path",
            str(resolve_project_path(self.output_path)),
        )

    @property
    def pedidos_glob(self) -> str:
        """Glob pattern que cobre todos os arquivos gzip de pedidos."""
        return self._spark_glob(self.pedidos_input_path, "pedidos-*.csv.gz")

    @property
    def pagamentos_glob(self) -> str:
        """Glob pattern que cobre todos os arquivos gzip de pagamentos."""
        return self._spark_glob(self.pagamentos_input_path, "pagamentos-*.json.gz")
