"""Testes da classe :class:`AppConfig`.

Variáveis de ambiente ``PROJETO_FINAL_*`` sobrescrevem os defaults (documentação).
"""

from __future__ import annotations

from pathlib import Path

import os

from config.app_config import AppConfig


def test_app_config_defaults_usam_prefixo_projeto_final(monkeypatch):
    """Sem env, valores padrão são determinísticos."""
    for key in list(os.environ.keys()):
        if key.startswith("PROJETO_FINAL_"):
            monkeypatch.delenv(key, raising=False)
    cfg = AppConfig()
    assert cfg.app_name == "pedidos-recusados-legitimos"
    assert cfg.ano_filtro == 2025
    assert cfg.output_compression == "snappy"
    assert cfg.output_mode == "overwrite"


def test_app_config_lê_variaveis_de_ambiente(monkeypatch):
    """Cada atributo pode ser configurado por env."""
    monkeypatch.setenv("PROJETO_FINAL_APP_NAME", "meu-app")
    monkeypatch.setenv("PROJETO_FINAL_ANO_FILTRO", "2024")
    monkeypatch.setenv("PROJETO_FINAL_LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("PROJETO_FINAL_SHUFFLE_PARTITIONS", "16")
    monkeypatch.setenv("PROJETO_FINAL_TIMEZONE", "UTC")
    cfg = AppConfig()
    assert cfg.app_name == "meu-app"
    assert cfg.ano_filtro == 2024
    assert cfg.log_level == "DEBUG"
    assert cfg.shuffle_partitions == 16
    assert cfg.timezone == "UTC"


def test_app_config_pedidos_glob_anexa_pattern_csv_gz(tmp_path: Path, monkeypatch):
    """``pedidos_glob`` aponta para ``pedidos-*.csv.gz`` sob o diretório configurado."""
    monkeypatch.setenv("PROJETO_FINAL_PEDIDOS_PATH", str(tmp_path))
    cfg = AppConfig()
    assert cfg.pedidos_glob.endswith("pedidos-*.csv.gz")
    assert tmp_path.resolve().as_posix() in cfg.pedidos_glob.replace("\\", "/")


def test_app_config_pagamentos_glob_anexa_pattern_json_gz(tmp_path: Path, monkeypatch):
    """``pagamentos_glob`` usa o padrão dos arquivos gzip JSON."""
    monkeypatch.setenv("PROJETO_FINAL_PAGAMENTOS_PATH", str(tmp_path))
    cfg = AppConfig()
    assert cfg.pagamentos_glob.endswith("pagamentos-*.json.gz")
