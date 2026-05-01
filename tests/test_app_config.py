"""Testes da classe :class:`AppConfig`.

Variáveis de ambiente ``PROJETO_FINAL_*`` sobrescrevem os defaults (documentação).
"""

from __future__ import annotations

from pathlib import Path

import os
import pytest

import config.app_config as app_config_module

from config.app_config import AppConfig, resolve_input_directory, resolve_project_path


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


def test_app_config_le_variaveis_de_ambiente(monkeypatch):
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


def test_app_config_safe_int_rejeita_valor_nao_numerico(monkeypatch):
    """Env var inválida gera ValueError com nome da variável."""
    monkeypatch.setenv("PROJETO_FINAL_ANO_FILTRO", "abc")
    with pytest.raises(ValueError, match="PROJETO_FINAL_ANO_FILTRO"):
        AppConfig()


def test_resolve_input_directory_absoluto(tmp_path: Path):
    """Caminho absoluto é apenas normalizado (resolve)."""
    d = tmp_path / "in"
    d.mkdir()
    assert resolve_input_directory(str(d)) == d.resolve()


def test_resolve_input_directory_relativo_a_raiz_do_projeto(monkeypatch, tmp_path: Path):
    """Relativo à raiz do projeto: mesma base que ``pedidos_glob`` / validação pré-flight."""
    fake_root = tmp_path / "repo_root"
    fake_root.mkdir()
    nested = fake_root / "data" / "pedidos"
    nested.mkdir(parents=True)
    monkeypatch.setattr(app_config_module, "_project_root", lambda: fake_root)
    assert resolve_input_directory("data/pedidos") == nested.resolve()


def test_resolve_project_path_relativo_a_raiz_do_projeto(monkeypatch, tmp_path: Path):
    """Resolver genérico também ancora relativos na raiz do projeto."""
    fake_root = tmp_path / "repo_root"
    fake_root.mkdir()
    monkeypatch.setattr(app_config_module, "_project_root", lambda: fake_root)
    assert resolve_project_path("data/output/relatorio") == (
        fake_root / "data" / "output" / "relatorio"
    ).resolve()


def test_app_config_output_path_relativo_fica_ancorado_na_raiz(monkeypatch, tmp_path: Path):
    """Saída relativa via env não depende do cwd; fica ancorada na raiz do repo."""
    fake_root = tmp_path / "repo_root"
    fake_root.mkdir()
    monkeypatch.setattr(app_config_module, "_project_root", lambda: fake_root)
    monkeypatch.setenv("PROJETO_FINAL_OUTPUT_PATH", "data/output/custom")
    cfg = AppConfig()
    assert Path(cfg.output_path) == (fake_root / "data" / "output" / "custom").resolve()
