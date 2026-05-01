"""Testes da classe :class:`SparkSessionManager`.

Usa mocks para não depender de uma segunda ``SparkSession`` real (``getOrCreate``
reutilizaria a sessão dos outros testes e ``stop()`` quebraria a fixture global).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from config.app_config import AppConfig
from spark.spark_session_manager import SparkSessionManager


@patch("spark.spark_session_manager.SparkSession")
def test_spark_session_manager_get_or_create_configura_builder_com_app_config(
    mock_spark_cls,
):
    """``appName``, shuffle partitions e timezone vêm do :class:`AppConfig`."""
    import dataclasses

    cfg = dataclasses.replace(
        AppConfig(),
        app_name="app-de-teste",
        shuffle_partitions=3,
        timezone="America/Sao_Paulo",
    )

    chain = MagicMock()
    mock_spark_cls.builder = chain
    chain.appName.return_value = chain
    chain.config.return_value = chain
    sessao_falsa = MagicMock()
    chain.getOrCreate.return_value = sessao_falsa

    mgr = SparkSessionManager(cfg)
    assert mgr.get_or_create() is sessao_falsa

    chain.appName.assert_called_once_with("app-de-teste")
    chamadas_config = [c[0] for c in chain.config.call_args_list]
    assert ("spark.sql.shuffle.partitions", "3") in chamadas_config
    assert ("spark.sql.session.timeZone", "America/Sao_Paulo") in chamadas_config
    assert ("spark.sql.adaptive.enabled", "true") in chamadas_config


@patch("spark.spark_session_manager.SparkSession")
def test_spark_session_manager_get_or_create_reutiliza_instancia_interna(
    mock_spark_cls,
):
    """Segunda chamada não reconstrói o builder: usa ``_session`` em cache."""
    chain = MagicMock()
    mock_spark_cls.builder = chain
    chain.appName.return_value = chain
    chain.config.return_value = chain
    chain.getOrCreate.return_value = MagicMock()

    mgr = SparkSessionManager(AppConfig())
    a = mgr.get_or_create()
    b = mgr.get_or_create()
    assert a is b
    chain.getOrCreate.assert_called_once()


def test_spark_session_manager_stop_sem_criar_sessao_nao_explode():
    """``stop()`` com ``_session is None`` é no-op (seguro)."""
    mgr = SparkSessionManager(AppConfig())
    mgr.stop()
