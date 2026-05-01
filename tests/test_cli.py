"""Testes unitários da lógica de bootstrap da CLI."""

from __future__ import annotations

from pipeline import cli


def test_ensure_pyspark_python_limpa_variaveis_no_windows_com_espacos(
    monkeypatch,
) -> None:
    """No Windows com espaços no caminho, não deve forçar PYSPARK_*."""
    monkeypatch.setattr(cli.os, "name", "nt")
    monkeypatch.setattr(cli.sys, "executable", r"C:\Users\Nome Com Espaco\python.exe")
    monkeypatch.setenv("PYSPARK_PYTHON", "python-antigo")
    monkeypatch.setenv("PYSPARK_DRIVER_PYTHON", "python-antigo")

    cli._ensure_pyspark_python()

    assert cli.os.getenv("PYSPARK_PYTHON") is None
    assert cli.os.getenv("PYSPARK_DRIVER_PYTHON") is None


def test_ensure_pyspark_python_define_variaveis_em_cenario_padrao(monkeypatch) -> None:
    """Sem espaço crítico no executável, usa o mesmo Python do processo atual."""
    monkeypatch.setattr(cli.os, "name", "posix")
    monkeypatch.setattr(cli.sys, "executable", "/usr/bin/python3")
    monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
    monkeypatch.delenv("PYSPARK_DRIVER_PYTHON", raising=False)

    cli._ensure_pyspark_python()

    assert cli.os.getenv("PYSPARK_PYTHON") == "/usr/bin/python3"
    assert cli.os.getenv("PYSPARK_DRIVER_PYTHON") == "/usr/bin/python3"


def test_configure_stdio_utf8_windows_chama_reconfigure_no_windows(monkeypatch) -> None:
    encodings: list[str] = []

    class _FakeStream:
        def reconfigure(self, *, encoding: str) -> None:
            encodings.append(encoding)

    monkeypatch.setattr(cli.sys, "platform", "win32")
    monkeypatch.setattr(cli.sys, "stdout", _FakeStream())
    monkeypatch.setattr(cli.sys, "stderr", _FakeStream())

    cli._configure_stdio_utf8_windows()

    assert encodings == ["utf-8", "utf-8"]


def test_configure_stdio_utf8_windows_no_op_fora_do_windows(monkeypatch) -> None:
    monkeypatch.setattr(cli.sys, "platform", "linux")
    called = False

    def _boom(*_a, **_k):
        nonlocal called
        called = True

    monkeypatch.setattr(cli.sys.stdout, "reconfigure", _boom)

    cli._configure_stdio_utf8_windows()

    assert called is False
