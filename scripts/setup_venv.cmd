@echo off
setlocal EnableExtensions
cd /d "%~dp0.."
if not exist "requirements.txt" (
  echo ERRO: requirements.txt nao encontrado. Abra o CMD na raiz do repositorio ou execute: scripts\setup_venv.cmd
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  if errorlevel 1 exit /b 1
) else (
  echo Usando .venv existente. Para recriar, apague a pasta .venv e rode este script de novo.
)

call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

echo.
echo OK: dependencias instaladas no .venv
echo Proximo: python main.py   (nesta mesma janela o venv ja esta ativo)
