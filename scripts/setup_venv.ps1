#Requires -Version 5.1
$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

if (-not (Test-Path (Join-Path $RepoRoot "requirements.txt"))) {
    Write-Error "requirements.txt nao encontrado em: $RepoRoot"
}

# Permite Activate.ps1 apenas neste processo (sem alterar a politica da maquina).
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned -Force

$venvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    python -m venv .venv
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} else {
    Write-Host "Usando .venv existente. Para recriar, apague a pasta .venv e rode este script de novo."
}

& (Join-Path $RepoRoot ".venv\Scripts\Activate.ps1")

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "OK: dependencias instaladas no .venv" -ForegroundColor Green
Write-Host "Proximo: python main.py" -ForegroundColor Green
