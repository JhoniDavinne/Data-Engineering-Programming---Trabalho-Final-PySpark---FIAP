#!/usr/bin/env bash
# Git Bash / MSYS: nao rode "python -m venv" de novo se .venv ja existe — no Windows isso
# costuma dar Errno 13 (Permission denied) ao substituir python.exe bloqueado.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

RECREATE=0
for arg in "$@"; do
  case "$arg" in
    --recreate|-f) RECREATE=1 ;;
  esac
done

if [[ ! -f requirements.txt ]]; then
  echo "ERRO: requirements.txt nao encontrado em $REPO_ROOT" >&2
  exit 1
fi

remove_venv_windows() {
  if [[ ! -d .venv ]]; then
    return 0
  fi
  echo "Removendo .venv (via cmd para evitar arquivos bloqueados no Windows)..."
  if command -v cygpath >/dev/null 2>&1; then
    local win_root
    win_root="$(cygpath -w "$REPO_ROOT")"
    MSYS2_ARG_CONV_EXCL='*' cmd.exe //c "cd /d \"${win_root}\" && if exist .venv rd /s /q .venv"
  else
    rm -rf .venv
  fi
}

if [[ "$RECREATE" -eq 1 ]]; then
  echo "Dica: rode 'deactivate' se o prompt mostrar (.venv) e feche outros terminais que usem este projeto."
  remove_venv_windows
fi

if [[ -f .venv/Scripts/python.exe ]] || [[ -f .venv/bin/python ]]; then
  echo "Usando .venv existente (pulando 'python -m venv'). Para recriar: bash scripts/setup_venv.sh --recreate"
else
  python -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/Scripts/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo ""
echo "OK: dependencias instaladas no .venv"
echo "Proximo: python main.py"
