#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/venv}"
MANAGED_VENV="0"

if [[ -n "${PYTHON:-}" ]]; then
  PYTHON_BIN="$PYTHON"
elif [[ -x "$ROOT_DIR/src/producer/venv/bin/python3" ]]; then
  VENV_DIR="$ROOT_DIR/src/producer/venv"
  PYTHON_BIN="$VENV_DIR/bin/python3"
elif [[ -x "$ROOT_DIR/src/producer/venv/bin/python" ]]; then
  VENV_DIR="$ROOT_DIR/src/producer/venv"
  PYTHON_BIN="$VENV_DIR/bin/python"
elif [[ -x "/src/producer/venv/bin/python3" ]]; then
  VENV_DIR="/src/producer/venv"
  PYTHON_BIN="$VENV_DIR/bin/python3"
elif [[ -x "/src/producer/venv/bin/python" ]]; then
  VENV_DIR="/src/producer/venv"
  PYTHON_BIN="$VENV_DIR/bin/python"
elif [[ -x "$VENV_DIR/bin/python3" ]]; then
  PYTHON_BIN="$VENV_DIR/bin/python3"
  MANAGED_VENV="1"
elif [[ -x "$VENV_DIR/bin/python" ]]; then
  PYTHON_BIN="$VENV_DIR/bin/python"
  MANAGED_VENV="1"
else
  echo "Criando venv em $VENV_DIR..."
  python3 -m venv "$VENV_DIR"
  PYTHON_BIN="$VENV_DIR/bin/python3"
  MANAGED_VENV="1"
fi

if ! "$PYTHON_BIN" -c "import kafka, pandas, prometheus_client, psycopg" >/dev/null 2>&1; then
  if [[ ! -w "$VENV_DIR" ]]; then
    echo "Dependencias Python ausentes para: $PYTHON_BIN" >&2
    echo "Nao tenho permissao para instalar nesse ambiente." >&2
    echo "Use um venv gravavel, por exemplo:" >&2
    echo "  VENV_DIR=\$HOME/projeto-ssc0904-venv scripts/run_all_experiments.sh" >&2
    echo "ou instale manualmente:" >&2
    echo "  $PYTHON_BIN -m pip install -r $ROOT_DIR/requirements.txt" >&2
    exit 1
  fi

  echo "Instalando dependencias Python em $VENV_DIR..."
  "$PYTHON_BIN" -m pip install -r "$ROOT_DIR/requirements.txt"
fi

echo "$PYTHON_BIN"
