#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS_FILE="$ROOT_DIR/experiments/scenarios.csv"
ITERATION="${ITERATION:-01}"
OVERWRITE="${OVERWRITE:-0}"
DRY_RUN="${DRY_RUN:-0}"
AUTO_ITERATION="${AUTO_ITERATION:-0}"
RESET_TOPICS_BETWEEN_FRAMEWORKS="${RESET_TOPICS_BETWEEN_FRAMEWORKS:-1}"
FAILED_RUNS=()
SKIPPED_RUNS=()

## @brief Verifica se o cenário informado representa uma falha.
## @param scenario Valor do parâmetro `scenario`.
## @return Código de saída zero em caso de sucesso.
is_fault_scenario() {
  local scenario="$1"
  [[ "$scenario" == fault_* ]]
}

## @brief Decide se o cenário pertence ao modo da campanha.
## @param scenario Nome do cenário avaliado.
## @return Código de saída zero em caso de sucesso.
should_run_scenario() {
  local scenario="$1"

  if [[ -n "${FAULT_SERVICE:-}" ]]; then
    is_fault_scenario "$scenario"
  else
    ! is_fault_scenario "$scenario"
  fi
}

## @brief Encontra a próxima iteração livre para framework e cenário.
## @param framework Valor do parâmetro `framework`.
## @param scenario Valor do parâmetro `scenario`.
## @return Código de saída zero em caso de sucesso.
next_iteration() {
  local framework="$1"
  local scenario="$2"
  local candidate
  local run_id

  for number in $(seq 1 99); do
    candidate="$(printf "%02d" "$number")"
    run_id="${framework}_${scenario}_${candidate}"
    if [[ ! -f "$ROOT_DIR/results/raw/${run_id}.jsonl" ]] && ! run_exists_in_database "$run_id"; then
      echo "$candidate"
      return 0
    fi
  done

  echo "Nao ha iteracao livre entre 01 e 99 para ${framework}_${scenario}." >&2
  return 1
}

## @brief Verifica se uma execução já existe no PostgreSQL.
## @param run_id Valor do parâmetro `run_id`.
## @return Código de saída zero em caso de sucesso.
run_exists_in_database() {
  local run_id="$1"

  if [[ -z "${DATABASE_URL:-}" ]]; then
    return 1
  fi

  "$(database_python_bin)" - "$run_id" <<'PY'
import os
import sys

run_id = sys.argv[1]
database_url = os.environ.get("DATABASE_URL")

try:
    import psycopg
except ImportError:
    sys.exit(2)

query = """
select exists (
    select 1
    from processed_events
    where to_regclass('processed_events') is not null
      and run_id = %s
    union all
    select 1
    from processed_run_summaries
    where to_regclass('processed_run_summaries') is not null
      and run_id = %s
    union all
    select 1
    from processed_event_buckets
    where to_regclass('processed_event_buckets') is not null
      and run_id = %s
    union all
    select 1
    from run_metadata
    where to_regclass('run_metadata') is not null
      and run_id = %s
    limit 1
)
"""

with psycopg.connect(database_url) as connection:
    with connection.cursor() as cursor:
        cursor.execute(query, (run_id, run_id, run_id, run_id))
        exists = cursor.fetchone()[0]

sys.exit(0 if exists else 1)
PY
  case "$?" in
    0)
      return 0
      ;;
    1)
      return 1
      ;;
    *)
      echo "Aviso: nao foi possivel consultar DATABASE_URL para AUTO_ITERATION; usando apenas arquivos locais." >&2
      return 1
      ;;
  esac
}

## @brief Seleciona o interpretador Python usado para acessar o banco.
## @return Código de saída zero em caso de sucesso.
database_python_bin() {
  if [[ -n "${PYTHON:-}" ]]; then
    echo "$PYTHON"
  elif [[ -x "$ROOT_DIR/venv/bin/python3" ]]; then
    echo "$ROOT_DIR/venv/bin/python3"
  elif [[ -x "$ROOT_DIR/src/producer/venv/bin/python3" ]]; then
    echo "$ROOT_DIR/src/producer/venv/bin/python3"
  elif [[ -x "$ROOT_DIR/venv-dashboard/bin/python" ]]; then
    echo "$ROOT_DIR/venv-dashboard/bin/python"
  else
    echo "python3"
  fi
}

cd "$ROOT_DIR"

if [[ "$DRY_RUN" != "1" ]]; then
  scripts/prepare_runtime.sh
  docker compose up -d --build postgres zookeeper kafka cadvisor prometheus grafana
fi

for FRAMEWORK in spark flink; do
  if [[ "$RESET_TOPICS_BETWEEN_FRAMEWORKS" == "1" && "$DRY_RUN" != "1" ]]; then
    echo "========================================"
    echo "Resetando topicos antes dos testes $FRAMEWORK"
    echo "========================================"
    scripts/stop_streaming_jobs.sh
    RESET_TOPICS=1 bash scripts/reset_kafka.sh
  fi

  while IFS=, read -r SCENARIO _EPS _DURATION <&3; do
    if [[ "$SCENARIO" == "scenario" ]]; then
      continue
    fi

    if ! should_run_scenario "$SCENARIO"; then
      continue
    fi

    RUN_ITERATION="$ITERATION"
    if [[ "$AUTO_ITERATION" == "1" && "$OVERWRITE" != "1" ]]; then
      RUN_ITERATION="$(next_iteration "$FRAMEWORK" "$SCENARIO")"
    fi

    RUN_ID="${FRAMEWORK}_${SCENARIO}_${RUN_ITERATION}"
    OUTPUT_FILE="$ROOT_DIR/results/raw/${RUN_ID}.jsonl"

    echo "========================================"
    echo "Executando $FRAMEWORK / $SCENARIO / iteracao $RUN_ITERATION"
    echo "========================================"

    if [[ "$AUTO_ITERATION" != "1" && "$OVERWRITE" != "1" ]] \
      && { [[ -f "$OUTPUT_FILE" ]] || run_exists_in_database "$RUN_ID"; }; then
      echo "Pulando $RUN_ID: resultado ja existe em arquivo local ou no banco."
      echo "Use OVERWRITE=1 scripts/run_all_experiments.sh para reexecutar."
      echo "Use AUTO_ITERATION=1 scripts/run_all_experiments.sh para criar novos arquivos."
      SKIPPED_RUNS+=("$RUN_ID")
      continue
    fi

    if [[ "$DRY_RUN" == "1" ]]; then
      echo "DRY_RUN=1: executaria $RUN_ID"
      continue
    fi

    if ! AUTO_ITERATION=0 RESET_TOPICS_PER_RUN=0 scripts/run_experiment.sh \
        --framework "$FRAMEWORK" \
        --scenario "$SCENARIO" \
        --iteration "$RUN_ITERATION" </dev/null; then
      echo "Falha em $RUN_ID; continuando para o proximo experimento." >&2
      FAILED_RUNS+=("$RUN_ID")
    fi
  done 3< "$SCENARIOS_FILE"
done

echo "========================================"
echo "Resumo da matriz experimental"
echo "========================================"

if [[ "${#SKIPPED_RUNS[@]}" -gt 0 ]]; then
  echo "Pulados por ja existirem: ${SKIPPED_RUNS[*]}"
fi

if [[ "${#FAILED_RUNS[@]}" -gt 0 ]]; then
  echo "Falharam: ${FAILED_RUNS[*]}" >&2
  exit 1
fi

echo "Matriz finalizada."
