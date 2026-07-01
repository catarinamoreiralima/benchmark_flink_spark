#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FAULT_RUNS="${FAULT_RUNS:-5}"
FRAMEWORKS="${FRAMEWORKS:-spark flink}"
FAULT_SCENARIOS="${FAULT_SCENARIOS:-fault_medium fault_high}"
FAULT_ACTIONS="${FAULT_ACTIONS:-kill stop-start}"
FAULT_DELAY_SECONDS="${FAULT_DELAY_SECONDS:-120}"
FAULT_DURATION_SECONDS="${FAULT_DURATION_SECONDS:-10}"
FAULT_COUNT="${FAULT_COUNT:-2}"
WRITE_JSONL="${WRITE_JSONL:-0}"
AUTO_ITERATION="${AUTO_ITERATION:-1}"
LOAD_ENV="${LOAD_ENV:-1}"
DB_STORAGE_MODE="${DB_STORAGE_MODE:-raw}"
DB_BUCKET_SECONDS="${DB_BUCKET_SECONDS:-0.25}"
FAILED_RUNS=()
FAILED_COMMANDS=()

cd "$ROOT_DIR"

if [[ "$LOAD_ENV" == "1" && -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
fi

## @brief Retorna o serviço sujeito a falha para um framework.
## @param framework Valor do parâmetro `framework`.
## @return Código de saída zero em caso de sucesso.
fault_service_for_framework() {
  local framework="$1"

  case "$framework" in
    spark)
      echo "spark-worker"
      ;;
    flink)
      echo "taskmanager"
      ;;
    *)
      echo "Framework desconhecido: $framework" >&2
      return 1
      ;;
  esac
}

## @brief Monta o comando para repetir uma execução com falha.
## @param framework Valor do parâmetro `framework`.
## @param scenario Valor do parâmetro `scenario`.
## @param action Valor do parâmetro `action`.
## @param service Valor do parâmetro `service`.
## @return Código de saída zero em caso de sucesso.
rerun_command() {
  local framework="$1"
  local scenario="$2"
  local action="$3"
  local service="$4"

  cat <<EOF
WRITE_JSONL=$WRITE_JSONL AUTO_ITERATION=$AUTO_ITERATION DB_STORAGE_MODE=$DB_STORAGE_MODE DB_BUCKET_SECONDS=$DB_BUCKET_SECONDS FAULT_SERVICE=$service FAULT_COUNT=$FAULT_COUNT FAULT_ACTION=$action FAULT_DELAY_SECONDS=$FAULT_DELAY_SECONDS FAULT_DURATION_SECONDS=$FAULT_DURATION_SECONDS scripts/run_experiment.sh --framework $framework --scenario $scenario
EOF
}

## @brief Executa um experimento com injeção de falha.
## @param framework Valor do parâmetro `framework`.
## @param scenario Valor do parâmetro `scenario`.
## @param action Valor do parâmetro `action`.
## @param round Valor do parâmetro `round`.
## @return Código de saída zero em caso de sucesso.
run_fault_experiment() {
  local framework="$1"
  local scenario="$2"
  local action="$3"
  local round="$4"
  local service

  service="$(fault_service_for_framework "$framework")"

  echo "========================================"
  echo "Falha $round/$FAULT_RUNS: $framework / $scenario / $action / $service"
  echo "========================================"

  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo "DRY_RUN=1: executaria ${framework}_${scenario} com FAULT_SERVICE=$service FAULT_ACTION=$action"
    return 0
  fi

  WRITE_JSONL="$WRITE_JSONL" \
  AUTO_ITERATION="$AUTO_ITERATION" \
  DB_STORAGE_MODE="$DB_STORAGE_MODE" \
  DB_BUCKET_SECONDS="$DB_BUCKET_SECONDS" \
  FAULT_SERVICE="$service" \
  FAULT_COUNT="$FAULT_COUNT" \
  FAULT_ACTION="$action" \
  FAULT_DELAY_SECONDS="$FAULT_DELAY_SECONDS" \
  FAULT_DURATION_SECONDS="$FAULT_DURATION_SECONDS" \
  scripts/run_experiment.sh \
    --framework "$framework" \
    --scenario "$scenario"
}

for round in $(seq 1 "$FAULT_RUNS"); do
  for framework in $FRAMEWORKS; do
    for scenario in $FAULT_SCENARIOS; do
      for action in $FAULT_ACTIONS; do
        service="$(fault_service_for_framework "$framework")"
        if ! run_fault_experiment "$framework" "$scenario" "$action" "$round"; then
          label="${framework}_${scenario}_${action}_round_${round}"
          echo "Falha em $framework / $scenario / $action / rodada $round; continuando campanha." >&2
          FAILED_RUNS+=("$label")
          FAILED_COMMANDS+=("$label: $(rerun_command "$framework" "$scenario" "$action" "$service")")
        fi
      done
    done
  done
done

echo "========================================"
echo "Campanha de falhas finalizada."
echo "========================================"

if [[ "${#FAILED_RUNS[@]}" -gt 0 ]]; then
  echo "Falharam: ${FAILED_RUNS[*]}" >&2
  echo
  echo "Comandos para refazer individualmente:"
  for command in "${FAILED_COMMANDS[@]}"; do
    echo "- $command"
  done
  exit 1
fi

echo "Todas as execucoes de falha terminaram sem erro."
