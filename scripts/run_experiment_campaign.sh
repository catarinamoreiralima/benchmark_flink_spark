#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NORMAL_RUNS="${NORMAL_RUNS:-5}"
FAULT_RUNS="${FAULT_RUNS:-5}"
FAULT_DELAY_SECONDS="${FAULT_DELAY_SECONDS:-120}"
FAULT_DURATION_SECONDS="${FAULT_DURATION_SECONDS:-10}"
FAULT_COUNT="${FAULT_COUNT:-2}"
FAULT_ACTIONS="${FAULT_ACTIONS:-kill stop-start}"
FAULT_SCENARIOS="${FAULT_SCENARIOS:-fault_medium fault_high}"
WRITE_JSONL="${WRITE_JSONL:-0}"
AUTO_ITERATION="${AUTO_ITERATION:-1}"
LOAD_ENV="${LOAD_ENV:-1}"
DB_STORAGE_MODE="${DB_STORAGE_MODE:-bucket}"
NORMAL_DB_BUCKET_SECONDS="${NORMAL_DB_BUCKET_SECONDS:-0.5}"
FAULT_DB_BUCKET_SECONDS="${FAULT_DB_BUCKET_SECONDS:-0.25}"
FAILED_RUNS=()
FAILED_COMMANDS=()

cd "$ROOT_DIR"

if [[ "$LOAD_ENV" == "1" && -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
fi

## @brief Executa uma rodada da matriz de cenários normais.
## @param round Valor do parâmetro `round`.
## @return Código de saída zero em caso de sucesso.
run_normal_matrix() {
  local round="$1"

  echo "========================================"
  echo "Rodada normal $round/$NORMAL_RUNS"
  echo "========================================"

  env \
    -u FAULT_SERVICE \
    -u FAULT_ACTION \
    -u FAULT_COUNT \
    -u FAULT_DELAY_SECONDS \
    -u FAULT_DURATION_SECONDS \
    WRITE_JSONL="$WRITE_JSONL" \
    AUTO_ITERATION="$AUTO_ITERATION" \
    DB_STORAGE_MODE="$DB_STORAGE_MODE" \
    DB_BUCKET_SECONDS="$NORMAL_DB_BUCKET_SECONDS" \
    scripts/run_all_experiments.sh
}

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

## @brief Monta o comando para repetir uma rodada normal.
## @return Comando shell que repete a matriz de cenários normais.
rerun_normal_command() {
  cat <<EOF
WRITE_JSONL=$WRITE_JSONL AUTO_ITERATION=$AUTO_ITERATION DB_STORAGE_MODE=$DB_STORAGE_MODE DB_BUCKET_SECONDS=$NORMAL_DB_BUCKET_SECONDS scripts/run_all_experiments.sh
EOF
}

## @brief Monta o comando para repetir uma execução com falha.
## @param framework Valor do parâmetro `framework`.
## @param scenario Valor do parâmetro `scenario`.
## @param action Valor do parâmetro `action`.
## @param service Valor do parâmetro `service`.
## @return Código de saída zero em caso de sucesso.
rerun_fault_command() {
  local framework="$1"
  local scenario="$2"
  local action="$3"
  local service="$4"

  cat <<EOF
WRITE_JSONL=$WRITE_JSONL AUTO_ITERATION=$AUTO_ITERATION DB_STORAGE_MODE=$DB_STORAGE_MODE DB_BUCKET_SECONDS=$FAULT_DB_BUCKET_SECONDS FAULT_SERVICE=$service FAULT_COUNT=$FAULT_COUNT FAULT_ACTION=$action FAULT_DELAY_SECONDS=$FAULT_DELAY_SECONDS FAULT_DURATION_SECONDS=$FAULT_DURATION_SECONDS scripts/run_experiment.sh --framework $framework --scenario $scenario
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
  DB_BUCKET_SECONDS="$FAULT_DB_BUCKET_SECONDS" \
  FAULT_SERVICE="$service" \
  FAULT_COUNT="$FAULT_COUNT" \
  FAULT_ACTION="$action" \
  FAULT_DELAY_SECONDS="$FAULT_DELAY_SECONDS" \
  FAULT_DURATION_SECONDS="$FAULT_DURATION_SECONDS" \
  scripts/run_experiment.sh \
    --framework "$framework" \
    --scenario "$scenario"
}

for round in $(seq 1 "$NORMAL_RUNS"); do
  if ! run_normal_matrix "$round"; then
    echo "Falha na rodada normal $round; continuando campanha." >&2
    FAILED_RUNS+=("normal_round_${round}")
    FAILED_COMMANDS+=("normal_round_${round}: $(rerun_normal_command)")
  fi
done

for round in $(seq 1 "$FAULT_RUNS"); do
  for framework in spark flink; do
    for scenario in $FAULT_SCENARIOS; do
      for action in $FAULT_ACTIONS; do
        if ! run_fault_experiment "$framework" "$scenario" "$action" "$round"; then
          echo "Falha em $framework / $scenario / $action / rodada $round; continuando campanha." >&2
          service="$(fault_service_for_framework "$framework")"
          FAILED_RUNS+=("${framework}_${scenario}_${action}_round_${round}")
          FAILED_COMMANDS+=("${framework}_${scenario}_${action}_round_${round}: $(rerun_fault_command "$framework" "$scenario" "$action" "$service")")
        fi
      done
    done
  done
done

echo "========================================"
echo "Campanha experimental finalizada."
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

echo "Todas as execucoes da campanha terminaram sem erro."
