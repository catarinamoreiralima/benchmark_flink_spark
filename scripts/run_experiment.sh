#!/bin/bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCENARIOS_FILE="$ROOT_DIR/experiments/scenarios.csv"
IDLE_TIMEOUT="${IDLE_TIMEOUT:-120}"
ITERATION="${ITERATION:-01}"
AUTO_ITERATION="${AUTO_ITERATION:-0}"
OVERWRITE="${OVERWRITE:-0}"
RESET_TOPICS_PER_RUN="${RESET_TOPICS_PER_RUN:-1}"
STOP_OTHER_FRAMEWORK="${STOP_OTHER_FRAMEWORK:-1}"
FAULT_SERVICE="${FAULT_SERVICE:-}"
FAULT_DELAY_SECONDS="${FAULT_DELAY_SECONDS:-30}"
FAULT_DURATION_SECONDS="${FAULT_DURATION_SECONDS:-10}"
FAULT_ACTION="${FAULT_ACTION:-stop-start}"
FAULT_COUNT="${FAULT_COUNT:-2}"
FLINK_PARALLELISM="${FLINK_PARALLELISM:-4}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-768m}"
SPARK_TOTAL_EXECUTOR_CORES="${SPARK_TOTAL_EXECUTOR_CORES:-4}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-4}"
EXPORT_RESOURCE_USAGE="${EXPORT_RESOURCE_USAGE:-1}"
RESOURCE_STEP_SECONDS="${RESOURCE_STEP_SECONDS:-5}"

## @brief Exibe a forma de uso e as opções aceitas pelo script.
## @return Código de saída zero em caso de sucesso.
usage() {
  echo "Uso: $0 --framework spark|flink --scenario NOME_DO_CENARIO [--iteration 01]"
  echo "Cenarios sao lidos de experiments/scenarios.csv."
  echo
  echo "Falhas opcionais via ambiente:"
  echo "  FAULT_SERVICE=taskmanager|jobmanager|spark-worker|spark-master|kafka"
  echo "  FAULT_ACTION=stop-start|restart|kill"
  echo "  FAULT_COUNT=2 FAULT_DELAY_SECONDS=30 FAULT_DURATION_SECONDS=10"
  echo
  echo "Paralelismo:"
  echo "  FLINK_PARALLELISM=4"
  echo "  SPARK_EXECUTOR_CORES=1 SPARK_EXECUTOR_MEMORY=768m SPARK_TOTAL_EXECUTOR_CORES=4 SPARK_SQL_SHUFFLE_PARTITIONS=4"
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

FRAMEWORK=""
SCENARIO=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --framework)
      FRAMEWORK="${2:-}"
      shift 2
      ;;
    --scenario)
      SCENARIO="${2:-}"
      shift 2
      ;;
    --iteration)
      ITERATION="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Argumento desconhecido: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ "$FRAMEWORK" != "spark" && "$FRAMEWORK" != "flink" ]]; then
  echo "--framework deve ser spark ou flink." >&2
  usage
  exit 1
fi

if [[ -z "$SCENARIO" ]]; then
  echo "--scenario e obrigatorio." >&2
  usage
  exit 1
fi

SCENARIO_ROW="$(awk -F, -v scenario="$SCENARIO" 'NR > 1 && $1 == scenario { print $0 }' "$SCENARIOS_FILE")"
if [[ -z "$SCENARIO_ROW" ]]; then
  echo "Cenario '$SCENARIO' nao encontrado em $SCENARIOS_FILE." >&2
  exit 1
fi

EPS="$(echo "$SCENARIO_ROW" | awk -F, '{ print $2 }')"
DURATION="$(echo "$SCENARIO_ROW" | awk -F, '{ print $3 }')"
MAX_EVENTS="$(awk -v eps="$EPS" -v duration="$DURATION" 'BEGIN { printf "%d", eps * duration }')"

if [[ "$AUTO_ITERATION" == "1" && "$OVERWRITE" != "1" ]]; then
  ITERATION="$(next_iteration "$FRAMEWORK" "$SCENARIO")"
fi

RUN_ID="${FRAMEWORK}_${SCENARIO}_${ITERATION}"
OUTPUT_FILE="$ROOT_DIR/results/raw/${RUN_ID}.jsonl"

if [[ -f "$OUTPUT_FILE" && "$OVERWRITE" != "1" ]]; then
  echo "Arquivo de resultado ja existe: $OUTPUT_FILE" >&2
  echo "Use OVERWRITE=1 para sobrescrever." >&2
  exit 1
fi

if [[ "$OVERWRITE" != "1" ]] && run_exists_in_database "$RUN_ID"; then
  echo "Run ja existe no banco: $RUN_ID" >&2
  echo "Use AUTO_ITERATION=1 para escolher a proxima iteracao ou OVERWRITE=1 para reexecutar." >&2
  exit 1
fi

if [[ -n "$FAULT_SERVICE" ]]; then
  case "$FRAMEWORK:$FAULT_SERVICE" in
    spark:spark-worker|spark:spark-master|spark:kafka|spark:zookeeper|\
    flink:taskmanager|flink:jobmanager|flink:kafka|flink:zookeeper)
      ;;
    *)
      echo "FAULT_SERVICE='$FAULT_SERVICE' nao combina com --framework '$FRAMEWORK'." >&2
      echo "Use taskmanager/jobmanager para Flink, ou spark-worker/spark-master para Spark." >&2
      exit 1
      ;;
  esac
fi

cd "$ROOT_DIR"

scripts/prepare_runtime.sh
PYTHON_BIN="$(scripts/ensure_python_env.sh | tail -1)"
echo "Python do experimento: $PYTHON_BIN"

# Alterado: Caminho de checagem do JAR do Flink para dentro de src/
if [[ "$FRAMEWORK" == "flink" && ! -f src/jobs/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar ]]; then
  echo "Aviso: conector Kafka do Flink nao encontrado em src/jobs/flink/lib/."
fi

echo "Subindo containers..."
if [[ "$FRAMEWORK" == "spark" ]]; then
  if [[ "$STOP_OTHER_FRAMEWORK" == "1" ]]; then
    docker compose stop jobmanager taskmanager >/dev/null 2>&1 || true
  fi
  docker compose up -d --build postgres zookeeper kafka spark-master spark-worker cadvisor prometheus grafana
else
  if [[ "$STOP_OTHER_FRAMEWORK" == "1" ]]; then
    docker compose stop spark-master spark-worker >/dev/null 2>&1 || true
  fi
  docker compose up -d --build postgres zookeeper kafka jobmanager taskmanager cadvisor prometheus grafana
fi

if [[ "$RESET_TOPICS_PER_RUN" == "1" ]]; then
  echo "Resetando topicos Kafka..."
else
  echo "Garantindo topicos Kafka..."
fi
# Alterado: Chamada do script agora aponta para a pasta scripts/
RESET_TOPICS="$RESET_TOPICS_PER_RUN" bash scripts/reset_kafka.sh

if [[ "$OVERWRITE" == "1" ]]; then
  rm -f "$OUTPUT_FILE"
fi

if [[ -n "${DATABASE_URL:-}" ]]; then
  RUN_MODE="normal"
  if [[ -n "$FAULT_SERVICE" ]]; then
    RUN_MODE="fault"
  fi

  "$PYTHON_BIN" scripts/export_run_metadata_to_db.py \
    --run-id "$RUN_ID" \
    --framework "$FRAMEWORK" \
    --scenario "$SCENARIO" \
    --iteration "$ITERATION" \
    --eps "$EPS" \
    --duration-seconds "$DURATION" \
    --max-events "$MAX_EVENTS" \
    --mode "$RUN_MODE" \
    --fault-service "${FAULT_SERVICE:-}" \
    --fault-action "${FAULT_ACTION:-}" \
    --fault-count "$FAULT_COUNT" \
    --fault-delay-seconds "$FAULT_DELAY_SECONDS" \
    --fault-duration-seconds "$FAULT_DURATION_SECONDS" || echo "Aviso: nao foi possivel importar metadados da execucao para o banco." >&2
fi

## @brief Encerra processos auxiliares e cancela jobs ainda ativos.
## @return Código de saída zero em caso de sucesso.
cleanup() {
  if [[ -n "${FAULT_PID:-}" ]] && kill -0 "$FAULT_PID" 2>/dev/null; then
    kill "$FAULT_PID" 2>/dev/null || true
  fi

  if [[ -n "${COLLECTOR_PID:-}" ]] && kill -0 "$COLLECTOR_PID" 2>/dev/null; then
    kill "$COLLECTOR_PID" 2>/dev/null || true
  fi

  if [[ -n "${SPARK_PID:-}" ]] && kill -0 "$SPARK_PID" 2>/dev/null; then
    kill "$SPARK_PID" 2>/dev/null || true
  fi

  if [[ -n "${FLINK_JOB_ID:-}" ]]; then
    docker compose exec -T jobmanager flink cancel "$FLINK_JOB_ID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "Iniciando coletor: run_id=$RUN_ID"
# Alterado: Caminho do collector.py para dentro de src/
"$PYTHON_BIN" src/collector/collector.py \
  --run-id "$RUN_ID" \
  --group-id "collector-$RUN_ID" \
  --output-file "$OUTPUT_FILE" \
  --max-messages "$MAX_EVENTS" \
  --idle-timeout "$IDLE_TIMEOUT" &
COLLECTOR_PID="$!"

sleep 3

if [[ "$FRAMEWORK" == "spark" ]]; then
  CHECKPOINT="/tmp/spark-checkpoints/${RUN_ID}"
  # O experimento recria os topicos Kafka a cada execucao. Se o checkpoint
  # antigo ficar vivo, o Spark tenta retomar offsets que nao existem mais.
  docker compose exec -T spark-master rm -rf "$CHECKPOINT"

  echo "Submetendo job Spark..."
  docker compose exec -T spark-master /opt/spark/bin/spark-submit \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf "spark.executor.cores=$SPARK_EXECUTOR_CORES" \
    --conf "spark.executor.memory=$SPARK_EXECUTOR_MEMORY" \
    --conf "spark.cores.max=$SPARK_TOTAL_EXECUTOR_CORES" \
    --conf "spark.default.parallelism=$SPARK_TOTAL_EXECUTOR_CORES" \
    --conf "spark.sql.shuffle.partitions=$SPARK_SQL_SHUFFLE_PARTITIONS" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    /opt/benchmark/jobs/spark/spark_job.py \
    --starting-offsets earliest \
    --checkpoint-location "$CHECKPOINT" &
  SPARK_PID="$!"
  sleep "${JOB_STARTUP_WAIT_SECONDS:-20}"
else
  echo "Submetendo job Flink..."
  FLINK_OUTPUT="$(docker compose exec -T jobmanager flink run -d \
    -py /opt/benchmark/jobs/flink/flink_job.py \
    --bootstrap-server kafka:29092 \
    --input-topic transactions \
    --output-topic processed-transactions \
    --group-id "$RUN_ID" \
    --starting-offsets earliest \
    --parallelism "$FLINK_PARALLELISM")"
  echo "$FLINK_OUTPUT"
  FLINK_JOB_ID="$(echo "$FLINK_OUTPUT" | awk '/JobID/ { print $NF }' | tail -1)"
  if [[ -z "$FLINK_JOB_ID" ]]; then
    echo "Nao foi possivel identificar o JobID do Flink." >&2
    exit 1
  fi
  sleep "${JOB_STARTUP_WAIT_SECONDS:-10}"
fi

if [[ -n "$FAULT_SERVICE" ]]; then
  echo "Agendando falha: service=$FAULT_SERVICE count=$FAULT_COUNT action=$FAULT_ACTION delay=${FAULT_DELAY_SECONDS}s duration=${FAULT_DURATION_SECONDS}s"
  PYTHON="$PYTHON_BIN" scripts/inject_fault.sh \
    --run-id "$RUN_ID" \
    --service "$FAULT_SERVICE" \
    --action "$FAULT_ACTION" \
    --count "$FAULT_COUNT" \
    --delay-seconds "$FAULT_DELAY_SECONDS" \
    --duration-seconds "$FAULT_DURATION_SECONDS" &
  FAULT_PID="$!"
fi

echo "Gerando eventos: scenario=$SCENARIO eps=$EPS duration=${DURATION}s max_events=$MAX_EVENTS"
# Alterado: Caminho do producer.py para dentro de src/
"$PYTHON_BIN" src/producer/producer.py \
  --eps "$EPS" \
  --duration "$DURATION" \
  --max-events "$MAX_EVENTS" \
  --scenario "$SCENARIO" \
  --run-id "$RUN_ID" \
  --quiet

echo "Aguardando coletor finalizar..."
wait "$COLLECTOR_PID"
COLLECTOR_PID=""

if [[ -n "${FAULT_PID:-}" ]]; then
  wait "$FAULT_PID" || true
  FAULT_PID=""
fi

if [[ -f "$OUTPUT_FILE" ]]; then
  SAVED="$(wc -l < "$OUTPUT_FILE" | tr -d ' ')"
else
  SAVED="jsonl-desativado"
fi
echo "Experimento finalizado: $RUN_ID"
echo "Arquivo: $OUTPUT_FILE"
echo "Mensagens salvas: $SAVED/$MAX_EVENTS"

if [[ "$EXPORT_RESOURCE_USAGE" == "1" && -n "${DATABASE_URL:-}" ]]; then
  echo "Exportando uso de RAM e CPU do Prometheus para o banco..."
  RESOURCE_STEP_SECONDS="$RESOURCE_STEP_SECONDS" "$PYTHON_BIN" scripts/export_prometheus_resources_to_db.py \
    --run-id "$RUN_ID" || echo "Aviso: nao foi possivel exportar RAM e CPU para o banco." >&2
fi
