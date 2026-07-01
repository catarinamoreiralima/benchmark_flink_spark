#!/bin/bash

set -euo pipefail

## @brief Exibe a forma de uso e as opções aceitas pelo script.
## @return Código de saída zero em caso de sucesso.
usage() {
  echo "Uso: $0 --run-id RUN_ID --service SERVICE [--count N] [--delay-seconds N] [--duration-seconds N] [--action restart|stop-start|kill]"
}

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID=""
SERVICE=""
DELAY_SECONDS="${FAULT_DELAY_SECONDS:-30}"
DURATION_SECONDS="${FAULT_DURATION_SECONDS:-10}"
ACTION="${FAULT_ACTION:-stop-start}"
COUNT="${FAULT_COUNT:-2}"
OUTPUT_DIR="${FAULT_OUTPUT_DIR:-$ROOT_DIR/results/faults}"
PYTHON_BIN="${PYTHON:-python3}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id)
      RUN_ID="${2:-}"
      shift 2
      ;;
    --service)
      SERVICE="${2:-}"
      shift 2
      ;;
    --delay-seconds)
      DELAY_SECONDS="${2:-}"
      shift 2
      ;;
    --duration-seconds)
      DURATION_SECONDS="${2:-}"
      shift 2
      ;;
    --action)
      ACTION="${2:-}"
      shift 2
      ;;
    --count)
      COUNT="${2:-}"
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

if [[ -z "$RUN_ID" || -z "$SERVICE" ]]; then
  usage >&2
  exit 1
fi

if [[ "$ACTION" != "restart" && "$ACTION" != "stop-start" && "$ACTION" != "kill" ]]; then
  echo "--action deve ser restart, stop-start ou kill." >&2
  exit 1
fi

if ! [[ "$COUNT" =~ ^[0-9]+$ ]] || [[ "$COUNT" -lt 1 ]]; then
  echo "--count deve ser um inteiro maior que zero." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
FAULT_FILE="$OUTPUT_DIR/${RUN_ID}.jsonl"

# Cada run possui um unico experimento de falha. Evita reimportar eventos
# antigos caso o mesmo run_id seja reutilizado com OVERWRITE=1.
: > "$FAULT_FILE"

## @brief Obtém o instante atual em milissegundos.
## @return Timestamp Unix atual em milissegundos.
now_ms() {
  "$PYTHON_BIN" -c 'import time; print(time.time_ns() // 1_000_000)'
}

## @brief Registra um evento de falha no arquivo JSONL.
## @param phase Valor do parâmetro `phase`.
## @param timestamp_ms Valor do parâmetro `timestamp_ms`.
## @param container Valor do parâmetro `container`.
## @return Código de saída zero em caso de sucesso.
write_event() {
  local phase="$1"
  local timestamp_ms="$2"
  local container="${3:-}"

  printf '{"run_id":"%s","service":"%s","container":"%s","action":"%s","phase":"%s","timestamp_ms":%s,"delay_seconds":%s,"duration_seconds":%s,"count":%s}\n' \
    "$RUN_ID" "$SERVICE" "$container" "$ACTION" "$phase" "$timestamp_ms" "$DELAY_SECONDS" "$DURATION_SECONDS" "$COUNT" >> "$FAULT_FILE"
}

## @brief Seleciona os containers que receberão a falha.
## @return Código de saída zero em caso de sucesso.
select_target_containers() {
  TARGET_CONTAINERS=()

  while IFS= read -r container_id; do
    if [[ -z "$container_id" ]]; then
      continue
    fi

    container_name="$(docker inspect --format '{{.Name}}' "$container_id" | sed 's#^/##')"
    TARGET_CONTAINERS+=("$container_name")

    if [[ "${#TARGET_CONTAINERS[@]}" -ge "$COUNT" ]]; then
      break
    fi
  done < <(docker compose ps -q "$SERVICE")
}

cd "$ROOT_DIR"

echo "Fault injector armado: run_id=$RUN_ID service=$SERVICE count=$COUNT action=$ACTION delay=${DELAY_SECONDS}s duration=${DURATION_SECONDS}s"
sleep "$DELAY_SECONDS"

select_target_containers
if [[ "${#TARGET_CONTAINERS[@]}" -lt "$COUNT" ]]; then
  echo "Servico '$SERVICE' tem apenas ${#TARGET_CONTAINERS[@]} container(s) em execucao no momento da falha; solicitado: $COUNT." >&2
  echo "Verifique o estado com: docker compose ps $SERVICE" >&2
  exit 1
fi

echo "Containers alvo: ${TARGET_CONTAINERS[*]}"

FAULT_START_MS="$(now_ms)"
write_event "fault_start" "$FAULT_START_MS"
for container_name in "${TARGET_CONTAINERS[@]}"; do
  write_event "container_fault_start" "$FAULT_START_MS" "$container_name"
done

echo "Injetando falha: $ACTION em ${TARGET_CONTAINERS[*]}"

case "$ACTION" in
  restart)
    docker restart "${TARGET_CONTAINERS[@]}"
    FAULT_END_MS="$(now_ms)"
    ;;
  stop-start)
    docker stop "${TARGET_CONTAINERS[@]}"
    sleep "$DURATION_SECONDS"
    docker start "${TARGET_CONTAINERS[@]}"
    FAULT_END_MS="$(now_ms)"
    ;;
  kill)
    docker kill "${TARGET_CONTAINERS[@]}"
    FAULT_END_MS="$(now_ms)"
    ;;
esac

write_event "fault_end" "$FAULT_END_MS"
for container_name in "${TARGET_CONTAINERS[@]}"; do
  write_event "container_fault_end" "$FAULT_END_MS" "$container_name"
done

if [[ -n "${DATABASE_URL:-}" ]]; then
  "$PYTHON_BIN" scripts/export_faults_to_db.py \
    --run-id "$RUN_ID" \
    --fault-file "$FAULT_FILE" || echo "Aviso: nao foi possivel importar falhas para o banco." >&2
fi

echo "Falha concluida. Log: $FAULT_FILE"
