#!/bin/bash

set -euo pipefail

echo "Cancelando jobs Flink ativos..."
docker compose exec -T jobmanager bash -lc '
  flink list -r 2>/dev/null \
    | awk "/RUNNING/ { print \$4 }" \
    | while read -r job_id; do
        if [ -n "$job_id" ]; then
          flink cancel "$job_id" >/dev/null 2>&1 || true
        fi
      done
' || true

echo "Parando processos spark-submit antigos..."
docker compose exec -T spark-master bash -lc '
  pkill -f "/opt/benchmark/jobs/spark/spark_job.py" 2>/dev/null || true
' || true
