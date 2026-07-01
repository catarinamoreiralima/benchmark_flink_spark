#!/bin/bash

set -e

# Topico de entrada e topico de saida do benchmark.
TOPICS=("transactions" "processed-transactions")
RESET_TOPICS="${RESET_TOPICS:-0}"

echo "Aguardando Kafka ficar pronto..."

for ATTEMPT in $(seq 1 60); do
  if docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi

  if [ "$ATTEMPT" -eq 60 ]; then
    echo "Kafka nao ficou pronto apos 60s." >&2
    exit 1
  fi

  sleep 1
done

if [ "$RESET_TOPICS" != "1" ]; then
  for TOPIC in "${TOPICS[@]}"; do
    echo "Garantindo tópico $TOPIC..."

    docker compose exec -T kafka \
      kafka-topics --bootstrap-server localhost:9092 \
      --create --if-not-exists --topic "$TOPIC" \
      --partitions 1 --replication-factor 1
  done

  echo "Tópicos prontos."
  exit 0
fi

## @brief Verifica se um tópico existe no Kafka.
## @param TOPIC Valor do parâmetro `TOPIC`.
## @return Código de saída zero em caso de sucesso.
topic_exists() {
  local TOPIC="$1"

  docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --list | grep -Fxq "$TOPIC"
}

for TOPIC in "${TOPICS[@]}"; do
  echo "Deletando tópico $TOPIC..."

  # O topico pode ainda nao existir em uma primeira execucao.
  docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --delete --if-exists --topic "$TOPIC" || true
done

echo "Aguardando remoção..."

for TOPIC in "${TOPICS[@]}"; do
  for ATTEMPT in $(seq 1 30); do
    if ! topic_exists "$TOPIC"; then
      break
    fi

    if [ "$ATTEMPT" -eq 30 ]; then
      echo "Tópico $TOPIC ainda existe após aguardar remoção." >&2
      exit 1
    fi

    sleep 1
  done
done

for TOPIC in "${TOPICS[@]}"; do
  echo "Recriando tópico $TOPIC..."

  # Por enquanto usamos uma particao para simplificar a validacao inicial.
  # Depois da versao Spark/Flink funcionar, vale testar mais particoes.
  docker compose exec -T kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$TOPIC" \
    --partitions 1 --replication-factor 1
done

echo "Tópicos resetados com sucesso!"
