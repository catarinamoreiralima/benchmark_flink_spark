import argparse
import json
import os
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer


DEFAULT_KAFKA_JAR = os.path.join(
    os.path.dirname(__file__),
    "lib",
    "flink-sql-connector-kafka-3.1.0-1.18.jar",
)
WINDOW_SIZE_MS = 30_000
ACCOUNT_STATE = {}
WINDOW_STATE = {}


def parse_args():
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Job Flink minimo para validar o pipeline Kafka -> Flink -> Kafka."
    )
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--input-topic", default="transactions")
    parser.add_argument("--output-topic", default="processed-transactions")
    parser.add_argument("--group-id", default="flink-minimal-stream-benchmark")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=5000)
    parser.add_argument("--kafka-connector-jar", default=DEFAULT_KAFKA_JAR)
    parser.add_argument("--parallelism", type=int, default=10)
    return parser.parse_args()


def now_ms():
    """! @brief Obtém o instante atual em milissegundos desde a época Unix.
    @return Resultado produzido pela operação.
    """
    return time.time_ns() // 1_000_000


def update_account_state(run_id, account_id, amount, is_fraud):
    """! @brief Atualiza as métricas acumuladas de uma conta.
    @param run_id Identificador único da execução.
    @param account_id Valor do parâmetro `account_id`.
    @param amount Valor monetário da transação.
    @param is_fraud Indica se a transação foi classificada como fraude.
    @return Resultado produzido pela operação.
    """
    key = (run_id, account_id or "unknown")
    state = ACCOUNT_STATE.setdefault(
        key,
        {
            "transaction_count": 0,
            "total_amount": 0.0,
            "max_amount": 0.0,
            "fraud_count": 0,
        },
    )
    state["transaction_count"] += 1
    state["total_amount"] += amount
    state["max_amount"] = max(state["max_amount"], amount)
    if is_fraud:
        state["fraud_count"] += 1

    return {
        "transaction_count": state["transaction_count"],
        "total_amount": state["total_amount"],
        "avg_amount": state["total_amount"] / state["transaction_count"],
        "max_amount": state["max_amount"],
        "fraud_count": state["fraud_count"],
    }


def update_window_state(run_id, scenario, channel, tx_type, sent_at_ms, amount, is_fraud):
    """! @brief Atualiza as métricas acumuladas de uma janela temporal.
    @param run_id Identificador único da execução.
    @param scenario Cenário experimental selecionado.
    @param channel Valor do parâmetro `channel`.
    @param tx_type Valor do parâmetro `tx_type`.
    @param sent_at_ms Instante de envio do evento, em milissegundos.
    @param amount Valor monetário da transação.
    @param is_fraud Indica se a transação foi classificada como fraude.
    @return Resultado produzido pela operação.
    """
    bucket_start_ms = int(sent_at_ms or now_ms()) // WINDOW_SIZE_MS * WINDOW_SIZE_MS
    key = (
        run_id,
        scenario or "unknown",
        channel or "unknown",
        tx_type or "unknown",
        bucket_start_ms,
    )
    state = WINDOW_STATE.setdefault(
        key,
        {
            "transaction_count": 0,
            "total_amount": 0.0,
            "fraud_count": 0,
        },
    )
    state["transaction_count"] += 1
    state["total_amount"] += amount
    if is_fraud:
        state["fraud_count"] += 1

    return {
        "bucket_start_ms": bucket_start_ms,
        "transaction_count": state["transaction_count"],
        "total_amount": state["total_amount"],
        "avg_amount": state["total_amount"] / state["transaction_count"],
        "fraud_count": state["fraud_count"],
    }


def build_result(payload, run_id, scenario, sent_at_ms):
    """! @brief Aplica regras de fraude e monta o resultado processado.
    @param payload Dados da transação processada.
    @param run_id Identificador único da execução.
    @param scenario Cenário experimental selecionado.
    @param sent_at_ms Instante de envio do evento, em milissegundos.
    @return Resultado produzido pela operação.
    """
    payload = payload or {}
    
    # Extração segura e tipagem das variáveis que usaremos nas regras
    account_id = str(payload.get("AccountID", ""))
    amount = float(payload.get("TransactionAmount", 0.0))
    tx_type = str(payload.get("TransactionType", ""))
    channel = str(payload.get("Channel", ""))
    age = int(payload.get("CustomerAge", 0))
    login_attempts = int(payload.get("LoginAttempts", 0))

    # --- PROCESSAMENTO (DETECÇÃO DE FRAUDE) ---
    is_high_amount = amount > 10000.0
    is_channel_mismatch = (tx_type == "Debit") and (channel in ["Online", "Mobile"])
    is_age_anomaly = (age > 80) and (channel == "Mobile" or login_attempts > 3)

    # Consolidação das regras: qualquer anomalia dispara o alerta de fraude
    is_fraud = is_high_amount or is_channel_mismatch or is_age_anomaly

    return {
        "TransactionID": payload.get("TransactionID"),
        "AccountID": account_id,
        "MerchantID": payload.get("MerchantID"),
        "TransactionType": tx_type,
        "Channel": channel,
        "TransactionAmount": amount,
        "CustomerAge": age,
        "LoginAttempts": login_attempts,
        "AccountBalance": payload.get("AccountBalance"),
        "is_fraud": is_fraud,
        "window_metrics": update_window_state(
            run_id,
            scenario,
            channel,
            tx_type,
            sent_at_ms,
            amount,
            is_fraud,
        ),
        "account_state": update_account_state(run_id, account_id, amount, is_fraud),
    }


def enrich_record(raw_value):
    """! @brief Transforma uma mensagem Kafka em um evento Flink enriquecido.
    @param raw_value Conteúdo bruto recebido do Kafka.
    @return Resultado produzido pela operação.
    """
    try:
        event = json.loads(raw_value)
    except json.JSONDecodeError:
        return []

    run_id = event.get("run_id")
    event_id = event.get("event_id")
    sent_at_ms = event.get("sent_at_ms")

    # Mantem o mesmo comportamento do Spark: mensagens sem contrato basico
    # sao ignoradas, em vez de derrubar o job de streaming.
    if run_id is None or event_id is None:
        return []

    processed_at_ms = now_ms()
    latency_ms = None
    if sent_at_ms is not None:
        latency_ms = processed_at_ms - int(sent_at_ms)

    output_record = {
        "framework": "flink",
        "run_id": run_id,
        "event_id": event_id,
        "sent_at_ms": sent_at_ms,
        "processed_at_ms": processed_at_ms,
        "latency_ms": latency_ms,
        "scenario": event.get("scenario"),
        "result": build_result(event.get("payload"), run_id, event.get("scenario"), sent_at_ms),
    }

    return [json.dumps(output_record, ensure_ascii=False)]


def add_kafka_connector(env, jar_path):
    """! @brief Registra o conector Kafka no ambiente de execução Flink.
    @param env Ambiente de execução Flink.
    @param jar_path Caminho do arquivo JAR do conector.
    """
    if not os.path.exists(jar_path):
        raise FileNotFoundError(
            "Kafka connector JAR nao encontrado. Baixe "
            "flink-sql-connector-kafka-3.1.0-1.18.jar em jobs/flink/lib/ "
            "ou informe --kafka-connector-jar."
        )

    env.add_jars(f"file://{os.path.abspath(jar_path)}")


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    if args.checkpoint_interval_ms > 0:
        env.enable_checkpointing(args.checkpoint_interval_ms)

    add_kafka_connector(env, args.kafka_connector_jar)

    consumer_props = {
        "bootstrap.servers": args.bootstrap_server,
        "group.id": args.group_id,
        "auto.offset.reset": args.starting_offsets,
    }
    producer_props = {"bootstrap.servers": args.bootstrap_server}

    consumer = FlinkKafkaConsumer(
        topics=args.input_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=consumer_props,
    )
    if args.starting_offsets == "earliest":
        consumer.set_start_from_earliest()
    else:
        consumer.set_start_from_latest()

    input_stream = env.add_source(consumer)

    output_stream = input_stream.flat_map(enrich_record, output_type=Types.STRING())

    output_stream.add_sink(
        FlinkKafkaProducer(
            topic=args.output_topic,
            serialization_schema=SimpleStringSchema(),
            producer_config=producer_props,
        )
    )

    env.execute("flink-minimal-stream-benchmark")


if __name__ == "__main__":
    main()
