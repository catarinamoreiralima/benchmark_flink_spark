import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    expr,
    from_json,
    lit,
    struct,
    sum as spark_sum,
    to_json,
    when,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

WINDOW_SIZE_MS = 30_000
ACCOUNT_STATE = {}
WINDOW_STATE = {}


def parse_args():
    # Mantemos tudo parametrizavel para reaproveitar o job em varios cenarios.
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Job Spark minimo para validar o pipeline Kafka -> Spark -> Kafka."
    )
    parser.add_argument("--master", default="spark://spark-master:7077")
    parser.add_argument("--bootstrap-server", default="kafka:29092")
    parser.add_argument("--input-topic", default="transactions")
    parser.add_argument("--output-topic", default="processed-transactions")
    parser.add_argument("--checkpoint-location", default="/tmp/spark-checkpoints/minimal-job")
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="latest")
    parser.add_argument("--trigger", default="1 second")
    return parser.parse_args()


def build_schema():
    # Schema explicito evita inferencia em streaming e deixa o contrato documentado.
    # O payload completo tem mais colunas, mas o primeiro job so precisa destas.
    """! @brief Constrói o schema Spark utilizado para interpretar os eventos.
    @return Resultado produzido pela operação.
    """
    payload_schema = StructType(
        [
            StructField("TransactionID", StringType()),
            StructField("AccountID", StringType()),
            StructField("TransactionAmount", DoubleType()),
            StructField("TransactionType", StringType()),
            StructField("Channel", StringType()),
            StructField("CustomerAge", LongType()),
            StructField("LoginAttempts", LongType()),
            StructField("AccountBalance", DoubleType()),
            StructField("MerchantID", StringType()),
        ]
    )

    return StructType(
        [
            StructField("run_id", StringType()),
            StructField("event_id", LongType()),
            StructField("sent_at_ms", LongType()),
            StructField("scenario", StringType()),
            StructField("payload", payload_schema),
        ]
    )


def build_enriched_events(events):
    # current_timestamp e avaliado pelo Spark no momento de processamento do batch.
    """! @brief Converte eventos Kafka em registros enriquecidos com métricas e fraude.
    @param events Valor do parâmetro `events`.
    @return Resultado produzido pela operação.
    """
    processed_at_ms = expr("unix_millis(current_timestamp())")

    # O Kafka entrega value como bytes; aqui convertemos para string e parseamos JSON.
    parsed = events.select(
        from_json(col("value").cast("string"), build_schema()).alias("event")
    ).select("event.*")

    # Descarta mensagens malformadas sem derrubar o job de streaming.
    parsed = parsed.where(col("run_id").isNotNull() & col("event_id").isNotNull())

    # Mantem o contrato esperado pelo collector e pelos futuros scripts de analise.
    enriched = parsed.withColumn("processed_at_ms", processed_at_ms).withColumn(
        "latency_ms",
        col("processed_at_ms") - col("sent_at_ms"),
    )

    # --- PROCESSAMENTO (DETECÇÃO DE FRAUDE) ---
    
    # Regra 1: Valor muito alto
    is_high_amount = col("payload.TransactionAmount") > 10000.0
    
    # Regra 2: Incompatibilidade de canal (Débito ocorrendo Online/Mobile)
    is_channel_mismatch = (col("payload.TransactionType") == "Debit") & \
                          (col("payload.Channel").isin("Online", "Mobile"))
                          
    # Regra 3: Anomalia de Idade/Tecnologia (Idoso usando Mobile OU errando login)
    is_age_anomaly = (col("payload.CustomerAge") > 70) & \
                     ((col("payload.Channel") == "Mobile") | (col("payload.LoginAttempts") > 3))

    # Consolida as regras: se qualquer uma for verdadeira, é fraude
    is_fraud_condition = is_high_amount | is_channel_mismatch | is_age_anomaly

    # Aplica a condição criando a coluna 'is_fraud'
    enriched = enriched.withColumn("is_fraud", when(is_fraud_condition, True).otherwise(False))

    return enriched.withColumn(
        "window_bucket_ms",
        (col("sent_at_ms") / lit(WINDOW_SIZE_MS)).cast("long") * lit(WINDOW_SIZE_MS),
    )


def add_batch_features(enriched):
    """! @brief Acrescenta agregações acumuladas por conta e janela ao microbatch.
    @param enriched Valor do parâmetro `enriched`.
    @return Resultado produzido pela operação.
    """
    spark = enriched.sparkSession
    base = (
        enriched.withColumn(
            "metric_channel",
            when(col("payload.Channel").isNull(), lit("unknown")).otherwise(col("payload.Channel")),
        )
        .withColumn(
            "metric_transaction_type",
            when(col("payload.TransactionType").isNull(), lit("unknown")).otherwise(
                col("payload.TransactionType")
            ),
        )
        .withColumn(
            "metric_account_id",
            when(col("payload.AccountID").isNull(), lit("unknown")).otherwise(col("payload.AccountID")),
        )
    )

    batch_account_metrics = (
        base.groupBy("run_id", "metric_account_id")
        .agg(
            count(lit(1)).alias("batch_count"),
            spark_sum(col("payload.TransactionAmount")).alias("batch_total_amount"),
            spark_sum(when(col("is_fraud"), lit(1)).otherwise(lit(0))).alias("batch_fraud_count"),
        )
        .collect()
    )

    account_rows = []
    for row in batch_account_metrics:
        key = (row["run_id"], row["metric_account_id"])
        state = ACCOUNT_STATE.setdefault(
            key,
            {
                "transaction_count": 0,
                "total_amount": 0.0,
                "fraud_count": 0,
            },
        )
        state["transaction_count"] += int(row["batch_count"])
        state["total_amount"] += float(row["batch_total_amount"] or 0.0)
        state["fraud_count"] += int(row["batch_fraud_count"] or 0)
        account_rows.append(
            {
                "run_id": row["run_id"],
                "metric_account_id": row["metric_account_id"],
                "account_transaction_count": state["transaction_count"],
                "account_total_amount": state["total_amount"],
                "account_avg_amount": state["total_amount"] / state["transaction_count"],
                "account_fraud_count": state["fraud_count"],
            }
        )

    batch_window_metrics = (
        base.groupBy(
            "run_id",
            "scenario",
            "window_bucket_ms",
            "metric_channel",
            "metric_transaction_type",
        )
        .agg(
            count(lit(1)).alias("batch_count"),
            spark_sum(col("payload.TransactionAmount")).alias("batch_total_amount"),
            spark_sum(when(col("is_fraud"), lit(1)).otherwise(lit(0))).alias("batch_fraud_count"),
        )
        .collect()
    )

    window_rows = []
    for row in batch_window_metrics:
        key = (
            row["run_id"],
            row["scenario"],
            row["window_bucket_ms"],
            row["metric_channel"],
            row["metric_transaction_type"],
        )
        state = WINDOW_STATE.setdefault(
            key,
            {
                "transaction_count": 0,
                "total_amount": 0.0,
                "fraud_count": 0,
            },
        )
        state["transaction_count"] += int(row["batch_count"])
        state["total_amount"] += float(row["batch_total_amount"] or 0.0)
        state["fraud_count"] += int(row["batch_fraud_count"] or 0)
        window_rows.append(
            {
                "run_id": row["run_id"],
                "scenario": row["scenario"],
                "window_bucket_ms": row["window_bucket_ms"],
                "metric_channel": row["metric_channel"],
                "metric_transaction_type": row["metric_transaction_type"],
                "window_transaction_count": state["transaction_count"],
                "window_total_amount": state["total_amount"],
                "window_avg_amount": state["total_amount"] / state["transaction_count"],
                "window_fraud_count": state["fraud_count"],
            }
        )

    account_metrics = spark.createDataFrame(account_rows)
    window_metrics = spark.createDataFrame(window_rows)

    return (
        base.join(
            window_metrics,
            [
                "run_id",
                "scenario",
                "window_bucket_ms",
                "metric_channel",
                "metric_transaction_type",
            ],
            "left",
        )
        .join(
            account_metrics,
            ["run_id", "metric_account_id"],
            "left",
        )
        .drop("metric_channel", "metric_transaction_type", "metric_account_id")
    )


def build_output_from_enriched(enriched):
    """! @brief Serializa eventos enriquecidos no contrato de saída do Kafka.
    @param enriched Valor do parâmetro `enriched`.
    @return Resultado produzido pela operação.
    """
    result = struct(
        col("payload.TransactionID").alias("TransactionID"),
        col("payload.AccountID").alias("AccountID"),
        col("payload.MerchantID").alias("MerchantID"),
        col("payload.TransactionType").alias("TransactionType"),
        col("payload.Channel").alias("Channel"),
        col("payload.TransactionAmount").alias("TransactionAmount"),
        col("payload.CustomerAge").alias("CustomerAge"),
        col("payload.LoginAttempts").alias("LoginAttempts"),
        col("payload.AccountBalance").alias("AccountBalance"),
        col("is_fraud").alias("is_fraud"),
        struct(
            col("window_bucket_ms").alias("bucket_start_ms"),
            col("window_transaction_count").alias("transaction_count"),
            col("window_total_amount").alias("total_amount"),
            col("window_avg_amount").alias("avg_amount"),
            col("window_fraud_count").alias("fraud_count"),
        ).alias("window_metrics"),
        struct(
            col("account_transaction_count").alias("transaction_count"),
            col("account_total_amount").alias("total_amount"),
            col("account_avg_amount").alias("avg_amount"),
            col("account_fraud_count").alias("fraud_count"),
        ).alias("account_state"),
    )

    output_record = struct(
        lit("spark").alias("framework"),
        col("run_id"),
        col("event_id"),
        col("sent_at_ms"),
        col("processed_at_ms"),
        col("latency_ms"),
        col("scenario"),
        result.alias("result"),
    )

    # O sink Kafka espera colunas key/value. Ambas precisam ser strings ou bytes.
    return enriched.select(
        concat_ws(":", col("run_id"), col("event_id")).cast("string").alias("key"),
        to_json(output_record).alias("value"),
    )


def process_batch(batch_df, _batch_id, bootstrap_server, output_topic):
    """! @brief Processa e publica um microbatch do Spark Structured Streaming.
    @param batch_df DataFrame correspondente ao microbatch.
    @param _batch_id Identificador interno do microbatch.
    @param bootstrap_server Endereço do broker Kafka.
    @param output_topic Tópico Kafka de saída.
    """
    enriched = build_enriched_events(batch_df)
    if enriched.rdd.isEmpty():
        return

    output = build_output_from_enriched(add_batch_features(enriched))
    output.write.format("kafka").option("kafka.bootstrap.servers", bootstrap_server).option(
        "topic", output_topic
    ).save()


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()

    # O master aponta para o servico Spark criado no docker-compose.
    spark = (
        SparkSession.builder.appName("spark-minimal-stream-benchmark")
        .master(args.master)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Fonte de entrada: eventos crus gerados pelo producer.py.
    input_events = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_server)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Saida: eventos processados no mesmo formato que o collector.py espera.
    query = (
        input_events.writeStream.foreachBatch(
            lambda batch_df, batch_id: process_batch(
                batch_df,
                batch_id,
                args.bootstrap_server,
                args.output_topic,
            )
        )
        .option("checkpointLocation", args.checkpoint_location)
        .outputMode("append")
        # Trigger fixo para deixar claro o micro-batching do Spark.
        .trigger(processingTime=args.trigger)
        .start()
    )

    # Mantem o processo vivo enquanto o streaming estiver ativo.
    query.awaitTermination()


if __name__ == "__main__":
    main()
