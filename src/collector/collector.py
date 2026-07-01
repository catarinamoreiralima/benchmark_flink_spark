import argparse
import json
import os
import time
from math import ceil
from contextlib import nullcontext

from kafka import KafkaConsumer
from prometheus_client import start_http_server, Histogram, Counter


# Usamos um Histograma para medir latência (ele agrupa automaticamente em "buckets" ou faixas)
# Labels nos permitem separar a latência do Spark e do Flink no Grafana.
LATENCY_HISTOGRAM = Histogram(
    'pipeline_latency_milliseconds', 
    'Latência fim a fim do processamento do stream em ms',
    ['framework', 'scenario', 'run_id']
)

# Contador extra para monitorar quantas mensagens chegaram ao final com sucesso
MESSAGES_PROCESSED = Counter(
    'pipeline_messages_collected_total',
    'Total de mensagens salvas pelo collector',
    ['framework', 'scenario', 'run_id']
)

# Saida padrao dos dados crus coletados. Os scripts de analise podem ler esse
# diretorio sem depender diretamente do Kafka.
DEFAULT_OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "results",
    "raw",
)


def parse_args():
    # O coletor pode rodar para uma execucao completa ou para testes pequenos.
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Coleta resultados processados do Kafka e salva em JSONL."
    )
    parser.add_argument(
        "--bootstrap-server",
        default="localhost:9092"
    )
    parser.add_argument(
        "--topic",
        default="processed-transactions"
    )
    parser.add_argument(
        "--group-id",
        default="results-collector"
    )
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_OUTPUT_DIR
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Identificador da execucao. Tambem e usado como filtro, se informado.",
    )
    parser.add_argument(
        "--output-file",
        default=None,
        help="Arquivo de saida. Se omitido, sera criado no diretorio de resultados.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Para a coleta apos receber esta quantidade de mensagens.",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="Para a coleta apos N segundos sem novas mensagens.",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=["earliest", "latest"],
        default="earliest",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=8000,
        help="Porta para expor métricas"
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="URL Postgres/Neon para salvar resultados compactos.",
    )
    parser.add_argument(
        "--db-batch-size",
        type=int,
        default=int(os.environ.get("DB_BATCH_SIZE", "500")),
        help="Quantidade de registros por insert em lote no Postgres.",
    )
    parser.add_argument(
        "--write-jsonl",
        choices=["0", "1"],
        default=os.environ.get("WRITE_JSONL", "1"),
        help="Define se o coletor tambem grava JSONL local.",
    )
    parser.add_argument(
        "--db-storage-mode",
        choices=["bucket", "raw"],
        default=os.environ.get("DB_STORAGE_MODE", "bucket"),
        help="bucket salva agregados compactos; raw salva uma linha por mensagem.",
    )
    parser.add_argument(
        "--db-bucket-seconds",
        type=float,
        default=float(os.environ.get("DB_BUCKET_SECONDS", "5")),
        help="Tamanho dos buckets salvos no banco quando DB_STORAGE_MODE=bucket.",
    )
    return parser.parse_args()


def now_ms():
    # Marca quando o resultado chegou ao coletor. Nao substitui processed_at_ms.
    """! @brief Obtém o instante atual em milissegundos desde a época Unix.
    @return Resultado produzido pela operação.
    """
    return time.time_ns() // 1_000_000


def latency_ms(record):
    """! @brief Extrai a latência em milissegundos de um registro.
    @param record Registro que será processado.
    @return Resultado produzido pela operação.
    """
    value = record.get("latency_ms")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def percentile(values, percent):
    """! @brief Calcula um percentil sobre uma coleção de valores.
    @param values Valores usados no cálculo.
    @param percent Percentil desejado, entre 0 e 100.
    @return Resultado produzido pela operação.
    """
    if not values:
        return None

    ordered = sorted(values)
    index = max(0, ceil((percent / 100) * len(ordered)) - 1)
    return ordered[index]


def build_output_path(output_dir, output_file, run_id):
    # Se o usuario passou um arquivo especifico, respeitamos esse caminho.
    """! @brief Define o caminho do arquivo JSONL de uma execução.
    @param output_dir Valor do parâmetro `output_dir`.
    @param output_file Valor do parâmetro `output_file`.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    if output_file:
        parent_dir = os.path.dirname(output_file)
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        return output_file

    # Sem arquivo explicito, cada run_id vira um arquivo independente.
    os.makedirs(output_dir, exist_ok=True)
    name = run_id or f"collector_{int(time.time())}"
    return os.path.join(output_dir, f"{name}.jsonl")


def decode_message(raw_value):
    # Kafka entrega bytes; os jobs de processamento devem publicar JSON.
    """! @brief Decodifica uma mensagem Kafka contendo JSON.
    @param raw_value Conteúdo bruto recebido do Kafka.
    @return Resultado produzido pela operação.
    """
    return json.loads(raw_value.decode("utf-8"))


def should_keep(record, run_id):
    # Quando o run_id e informado, o mesmo coletor pode ficar preso a uma execucao.
    """! @brief Verifica se um registro pertence à execução selecionada.
    @param record Registro que será processado.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    return run_id is None or record.get("run_id") == run_id


def write_record(output, record):
    # JSONL: uma mensagem por linha, formato simples de ler com pandas depois.
    """! @brief Grava um registro no arquivo JSONL.
    @param output Arquivo de saída aberto para escrita.
    @param record Registro que será processado.
    """
    output.write(json.dumps(record, ensure_ascii=False) + "\n")
    output.flush()


class RawPostgresSink:
    """! @brief Persiste individualmente os eventos processados no PostgreSQL.
    """
    def __init__(self, database_url, batch_size):
        """! @brief Inicializa a instância e seus recursos.
        @param database_url URL de conexão com o PostgreSQL.
        @param batch_size Valor do parâmetro `batch_size`.
        """
        import psycopg

        self.database_url = database_url
        self.batch_size = batch_size
        self.connection = psycopg.connect(database_url)
        self.pending = []
        self._ensure_schema()

    def _ensure_schema(self):
        """! @brief Cria ou atualiza as estruturas necessárias no banco de dados.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                create table if not exists processed_events (
                    id bigserial primary key,
                    run_id text,
                    framework text,
                    scenario text,
                    event_id integer,
                    sent_at_ms bigint,
                    processed_at_ms bigint,
                    collected_at_ms bigint not null,
                    latency_ms double precision
                )
                """
            )
            cursor.execute(
                """
                create index if not exists processed_events_run_id_collected_idx
                on processed_events (run_id, collected_at_ms)
                """
            )
        self.connection.commit()

    def write(self, record):
        """! @brief Acumula um registro para persistência.
        @param record Registro que será processado.
        """
        self.pending.append(
            (
                record.get("run_id"),
                record.get("framework"),
                record.get("scenario"),
                record.get("event_id"),
                record.get("sent_at_ms"),
                record.get("processed_at_ms"),
                record.get("collected_at_ms"),
                latency_ms(record),
            )
        )

        if len(self.pending) >= self.batch_size:
            self.flush()

    def flush(self):
        """! @brief Persiste os registros pendentes no banco de dados.
        """
        if not self.pending:
            return

        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                insert into processed_events (
                    run_id,
                    framework,
                    scenario,
                    event_id,
                    sent_at_ms,
                    processed_at_ms,
                    collected_at_ms,
                    latency_ms
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                self.pending,
            )
        self.connection.commit()
        self.pending = []

    def close(self):
        """! @brief Finaliza a persistência e libera a conexão.
        """
        try:
            self.flush()
        finally:
            self.connection.close()


class BucketedPostgresSink:
    """! @brief Agrega e persiste eventos em buckets temporais no PostgreSQL.
    """
    def __init__(self, database_url, batch_size, bucket_seconds):
        """! @brief Inicializa a instância e seus recursos.
        @param database_url URL de conexão com o PostgreSQL.
        @param batch_size Valor do parâmetro `batch_size`.
        @param bucket_seconds Tamanho do bucket temporal, em segundos.
        """
        import psycopg

        self.batch_size = batch_size
        self.bucket_ms = int(bucket_seconds * 1000)
        self.connection = psycopg.connect(database_url)
        self.first_sent_at_ms = None
        self.first_collected_at_ms = None
        self.last_collected_at_ms = None
        self.framework = None
        self.scenario = None
        self.run_id = None
        self.eps = None
        self.metadata_loaded = False
        self.messages_since_flush = 0
        self.buckets = {}
        self.dirty_bucket_offsets = set()
        self.event_ids = set()
        self.latencies = []
        self.min_event_id = None
        self.max_event_id = None
        self._ensure_schema()

    def _ensure_schema(self):
        """! @brief Cria ou atualiza as estruturas necessárias no banco de dados.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                create table if not exists processed_event_buckets (
                    run_id text not null,
                    framework text,
                    scenario text,
                    bucket_offset_ms bigint not null,
                    bucket_start_ms bigint not null,
                    bucket_end_ms bigint not null,
                    messages integer not null,
                    distinct_events integer not null,
                    min_event_id integer,
                    max_event_id integer,
                    avg_latency_ms double precision,
                    median_latency_ms double precision,
                    p95_latency_ms double precision,
                    max_latency_ms double precision,
                    updated_at timestamptz not null default now(),
                    primary key (run_id, bucket_offset_ms)
                )
                """
            )
            cursor.execute(
                """
                create index if not exists processed_event_buckets_run_idx
                on processed_event_buckets (run_id, bucket_offset_ms)
                """
            )
            cursor.execute(
                """
                create table if not exists processed_run_summaries (
                    run_id text primary key,
                    framework text,
                    scenario text,
                    messages integer not null,
                    distinct_events integer not null,
                    min_event_id integer,
                    max_event_id integer,
                    first_sent_at_ms bigint,
                    first_collected_at_ms bigint,
                    last_collected_at_ms bigint,
                    avg_latency_ms double precision,
                    median_latency_ms double precision,
                    p95_latency_ms double precision,
                    max_latency_ms double precision,
                    updated_at timestamptz not null default now()
                )
                """
            )
            cursor.execute(
                "alter table processed_run_summaries add column if not exists first_sent_at_ms bigint"
            )
            cursor.execute(
                """
                create index if not exists processed_run_summaries_group_idx
                on processed_run_summaries (framework, scenario)
                """
            )
        self.connection.commit()

    def _bucket_for(self, collected_at_ms):
        """! @brief Calcula o deslocamento temporal do bucket de um registro.
        @param collected_at_ms Valor do parâmetro `collected_at_ms`.
        @return Resultado produzido pela operação.
        """
        if self.first_sent_at_ms is None:
            self.first_sent_at_ms = collected_at_ms

        return ((collected_at_ms - self.first_sent_at_ms) // self.bucket_ms) * self.bucket_ms

    def _load_run_metadata(self):
        """! @brief Carrega os metadados necessários para alinhar a execução.
        """
        if self.metadata_loaded or not self.run_id:
            return

        with self.connection.cursor() as cursor:
            cursor.execute("select eps from run_metadata where run_id = %s", (self.run_id,))
            row = cursor.fetchone()
        self.eps = float(row[0]) if row and row[0] else None
        self.metadata_loaded = True

    def write(self, record):
        """! @brief Acumula um registro para persistência.
        @param record Registro que será processado.
        """
        collected_at_ms = record.get("collected_at_ms")
        if collected_at_ms is None:
            return

        self.run_id = record.get("run_id") or self.run_id
        self.framework = record.get("framework") or self.framework
        self.scenario = record.get("scenario") or self.scenario
        self._load_run_metadata()
        self.last_collected_at_ms = collected_at_ms
        if self.first_collected_at_ms is None:
            self.first_collected_at_ms = collected_at_ms

        sent_at_ms = record.get("sent_at_ms")
        event_id = record.get("event_id")
        if isinstance(sent_at_ms, (int, float)):
            if self.first_sent_at_ms is None and isinstance(event_id, int) and self.eps:
                elapsed_before_event_ms = round((event_id - 1) * 1000 / self.eps)
                self.first_sent_at_ms = int(sent_at_ms) - elapsed_before_event_ms
            elif self.first_sent_at_ms is None:
                self.first_sent_at_ms = int(sent_at_ms)
        if isinstance(event_id, int):
            self.event_ids.add(event_id)
            self.min_event_id = event_id if self.min_event_id is None else min(self.min_event_id, event_id)
            self.max_event_id = event_id if self.max_event_id is None else max(self.max_event_id, event_id)

        latency = latency_ms(record)
        if latency is not None:
            self.latencies.append(latency)

        bucket_offset_ms = self._bucket_for(collected_at_ms)
        bucket = self.buckets.setdefault(
            bucket_offset_ms,
            {
                "messages": 0,
                "event_ids": set(),
                "min_event_id": None,
                "max_event_id": None,
                "latencies": [],
            },
        )
        bucket["messages"] += 1
        if isinstance(event_id, int):
            bucket["event_ids"].add(event_id)
            bucket["min_event_id"] = event_id if bucket["min_event_id"] is None else min(bucket["min_event_id"], event_id)
            bucket["max_event_id"] = event_id if bucket["max_event_id"] is None else max(bucket["max_event_id"], event_id)
        if latency is not None:
            bucket["latencies"].append(latency)

        self.dirty_bucket_offsets.add(bucket_offset_ms)
        self.messages_since_flush += 1

        if self.messages_since_flush >= self.batch_size:
            self.flush()

    def _latency_stats(self, values):
        """! @brief Calcula as estatísticas de latência de um conjunto de valores.
        @param values Valores usados no cálculo.
        @return Resultado produzido pela operação.
        """
        if not values:
            return (None, None, None, None)
        return (
            sum(values) / len(values),
            percentile(values, 50),
            percentile(values, 95),
            max(values),
        )

    def flush(self):
        """! @brief Persiste os registros pendentes no banco de dados.
        """
        if not self.run_id:
            return

        dirty_offsets = sorted(self.dirty_bucket_offsets)
        with self.connection.cursor() as cursor:
            for bucket_offset_ms in dirty_offsets:
                bucket = self.buckets[bucket_offset_ms]
                avg_latency, median_latency, p95_latency, max_latency = self._latency_stats(bucket["latencies"])
                bucket_start_ms = self.first_sent_at_ms + bucket_offset_ms
                bucket_end_ms = bucket_start_ms + self.bucket_ms
                cursor.execute(
                    """
                    insert into processed_event_buckets (
                        run_id,
                        framework,
                        scenario,
                        bucket_offset_ms,
                        bucket_start_ms,
                        bucket_end_ms,
                        messages,
                        distinct_events,
                        min_event_id,
                        max_event_id,
                        avg_latency_ms,
                        median_latency_ms,
                        p95_latency_ms,
                        max_latency_ms
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (run_id, bucket_offset_ms) do update set
                        framework = excluded.framework,
                        scenario = excluded.scenario,
                        bucket_start_ms = excluded.bucket_start_ms,
                        bucket_end_ms = excluded.bucket_end_ms,
                        messages = excluded.messages,
                        distinct_events = excluded.distinct_events,
                        min_event_id = excluded.min_event_id,
                        max_event_id = excluded.max_event_id,
                        avg_latency_ms = excluded.avg_latency_ms,
                        median_latency_ms = excluded.median_latency_ms,
                        p95_latency_ms = excluded.p95_latency_ms,
                        max_latency_ms = excluded.max_latency_ms,
                        updated_at = now()
                    """,
                    (
                        self.run_id,
                        self.framework,
                        self.scenario,
                        bucket_offset_ms,
                        bucket_start_ms,
                        bucket_end_ms,
                        bucket["messages"],
                        len(bucket["event_ids"]),
                        bucket["min_event_id"],
                        bucket["max_event_id"],
                        avg_latency,
                        median_latency,
                        p95_latency,
                        max_latency,
                    ),
                )

            avg_latency, median_latency, p95_latency, max_latency = self._latency_stats(self.latencies)
            cursor.execute(
                """
                insert into processed_run_summaries (
                    run_id,
                    framework,
                    scenario,
                    messages,
                    distinct_events,
                    min_event_id,
                    max_event_id,
                    first_sent_at_ms,
                    first_collected_at_ms,
                    last_collected_at_ms,
                    avg_latency_ms,
                    median_latency_ms,
                    p95_latency_ms,
                    max_latency_ms
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (run_id) do update set
                    framework = excluded.framework,
                    scenario = excluded.scenario,
                    messages = excluded.messages,
                    distinct_events = excluded.distinct_events,
                    min_event_id = excluded.min_event_id,
                    max_event_id = excluded.max_event_id,
                    first_sent_at_ms = excluded.first_sent_at_ms,
                    first_collected_at_ms = excluded.first_collected_at_ms,
                    last_collected_at_ms = excluded.last_collected_at_ms,
                    avg_latency_ms = excluded.avg_latency_ms,
                    median_latency_ms = excluded.median_latency_ms,
                    p95_latency_ms = excluded.p95_latency_ms,
                    max_latency_ms = excluded.max_latency_ms,
                    updated_at = now()
                """,
                (
                    self.run_id,
                    self.framework,
                    self.scenario,
                    sum(bucket["messages"] for bucket in self.buckets.values()),
                    len(self.event_ids),
                    self.min_event_id,
                    self.max_event_id,
                    self.first_sent_at_ms,
                    self.first_collected_at_ms,
                    self.last_collected_at_ms,
                    avg_latency,
                    median_latency,
                    p95_latency,
                    max_latency,
                ),
            )
        self.connection.commit()
        self.messages_since_flush = 0
        self.dirty_bucket_offsets = set()

    def close(self):
        """! @brief Finaliza a persistência e libera a conexão.
        """
        try:
            self.flush()
        finally:
            self.connection.close()


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()
    should_write_jsonl = args.write_jsonl == "1"
    output_path = build_output_path(args.output_dir, args.output_file, args.run_id) if should_write_jsonl else None
    db_sink = None

    if args.database_url:
        print(
            "Conectando ao Postgres/Neon para salvar resultados "
            f"em modo {args.db_storage_mode}..."
        )
        if args.db_storage_mode == "raw":
            db_sink = RawPostgresSink(args.database_url, args.db_batch_size)
        else:
            db_sink = BucketedPostgresSink(
                args.database_url,
                args.db_batch_size,
                args.db_bucket_seconds,
            )

    print(f"Iniciando servidor de métricas Prometheus na porta {args.metrics_port}...")
    start_http_server(args.metrics_port, addr="0.0.0.0")

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset=args.auto_offset_reset,
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=decode_message,
        consumer_timeout_ms=1000, 
    )

    print(
        "Iniciando coleta: "
        f"topic={args.topic}, bootstrap={args.bootstrap_server}, "
        f"output={output_path or 'desativado'}, "
        f"database={'ativado' if db_sink else 'desativado'}"
    )

    saved = 0
    last_message_at = time.monotonic()

    try:
        output_context = open(output_path, "a", encoding="utf-8") if should_write_jsonl else nullcontext(None)
        with output_context as output:
            while True:
                got_message = False

                for message in consumer:
                    # Cada mensagem valida vira uma linha no arquivo de saida.
                    got_message = True
                    record = message.value

                    if not should_keep(record, args.run_id):
                        continue

                    last_message_at = time.monotonic()

                    # Exposição da métrica de latência para Prometheus.
                    fw = record.get("framework", "unknown")
                    scen = record.get("scenario", "unknown")
                    metric_run_id = record.get("run_id", args.run_id or "unknown")
                    latency = record.get("latency_ms")
                    
                    if latency is not None:
                        if latency > 60000:
                            latency = latency // 1_000_000
                        LATENCY_HISTOGRAM.labels(
                            framework=fw,
                            scenario=scen,
                            run_id=metric_run_id,
                        ).observe(latency)
                        MESSAGES_PROCESSED.labels(
                            framework=fw,
                            scenario=scen,
                            run_id=metric_run_id,
                        ).inc()

                    record["collected_at_ms"] = now_ms()
                    if output is not None:
                        write_record(output, record)
                    if db_sink is not None:
                        db_sink.write(record)

                    saved += 1

                    if args.max_messages is not None and saved >= args.max_messages:
                        print(f"Limite atingido: {saved} mensagens salvas.")
                        if db_sink is not None:
                            db_sink.flush()
                        time.sleep(15)
                        return

                # Em experimentos automatizados, isso evita deixar o coletor preso.
                if not got_message and args.idle_timeout is not None:
                    idle_for = time.monotonic() - last_message_at
                    if idle_for >= args.idle_timeout:
                        print(
                            "Tempo sem mensagens atingido: "
                            f"{idle_for:.1f}s, {saved} mensagens salvas."
                        )
                        return

    except KeyboardInterrupt:
        print("\nColeta interrompida pelo usuario.")
    finally:
        if db_sink is not None:
            db_sink.close()
        consumer.close()

    print(f"Finalizado: {saved} mensagens salvas.")


if __name__ == "__main__":
    main()
