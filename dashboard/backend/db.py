import os
from contextlib import contextmanager

import psycopg
from psycopg.rows import dict_row


def database_url():
    """! @brief Obtém a URL de conexão com o PostgreSQL.
    @return Resultado produzido pela operação.
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL nao definido.")
    return url


@contextmanager
def get_connection():
    """! @brief Abre uma conexão com o PostgreSQL configurado.
    """
    with psycopg.connect(database_url(), row_factory=dict_row) as connection:
        yield connection


def fetch_all(query, params=None):
    """! @brief Executa uma consulta e retorna todas as linhas como dicionários.
    @param query Consulta SQL que será executada.
    @param params Parâmetros associados à consulta.
    @return Resultado produzido pela operação.
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or {})
            return [dict(row) for row in cursor.fetchall()]


def fetch_one(query, params=None):
    """! @brief Executa uma consulta e retorna a primeira linha como dicionário.
    @param query Consulta SQL que será executada.
    @param params Parâmetros associados à consulta.
    @return Resultado produzido pela operação.
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or {})
            row = cursor.fetchone()
            return dict(row) if row else None


def ensure_metadata_schema():
    """! @brief Garante a existência das tabelas de metadados e métricas.
    """
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                create table if not exists run_metadata (
                    run_id text primary key,
                    framework text not null,
                    scenario text not null,
                    iteration text not null,
                    mode text not null,
                    eps double precision,
                    duration_seconds double precision,
                    max_events integer,
                    fault_service text,
                    fault_action text,
                    fault_count integer,
                    fault_delay_seconds double precision,
                    fault_duration_seconds double precision,
                    created_at timestamptz not null default now(),
                    updated_at timestamptz not null default now()
                )
                """
            )
            cursor.execute("alter table run_metadata add column if not exists framework text")
            cursor.execute("alter table run_metadata add column if not exists scenario text")
            cursor.execute("alter table run_metadata add column if not exists iteration text")
            cursor.execute("alter table run_metadata add column if not exists mode text")
            cursor.execute("alter table run_metadata add column if not exists eps double precision")
            cursor.execute(
                "alter table run_metadata add column if not exists duration_seconds double precision"
            )
            cursor.execute("alter table run_metadata add column if not exists max_events integer")
            cursor.execute("alter table run_metadata add column if not exists fault_service text")
            cursor.execute("alter table run_metadata add column if not exists fault_action text")
            cursor.execute("alter table run_metadata add column if not exists fault_count integer")
            cursor.execute(
                "alter table run_metadata add column if not exists fault_delay_seconds double precision"
            )
            cursor.execute(
                "alter table run_metadata add column if not exists fault_duration_seconds double precision"
            )
            cursor.execute(
                "alter table run_metadata add column if not exists created_at timestamptz not null default now()"
            )
            cursor.execute(
                "alter table run_metadata add column if not exists updated_at timestamptz not null default now()"
            )
            cursor.execute(
                """
                create index if not exists run_metadata_group_idx
                on run_metadata (framework, scenario, mode, fault_service, fault_action)
                """
            )
            cursor.execute(
                """
                create table if not exists fault_events (
                    id bigserial primary key,
                    run_id text not null,
                    service text,
                    container text,
                    action text,
                    phase text,
                    timestamp_ms bigint,
                    delay_seconds integer,
                    duration_seconds integer,
                    count integer
                )
                """
            )
            cursor.execute(
                """
                create index if not exists fault_events_run_id_timestamp_idx
                on fault_events (run_id, timestamp_ms)
                """
            )
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
            cursor.execute(
                """
                create table if not exists resource_usage_buckets (
                    run_id text not null,
                    framework text,
                    scenario text,
                    bucket_offset_ms bigint not null,
                    bucket_start_ms bigint not null,
                    bucket_end_ms bigint not null,
                    memory_working_set_bytes double precision,
                    cpu_usage_cores double precision,
                    source text not null default 'prometheus:docker_total',
                    updated_at timestamptz not null default now(),
                    primary key (run_id, bucket_offset_ms)
                )
                """
            )
            cursor.execute(
                "alter table resource_usage_buckets add column if not exists cpu_usage_cores double precision"
            )
            cursor.execute(
                """
                create index if not exists resource_usage_buckets_run_idx
                on resource_usage_buckets (run_id, bucket_offset_ms)
                """
            )
        connection.commit()
