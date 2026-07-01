#!/usr/bin/env python3

import argparse
import json
import math
import os
import sys
import time
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_PROMETHEUS_URL = "http://localhost:9090"
DOCKER_CGROUP_PATTERN = r'/docker/.+|/system[.]slice/docker-.+[.]scope'
DEFAULT_MEMORY_QUERY = (
    f'sum(container_memory_working_set_bytes{{id=~"{DOCKER_CGROUP_PATTERN}"}})'
)
DEFAULT_CPU_QUERY = (
    f'sum(rate(container_cpu_usage_seconds_total{{id=~"{DOCKER_CGROUP_PATTERN}"}}[30s]))'
)


def parse_args():
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Exporta uso total de memoria dos containers Docker do Prometheus "
            "para o Postgres/Neon, cruzando os intervalos das runs ja salvas."
        )
    )
    parser.add_argument("--run-id", action="append", dest="run_ids")
    parser.add_argument("--framework", choices=["spark", "flink"], default=None)
    parser.add_argument("--scenario", default=None)
    parser.add_argument("--mode", choices=["normal", "fault"], default=None)
    parser.add_argument("--prometheus-url", default=os.environ.get("PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL))
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--step-seconds", type=float, default=float(os.environ.get("RESOURCE_STEP_SECONDS", "5")))
    parser.add_argument("--memory-query", default=os.environ.get("RESOURCE_MEMORY_PROMQL", DEFAULT_MEMORY_QUERY))
    parser.add_argument("--cpu-query", default=os.environ.get("RESOURCE_CPU_PROMQL", DEFAULT_CPU_QUERY))
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def load_dotenv():
    """! @brief Carrega variáveis do arquivo .env sem sobrescrever o ambiente.
    """
    env_path = os.path.join(ROOT_DIR, ".env")
    if not os.path.exists(env_path):
        return

    with open(env_path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :]
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip().strip("'").strip('"')
            os.environ.setdefault(key.strip(), value)


def prometheus_get(prometheus_url, path, params):
    """! @brief Executa uma consulta HTTP à API do Prometheus.
    @param prometheus_url Valor do parâmetro `prometheus_url`.
    @param path Caminho do recurso.
    @param params Parâmetros associados à consulta.
    @return Resultado produzido pela operação.
    """
    url = f"{prometheus_url.rstrip('/')}{path}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        details = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Prometheus retornou HTTP {error.code}: {details}"
        ) from error
    if payload.get("status") != "success":
        raise RuntimeError(f"Erro do Prometheus: {payload}")
    return payload["data"]


def ensure_schema(connection):
    """! @brief Cria ou atualiza as estruturas necessárias no banco de dados.
    @param connection Conexão ativa com o PostgreSQL.
    """
    with connection.cursor() as cursor:
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


def fetch_runs(connection, args):
    """! @brief Seleciona no banco as execuções que terão recursos exportados.
    @param connection Conexão ativa com o PostgreSQL.
    @param args Argumentos e opções da operação.
    @return Resultado produzido pela operação.
    """
    with connection.cursor() as cursor:
        cursor.execute(
            """
            with event_runs as (
                select
                    run_id,
                    framework,
                    scenario,
                    coalesce(first_sent_at_ms, first_collected_at_ms) as run_start_at_ms,
                    first_collected_at_ms,
                    last_collected_at_ms
                from processed_run_summaries
                union all
                select
                    e.run_id,
                    min(e.framework) as framework,
                    min(e.scenario) as scenario,
                    min(coalesce(e.sent_at_ms, e.collected_at_ms)) as run_start_at_ms,
                    min(e.collected_at_ms) as first_collected_at_ms,
                    max(e.collected_at_ms) as last_collected_at_ms
                from processed_events e
                where not exists (
                    select 1 from processed_run_summaries s where s.run_id = e.run_id
                )
                group by e.run_id
                union all
                select
                    b.run_id,
                    min(b.framework) as framework,
                    min(b.scenario) as scenario,
                    min(b.bucket_start_ms) as run_start_at_ms,
                    min(b.bucket_start_ms) as first_collected_at_ms,
                    max(b.bucket_end_ms) as last_collected_at_ms
                from processed_event_buckets b
                where not exists (
                    select 1 from processed_run_summaries s where s.run_id = b.run_id
                )
                  and not exists (
                    select 1 from processed_events e where e.run_id = b.run_id
                )
                group by b.run_id
            )
            select
                e.run_id,
                coalesce(m.framework, e.framework) as framework,
                coalesce(m.scenario, e.scenario) as scenario,
                coalesce(m.mode, case when e.run_id like '%%_fault_%%' then 'fault' else 'normal' end) as mode,
                e.run_start_at_ms,
                e.first_collected_at_ms,
                e.last_collected_at_ms
            from event_runs e
            left join run_metadata m on e.run_id = m.run_id
            where (cast(%(run_ids)s as text[]) is null or e.run_id = any(cast(%(run_ids)s as text[])))
              and (cast(%(framework)s as text) is null or coalesce(m.framework, e.framework) = %(framework)s)
              and (cast(%(scenario)s as text) is null or coalesce(m.scenario, e.scenario) = %(scenario)s)
              and (cast(%(mode)s as text) is null or coalesce(m.mode, case when e.run_id like '%%_fault_%%' then 'fault' else 'normal' end) = %(mode)s)
              and e.run_start_at_ms is not null
              and e.last_collected_at_ms is not null
            order by e.run_start_at_ms
            """,
            {
                "run_ids": args.run_ids,
                "framework": args.framework,
                "scenario": args.scenario,
                "mode": args.mode,
            },
        )
        return cursor.fetchall()


def query_prometheus_series(args, query, run):
    """! @brief Consulta uma série temporal do Prometheus para uma execução.
    @param args Argumentos e opções da operação.
    @param query Consulta SQL que será executada.
    @param run Valor do parâmetro `run`.
    @return Resultado produzido pela operação.
    """
    start_seconds = run["run_start_at_ms"] / 1000
    end_seconds = run["last_collected_at_ms"] / 1000
    step = f"{args.step_seconds:g}s"
    data = prometheus_get(
        args.prometheus_url,
        "/api/v1/query_range",
        {
            "query": query,
            "start": f"{start_seconds:.3f}",
            "end": f"{end_seconds:.3f}",
            "step": step,
        },
    )
    result = data.get("result", [])
    if not result:
        return {}

    values = result[0].get("values", [])
    first_ms = run["run_start_at_ms"]
    series = {}
    for timestamp_seconds, value in values:
        numeric_value = float(value)
        if not math.isfinite(numeric_value):
            continue
        series[max(0, int(float(timestamp_seconds) * 1000) - first_ms)] = numeric_value
    return series


def query_run_points(args, run):
    """! @brief Combina séries de CPU e memória em pontos temporais.
    @param args Argumentos e opções da operação.
    @param run Valor do parâmetro `run`.
    @return Resultado produzido pela operação.
    """
    memory_values = query_prometheus_series(args, args.memory_query, run)
    cpu_values = query_prometheus_series(args, args.cpu_query, run)
    if not memory_values and not cpu_values:
        print(
            f"Aviso: Prometheus nao retornou RAM nem CPU para run_id={run['run_id']}. "
            "Verifique os labels id das metricas do cAdvisor.",
            file=sys.stderr,
        )
    offsets = sorted(set(memory_values) | set(cpu_values))

    points = []
    step_ms = int(args.step_seconds * 1000)
    first_ms = run["run_start_at_ms"]
    for bucket_offset_ms in offsets:
        bucket_start_ms = first_ms + bucket_offset_ms
        points.append(
            {
                "run_id": run["run_id"],
                "framework": run["framework"],
                "scenario": run["scenario"],
                "bucket_offset_ms": bucket_offset_ms,
                "bucket_start_ms": bucket_start_ms,
                "bucket_end_ms": bucket_start_ms + step_ms,
                "memory_working_set_bytes": memory_values.get(bucket_offset_ms),
                "cpu_usage_cores": cpu_values.get(bucket_offset_ms),
            }
        )
    return points


def write_points(connection, points):
    """! @brief Persiste pontos de uso de recursos no PostgreSQL.
    @param connection Conexão ativa com o PostgreSQL.
    @param points Valor do parâmetro `points`.
    """
    if not points:
        return

    with connection.cursor() as cursor:
        cursor.executemany(
            """
            insert into resource_usage_buckets (
                run_id,
                framework,
                scenario,
                bucket_offset_ms,
                bucket_start_ms,
                bucket_end_ms,
                memory_working_set_bytes,
                cpu_usage_cores,
                source
            )
            values (
                %(run_id)s,
                %(framework)s,
                %(scenario)s,
                %(bucket_offset_ms)s,
                %(bucket_start_ms)s,
                %(bucket_end_ms)s,
                %(memory_working_set_bytes)s,
                %(cpu_usage_cores)s,
                'prometheus:docker_total'
            )
            on conflict (run_id, bucket_offset_ms) do update set
                framework = excluded.framework,
                scenario = excluded.scenario,
                bucket_start_ms = excluded.bucket_start_ms,
                bucket_end_ms = excluded.bucket_end_ms,
                memory_working_set_bytes = excluded.memory_working_set_bytes,
                cpu_usage_cores = excluded.cpu_usage_cores,
                source = excluded.source,
                updated_at = now()
            """,
            points,
        )
    connection.commit()


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    load_dotenv()
    args = parse_args()
    if not args.database_url:
        args.database_url = os.environ.get("DATABASE_URL")
    if not args.database_url:
        raise SystemExit("DATABASE_URL nao definido.")

    import psycopg
    from psycopg.rows import dict_row

    started_at = time.time()
    total_points = 0
    with psycopg.connect(args.database_url, row_factory=dict_row) as connection:
        ensure_schema(connection)
        runs = fetch_runs(connection, args)
        print(f"Runs encontradas: {len(runs)}")

        for index, run in enumerate(runs, start=1):
            points = query_run_points(args, run)
            total_points += len(points)
            print(
                f"[{index}/{len(runs)}] {run['run_id']}: "
                f"{len(points)} ponto(s) de memoria"
            )
            if not args.dry_run:
                write_points(connection, points)

    elapsed = time.time() - started_at
    action = "Simularia exportacao" if args.dry_run else "Exportacao concluida"
    print(f"{action}: {total_points} ponto(s), {elapsed:.1f}s")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
