import math
import os
import subprocess
import time
from pathlib import Path
from typing import Literal, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from db import ensure_metadata_schema, fetch_all, fetch_one, get_connection


app = FastAPI(title="Streaming Benchmark Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


T_CRITICAL_95 = {
    1: 12.706,
    2: 4.303,
    3: 3.182,
    4: 2.776,
    5: 2.571,
    6: 2.447,
    7: 2.365,
    8: 2.306,
    9: 2.262,
    10: 2.228,
    11: 2.201,
    12: 2.179,
    13: 2.160,
    14: 2.145,
    15: 2.131,
    16: 2.120,
    17: 2.110,
    18: 2.101,
    19: 2.093,
    20: 2.086,
    21: 2.080,
    22: 2.074,
    23: 2.069,
    24: 2.064,
    25: 2.060,
    26: 2.056,
    27: 2.052,
    28: 2.048,
    29: 2.045,
    30: 2.042,
}

ROOT_DIR = Path(__file__).resolve().parents[2]
EXPERIMENT_LOG_DIR = ROOT_DIR / "results" / "dashboard-runs"
EXPERIMENT_JOBS = {}
FRAMEWORK_VALUES = {"spark", "flink"}
NORMAL_SCENARIOS = {"low", "medium", "high"}
FAULT_SCENARIOS = {"fault_medium", "fault_high"}
FAULT_ACTION_VALUES = {"kill", "stop-start"}
FAULT_SERVICE_BY_FRAMEWORK = {
    "spark": "spark-worker",
    "flink": "taskmanager",
}


class ExperimentRequest(BaseModel):
    """! @brief Representa os parâmetros enviados para iniciar um experimento.
    """
    framework: Literal["spark", "flink"]
    scenario: str
    mode: Literal["normal", "fault"] = "normal"
    fault_action: Optional[Literal["kill", "stop-start"]] = None
    fault_delay_seconds: int = Field(default=120, ge=0, le=3600)
    fault_duration_seconds: int = Field(default=10, ge=0, le=3600)


def t_critical_95(sample_count):
    """! @brief Retorna o valor crítico para um intervalo de confiança de 95%.
    @param sample_count Valor do parâmetro `sample_count`.
    @return Resultado produzido pela operação.
    """
    if sample_count is None or sample_count < 2:
        return None
    degrees = int(sample_count) - 1
    return T_CRITICAL_95.get(degrees, 1.96)


def add_ci95(row, mean_key, stddev_key, sample_count_key, prefix):
    """! @brief Acrescenta os limites do intervalo de confiança de 95% a um resultado.
    @param row Valor do parâmetro `row`.
    @param mean_key Valor do parâmetro `mean_key`.
    @param stddev_key Valor do parâmetro `stddev_key`.
    @param sample_count_key Valor do parâmetro `sample_count_key`.
    @param prefix Valor do parâmetro `prefix`.
    @return Resultado produzido pela operação.
    """
    mean = row.get(mean_key)
    stddev = row.get(stddev_key)
    sample_count = row.get(sample_count_key)
    low_key = f"{prefix}_ci95_low_ms"
    high_key = f"{prefix}_ci95_high_ms"
    margin_key = f"{prefix}_ci95_margin_ms"

    row[low_key] = None
    row[high_key] = None
    row[margin_key] = None

    if mean is None or stddev is None or sample_count is None or sample_count < 2:
        return row

    critical = t_critical_95(sample_count)
    if critical is None:
        return row

    margin = critical * (float(stddev) / math.sqrt(float(sample_count)))
    row[low_key] = float(mean) - margin
    row[high_key] = float(mean) + margin
    row[margin_key] = margin
    return row


def cpu_total_cores():
    """! @brief Determina o total de núcleos usado para normalizar o consumo de CPU.
    @return Resultado produzido pela operação.
    """
    configured = os.environ.get("RESOURCE_CPU_TOTAL_CORES")
    if configured:
        try:
            value = float(configured)
            if value > 0:
                return value
        except ValueError:
            pass
    return float(os.cpu_count() or 1)


@app.on_event("startup")
def startup():
    """! @brief Inicializa o schema do banco quando a API é iniciada.
    """
    ensure_metadata_schema()


def parse_run_id(run_id):
    """! @brief Decompõe um identificador de execução em seus componentes.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    parts = run_id.rsplit("_", 2)
    if len(parts) != 3:
        return {"framework": None, "scenario": None, "iteration": None}
    return {"framework": parts[0], "scenario": parts[1], "iteration": parts[2]}


def ensure_run_exists(run_id):
    """! @brief Valida que uma execução possui dados persistidos.
    @param run_id Identificador único da execução.
    """
    row = fetch_one(
        """
        select 1 from processed_run_summaries where run_id = %(run_id)s
        union all
        select 1 from processed_event_buckets where run_id = %(run_id)s
        union all
        select 1 from processed_events where run_id = %(run_id)s
        limit 1
        """,
        {"run_id": run_id},
    )
    if row is None:
        raise HTTPException(status_code=404, detail=f"run_id nao encontrado: {run_id}")


@app.get("/api/health")
def health():
    """! @brief Informa o estado de disponibilidade da API.
    @return Resultado produzido pela operação.
    """
    return {"status": "ok"}


def experiment_status(job):
    """! @brief Converte o estado de um subprocesso em uma resposta da API.
    @param job Metadados e processo do experimento.
    @return Resultado produzido pela operação.
    """
    process = job["process"]
    return_code = process.poll()
    if return_code is None:
        status = "running"
    elif return_code == 0:
        status = "finished"
    else:
        status = "failed"

    return {
        "id": job["id"],
        "pid": process.pid,
        "status": status,
        "return_code": return_code,
        "framework": job["framework"],
        "scenario": job["scenario"],
        "mode": job["mode"],
        "fault_action": job["fault_action"],
        "fault_service": job["fault_service"],
        "started_at_ms": job["started_at_ms"],
        "log_file": str(job["log_file"]),
    }


def tail_file(path, max_lines=40):
    """! @brief Lê as últimas linhas de um arquivo de log.
    @param path Caminho do recurso.
    @param max_lines Valor do parâmetro `max_lines`.
    @return Resultado produzido pela operação.
    """
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", errors="replace") as handle:
        lines = handle.readlines()
    return [line.rstrip("\n") for line in lines[-max_lines:]]


@app.post("/api/experiments/start")
def start_experiment(request: ExperimentRequest):
    """! @brief Inicia uma execução do benchmark em um subprocesso.
    @param request Dados enviados na requisição.
    @return Resultado produzido pela operação.
    """
    valid_scenarios = FAULT_SCENARIOS if request.mode == "fault" else NORMAL_SCENARIOS
    if request.scenario not in valid_scenarios:
        raise HTTPException(
            status_code=400,
            detail=f"cenario invalido para modo {request.mode}: {request.scenario}",
        )

    fault_service = ""
    fault_action = ""
    if request.mode == "fault":
        fault_service = FAULT_SERVICE_BY_FRAMEWORK[request.framework]
        fault_action = request.fault_action or "kill"
        if fault_action not in FAULT_ACTION_VALUES:
            raise HTTPException(status_code=400, detail=f"acao de falha invalida: {fault_action}")

    EXPERIMENT_LOG_DIR.mkdir(parents=True, exist_ok=True)
    started_at_ms = int(time.time() * 1000)
    job_id = f"{request.framework}_{request.scenario}_{request.mode}_{started_at_ms}"
    log_file = EXPERIMENT_LOG_DIR / f"{job_id}.log"

    env = os.environ.copy()
    env.update(
        {
            "AUTO_ITERATION": "1",
            "WRITE_JSONL": "0",
            "RESET_TOPICS_PER_RUN": "1",
            "STOP_OTHER_FRAMEWORK": "1",
            "DB_STORAGE_MODE": "raw" if request.mode == "fault" else "bucket",
            "DB_BUCKET_SECONDS": "0.5",
        }
    )
    if request.mode == "fault":
        env.update(
            {
                "FAULT_SERVICE": fault_service,
                "FAULT_ACTION": fault_action,
                "FAULT_COUNT": "2",
                "FAULT_DELAY_SECONDS": str(request.fault_delay_seconds),
                "FAULT_DURATION_SECONDS": str(request.fault_duration_seconds),
            }
        )

    command = [
        "scripts/run_experiment.sh",
        "--framework",
        request.framework,
        "--scenario",
        request.scenario,
    ]

    try:
        log_handle = log_file.open("a", encoding="utf-8")
        process = subprocess.Popen(
            command,
            cwd=ROOT_DIR,
            env=env,
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
    except OSError as exc:
        raise HTTPException(status_code=500, detail=f"nao foi possivel iniciar experimento: {exc}") from exc

    EXPERIMENT_JOBS[job_id] = {
        "id": job_id,
        "process": process,
        "framework": request.framework,
        "scenario": request.scenario,
        "mode": request.mode,
        "fault_action": fault_action,
        "fault_service": fault_service,
        "started_at_ms": started_at_ms,
        "log_file": log_file,
    }

    return experiment_status(EXPERIMENT_JOBS[job_id])


@app.get("/api/experiments")
def list_experiment_jobs():
    """! @brief Lista os experimentos iniciados pela instância atual da API.
    @return Resultado produzido pela operação.
    """
    return [experiment_status(job) for job in sorted(
        EXPERIMENT_JOBS.values(),
        key=lambda item: item["started_at_ms"],
        reverse=True,
    )]


@app.get("/api/experiments/{job_id}/log")
def experiment_log(job_id: str, lines: int = Query(40, ge=1, le=300)):
    """! @brief Retorna o estado e as últimas linhas do log de um experimento.
    @param job_id Identificador do processo iniciado pelo dashboard.
    @param lines Quantidade máxima de linhas retornadas.
    @return Resultado produzido pela operação.
    """
    job = EXPERIMENT_JOBS.get(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"job nao encontrado: {job_id}")

    return {
        **experiment_status(job),
        "lines": tail_file(job["log_file"], lines),
    }


@app.get("/api/runs")
def list_runs(
    framework: Optional[str] = None,
    scenario: Optional[str] = None,
    mode: Optional[str] = Query(None, pattern="^(normal|fault)$"),
    fault_service: Optional[str] = None,
    fault_action: Optional[str] = None,
):
    """! @brief Lista execuções persistidas aplicando os filtros informados.
    @param framework Framework de processamento selecionado.
    @param scenario Cenário experimental selecionado.
    @param mode Valor do parâmetro `mode`.
    @param fault_service Valor do parâmetro `fault_service`.
    @param fault_action Valor do parâmetro `fault_action`.
    @return Resultado produzido pela operação.
    """
    rows = fetch_all(
        """
        with event_runs as (
            select
                run_id,
                framework as event_framework,
                scenario as event_scenario,
                messages,
                min_event_id,
                max_event_id,
                first_sent_at_ms,
                first_collected_at_ms,
                last_collected_at_ms,
                avg_latency_ms,
                p95_latency_ms
            from processed_run_summaries
            union all
            select
                e.run_id,
                min(e.framework) as event_framework,
                min(e.scenario) as event_scenario,
                count(*) as messages,
                min(e.event_id) as min_event_id,
                max(e.event_id) as max_event_id,
                min(e.sent_at_ms) as first_sent_at_ms,
                min(e.collected_at_ms) as first_collected_at_ms,
                max(e.collected_at_ms) as last_collected_at_ms,
                avg(e.latency_ms) as avg_latency_ms,
                percentile_cont(0.95) within group (order by e.latency_ms) as p95_latency_ms
            from processed_events e
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = e.run_id
            )
            group by e.run_id
            union all
            select
                b.run_id,
                min(b.framework) as event_framework,
                min(b.scenario) as event_scenario,
                sum(b.messages) as messages,
                min(b.min_event_id) as min_event_id,
                max(b.max_event_id) as max_event_id,
                null::bigint as first_sent_at_ms,
                min(b.bucket_start_ms) as first_collected_at_ms,
                max(b.bucket_end_ms) as last_collected_at_ms,
                sum(b.avg_latency_ms * b.messages) / nullif(sum(b.messages), 0) as avg_latency_ms,
                avg(b.p95_latency_ms) as p95_latency_ms
            from processed_event_buckets b
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = b.run_id
            )
              and not exists (
                select 1 from processed_events e where e.run_id = b.run_id
            )
            group by b.run_id
        ),
        fault_summary as (
            select
                run_id,
                max(service) filter (where phase = 'fault_start') as inferred_fault_service,
                max(action) filter (where phase = 'fault_start') as inferred_fault_action,
                max(count) filter (where phase = 'fault_start') as inferred_fault_count
            from fault_events
            group by run_id
        )
        select
            e.run_id,
            coalesce(m.framework, e.event_framework) as framework,
            coalesce(m.scenario, e.event_scenario) as scenario,
            coalesce(m.iteration, split_part(e.run_id, '_', 3)) as iteration,
            coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) as mode,
            coalesce(m.fault_service, f.inferred_fault_service) as fault_service,
            coalesce(m.fault_action, f.inferred_fault_action) as fault_action,
            coalesce(m.fault_count, f.inferred_fault_count) as fault_count,
            m.eps,
            m.duration_seconds,
            m.max_events,
            e.messages,
            e.min_event_id,
            e.max_event_id,
            e.first_sent_at_ms,
            e.first_collected_at_ms,
            e.last_collected_at_ms,
            e.avg_latency_ms,
            e.p95_latency_ms
        from event_runs e
        left join run_metadata m on e.run_id = m.run_id
        left join fault_summary f on e.run_id = f.run_id
        where (cast(%(framework)s as text) is null or coalesce(m.framework, e.event_framework) = %(framework)s)
          and (cast(%(scenario)s as text) is null or coalesce(m.scenario, e.event_scenario) = %(scenario)s)
          and (cast(%(mode)s as text) is null or coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) = %(mode)s)
          and (cast(%(fault_service)s as text) is null or coalesce(m.fault_service, f.inferred_fault_service) = %(fault_service)s)
          and (cast(%(fault_action)s as text) is null or coalesce(m.fault_action, f.inferred_fault_action) = %(fault_action)s)
        order by e.last_collected_at_ms desc
        """,
        {
            "framework": framework,
            "scenario": scenario,
            "mode": mode,
            "fault_service": fault_service,
            "fault_action": fault_action,
        },
    )
    return rows


@app.get("/api/runs/{run_id}/summary")
def run_summary(run_id: str, warmup_seconds: float = Query(0, ge=0)):
    """! @brief Calcula o resumo estatístico de uma execução.
    @param run_id Identificador único da execução.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @return Resultado produzido pela operação.
    """
    ensure_run_exists(run_id)
    summary = fetch_one(
        """
        with compact_all as (
            select *
            from processed_run_summaries
            where run_id = %(run_id)s
        ),
        raw_all as (
            select
                e.run_id,
                count(*) as messages,
                count(distinct e.event_id) as distinct_events,
                min(e.event_id) as min_event_id,
                max(e.event_id) as max_event_id,
                min(e.sent_at_ms) as first_sent_at_ms,
                min(e.collected_at_ms) as first_collected_at_ms,
                max(e.collected_at_ms) as last_collected_at_ms
            from processed_events e
            where e.run_id = %(run_id)s
              and not exists (select 1 from compact_all)
            group by e.run_id
        ),
        all_events as (
            select
                run_id,
                messages,
                distinct_events,
                min_event_id,
                max_event_id,
                first_sent_at_ms,
                first_collected_at_ms,
                last_collected_at_ms
            from compact_all
            union all
            select *
            from raw_all
        ),
        compact_latency as (
            select
                sum(b.messages) as measured_messages,
                count(avg_latency_ms) as latency_sample_count,
                sum(b.avg_latency_ms * b.messages) / nullif(sum(b.messages), 0) as avg_latency_ms,
                avg(b.median_latency_ms) as median_latency_ms,
                avg(b.p95_latency_ms) as p95_latency_ms,
                stddev_samp(b.avg_latency_ms) as stddev_latency_ms,
                max(b.max_latency_ms) as max_latency_ms
            from processed_event_buckets b
            cross join all_events a
            where b.run_id = %(run_id)s
              and b.bucket_offset_ms >= %(warmup_ms)s
        ),
        raw_latency as (
            select
                count(*) as measured_messages,
                count(latency_ms) as latency_sample_count,
                avg(latency_ms) as avg_latency_ms,
                percentile_cont(0.5) within group (order by latency_ms) as median_latency_ms,
                percentile_cont(0.95) within group (order by latency_ms) as p95_latency_ms,
                stddev_samp(latency_ms) as stddev_latency_ms,
                max(latency_ms) as max_latency_ms
            from processed_events e
            cross join all_events a
            where e.run_id = %(run_id)s
              and e.latency_ms is not null
              and e.collected_at_ms
                  >= coalesce(a.first_sent_at_ms, a.first_collected_at_ms) + %(warmup_ms)s
              and not exists (
                  select 1 from processed_event_buckets b where b.run_id = %(run_id)s
              )
        ),
        latency as (
            select * from compact_latency
            where exists (select 1 from processed_event_buckets b where b.run_id = %(run_id)s)
            union all
            select * from raw_latency
            where not exists (select 1 from processed_event_buckets b where b.run_id = %(run_id)s)
        )
        select
            a.messages,
            a.distinct_events,
            a.min_event_id,
            a.max_event_id,
            greatest((a.max_event_id - a.min_event_id + 1) - a.distinct_events, 0) as missing_event_ids,
            greatest(a.messages - a.distinct_events, 0) as duplicate_messages,
            a.first_sent_at_ms,
            a.first_collected_at_ms,
            a.last_collected_at_ms,
            l.measured_messages,
            l.latency_sample_count,
            l.avg_latency_ms,
            l.median_latency_ms,
            l.p95_latency_ms,
            l.stddev_latency_ms,
            l.max_latency_ms
        from all_events a
        cross join latency l
        """,
        {"run_id": run_id, "warmup_ms": int(warmup_seconds * 1000)},
    )
    if summary is None:
        raise HTTPException(status_code=404, detail=f"run_id nao encontrado: {run_id}")

    summary["run_id"] = run_id
    summary["warmup_seconds"] = warmup_seconds
    summary.update(parse_run_id(run_id))
    add_ci95(summary, "avg_latency_ms", "stddev_latency_ms", "latency_sample_count", "avg_latency")
    return summary


@app.delete("/api/runs/{run_id}")
def delete_run(run_id: str):
    """! @brief Remove todos os dados associados a uma execução.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    ensure_run_exists(run_id)
    tables = [
        "resource_usage_buckets",
        "fault_events",
        "processed_event_buckets",
        "processed_run_summaries",
        "processed_events",
        "run_metadata",
    ]
    deleted = {}

    with get_connection() as connection:
        with connection.cursor() as cursor:
            for table in tables:
                cursor.execute(f"delete from {table} where run_id = %(run_id)s", {"run_id": run_id})
                deleted[table] = cursor.rowcount
        connection.commit()

    return {"run_id": run_id, "deleted": deleted}


@app.get("/api/runs/{run_id}/latency-series")
def latency_series(
    run_id: str,
    bucket_seconds: float = Query(5, gt=0),
    warmup_seconds: float = Query(0, ge=0),
):
    """! @brief Retorna a série temporal de latência e throughput de uma execução.
    @param run_id Identificador único da execução.
    @param bucket_seconds Tamanho do bucket temporal, em segundos.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @return Resultado produzido pela operação.
    """
    ensure_run_exists(run_id)
    rows = fetch_all(
        """
        with bounds as (
            select coalesce(first_sent_at_ms, first_collected_at_ms) as first_ts
            from processed_run_summaries
            where run_id = %(run_id)s
            union all
            select min(coalesce(sent_at_ms, collected_at_ms)) as first_ts
            from processed_events
            where run_id = %(run_id)s
              and not exists (
                  select 1 from processed_run_summaries where run_id = %(run_id)s
              )
        ),
        compact as (
            select
                floor(b.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s as bucket_offset_ms,
                min(b.bucket_start_ms) as bucket_start_ms,
                sum(b.messages) as messages,
                sum(b.messages) * 1000.0 / %(bucket_ms)s as throughput_eps,
                sum(b.avg_latency_ms * b.messages) / nullif(sum(b.messages), 0) as avg_latency_ms,
                avg(b.median_latency_ms) as median_latency_ms,
                avg(b.p95_latency_ms) as p95_latency_ms,
                stddev_samp(b.avg_latency_ms) as stddev_latency_ms,
                max(b.max_latency_ms) as max_latency_ms
            from processed_event_buckets b
            cross join bounds bounds
            where b.run_id = %(run_id)s
              and b.bucket_offset_ms >= %(warmup_ms)s
            group by floor(b.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s
        ),
        raw as (
            select
                floor((e.collected_at_ms - b.first_ts) / %(bucket_ms)s)
                    * %(bucket_ms)s as bucket_offset_ms,
                min(e.collected_at_ms) as bucket_start_ms,
                count(*) as messages,
                count(*) * 1000.0 / %(bucket_ms)s as throughput_eps,
                avg(e.latency_ms) as avg_latency_ms,
                percentile_cont(0.5) within group (order by e.latency_ms) as median_latency_ms,
                percentile_cont(0.95) within group (order by e.latency_ms) as p95_latency_ms,
                stddev_samp(e.latency_ms) as stddev_latency_ms,
                max(e.latency_ms) as max_latency_ms
            from processed_events e
            cross join bounds b
            where e.run_id = %(run_id)s
              and e.collected_at_ms >= b.first_ts + %(warmup_ms)s
              and e.latency_ms is not null
              and not exists (
                  select 1 from processed_event_buckets buckets where buckets.run_id = %(run_id)s
              )
            group by floor((e.collected_at_ms - b.first_ts) / %(bucket_ms)s)
                * %(bucket_ms)s
        )
        select * from compact
        union all
        select * from raw
        order by bucket_offset_ms
        """,
        {
            "run_id": run_id,
            "bucket_ms": int(bucket_seconds * 1000),
            "warmup_ms": int(warmup_seconds * 1000),
        },
    )
    return {
        "run_id": run_id,
        "bucket_seconds": bucket_seconds,
        "warmup_seconds": warmup_seconds,
        "points": rows,
    }


@app.get("/api/runs/{run_id}/faults")
def run_faults(run_id: str):
    """! @brief Lista os eventos de falha associados a uma execução.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    rows = fetch_all(
        """
        select
            run_id,
            service,
            container,
            action,
            phase,
            timestamp_ms,
            delay_seconds,
            duration_seconds,
            count
        from fault_events
        where run_id = %(run_id)s
        order by timestamp_ms, id
        """,
        {"run_id": run_id},
    )
    return {"run_id": run_id, "faults": rows}


@app.get("/api/groups")
def list_groups():
    """! @brief Agrupa as execuções pelas dimensões experimentais disponíveis.
    @return Resultado produzido pela operação.
    """
    return fetch_all(
        """
        with event_runs as (
            select run_id, framework as event_framework, scenario as event_scenario
            from processed_run_summaries
            union all
            select e.run_id, min(e.framework) as event_framework, min(e.scenario) as event_scenario
            from processed_events e
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = e.run_id
            )
            group by e.run_id
            union all
            select b.run_id, min(b.framework) as event_framework, min(b.scenario) as event_scenario
            from processed_event_buckets b
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = b.run_id
            )
              and not exists (
                select 1 from processed_events e where e.run_id = b.run_id
            )
            group by b.run_id
        ),
        fault_summary as (
            select
                run_id,
                max(service) filter (where phase = 'fault_start') as inferred_fault_service,
                max(action) filter (where phase = 'fault_start') as inferred_fault_action
            from fault_events
            group by run_id
        ),
        runs as (
            select
                e.run_id,
                coalesce(m.framework, e.event_framework) as framework,
                coalesce(m.scenario, e.event_scenario) as scenario,
                coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) as mode,
                coalesce(m.fault_service, f.inferred_fault_service) as fault_service,
                coalesce(m.fault_action, f.inferred_fault_action) as fault_action
            from event_runs e
            left join run_metadata m on e.run_id = m.run_id
            left join fault_summary f on e.run_id = f.run_id
        )
        select
            framework,
            scenario,
            mode,
            fault_service,
            fault_action,
            count(*) as run_count
        from runs
        group by framework, scenario, mode, fault_service, fault_action
        order by framework, scenario, mode, fault_service, fault_action
        """
    )


@app.get("/api/aggregate")
def aggregate_runs(
    framework: str = Query(..., min_length=1),
    scenario: Optional[str] = None,
    mode: Optional[str] = Query(None, pattern="^(normal|fault)$"),
    fault_service: Optional[str] = None,
    fault_action: Optional[str] = None,
    warmup_seconds: float = Query(0, ge=0),
    run_ids: Optional[list[str]] = Query(None, alias="run_id"),
):
    """! @brief Agrega as métricas de múltiplas execuções equivalentes.
    @param framework Framework de processamento selecionado.
    @param scenario Cenário experimental selecionado.
    @param mode Valor do parâmetro `mode`.
    @param fault_service Valor do parâmetro `fault_service`.
    @param fault_action Valor do parâmetro `fault_action`.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @param run_ids Identificadores das execuções selecionadas.
    @return Resultado produzido pela operação.
    """
    rows = fetch_all(
        """
        with event_runs as (
            select
                run_id,
                framework as event_framework,
                scenario as event_scenario,
                coalesce(first_sent_at_ms, first_collected_at_ms) as first_ts
            from processed_run_summaries
            union all
            select
                e.run_id,
                min(e.framework) as event_framework,
                min(e.scenario) as event_scenario,
                min(coalesce(e.sent_at_ms, e.collected_at_ms)) as first_ts
            from processed_events e
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = e.run_id
            )
            group by e.run_id
            union all
            select
                b.run_id,
                min(b.framework) as event_framework,
                min(b.scenario) as event_scenario,
                min(b.bucket_start_ms) as first_ts
            from processed_event_buckets b
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = b.run_id
            )
              and not exists (
                select 1 from processed_events e where e.run_id = b.run_id
            )
            group by b.run_id
        ),
        fault_summary as (
            select
                run_id,
                max(service) filter (where phase = 'fault_start') as inferred_fault_service,
                max(action) filter (where phase = 'fault_start') as inferred_fault_action
            from fault_events
            group by run_id
        ),
        selected_runs as (
            select
                e.run_id,
                e.first_ts,
                coalesce(m.framework, e.event_framework) as framework,
                coalesce(m.scenario, e.event_scenario) as scenario,
                coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) as mode,
                coalesce(m.fault_service, f.inferred_fault_service) as fault_service,
                coalesce(m.fault_action, f.inferred_fault_action) as fault_action
            from event_runs e
            left join run_metadata m on e.run_id = m.run_id
            left join fault_summary f on e.run_id = f.run_id
            where coalesce(m.framework, e.event_framework) = %(framework)s
              and (cast(%(scenario)s as text) is null or coalesce(m.scenario, e.event_scenario) = %(scenario)s)
              and (cast(%(mode)s as text) is null or coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) = %(mode)s)
              and (cast(%(fault_service)s as text) is null or coalesce(m.fault_service, f.inferred_fault_service) = %(fault_service)s)
              and (cast(%(fault_action)s as text) is null or coalesce(m.fault_action, f.inferred_fault_action) = %(fault_action)s)
              and (cast(%(run_ids)s as text[]) is null or e.run_id = any(cast(%(run_ids)s as text[])))
        ),
        per_run as (
            select
                s.run_id,
                max(s.messages) as messages,
                max(s.distinct_events) as distinct_events,
                max(s.min_event_id) as min_event_id,
                max(s.max_event_id) as max_event_id,
                sum(b.avg_latency_ms * b.messages) / nullif(sum(b.messages), 0) as avg_latency_ms,
                avg(b.median_latency_ms) as median_latency_ms,
                avg(b.p95_latency_ms) as p95_latency_ms,
                stddev_samp(b.avg_latency_ms) as stddev_latency_ms,
                max(b.max_latency_ms) as max_latency_ms
            from processed_run_summaries s
            join selected_runs r on s.run_id = r.run_id
            left join processed_event_buckets b
              on b.run_id = s.run_id
             and b.bucket_offset_ms >= %(warmup_ms)s
            group by s.run_id
            union all
            select
                e.run_id,
                count(*) as messages,
                count(distinct e.event_id) as distinct_events,
                min(e.event_id) as min_event_id,
                max(e.event_id) as max_event_id,
                avg(e.latency_ms) filter (
                    where e.collected_at_ms >= r.first_ts + %(warmup_ms)s
                ) as avg_latency_ms,
                percentile_cont(0.5) within group (order by e.latency_ms) filter (
                    where e.collected_at_ms >= r.first_ts + %(warmup_ms)s
                ) as median_latency_ms,
                percentile_cont(0.95) within group (order by e.latency_ms) filter (
                    where e.collected_at_ms >= r.first_ts + %(warmup_ms)s
                ) as p95_latency_ms,
                stddev_samp(e.latency_ms) filter (
                    where e.collected_at_ms >= r.first_ts + %(warmup_ms)s
                ) as stddev_latency_ms,
                max(e.latency_ms) filter (
                    where e.collected_at_ms >= r.first_ts + %(warmup_ms)s
                ) as max_latency_ms
            from processed_events e
            join selected_runs r on e.run_id = r.run_id
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = e.run_id
            )
            group by e.run_id
        )
        select
            count(*) as run_count,
            sum(messages) as total_messages,
            sum(greatest((max_event_id - min_event_id + 1) - distinct_events, 0)) as total_missing_event_ids,
            avg(avg_latency_ms) as avg_of_run_avg_latency_ms,
            avg(median_latency_ms) as avg_of_run_median_latency_ms,
            avg(p95_latency_ms) as avg_of_run_p95_latency_ms,
            avg(stddev_latency_ms) as avg_of_run_stddev_latency_ms,
            stddev_samp(avg_latency_ms) as stddev_of_run_avg_latency_ms,
            max(max_latency_ms) as max_latency_ms
        from per_run
        """,
        {
            "framework": framework,
            "scenario": scenario,
            "mode": mode,
            "fault_service": fault_service,
            "fault_action": fault_action,
            "warmup_ms": int(warmup_seconds * 1000),
            "run_ids": run_ids,
        },
    )
    result = rows[0] if rows else {}
    result.update(
        {
            "framework": framework,
            "scenario": scenario,
            "mode": mode,
            "fault_service": fault_service,
            "fault_action": fault_action,
            "warmup_seconds": warmup_seconds,
            "selected_run_ids": run_ids,
        }
    )
    add_ci95(
        result,
        "avg_of_run_avg_latency_ms",
        "stddev_of_run_avg_latency_ms",
        "run_count",
        "avg_of_run_avg_latency",
    )
    return result


@app.get("/api/aggregate/latency-series")
def aggregate_latency_series(
    framework: str = Query(..., min_length=1),
    scenario: Optional[str] = None,
    mode: Optional[str] = Query(None, pattern="^(normal|fault)$"),
    fault_service: Optional[str] = None,
    fault_action: Optional[str] = None,
    bucket_seconds: float = Query(5, gt=0),
    warmup_seconds: float = Query(0, ge=0),
    run_ids: Optional[list[str]] = Query(None, alias="run_id"),
):
    """! @brief Agrega séries temporais de latência e throughput.
    @param framework Framework de processamento selecionado.
    @param scenario Cenário experimental selecionado.
    @param mode Valor do parâmetro `mode`.
    @param fault_service Valor do parâmetro `fault_service`.
    @param fault_action Valor do parâmetro `fault_action`.
    @param bucket_seconds Tamanho do bucket temporal, em segundos.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @param run_ids Identificadores das execuções selecionadas.
    @return Resultado produzido pela operação.
    """
    rows = fetch_all(
        """
        with event_runs as (
            select
                run_id,
                framework as event_framework,
                scenario as event_scenario,
                coalesce(first_sent_at_ms, first_collected_at_ms) as first_ts
            from processed_run_summaries
            union all
            select
                e.run_id,
                min(e.framework) as event_framework,
                min(e.scenario) as event_scenario,
                min(coalesce(e.sent_at_ms, e.collected_at_ms)) as first_ts
            from processed_events e
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = e.run_id
            )
            group by e.run_id
            union all
            select
                b.run_id,
                min(b.framework) as event_framework,
                min(b.scenario) as event_scenario,
                min(b.bucket_start_ms) as first_ts
            from processed_event_buckets b
            where not exists (
                select 1 from processed_run_summaries s where s.run_id = b.run_id
            )
              and not exists (
                select 1 from processed_events e where e.run_id = b.run_id
            )
            group by b.run_id
        ),
        fault_summary as (
            select
                run_id,
                max(service) filter (where phase = 'fault_start') as inferred_fault_service,
                max(action) filter (where phase = 'fault_start') as inferred_fault_action
            from fault_events
            group by run_id
        ),
        selected_runs as (
            select
                e.run_id,
                e.first_ts,
                coalesce(m.framework, e.event_framework) as framework,
                coalesce(m.scenario, e.event_scenario) as scenario,
                coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) as mode,
                coalesce(m.fault_service, f.inferred_fault_service) as fault_service,
                coalesce(m.fault_action, f.inferred_fault_action) as fault_action
            from event_runs e
            left join run_metadata m on e.run_id = m.run_id
            left join fault_summary f on e.run_id = f.run_id
            where coalesce(m.framework, e.event_framework) = %(framework)s
              and (cast(%(scenario)s as text) is null or coalesce(m.scenario, e.event_scenario) = %(scenario)s)
              and (cast(%(mode)s as text) is null or coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) = %(mode)s)
              and (cast(%(fault_service)s as text) is null or coalesce(m.fault_service, f.inferred_fault_service) = %(fault_service)s)
              and (cast(%(fault_action)s as text) is null or coalesce(m.fault_action, f.inferred_fault_action) = %(fault_action)s)
              and (cast(%(run_ids)s as text[]) is null or e.run_id = any(cast(%(run_ids)s as text[])))
        ),
        per_run_bucket as (
            select
                b.run_id,
                floor(b.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s as bucket_offset_ms,
                sum(b.messages) as messages,
                sum(b.messages) * 1000.0 / %(bucket_ms)s as throughput_eps,
                sum(b.avg_latency_ms * b.messages) / nullif(sum(b.messages), 0) as avg_latency_ms,
                avg(b.median_latency_ms) as median_latency_ms,
                avg(b.p95_latency_ms) as p95_latency_ms,
                stddev_samp(b.avg_latency_ms) as stddev_latency_ms,
                max(b.max_latency_ms) as max_latency_ms
            from processed_event_buckets b
            join selected_runs r on b.run_id = r.run_id
            where b.bucket_offset_ms >= %(warmup_ms)s
            group by b.run_id, floor(b.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s
            union all
            select
                e.run_id,
                floor((e.collected_at_ms - r.first_ts) / %(bucket_ms)s)
                    * %(bucket_ms)s as bucket_offset_ms,
                count(*) as messages,
                count(*) * 1000.0 / %(bucket_ms)s as throughput_eps,
                avg(e.latency_ms) as avg_latency_ms,
                percentile_cont(0.5) within group (order by e.latency_ms) as median_latency_ms,
                percentile_cont(0.95) within group (order by e.latency_ms) as p95_latency_ms,
                stddev_samp(e.latency_ms) as stddev_latency_ms,
                max(e.latency_ms) as max_latency_ms
            from processed_events e
            join selected_runs r on e.run_id = r.run_id
            where e.latency_ms is not null
              and e.collected_at_ms >= r.first_ts + %(warmup_ms)s
              and not exists (
                  select 1 from processed_event_buckets b where b.run_id = e.run_id
              )
            group by e.run_id,
                floor((e.collected_at_ms - r.first_ts) / %(bucket_ms)s)
                    * %(bucket_ms)s
        )
        select
            bucket_offset_ms,
            count(*) as run_count,
            sum(messages) as messages,
            avg(throughput_eps) as throughput_eps,
            avg(avg_latency_ms) as avg_latency_ms,
            avg(median_latency_ms) as median_latency_ms,
            avg(p95_latency_ms) as p95_latency_ms,
            avg(stddev_latency_ms) as stddev_latency_ms,
            stddev_samp(avg_latency_ms) as stddev_of_run_avg_latency_ms,
            max(max_latency_ms) as max_latency_ms
        from per_run_bucket
        group by bucket_offset_ms
        order by bucket_offset_ms
        """,
        {
            "framework": framework,
            "scenario": scenario,
            "mode": mode,
            "fault_service": fault_service,
            "fault_action": fault_action,
            "bucket_ms": int(bucket_seconds * 1000),
            "warmup_ms": int(warmup_seconds * 1000),
            "run_ids": run_ids,
        },
    )
    return {
        "framework": framework,
        "scenario": scenario,
        "mode": mode,
        "fault_service": fault_service,
        "fault_action": fault_action,
        "bucket_seconds": bucket_seconds,
        "warmup_seconds": warmup_seconds,
        "selected_run_ids": run_ids,
        "points": rows,
    }


@app.get("/api/runs/{run_id}/resource-series")
def resource_series(
    run_id: str,
    bucket_seconds: float = Query(5, gt=0),
    warmup_seconds: float = Query(0, ge=0),
):
    """! @brief Retorna a série de CPU e memória de uma execução.
    @param run_id Identificador único da execução.
    @param bucket_seconds Tamanho do bucket temporal, em segundos.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @return Resultado produzido pela operação.
    """
    ensure_run_exists(run_id)
    rows = fetch_all(
        """
        select
            floor(bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s as bucket_offset_ms,
            min(bucket_start_ms) as bucket_start_ms,
            avg(memory_working_set_bytes) as avg_memory_working_set_bytes,
            max(memory_working_set_bytes) as max_memory_working_set_bytes,
            avg(cpu_usage_cores) as avg_cpu_usage_cores,
            max(cpu_usage_cores) as max_cpu_usage_cores,
            avg(cpu_usage_cores) * 100.0 / %(cpu_total_cores)s as avg_cpu_usage_percent,
            max(cpu_usage_cores) * 100.0 / %(cpu_total_cores)s as max_cpu_usage_percent
        from resource_usage_buckets
        where run_id = %(run_id)s
          and bucket_offset_ms >= %(warmup_ms)s
        group by floor(bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s
        order by bucket_offset_ms
        """,
        {
            "run_id": run_id,
            "bucket_ms": int(bucket_seconds * 1000),
            "warmup_ms": int(warmup_seconds * 1000),
            "cpu_total_cores": cpu_total_cores(),
        },
    )
    return {
        "run_id": run_id,
        "bucket_seconds": bucket_seconds,
        "warmup_seconds": warmup_seconds,
        "source": "prometheus:docker_total",
        "description": "Total Docker container RAM and CPU usage during the run.",
        "cpu_total_cores": cpu_total_cores(),
        "points": rows,
    }


@app.get("/api/aggregate/resource-series")
def aggregate_resource_series(
    framework: str = Query(..., min_length=1),
    scenario: Optional[str] = None,
    mode: Optional[str] = Query(None, pattern="^(normal|fault)$"),
    fault_service: Optional[str] = None,
    fault_action: Optional[str] = None,
    bucket_seconds: float = Query(5, gt=0),
    warmup_seconds: float = Query(0, ge=0),
    run_ids: Optional[list[str]] = Query(None, alias="run_id"),
):
    """! @brief Agrega séries de CPU e memória de múltiplas execuções.
    @param framework Framework de processamento selecionado.
    @param scenario Cenário experimental selecionado.
    @param mode Valor do parâmetro `mode`.
    @param fault_service Valor do parâmetro `fault_service`.
    @param fault_action Valor do parâmetro `fault_action`.
    @param bucket_seconds Tamanho do bucket temporal, em segundos.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @param run_ids Identificadores das execuções selecionadas.
    @return Resultado produzido pela operação.
    """
    rows = fetch_all(
        """
        with fault_summary as (
            select
                run_id,
                max(service) filter (where phase = 'fault_start') as inferred_fault_service,
                max(action) filter (where phase = 'fault_start') as inferred_fault_action
            from fault_events
            group by run_id
        ),
        selected_runs as (
            select
                r.run_id,
                coalesce(m.framework, r.framework) as framework,
                coalesce(m.scenario, r.scenario) as scenario,
                coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) as mode,
                coalesce(m.fault_service, f.inferred_fault_service) as fault_service,
                coalesce(m.fault_action, f.inferred_fault_action) as fault_action
            from resource_usage_buckets r
            left join run_metadata m on r.run_id = m.run_id
            left join fault_summary f on r.run_id = f.run_id
            group by
                r.run_id,
                r.framework,
                r.scenario,
                m.framework,
                m.scenario,
                m.mode,
                m.fault_service,
                m.fault_action,
                f.run_id,
                f.inferred_fault_service,
                f.inferred_fault_action
            having coalesce(m.framework, r.framework) = %(framework)s
               and (cast(%(scenario)s as text) is null or coalesce(m.scenario, r.scenario) = %(scenario)s)
               and (cast(%(mode)s as text) is null or coalesce(m.mode, case when f.run_id is null then 'normal' else 'fault' end) = %(mode)s)
               and (cast(%(fault_service)s as text) is null or coalesce(m.fault_service, f.inferred_fault_service) = %(fault_service)s)
               and (cast(%(fault_action)s as text) is null or coalesce(m.fault_action, f.inferred_fault_action) = %(fault_action)s)
               and (cast(%(run_ids)s as text[]) is null or r.run_id = any(cast(%(run_ids)s as text[])))
        ),
        per_run_bucket as (
            select
                r.run_id,
                floor(r.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s as bucket_offset_ms,
                avg(r.memory_working_set_bytes) as avg_memory_working_set_bytes,
                max(r.memory_working_set_bytes) as max_memory_working_set_bytes,
                avg(r.cpu_usage_cores) as avg_cpu_usage_cores,
                max(r.cpu_usage_cores) as max_cpu_usage_cores,
                avg(r.cpu_usage_cores) * 100.0 / %(cpu_total_cores)s as avg_cpu_usage_percent,
                max(r.cpu_usage_cores) * 100.0 / %(cpu_total_cores)s as max_cpu_usage_percent
            from resource_usage_buckets r
            join selected_runs s on r.run_id = s.run_id
            where r.bucket_offset_ms >= %(warmup_ms)s
            group by r.run_id, floor(r.bucket_offset_ms / %(bucket_ms)s) * %(bucket_ms)s
        )
        select
            bucket_offset_ms,
            count(*) as run_count,
            avg(avg_memory_working_set_bytes) as avg_memory_working_set_bytes,
            max(max_memory_working_set_bytes) as max_memory_working_set_bytes,
            avg(avg_cpu_usage_cores) as avg_cpu_usage_cores,
            max(max_cpu_usage_cores) as max_cpu_usage_cores,
            avg(avg_cpu_usage_percent) as avg_cpu_usage_percent,
            max(max_cpu_usage_percent) as max_cpu_usage_percent
        from per_run_bucket
        group by bucket_offset_ms
        order by bucket_offset_ms
        """,
        {
            "framework": framework,
            "scenario": scenario,
            "mode": mode,
            "fault_service": fault_service,
            "fault_action": fault_action,
            "bucket_ms": int(bucket_seconds * 1000),
            "warmup_ms": int(warmup_seconds * 1000),
            "run_ids": run_ids,
            "cpu_total_cores": cpu_total_cores(),
        },
    )
    return {
        "framework": framework,
        "scenario": scenario,
        "mode": mode,
        "fault_service": fault_service,
        "fault_action": fault_action,
        "bucket_seconds": bucket_seconds,
        "warmup_seconds": warmup_seconds,
        "selected_run_ids": run_ids,
        "source": "prometheus:docker_total",
        "description": "Average total Docker container RAM and CPU usage across selected runs.",
        "cpu_total_cores": cpu_total_cores(),
        "points": rows,
    }


@app.get("/api/compare")
def compare_runs(
    left: str = Query(..., min_length=1),
    right: str = Query(..., min_length=1),
    warmup_seconds: float = Query(0, ge=0),
):
    """! @brief Retorna lado a lado os resumos de duas execuções.
    @param left Valor do parâmetro `left`.
    @param right Valor do parâmetro `right`.
    @param warmup_seconds Período inicial ignorado, em segundos.
    @return Resultado produzido pela operação.
    """
    return {
        "left": run_summary(left, warmup_seconds),
        "right": run_summary(right, warmup_seconds),
    }
