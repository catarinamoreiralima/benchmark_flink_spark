#!/usr/bin/env python3

import argparse
import json
import os
from math import ceil
from statistics import mean, median


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_args():
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Estima downtime/recovery gap a partir do JSONL do coletor e do log de falhas."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--raw-file",
        default=None,
        help="Arquivo JSONL de resultados. Padrao: results/raw/<run_id>.jsonl",
    )
    parser.add_argument(
        "--fault-file",
        default=None,
        help="Arquivo JSONL de falhas. Padrao: results/faults/<run_id>.jsonl",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="URL Postgres/Neon para ler resultados quando o JSONL nao existir.",
    )
    parser.add_argument(
        "--latency-window-seconds",
        type=float,
        default=15.0,
        help="Janela antes/depois da falha para comparar latencia localmente.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=0.0,
        help="Ignora os primeiros N segundos nas metricas de gap e latencia.",
    )
    return parser.parse_args()


def read_jsonl(path):
    """! @brief Lê registros JSON de um arquivo no formato JSON Lines.
    @param path Caminho do recurso.
    @return Resultado produzido pela operação.
    """
    records = []
    if not os.path.exists(path):
        return records

    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def read_db_records(database_url, run_id):
    """! @brief Lê do PostgreSQL os eventos de uma execução.
    @param database_url URL de conexão com o PostgreSQL.
    @param run_id Identificador único da execução.
    @return Resultado produzido pela operação.
    """
    import psycopg
    from psycopg.rows import dict_row

    query = """
        select
            run_id,
            framework,
            scenario,
            event_id,
            sent_at_ms,
            processed_at_ms,
            collected_at_ms,
            latency_ms
        from processed_events
        where run_id = %s
        order by collected_at_ms
    """

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, (run_id,))
            return [dict(row) for row in cursor.fetchall()]


def timestamp(record):
    """! @brief Seleciona o timestamp mais adequado de um registro.
    @param record Registro que será processado.
    @return Resultado produzido pela operação.
    """
    return record.get("collected_at_ms") or record.get("processed_at_ms")


def format_ms(value):
    """! @brief Formata uma duração em milissegundos para exibição.
    @param value Valor do parâmetro `value`.
    @return Resultado produzido pela operação.
    """
    if value is None:
        return "n/a"
    return f"{value:.0f} ms ({value / 1000:.2f}s)"


def latency(record):
    """! @brief Extrai e normaliza a latência de um registro.
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


def latency_stats(records):
    """! @brief Calcula estatísticas descritivas das latências coletadas.
    @param records Registros que serão processados.
    @return Resultado produzido pela operação.
    """
    values = [latency(record) for record in records]
    values = [value for value in values if value is not None]

    if not values:
        return None

    return {
        "count": len(values),
        "avg": mean(values),
        "median": median(values),
        "p95": percentile(values, 95),
        "max": max(values),
    }


def print_latency_stats(label, stats):
    """! @brief Imprime estatísticas de latência em formato legível.
    @param label Valor do parâmetro `label`.
    @param stats Valor do parâmetro `stats`.
    """
    if stats is None:
        print(f"{label}: n/a")
        return

    print(
        f"{label}: "
        f"n={stats['count']} "
        f"avg={format_ms(stats['avg'])} "
        f"median={format_ms(stats['median'])} "
        f"p95={format_ms(stats['p95'])} "
        f"max={format_ms(stats['max'])}"
    )


def print_gap(label, gap):
    """! @brief Imprime um intervalo entre mensagens em formato legível.
    @param label Valor do parâmetro `label`.
    @param gap Valor do parâmetro `gap`.
    """
    if gap is None:
        print(f"{label}: n/a")
        return

    print(
        f"{label}: "
        f"{format_ms(gap['gap_ms'])} "
        f"between events {gap['before_event_id']} and {gap['after_event_id']}"
    )


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()
    raw_file = args.raw_file or os.path.join(ROOT_DIR, "results", "raw", f"{args.run_id}.jsonl")
    fault_file = args.fault_file or os.path.join(ROOT_DIR, "results", "faults", f"{args.run_id}.jsonl")

    if os.path.exists(raw_file):
        data_source = "jsonl"
        all_records = [record for record in read_jsonl(raw_file) if timestamp(record) is not None]
    elif args.database_url:
        data_source = "database"
        all_records = [
            record for record in read_db_records(args.database_url, args.run_id)
            if timestamp(record) is not None
        ]
    else:
        data_source = "jsonl-missing"
        all_records = []

    faults = read_jsonl(fault_file)

    all_records.sort(key=timestamp)
    event_ids = sorted(
        record["event_id"] for record in all_records if isinstance(record.get("event_id"), int)
    )

    warmup_ms = int(args.warmup_seconds * 1000)
    first_timestamp = timestamp(all_records[0]) if all_records else None
    warmup_cutoff = first_timestamp + warmup_ms if first_timestamp is not None else None
    records = [
        record
        for record in all_records
        if warmup_cutoff is None or timestamp(record) >= warmup_cutoff
    ]

    gaps = []
    for previous, current in zip(records, records[1:]):
        previous_ts = timestamp(previous)
        current_ts = timestamp(current)
        gaps.append(
            {
                "gap_ms": current_ts - previous_ts,
                "before_event_id": previous.get("event_id"),
                "after_event_id": current.get("event_id"),
                "before_ts": previous_ts,
                "after_ts": current_ts,
            }
        )

    max_gap = max(gaps, key=lambda gap: gap["gap_ms"], default=None)
    normal_gap = median([gap["gap_ms"] for gap in gaps]) if gaps else None

    fault_start = next((fault for fault in faults if fault.get("phase") == "fault_start"), None)
    fault_end = next((fault for fault in faults if fault.get("phase") == "fault_end"), None)
    fault_window_gap = None
    pre_fault_gap = None
    post_fault_gap = None
    latency_sections = None

    if fault_start:
        start_ms = fault_start.get("timestamp_ms")
        end_ms = fault_end.get("timestamp_ms") if fault_end else start_ms
        latency_window_ms = int(args.latency_window_seconds * 1000)
        spanning_gaps = [
            gap
            for gap in gaps
            if gap["before_ts"] <= start_ms <= gap["after_ts"]
            or gap["before_ts"] >= start_ms
        ]
        fault_window_gap = max(spanning_gaps, key=lambda gap: gap["gap_ms"], default=None)
        pre_fault_gaps = [gap for gap in gaps if gap["after_ts"] < start_ms]
        post_fault_gaps = [gap for gap in gaps if gap["before_ts"] > end_ms]
        pre_fault_gap = max(pre_fault_gaps, key=lambda gap: gap["gap_ms"], default=None)
        post_fault_gap = max(post_fault_gaps, key=lambda gap: gap["gap_ms"], default=None)
        latency_sections = {
            "pre_all": [
                record for record in records if timestamp(record) < start_ms
            ],
            "post_all": [
                record for record in records if timestamp(record) > end_ms
            ],
            "pre_window": [
                record
                for record in records
                if start_ms - latency_window_ms <= timestamp(record) < start_ms
            ],
            "post_window": [
                record
                for record in records
                if end_ms < timestamp(record) <= end_ms + latency_window_ms
            ],
            "fault_interval": [
                record
                for record in records
                if start_ms <= timestamp(record) <= end_ms
            ],
        }

    missing_events = []
    if event_ids:
        seen = set(event_ids)
        missing_events = [event_id for event_id in range(min(event_ids), max(event_ids) + 1) if event_id not in seen]

    print(f"Run: {args.run_id}")
    print(f"Raw file: {raw_file}")
    print(f"Data source: {data_source}")
    print(f"Fault file: {fault_file}")
    print(f"Messages collected: {len(all_records)}")
    if args.warmup_seconds > 0:
        print(
            f"Warmup ignored: first {args.warmup_seconds:g}s "
            f"({len(all_records) - len(records)} messages excluded from gap/latency metrics)"
        )

    if event_ids:
        print(f"Event id range: {min(event_ids)}..{max(event_ids)}")
        print(f"Missing event ids: {len(missing_events)}")

    if fault_start:
        print(
            "Fault: "
            f"{fault_start.get('action')} {fault_start.get('service')} "
            f"start={fault_start.get('timestamp_ms')} "
            f"end={fault_end.get('timestamp_ms') if fault_end else 'n/a'}"
        )
    else:
        print("Fault: no fault log found")

    print(f"Typical inter-message gap: {format_ms(normal_gap)}")

    print_gap("Largest observed gap", max_gap)

    if fault_start:
        print("Gap comparison:")
        print_gap("  Largest gap before fault", pre_fault_gap)
        print_gap("  Largest gap after recovery", post_fault_gap)
        print_gap("  Largest gap after/around fault", fault_window_gap)

    if latency_sections:
        window = args.latency_window_seconds
        print("Latency comparison:")
        print_latency_stats("  Before fault (all)", latency_stats(latency_sections["pre_all"]))
        print_latency_stats("  After recovery (all)", latency_stats(latency_sections["post_all"]))
        print_latency_stats(
            f"  Before fault ({window:g}s window)",
            latency_stats(latency_sections["pre_window"]),
        )
        print_latency_stats(
            f"  After recovery ({window:g}s window)",
            latency_stats(latency_sections["post_window"]),
        )
        print_latency_stats("  During fault interval", latency_stats(latency_sections["fault_interval"]))


if __name__ == "__main__":
    main()
