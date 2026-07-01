#!/usr/bin/env python3

import argparse
import os


def parse_args():
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Registra metadados de uma execucao no Postgres/Neon."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--framework", required=True)
    parser.add_argument("--scenario", required=True)
    parser.add_argument("--iteration", required=True)
    parser.add_argument("--eps", type=float, required=True)
    parser.add_argument("--duration-seconds", type=float, required=True)
    parser.add_argument("--max-events", type=int, required=True)
    parser.add_argument("--mode", choices=["normal", "fault"], required=True)
    parser.add_argument("--fault-service", default=None)
    parser.add_argument("--fault-action", default=None)
    parser.add_argument("--fault-count", type=int, default=None)
    parser.add_argument("--fault-delay-seconds", type=float, default=None)
    parser.add_argument("--fault-duration-seconds", type=float, default=None)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="URL Postgres/Neon. Tambem pode vir de DATABASE_URL.",
    )
    return parser.parse_args()


def ensure_schema(connection):
    """! @brief Cria ou atualiza as estruturas necessárias no banco de dados.
    @param connection Conexão ativa com o PostgreSQL.
    """
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
        cursor.execute(
            """
            create index if not exists run_metadata_group_idx
            on run_metadata (framework, scenario, mode, fault_service, fault_action)
            """
        )
    connection.commit()


def upsert_metadata(args):
    """! @brief Insere ou atualiza os metadados de uma execução.
    @param args Argumentos e opções da operação.
    """
    import psycopg

    with psycopg.connect(args.database_url) as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                insert into run_metadata (
                    run_id,
                    framework,
                    scenario,
                    iteration,
                    mode,
                    eps,
                    duration_seconds,
                    max_events,
                    fault_service,
                    fault_action,
                    fault_count,
                    fault_delay_seconds,
                    fault_duration_seconds
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                on conflict (run_id) do update set
                    framework = excluded.framework,
                    scenario = excluded.scenario,
                    iteration = excluded.iteration,
                    mode = excluded.mode,
                    eps = excluded.eps,
                    duration_seconds = excluded.duration_seconds,
                    max_events = excluded.max_events,
                    fault_service = excluded.fault_service,
                    fault_action = excluded.fault_action,
                    fault_count = excluded.fault_count,
                    fault_delay_seconds = excluded.fault_delay_seconds,
                    fault_duration_seconds = excluded.fault_duration_seconds,
                    updated_at = now()
                """,
                (
                    args.run_id,
                    args.framework,
                    args.scenario,
                    args.iteration,
                    args.mode,
                    args.eps,
                    args.duration_seconds,
                    args.max_events,
                    args.fault_service,
                    args.fault_action,
                    args.fault_count,
                    args.fault_delay_seconds,
                    args.fault_duration_seconds,
                ),
            )
        connection.commit()


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL nao definido.")

    upsert_metadata(args)
    print(f"Metadados importados para o banco: run_id={args.run_id}, mode={args.mode}")


if __name__ == "__main__":
    main()
