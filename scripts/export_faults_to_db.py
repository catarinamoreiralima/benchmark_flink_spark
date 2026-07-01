#!/usr/bin/env python3

import argparse
import json
import os


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def parse_args():
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Importa o log JSONL de falhas para Postgres/Neon."
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--fault-file",
        default=None,
        help="Arquivo JSONL de falhas. Padrao: results/faults/<run_id>.jsonl",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="URL Postgres/Neon. Tambem pode vir de DATABASE_URL.",
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


def ensure_schema(connection):
    """! @brief Cria ou atualiza as estruturas necessárias no banco de dados.
    @param connection Conexão ativa com o PostgreSQL.
    """
    with connection.cursor() as cursor:
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
    connection.commit()


def import_faults(database_url, run_id, records):
    """! @brief Importa eventos de falha para o PostgreSQL.
    @param database_url URL de conexão com o PostgreSQL.
    @param run_id Identificador único da execução.
    @param records Registros que serão processados.
    """
    import psycopg

    with psycopg.connect(database_url) as connection:
        ensure_schema(connection)
        with connection.cursor() as cursor:
            # O arquivo JSONL local e a fonte de verdade para a execucao.
            # Reimportar o mesmo run_id substitui os eventos anteriores.
            cursor.execute("delete from fault_events where run_id = %s", (run_id,))
            cursor.executemany(
                """
                insert into fault_events (
                    run_id,
                    service,
                    container,
                    action,
                    phase,
                    timestamp_ms,
                    delay_seconds,
                    duration_seconds,
                    count
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    (
                        record.get("run_id") or run_id,
                        record.get("service"),
                        record.get("container"),
                        record.get("action"),
                        record.get("phase"),
                        record.get("timestamp_ms"),
                        record.get("delay_seconds"),
                        record.get("duration_seconds"),
                        record.get("count"),
                    )
                    for record in records
                ],
            )
        connection.commit()


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()
    if not args.database_url:
        raise SystemExit("DATABASE_URL nao definido.")

    fault_file = args.fault_file or os.path.join(
        ROOT_DIR, "results", "faults", f"{args.run_id}.jsonl"
    )
    records = read_jsonl(fault_file)
    if not records:
        print(f"Nenhum evento de falha encontrado em {fault_file}.")
        return

    import_faults(args.database_url, args.run_id, records)
    print(f"Falhas importadas para o banco: run_id={args.run_id}, eventos={len(records)}")


if __name__ == "__main__":
    main()
