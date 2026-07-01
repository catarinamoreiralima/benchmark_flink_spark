import argparse
import json
import os
import random
import time
from itertools import count

import pandas as pd
from kafka import KafkaProducer


# Caminho padrao do dataset versionado no repositorio. Usar o arquivo local
# evita depender do Kaggle durante a execucao dos experimentos.
DEFAULT_DATASET = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data",
    "bank_transactions_data.csv",
)


def parse_args():
    # Os parametros permitem repetir o mesmo cenario mudando apenas a linha de comando.
    """! @brief Interpreta os argumentos recebidos pela linha de comando.
    @return Resultado produzido pela operação.
    """
    parser = argparse.ArgumentParser(
        description="Gera um stream parametrizavel de transacoes no Kafka."
    )
    parser.add_argument("--bootstrap-server", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--data", default=DEFAULT_DATASET)
    parser.add_argument("--eps", type=float, default=10.0, help="Eventos por segundo.")
    parser.add_argument(
        "--duration",
        type=float,
        default=10.0,
        help="Duracao da execucao em segundos.",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Limite opcional de eventos. Se omitido, usa eps * duration.",
    )
    parser.add_argument("--scenario", default="test")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Nao imprime cada evento enviado.",
    )
    return parser.parse_args()


def clean_value(value):
    # O JSON padrao nao serializa NaN nem tipos numpy usados pelo pandas.
    """! @brief Converte um valor do dataset para um tipo serializável em JSON.
    @param value Valor do parâmetro `value`.
    @return Resultado produzido pela operação.
    """
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def clean_record(record):
    # Normaliza todos os campos de uma linha do CSV antes de enviar para o Kafka.
    """! @brief Normaliza todos os campos de um registro do dataset.
    @param record Registro que será processado.
    @return Resultado produzido pela operação.
    """
    return {key: clean_value(value) for key, value in record.items()}


def now_ms():
    # Timestamp em milissegundos usado no calculo de latencia fim a fim.
    """! @brief Obtém o instante atual em milissegundos desde a época Unix.
    @return Resultado produzido pela operação.
    """
    return time.time_ns() // 1_000_000


def build_event(run_id, scenario, event_id, payload):
    # Modelo comum para todos os frameworks calcularem latencia do mesmo jeito.
    """! @brief Monta o contrato de evento publicado no Kafka.
    @param run_id Identificador único da execução.
    @param scenario Cenário experimental selecionado.
    @param event_id Identificador sequencial do evento.
    @param payload Dados da transação processada.
    @return Resultado produzido pela operação.
    """
    return {
        "run_id": run_id,
        "event_id": event_id,
        "sent_at_ms": now_ms(),
        "scenario": scenario,
        "payload": payload,
    }


def delivery_report(error):
    """! @brief Trata o resultado assíncrono do envio de uma mensagem.
    @param error Erro reportado pela operação assíncrona.
    """
    if error is not None:
        print(f"Erro ao enviar evento: {error}")


def main():
    """! @brief Executa o fluxo principal deste módulo.
    """
    args = parse_args()

    # Falhar cedo ajuda a evitar execucoes invalidas que gerariam resultados vazios.
    if args.eps <= 0:
        raise ValueError("--eps deve ser maior que zero.")
    if args.duration <= 0:
        raise ValueError("--duration deve ser maior que zero.")

    run_id = args.run_id or f"{args.scenario}_{int(time.time())}"
    total_events = args.max_events or int(args.eps * args.duration)
    if total_events <= 0:
        raise ValueError("A quantidade total de eventos deve ser maior que zero.")
    interval = 1.0 / args.eps

    # O CSV e carregado uma vez; durante o teste so fazemos amostragem em memoria.
    df = pd.read_csv(args.data)
    records = [clean_record(row) for row in df.to_dict(orient="records")]

    if not records:
        raise ValueError(f"Nenhum registro encontrado em {args.data}.")

    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        # Mantem a mensagem em JSON para facilitar o consumo por Spark, Flink e scripts.
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=5,
    )

    print(
        "Iniciando geracao: "
        f"run_id={run_id}, scenario={args.scenario}, eps={args.eps}, "
        f"duration={args.duration}s, max_events={total_events}, "
        f"topic={args.topic}, bootstrap={args.bootstrap_server}"
    )

    start = time.monotonic()
    # time.monotonic nao sofre ajustes do relogio do sistema durante a execucao.
    next_send_at = start
    sent_count = 0

    try:
        for event_id in count(start=1):
            if event_id > total_events:
                break

            # A amostragem com seed fixa mantem a carga reproduzivel entre execucoes.
            payload = rng.choice(records)
            event = build_event(run_id, args.scenario, event_id, payload)
            producer.send(
                args.topic,
                key=str(event_id),
                value=event,
            ).add_errback(delivery_report)
            sent_count = event_id

            if not args.quiet:
                print(f"Sent: event_id={event_id}, sent_at_ms={event['sent_at_ms']}")

            # Agenda pelo relogio monotonic para reduzir drift ao longo do teste.
            next_send_at += interval
            sleep_for = next_send_at - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)

    except KeyboardInterrupt:
        print("\nInterrompido pelo usuario.")
    finally:
        producer.flush()
        producer.close()

    elapsed = time.monotonic() - start
    print(f"Finalizado: {sent_count} eventos em {elapsed:.2f}s.")


if __name__ == "__main__":
    main()
