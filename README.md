# Projeto SSC0904 - Benchmark Spark vs Flink

Este projeto compara pipelines de processamento de streams implementados com
Apache Spark Structured Streaming e Apache Flink, usando Apache Kafka como
broker. Os experimentos medem:

- latencia fim a fim;
- throughput;
- perda e duplicacao de eventos;
- comportamento sob falhas controladas;
- uso agregado de CPU e RAM dos containers Docker.

## Sumario

- [Arquitetura](#arquitetura)
- [Preparacao do ambiente](#preparacao-do-ambiente)
- [Dashboard local](#dashboard-local)
- [Cenarios e run IDs](#cenarios-e-run-ids)
- [Executar experimentos](#executar-experimentos)
- [Campanhas](#campanhas)
- [Falhas controladas](#falhas-controladas)
- [PostgreSQL](#postgresql)
- [CPU e RAM](#cpu-e-ram)
- [Analise dos resultados](#analise-dos-resultados)
- [Prometheus e Grafana](#prometheus-e-grafana)
- [Configuracao de recursos](#configuracao-de-recursos)
- [Troubleshooting](#troubleshooting)

## Arquitetura

```text
producer.py
    -> Kafka: transactions
    -> Spark ou Flink
    -> Kafka: processed-transactions
    -> collector.py
    -> PostgreSQL
    -> FastAPI
    -> React/Vite
```

O Prometheus coleta as metricas do collector e dos containers por meio do
cAdvisor. Ao final de cada experimento, as series de CPU e RAM da run sao
exportadas para o PostgreSQL.

Componentes principais:

- `src/producer/producer.py`: gera os eventos de transacoes;
- `src/jobs/spark/spark_job.py`: pipeline Spark Structured Streaming;
- `src/jobs/flink/flink_job.py`: pipeline PyFlink;
- `src/collector/collector.py`: coleta os resultados e expoe metricas;
- `scripts/run_experiment.sh`: orquestra uma run;
- `scripts/inject_fault.sh`: injeta falhas em containers;
- `dashboard/backend`: API FastAPI;
- `dashboard/frontend`: dashboard React/Vite;
- `docker/prometheus/prometheus.yml`: configuracao do Prometheus.

## Preparacao do ambiente

Entre no repositorio:

```bash
cd ~/projeto-ssc0904-grupo02
```

Crie a configuracao local:

```bash
cp .env.postgres.example .env
```

Confirme no `.env`:

```bash
export POSTGRES_PASSWORD=CHANGE_ME
export DATABASE_URL='postgresql://benchmark:CHANGE_ME@localhost:5432/benchmark'
export RESOURCE_CPU_TOTAL_CORES=4
export EXPORT_RESOURCE_USAGE=1
export RESOURCE_STEP_SECONDS=5
```

Prepare os arquivos montados e suba a infraestrutura:

```bash
scripts/prepare_runtime.sh
docker compose up -d --build
RESET_TOPICS=1 bash scripts/reset_kafka.sh
```

Servicos:

| Servico | URL local |
|---|---|
| Spark Master | `http://localhost:8080` |
| Flink | `http://localhost:8081` |
| Grafana | `http://localhost:3000` |
| Prometheus | `http://localhost:9090` |
| FastAPI | `http://localhost:8001` |
| Dashboard | `http://localhost:5012` |

## Dashboard local

O backend e o frontend podem permanecer ativos em uma sessao de terminal usando
`tmux`.

### Dependencias

Crie o ambiente Python do backend uma unica vez:

```bash
cd ~/projeto-ssc0904-grupo02
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/pip install -r dashboard/backend/requirements.txt
```

O frontend usa Vite 7 e requer Node.js `20.19+` ou `22.12+`.

```bash
cd dashboard/frontend
npm install
```

### Iniciar os servidores

Crie a sessao:

```bash
tmux new -s dashboard
```

Na primeira janela, execute o backend:

```bash
cd ~/projeto-ssc0904-grupo02/dashboard/backend
source ../../.env
../../venv/bin/python3 -m uvicorn app:app \
  --host 127.0.0.1 \
  --port 8001
```

Crie outra janela com `Ctrl+B`, solte as teclas e pressione `C`. Nela, execute:

```bash
cd ~/projeto-ssc0904-grupo02/dashboard/frontend
env -u VITE_API_BASE_URL npm run dev -- --port 5012
```

O Vite recebe as requisicoes em `5012` e encaminha `/api` ao backend em
`127.0.0.1:8001`. Por isso, nao e necessario expor a porta `8001`
publicamente.

Comandos uteis do `tmux`:

```text
Ctrl+B, depois N   proxima janela
Ctrl+B, depois P   janela anterior
Ctrl+B, depois D   desconectar sem encerrar
```

Retornar ou encerrar:

```bash
tmux attach -t dashboard
tmux kill-session -t dashboard
```

Validar:

```bash
curl http://127.0.0.1:8001/api/health
curl http://127.0.0.1:5012/api/health
curl http://127.0.0.1:5012/api/runs
```

O dashboard permite iniciar experimentos, excluir runs, selecionar runs para
agregacao e visualizar latencia, throughput, IC95, CPU, RAM e eventos de falha.
Nos logs de falha, o tempo e mostrado em segundos desde o inicio da run.

## Cenarios e run IDs

Os cenarios sao definidos em
[experiments/scenarios.csv](experiments/scenarios.csv):

| Cenario | Eventos/s | Duracao |
|---|---:|---:|
| `low` | 50 | 250s |
| `medium` | 100 | 250s |
| `high` | 400 | 250s |
| `fault_medium` | 100 | 250s |
| `fault_high` | 400 | 250s |

Formato:

```text
<framework>_<scenario>_<iteracao>
```

Exemplos:

```text
spark_low_01
flink_medium_03
spark_fault_high_05
```

## Executar experimentos diretamente, sem o Dashboard

### Run normal

```bash
source .env
WRITE_JSONL=0 \
DB_STORAGE_MODE=bucket \
DB_BUCKET_SECONDS=0.5 \
AUTO_ITERATION=1 \
scripts/run_experiment.sh \
  --framework spark \
  --scenario high
```

Sem `FAULT_SERVICE`, nenhuma falha e injetada.

### Iteracao manual

```bash
source .env
WRITE_JSONL=0 \
DB_STORAGE_MODE=bucket \
DB_BUCKET_SECONDS=0.5 \
OVERWRITE=1 \
scripts/run_experiment.sh \
  --framework spark \
  --scenario medium \
  --iteration 03
```

Variaveis principais:

| Variavel | Uso |
|---|---|
| `AUTO_ITERATION=1` | escolhe a proxima iteracao livre |
| `OVERWRITE=1` | permite reutilizar um run ID |
| `WRITE_JSONL=0` | desativa o arquivo JSONL local |
| `DB_STORAGE_MODE=bucket` | salva metricas compactadas |
| `DB_STORAGE_MODE=raw` | salva uma linha por mensagem |
| `DB_BUCKET_SECONDS=0.5` | define o tamanho do bucket |
| `IDLE_TIMEOUT=120` | encerra o collector apos inatividade |
| `RESET_TOPICS_PER_RUN=1` | reseta os topicos antes da run |
| `EXPORT_RESOURCE_USAGE=1` | exporta CPU e RAM ao final |

Com `AUTO_ITERATION=1`, o script consulta o Postgres para evitar reutilizar
IDs como `spark_low_01`. Essa consulta usa `DATABASE_URL` e o Python do venv do
projeto (`venv/bin/python3`). Se o log mostrar
`Aviso: nao foi possivel consultar DATABASE_URL para AUTO_ITERATION`, confirme:

```bash
source .env
venv/bin/python3 -c "import psycopg; print('ok')"
```

Se a consulta falhar e `WRITE_JSONL=0` estiver ativo, o script pode escolher de
novo uma iteracao antiga e atualizar a run existente no banco.

## Campanhas

Matriz normal para Spark e Flink:

```bash
source .env
AUTO_ITERATION=1 \
WRITE_JSONL=0 \
DB_STORAGE_MODE=bucket \
scripts/run_all_experiments.sh
```

Campanha completa:

```bash
source .env
scripts/run_experiment_campaign.sh
```

Somente falhas:

```bash
source .env
scripts/run_fault_campaign.sh
```

Por padrao, a campanha de falhas executa:

```text
frameworks: spark, flink
scenarios: fault_medium, fault_high
actions: kill, stop-start
repeticoes: 5
containers afetados por falha: 2
```

Para executar somente `kill`:

```bash
FAULT_ACTIONS=kill scripts/run_fault_campaign.sh
```

Os scripts registram as execucoes que falharam e imprimem comandos para
refaze-las individualmente.

## Falhas controladas

Spark:

```bash
source .env
WRITE_JSONL=0 \
DB_STORAGE_MODE=raw \
AUTO_ITERATION=1 \
FAULT_SERVICE=spark-worker \
FAULT_COUNT=2 \
FAULT_ACTION=kill \
FAULT_DELAY_SECONDS=120 \
scripts/run_experiment.sh \
  --framework spark \
  --scenario fault_high
```

Flink:

```bash
source .env
WRITE_JSONL=0 \
DB_STORAGE_MODE=raw \
AUTO_ITERATION=1 \
FAULT_SERVICE=taskmanager \
FAULT_COUNT=2 \
FAULT_ACTION=stop-start \
FAULT_DELAY_SECONDS=120 \
FAULT_DURATION_SECONDS=10 \
scripts/run_experiment.sh \
  --framework flink \
  --scenario fault_medium
```

| Acao | Comportamento |
|---|---|
| `kill` | encerra abruptamente os containers e nao os reinicia |
| `stop-start` | para, aguarda a duracao configurada e reinicia |

Cada run possui um arquivo de falha proprio. Ao reutilizar um run ID, o arquivo
e reiniciado para nao misturar eventos historicos.

## PostgreSQL

O servico `postgres` do Docker Compose usa o volume nomeado
`benchmark-postgres-data`. Os dados permanecem depois de:

```bash
docker compose stop
docker compose restart
docker compose down
```

Nao use o comando abaixo, pois ele remove os volumes:

```bash
docker compose down -v
```

Subir e testar o banco:

```bash
source .env
docker compose up -d postgres
docker compose exec -T postgres psql \
  -U benchmark \
  -d benchmark \
  -c 'select 1;'
```

Tabelas:

| Tabela | Conteudo |
|---|---|
| `run_metadata` | configuracao da run |
| `processed_events` | eventos em modo raw |
| `processed_event_buckets` | metricas compactadas |
| `processed_run_summaries` | resumo por run |
| `fault_events` | eventos de falha |
| `resource_usage_buckets` | CPU e RAM |

Apagar uma run pelo terminal:

```sql
begin;
delete from resource_usage_buckets where run_id = 'spark_medium_03';
delete from fault_events where run_id = 'spark_medium_03';
delete from processed_event_buckets where run_id = 'spark_medium_03';
delete from processed_run_summaries where run_id = 'spark_medium_03';
delete from processed_events where run_id = 'spark_medium_03';
delete from run_metadata where run_id = 'spark_medium_03';
commit;
```

O dashboard tambem oferece a acao `Delete run`.

Um PostgreSQL externo continua sendo compativel; basta alterar `DATABASE_URL`.

## CPU e RAM

Ao final de cada `scripts/run_experiment.sh`, o script
`scripts/export_prometheus_resources_to_db.py` consulta o Prometheus e salva as
series em `resource_usage_buckets`.

Configuracao recomendada:

```bash
export EXPORT_RESOURCE_USAGE=1
export RESOURCE_STEP_SECONDS=5
export RESOURCE_CPU_TOTAL_CORES=4
```

Exportar manualmente uma run cujo historico ainda exista no Prometheus:

```bash
source .env
venv/bin/python3 scripts/export_prometheus_resources_to_db.py \
  --run-id spark_medium_01
```

Consultar os dados:

```bash
docker compose exec -T postgres psql -U benchmark -d benchmark -c "
SELECT
  run_id,
  COUNT(*) AS points,
  COUNT(cpu_usage_cores) AS cpu_points,
  AVG(cpu_usage_cores) AS avg_cores,
  MAX(cpu_usage_cores) AS max_cores
FROM resource_usage_buckets
GROUP BY run_id
ORDER BY run_id;
"
```

O dashboard converte cores utilizados para percentual:

```text
CPU % = cpu_usage_cores / RESOURCE_CPU_TOTAL_CORES * 100
```

As metricas representam o total dos containers Docker, incluindo framework,
Kafka e servicos de suporte. Elas nao isolam apenas `spark-worker` ou
`taskmanager`.

## Analise dos resultados

```bash
source .env
scripts/analyze_fault_recovery.py \
  --run-id spark_fault_high_04 \
  --warmup-seconds 30
```

O analisador apresenta mensagens, IDs ausentes, gaps, latencia media, mediana,
p95, desvio padrao e valores maximos.

## Prometheus e Grafana

O Prometheus coleta:

- `cadvisor:8080`: CPU e RAM dos containers;
- `host.docker.internal:8000`: metricas do collector durante uma run;
- `localhost:9090`: metricas internas do Prometheus.

Verificar os alvos:

```bash
curl -s http://localhost:9090/api/v1/targets |
python3 -c '
import json, sys
for target in json.load(sys.stdin)["data"]["activeTargets"]:
    print(target["labels"].get("job"), target["health"], target.get("lastError", ""))
'
```

O alvo `python-collector` pode ficar `down` quando nao existe experimento em
execucao. O `cadvisor` deve permanecer `up`.

Latencia media:

```promql
rate(pipeline_latency_milliseconds_sum{framework="spark"}[1m])
/
rate(pipeline_latency_milliseconds_count{framework="spark"}[1m])
```

Throughput:

```promql
rate(pipeline_messages_collected_total{framework="flink"}[10s])
```

RAM total Docker:

```promql
sum(container_memory_working_set_bytes{id=~"/docker/.+|/system[.]slice/docker-.+[.]scope"})
```

CPU total Docker, em cores:

```promql
sum(rate(container_cpu_usage_seconds_total{id=~"/docker/.+|/system[.]slice/docker-.+[.]scope"}[30s]))
```

## Configuracao de recursos

Topologia padrao:

```text
Host: 4 CPUs
Spark: 4 workers, 1 core e 1024M por worker
Spark: maximo de 4 cores, executores de 768m
Flink: 4 TaskManagers, 1 slot e 1024m por TaskManager
Flink: paralelismo 4
Kafka: 1 broker e topicos com 4 particoes
Falha padrao: 2 workers ou TaskManagers
```

Spark:

```bash
SPARK_WORKER_SCALE=4 \
SPARK_WORKER_CORES=1 \
SPARK_WORKER_MEMORY=1024M \
SPARK_EXECUTOR_CORES=1 \
SPARK_EXECUTOR_MEMORY=768m \
SPARK_TOTAL_EXECUTOR_CORES=4 \
SPARK_SQL_SHUFFLE_PARTITIONS=4 \
scripts/run_experiment.sh \
  --framework spark \
  --scenario high
```

Flink:

```bash
FLINK_TASKMANAGER_SCALE=4 \
FLINK_TASKMANAGER_MEMORY=1024m \
FLINK_TASKMANAGER_SLOTS=1 \
FLINK_PARALLELISM=4 \
scripts/run_experiment.sh \
  --framework flink \
  --scenario high
```

## Troubleshooting

### Dashboard mostra `Load failed`

Teste toda a cadeia:

```bash
curl http://127.0.0.1:8001/api/health
curl http://127.0.0.1:5012/api/health
curl http://127.0.0.1:5012/api/runs
```

O frontend deve usar caminhos relativos `/api`. Inicie-o com:

```bash
env -u VITE_API_BASE_URL npm run dev -- --port 5012
```

### Porta ocupada

```bash
ss -ltnp | grep -E ':(8001|5012)'
```

Se os servidores estiverem no `tmux`, reinicie a sessao:

```bash
tmux kill-session -t dashboard
```

### Nenhum dado de CPU ou RAM

Durante uma run:

```bash
curl -G -s http://localhost:9090/api/v1/query \
  --data-urlencode 'query=sum(rate(container_cpu_usage_seconds_total{id=~"/docker/.+|/system[.]slice/docker-.+[.]scope"}[30s]))'
```

Depois da run, procure no log:

```text
Exportando uso de RAM e CPU do Prometheus para o banco...
```

### Spark nao aceita recursos

Se aparecer `Initial job has not accepted any resources`, abra a interface do
Spark e procure aplicacoes antigas em `RUNNING` ou `WAITING`:

```text
http://localhost:8080
```

Tambem confirme se os executores de `768m` cabem nos workers de `1024M`.

### Kafka nao reseta os topicos

```bash
docker compose ps -a
docker compose logs --tail=80 kafka
docker compose up -d kafka
RESET_TOPICS=1 bash scripts/reset_kafka.sh
```

### Flink sem Python

```bash
docker compose build --no-cache jobmanager taskmanager
docker compose up -d jobmanager taskmanager
```

## Limpeza antes de uma nova rodada

```bash
scripts/stop_streaming_jobs.sh
docker compose restart spark-master spark-worker taskmanager jobmanager
RESET_TOPICS=1 bash scripts/reset_kafka.sh
```

Esse procedimento nao remove o volume do PostgreSQL.
