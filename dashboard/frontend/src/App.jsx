import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import {
  getAggregate,
  getAggregateLatencySeries,
  getAggregateResourceSeries,
  deleteRun,
  getFaults,
  getLatencySeries,
  getResourceSeries,
  getRuns,
  getRunSummary,
  getExperimentJobs,
  startExperiment,
} from './api.js';

const FRAMEWORKS = ['spark', 'flink'];
const NORMAL_SCENARIOS = ['low', 'medium', 'high'];
const FAULT_SCENARIOS = ['fault_medium', 'fault_high'];
const MODES = ['normal', 'fault'];
const FAULT_SERVICE_BY_FRAMEWORK = {
  spark: ['spark-worker'],
  flink: ['taskmanager'],
};
const FAULT_ACTIONS = ['', 'kill', 'stop-start'];
const AGGREGATE_FAULT_ACTIONS = ['kill', 'stop-start'];
const REFRESH_INTERVALS = [
  ['0', 'off'],
  ['5', '5s'],
  ['10', '10s'],
  ['30', '30s'],
];

/**
 * @brief Obtém o estado inicial a partir dos parâmetros da URL.
 * @return {*} Resultado produzido pela função.
 */
function getInitialState() {
  const params = new URLSearchParams(window.location.search);
  const urlMode = params.get('mode');
  const mode = MODES.includes(urlMode) ? urlMode : 'normal';
  const scenarioOptions = mode === 'fault' ? FAULT_SCENARIOS : NORMAL_SCENARIOS;
  const urlScenario = params.get('scenario');
  const framework = FRAMEWORKS.includes(params.get('framework')) ? params.get('framework') : 'spark';
  const faultServices = FAULT_SERVICE_BY_FRAMEWORK[framework] || [];
  const faultService = faultServices.includes(params.get('fault_service'))
    ? params.get('fault_service')
    : mode === 'fault'
      ? faultServices[0] || ''
      : '';

  return {
    framework,
    scenario: scenarioOptions.includes(urlScenario) ? urlScenario : scenarioOptions[scenarioOptions.length - 1],
    mode,
    faultService,
    faultAction: FAULT_ACTIONS.includes(params.get('fault_action')) ? params.get('fault_action') : '',
    viewMode: params.get('view') === 'run' ? 'run' : 'aggregate',
    selectedRun: params.get('selected_run') || '',
    warmupSeconds: params.get('warmup') || '30',
    bucketSeconds: params.get('bucket') || '5',
    refreshSeconds: REFRESH_INTERVALS.some(([value]) => value === params.get('refresh'))
      ? params.get('refresh')
      : '5',
  };
}

/**
 * @brief Formata um valor numérico para exibição.
 * @param {*} value Valor de `value`.
 * @param {*} digits Valor de `digits`.
 * @return {*} Resultado produzido pela função.
 */
function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
  return Number(value).toLocaleString(undefined, {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  });
}

/**
 * @brief Formata uma duração em milissegundos.
 * @param {*} value Valor de `value`.
 * @return {*} Resultado produzido pela função.
 */
function formatMs(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
  return `${formatNumber(value, 0)} ms`;
}

/**
 * @brief Formata um intervalo de valores em milissegundos.
 * @param {*} low Valor de `low`.
 * @param {*} high Valor de `high`.
 * @return {*} Resultado produzido pela função.
 */
function formatMsRange(low, high) {
  if (
    low === null ||
    low === undefined ||
    high === null ||
    high === undefined ||
    Number.isNaN(Number(low)) ||
    Number.isNaN(Number(high))
  ) {
    return 'n/a';
  }
  return `${formatNumber(low, 0)}-${formatNumber(high, 0)} ms`;
}

/**
 * @brief Formata bytes em uma unidade legível.
 * @param {*} value Valor de `value`.
 * @return {*} Resultado produzido pela função.
 */
function formatBytes(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
  const gib = Number(value) / (1024 ** 3);
  if (gib >= 1) return `${formatNumber(gib, 2)} GiB`;
  return `${formatNumber(Number(value) / (1024 ** 2), 1)} MiB`;
}

/**
 * @brief Formata um valor percentual.
 * @param {*} value Valor de `value`.
 * @return {*} Resultado produzido pela função.
 */
function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return 'n/a';
  return `${formatNumber(value, 1)}%`;
}

/**
 * @brief Formata o identificador de um cenário.
 * @param {*} value Valor de `value`.
 * @return {*} Resultado produzido pela função.
 */
function formatScenario(value) {
  return value.replace('fault_', 'fault ');
}

/**
 * @brief Monta os cartões de métricas da visualização atual.
 * @param {*} data Valor de `data`.
 * @param {*} mode Valor de `mode`.
 * @return {*} Resultado produzido pela função.
 */
function metricCards(data, mode) {
  if (!data) return [];
  if (mode === 'aggregate') {
    return [
      ['Runs', data.run_count],
      ['Messages', data.total_messages],
      ['Missing IDs', data.total_missing_event_ids],
      ['Avg Latency', formatMs(data.avg_of_run_avg_latency_ms)],
      ['P95 Latency', formatMs(data.avg_of_run_p95_latency_ms)],
      ['Latency StdDev', formatMs(data.avg_of_run_stddev_latency_ms)],
      ['Run Avg StdDev', formatMs(data.stddev_of_run_avg_latency_ms)],
      [
        'Avg IC95',
        formatMsRange(data.avg_of_run_avg_latency_ci95_low_ms, data.avg_of_run_avg_latency_ci95_high_ms),
      ],
      ['Max Latency', formatMs(data.max_latency_ms)],
    ];
  }
  return [
    ['Messages', data.messages],
    ['Missing IDs', data.missing_event_ids],
    ['Duplicates', data.duplicate_messages],
    ['Avg Latency', formatMs(data.avg_latency_ms)],
    ['P95 Latency', formatMs(data.p95_latency_ms)],
    ['Latency StdDev', formatMs(data.stddev_latency_ms)],
    ['Avg IC95', formatMsRange(data.avg_latency_ci95_low_ms, data.avg_latency_ci95_high_ms)],
    ['Max Latency', formatMs(data.max_latency_ms)],
  ];
}

/**
 * @brief Renderiza um campo de seleção rotulado.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function Select({ label, value, onChange, children }) {
  return (
    <label className="field">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {children}
      </select>
    </label>
  );
}

/**
 * @brief Renderiza um campo numérico rotulado.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function NumberInput({ label, value, onChange, min = 0, step = 'any' }) {
  return (
    <label className="field compact">
      <span>{label}</span>
      <input
        min={min}
        step={step}
        type="number"
        value={value}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

/**
 * @brief Renderiza os cartões de resumo.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function SummaryCards({ data, mode }) {
  return (
    <section className="summary-grid">
      {metricCards(data, mode).map(([label, value]) => (
        <article className="metric-card" key={label}>
          <span>{label}</span>
          <strong>{typeof value === 'number' ? formatNumber(value, 0) : value}</strong>
        </article>
      ))}
    </section>
  );
}

/**
 * @brief Normaliza os pontos recebidos pela API para os gráficos.
 * @param {*} points Valor de `points`.
 * @return {*} Resultado produzido pela função.
 */
function toChartData(points = []) {
  return points.map((point) => ({
    ...point,
    seconds: Number(point.bucket_offset_ms || 0) / 1000,
  }));
}

/**
 * @brief Calcula o maior valor de um conjunto de séries.
 * @param {*} datasets Valor de `datasets`.
 * @param {*} keys Valor de `keys`.
 * @return {*} Resultado produzido pela função.
 */
function chartMax(datasets, keys) {
  const values = datasets.flatMap((points) =>
    points.flatMap((point) =>
      keys
        .map((key) => Number(point[key]))
        .filter((value) => Number.isFinite(value)),
    ),
  );
  if (values.length === 0) return undefined;
  const max = Math.max(...values);
  return max > 0 ? max * 1.08 : 1;
}

/**
 * @brief Renderiza o contêiner comum dos gráficos.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function ChartPanel({ title, children }) {
  return (
    <section className="chart-section">
      <h2>{title}</h2>
      <div className="chart-frame">{children}</div>
    </section>
  );
}

/**
 * @brief Renderiza os gráficos comparativos de um framework.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function ComparisonPanel({ framework, data, domains = {} }) {
  const chartData = toChartData(data.series);
  const resourceChartData = toChartData(data.resourceSeries);

  return (
    <section className="comparison-panel">
      <div className="comparison-panel-header">
        <h2>{framework}</h2>
        <span>{data.summary?.run_count ? `${data.summary.run_count} runs` : 'no runs'}</span>
      </div>
      <SummaryCards data={data.summary} mode="aggregate" />
      {chartData.length > 0 ? (
        <>
          <LatencyChart data={chartData} yDomain={domains.latency} />
          <ThroughputChart data={chartData} yDomain={domains.throughput} />
          {resourceChartData.length > 0 && (
            <>
              <MemoryChart data={resourceChartData} yDomain={domains.memory} />
              <CpuChart data={resourceChartData} yDomain={domains.cpu} />
            </>
          )}
        </>
      ) : (
        <p className="empty">No series available for {framework}.</p>
      )}
    </section>
  );
}

/**
 * @brief Renderiza o gráfico de latência.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function LatencyChart({ data, yDomain }) {
  return (
    <ChartPanel title="Latency">
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="seconds" unit="s" />
          <YAxis domain={yDomain} tickFormatter={(value) => `${Math.round(value)} ms`} />
          <Tooltip formatter={(value) => formatMs(value)} labelFormatter={(value) => `${value}s`} />
          <Legend />
          <Line type="monotone" dataKey="avg_latency_ms" stroke="#2563eb" dot={false} name="avg" />
          <Line type="monotone" dataKey="p95_latency_ms" stroke="#dc2626" dot={false} name="p95" />
          <Line type="monotone" dataKey="stddev_latency_ms" stroke="#9333ea" dot={false} name="stddev" />
        </LineChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}

/**
 * @brief Renderiza o gráfico de throughput.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function ThroughputChart({ data, yDomain }) {
  return (
    <ChartPanel title="Throughput">
      <ResponsiveContainer width="100%" height={280}>
        <AreaChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="seconds" unit="s" />
          <YAxis domain={yDomain} tickFormatter={(value) => `${Math.round(value)} eps`} />
          <Tooltip
            formatter={(value) => `${formatNumber(value, 1)} eps`}
            labelFormatter={(value) => `${value}s`}
          />
          <Area
            type="monotone"
            dataKey="throughput_eps"
            stroke="#047857"
            fill="#a7f3d0"
            name="throughput"
          />
        </AreaChart>
      </ResponsiveContainer>
    </ChartPanel>
  );
}

/**
 * @brief Renderiza o gráfico de uso de memória.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function MemoryChart({ data, yDomain }) {
  return (
    <ChartPanel title="Total Docker RAM">
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="seconds" unit="s" />
          <YAxis domain={yDomain} tickFormatter={(value) => formatBytes(value)} />
          <Tooltip formatter={(value) => formatBytes(value)} labelFormatter={(value) => `${value}s`} />
          <Legend />
          <Line
            type="monotone"
            dataKey="avg_memory_working_set_bytes"
            stroke="#7c3aed"
            dot={false}
            name="avg memory"
          />
          <Line
            type="monotone"
            dataKey="max_memory_working_set_bytes"
            stroke="#ea580c"
            dot={false}
            name="max memory"
          />
        </LineChart>
      </ResponsiveContainer>
      <p className="chart-note">
        Total Docker container RAM from Prometheus/cAdvisor; includes framework and support containers.
      </p>
    </ChartPanel>
  );
}

/**
 * @brief Renderiza o gráfico de uso de CPU.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function CpuChart({ data, yDomain }) {
  return (
    <ChartPanel title="Total Docker CPU">
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="seconds" unit="s" />
          <YAxis domain={yDomain} tickFormatter={(value) => formatPercent(value)} />
          <Tooltip formatter={(value) => formatPercent(value)} labelFormatter={(value) => `${value}s`} />
          <Legend />
          <Line
            type="monotone"
            dataKey="avg_cpu_usage_percent"
            stroke="#0f766e"
            dot={false}
            name="avg cpu"
          />
          <Line
            type="monotone"
            dataKey="max_cpu_usage_percent"
            stroke="#be123c"
            dot={false}
            name="max cpu"
          />
        </LineChart>
      </ResponsiveContainer>
      <p className="chart-note">
        Total Docker container CPU from Prometheus/cAdvisor, normalized by available CPU cores.
      </p>
    </ChartPanel>
  );
}

/**
 * @brief Renderiza os eventos de falha de uma execução.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function FaultList({ faults, runStartMs }) {
  const rows = faults?.faults || [];

  /**
   * @brief Calcula o tempo transcorrido desde o início da execução.
   * @param {*} timestampMs Valor de `timestampMs`.
   * @return {*} Resultado produzido pela função.
   */
  function formatElapsed(timestampMs) {
    const timestamp = Number(timestampMs);
    const start = Number(runStartMs);
    if (!Number.isFinite(timestamp) || !Number.isFinite(start)) return 'n/a';
    return `${formatNumber((timestamp - start) / 1000, 1)} s`;
  }

  return (
    <section className="fault-section">
      <h2>Fault Events</h2>
      {rows.length === 0 ? (
        <p className="empty">No fault events found for this run.</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th>Phase</th>
              <th>Service</th>
              <th>Action</th>
              <th>Node</th>
              <th>Elapsed</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((fault, index) => (
              <tr key={`${fault.phase}-${fault.container}-${index}`}>
                <td>{fault.phase}</td>
                <td>{fault.service}</td>
                <td>{fault.action}</td>
                <td>{fault.container || '-'}</td>
                <td>{formatElapsed(fault.timestamp_ms)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

/**
 * @brief Renderiza e controla o formulário de novos experimentos.
 * @param {Object} props Valor de `props`.
 * @return {*} Resultado produzido pela função.
 */
function ExperimentLauncher({ jobs, onStart, launching, message }) {
  const [form, setForm] = useState({
    framework: 'spark',
    mode: 'normal',
    scenario: 'medium',
    faultAction: 'kill',
    faultDelaySeconds: '120',
    faultDurationSeconds: '10',
  });
  const scenarioChoices = form.mode === 'fault' ? FAULT_SCENARIOS : NORMAL_SCENARIOS;

  /**
   * @brief Atualiza um campo do formulário de experimento.
   * @param {*} key Valor de `key`.
   * @param {*} value Valor de `value`.
   * @return {*} Resultado produzido pela função.
   */
  function update(key, value) {
    setForm((current) => {
      const next = { ...current, [key]: value };
      if (key === 'mode') {
        const choices = value === 'fault' ? FAULT_SCENARIOS : NORMAL_SCENARIOS;
        next.scenario = choices.includes(current.scenario) ? current.scenario : choices[0];
      }
      return next;
    });
  }

  /**
   * @brief Valida e envia o formulário de experimento.
   * @param {*} event Valor de `event`.
   * @return {*} Resultado produzido pela função.
   */
  function submit(event) {
    event.preventDefault();
    onStart({
      framework: form.framework,
      scenario: form.scenario,
      mode: form.mode,
      fault_action: form.mode === 'fault' ? form.faultAction : undefined,
      fault_delay_seconds: Number(form.faultDelaySeconds || 0),
      fault_duration_seconds: Number(form.faultDurationSeconds || 0),
    });
  }

  return (
    <section className="experiment-launcher">
      <div className="section-header">
        <div>
          <h2>Run Experiment</h2>
          <p>Starts one benchmark run and saves it to Postgres.</p>
        </div>
        {message && <span>{message}</span>}
      </div>
      <form className="launcher-form" onSubmit={submit}>
        <Select label="Framework" value={form.framework} onChange={(value) => update('framework', value)}>
          {FRAMEWORKS.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </Select>
        <Select label="Mode" value={form.mode} onChange={(value) => update('mode', value)}>
          {MODES.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </Select>
        <Select label="Scenario" value={form.scenario} onChange={(value) => update('scenario', value)}>
          {scenarioChoices.map((item) => (
            <option key={item} value={item}>
              {formatScenario(item)}
            </option>
          ))}
        </Select>
        {form.mode === 'fault' && (
          <>
            <Select label="Action" value={form.faultAction} onChange={(value) => update('faultAction', value)}>
              <option value="kill">kill</option>
              <option value="stop-start">stop-start</option>
            </Select>
            <NumberInput
              label="Delay"
              value={form.faultDelaySeconds}
              onChange={(value) => update('faultDelaySeconds', value)}
            />
            <NumberInput
              label="Duration"
              value={form.faultDurationSeconds}
              onChange={(value) => update('faultDurationSeconds', value)}
            />
          </>
        )}
        <button className="primary-button" type="submit" disabled={launching}>
          {launching ? 'Starting...' : 'Start experiment'}
        </button>
      </form>
      {jobs.length > 0 && (
        <table className="jobs-table">
          <thead>
            <tr>
              <th>Status</th>
              <th>Experiment</th>
              <th>PID</th>
              <th>Log</th>
            </tr>
          </thead>
          <tbody>
            {jobs.slice(0, 5).map((job) => (
              <tr key={job.id}>
                <td>{job.status}</td>
                <td>
                  {job.framework} / {formatScenario(job.scenario)} / {job.mode}
                  {job.fault_action ? ` / ${job.fault_action}` : ''}
                </td>
                <td>{job.pid}</td>
                <td>{job.log_file}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  );
}

/**
 * @brief Renderiza a aplicação principal do dashboard.
 * @return {*} Resultado produzido pela função.
 */
export default function App() {
  const [initialState] = useState(getInitialState);
  const [framework, setFramework] = useState(initialState.framework);
  const [scenario, setScenario] = useState(initialState.scenario);
  const [mode, setMode] = useState(initialState.mode);
  const [faultService, setFaultService] = useState(initialState.faultService);
  const [faultAction, setFaultAction] = useState(initialState.faultAction);
  const [viewMode, setViewMode] = useState(initialState.viewMode);
  const [selectedRun, setSelectedRun] = useState(initialState.selectedRun);
  const [warmupSeconds, setWarmupSeconds] = useState(initialState.warmupSeconds);
  const [bucketSeconds, setBucketSeconds] = useState(initialState.bucketSeconds);
  const [refreshSeconds, setRefreshSeconds] = useState(initialState.refreshSeconds);
  const [refreshTick, setRefreshTick] = useState(0);
  const [runs, setRuns] = useState([]);
  const [hasMounted, setHasMounted] = useState(false);
  const [comparison, setComparison] = useState({
    spark: { summary: null, series: [], resourceSeries: [] },
    flink: { summary: null, series: [], resourceSeries: [] },
  });
  const [summary, setSummary] = useState(null);
  const [series, setSeries] = useState([]);
  const [resourceSeries, setResourceSeries] = useState([]);
  const [faults, setFaults] = useState(null);
  const [experimentJobs, setExperimentJobs] = useState([]);
  const [launchingExperiment, setLaunchingExperiment] = useState(false);
  const [launchMessage, setLaunchMessage] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const hasLoadedRunsRef = useRef(false);
  const hasLoadedDataRef = useRef(false);
  const scenarioOptions = mode === 'fault' ? FAULT_SCENARIOS : NORMAL_SCENARIOS;
  const faultServiceOptions = mode === 'fault' ? FAULT_SERVICE_BY_FRAMEWORK[framework] || [] : [];
  const faultActionOptions = viewMode === 'aggregate' ? AGGREGATE_FAULT_ACTIONS : FAULT_ACTIONS;

  /**
   * @brief Monta os filtros de consulta para um framework.
   * @param {string} item Identificador do framework.
   * @return {Object} Filtros usados nas chamadas da API.
   */
  const frameworkFilters = (item) => ({
    framework: item,
    scenario,
    mode,
    fault_service: mode === 'fault' ? FAULT_SERVICE_BY_FRAMEWORK[item]?.[0] || '' : '',
    fault_action: mode === 'fault' ? faultAction : '',
  });

  const filters = useMemo(
    () => ({
      framework,
      scenario,
      mode,
      fault_service: mode === 'fault' ? faultService : '',
      fault_action: mode === 'fault' ? faultAction : '',
    }),
    [framework, scenario, mode, faultService, faultAction],
  );

  useEffect(() => {
    if (!scenarioOptions.includes(scenario)) {
      setScenario(scenarioOptions[scenarioOptions.length - 1]);
    }
  }, [scenario, scenarioOptions]);

  useEffect(() => {
    if (mode !== 'fault') {
      setFaultService('');
      return;
    }

    if (!faultServiceOptions.includes(faultService)) {
      setFaultService(faultServiceOptions[0] || '');
    }
  }, [mode, faultService, faultServiceOptions]);

  useEffect(() => {
    if (mode === 'fault' && !faultActionOptions.includes(faultAction)) {
      setFaultAction(faultActionOptions[0]);
    }
  }, [mode, faultAction, faultActionOptions]);

  useEffect(() => {
    const intervalSeconds = Number(refreshSeconds);
    if (!intervalSeconds) return undefined;

    const intervalId = window.setInterval(() => {
      setRefreshTick((current) => current + 1);
    }, intervalSeconds * 1000);

    return () => window.clearInterval(intervalId);
  }, [refreshSeconds]);

  useEffect(() => {
    let cancelled = false;

    getExperimentJobs()
      .then((data) => {
        if (!cancelled) setExperimentJobs(data);
      })
      .catch(() => {
        if (!cancelled) setExperimentJobs([]);
      });

    return () => {
      cancelled = true;
    };
  }, [refreshTick]);

  /**
   * @brief Inicia um experimento e atualiza seu estado na interface.
   * @param {*} payload Valor de `payload`.
   * @return {*} Resultado produzido pela função.
   */
  function handleStartExperiment(payload) {
    setLaunchingExperiment(true);
    setLaunchMessage('');
    setError('');

    startExperiment(payload)
      .then((job) => {
        setExperimentJobs((current) => [job, ...current.filter((item) => item.id !== job.id)]);
        setLaunchMessage(`Started ${job.framework}/${formatScenario(job.scenario)} with PID ${job.pid}`);
      })
      .catch((err) => {
        setError(err.message);
      })
      .finally(() => {
        setLaunchingExperiment(false);
      });
  }

  /**
   * @brief Confirma e remove a execução selecionada.
   * @return {*} Resultado produzido pela função.
   */
  function handleDeleteSelectedRun() {
    if (!selectedRun) return;
    const confirmed = window.confirm(
      `Delete run ${selectedRun} from all database tables? This cannot be undone.`,
    );
    if (!confirmed) return;

    setLoading(true);
    setError('');
    deleteRun(selectedRun)
      .then(() => {
        setRuns((current) => current.filter((run) => run.run_id !== selectedRun));
        setSelectedRun('');
        setSummary(null);
        setSeries([]);
        setResourceSeries([]);
        setFaults(null);
        setRefreshTick((current) => current + 1);
      })
      .catch((err) => {
        setError(err.message);
      })
      .finally(() => {
        setLoading(false);
      });
  }

  useEffect(() => {
    if (!hasMounted) return;
    setSelectedRun('');
  }, [framework, scenario, mode, faultService, faultAction, hasMounted]);

  useEffect(() => {
    setHasMounted(true);
  }, []);

  useEffect(() => {
    const params = new URLSearchParams();
    if (viewMode === 'run') params.set('framework', framework);
    params.set('scenario', scenario);
    params.set('mode', mode);
    params.set('view', viewMode);
    params.set('warmup', warmupSeconds || '0');
    params.set('bucket', bucketSeconds || '5');
    if (refreshSeconds !== '0') params.set('refresh', refreshSeconds);
    if (viewMode === 'run' && mode === 'fault' && faultService) params.set('fault_service', faultService);
    if (mode === 'fault' && faultAction) params.set('fault_action', faultAction);
    if (viewMode === 'run' && selectedRun) params.set('selected_run', selectedRun);

    const nextUrl = `${window.location.pathname}?${params.toString()}`;
    window.history.replaceState(null, '', nextUrl);
  }, [
    framework,
    scenario,
    mode,
    faultService,
    faultAction,
    viewMode,
    selectedRun,
    warmupSeconds,
    bucketSeconds,
    refreshSeconds,
  ]);

  useEffect(() => {
    let cancelled = false;
    setError('');
    if (viewMode !== 'run') {
      setRuns([]);
      return () => {
        cancelled = true;
      };
    }

    getRuns(filters)
      .then((data) => {
        if (cancelled) return;
        setRuns(data);
        const runIds = data.map((run) => run.run_id);
        if (data.length > 0) {
          setSelectedRun((current) => (runIds.includes(current) ? current : data[0].run_id));
        } else {
          setSelectedRun('');
        }
        hasLoadedRunsRef.current = true;
      })
      .catch((err) => {
        if (!cancelled && !hasLoadedRunsRef.current) setError(err.message);
      });
    return () => {
      cancelled = true;
    };
  }, [filters, refreshTick, viewMode]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError('');

    const common = { warmup_seconds: warmupSeconds || 0 };
    const seriesOptions = {
      ...common,
      bucket_seconds: bucketSeconds || 5,
    };
    const tasks =
      viewMode === 'aggregate'
        ? FRAMEWORKS.flatMap((item) => {
            const itemFilters = frameworkFilters(item);
            return [
              getAggregate({ ...itemFilters, ...common }).then((data) => {
                if (!cancelled) {
                  setComparison((current) => ({
                    ...current,
                    [item]: { ...current[item], summary: data },
                  }));
                }
              }),
              getAggregateLatencySeries({ ...itemFilters, ...seriesOptions }).then((data) => {
                if (!cancelled) {
                  setComparison((current) => ({
                    ...current,
                    [item]: { ...current[item], series: data.points || [] },
                  }));
                }
              }),
              getAggregateResourceSeries({ ...itemFilters, ...seriesOptions }).then((data) => {
                if (!cancelled) {
                  setComparison((current) => ({
                    ...current,
                    [item]: { ...current[item], resourceSeries: data.points || [] },
                  }));
                }
              }),
            ];
          })
        : selectedRun
          ? [
              getRunSummary(selectedRun, common).then((data) => {
                if (!cancelled) setSummary(data);
              }),
              getLatencySeries(selectedRun, seriesOptions).then((data) => {
                if (!cancelled) setSeries(data.points || []);
              }),
              getResourceSeries(selectedRun, seriesOptions).then((data) => {
                if (!cancelled) setResourceSeries(data.points || []);
              }),
              getFaults(selectedRun).then((data) => {
                if (!cancelled) setFaults(data);
              }),
            ]
          : [];

    if (tasks.length === 0) {
      if (viewMode === 'aggregate') {
        setComparison({
          spark: { summary: null, series: [], resourceSeries: [] },
          flink: { summary: null, series: [], resourceSeries: [] },
        });
      }
      setLoading(false);
      return () => {
        cancelled = true;
      };
    }

    Promise.all(tasks)
      .then(() => {
        if (!cancelled) {
          hasLoadedDataRef.current = true;
          setError('');
        }
      })
      .catch((err) => {
        if (!cancelled && !hasLoadedDataRef.current) setError(err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [filters, viewMode, selectedRun, warmupSeconds, bucketSeconds, refreshTick, scenario, mode, faultAction]);

  const activeSummary = summary;
  const chartData = toChartData(series);
  const resourceChartData = toChartData(resourceSeries);
  const comparisonSeries = FRAMEWORKS.map((item) => comparison[item]?.series || []);
  const comparisonResourceSeries = FRAMEWORKS.map((item) => comparison[item]?.resourceSeries || []);
  const comparisonDomains = {
    latency: [
      0,
      chartMax(comparisonSeries, ['avg_latency_ms', 'p95_latency_ms', 'stddev_latency_ms']) || 'auto',
    ],
    throughput: [0, chartMax(comparisonSeries, ['throughput_eps']) || 'auto'],
    memory: [
      0,
      chartMax(comparisonResourceSeries, ['avg_memory_working_set_bytes', 'max_memory_working_set_bytes']) || 'auto',
    ],
    cpu: [
      0,
      chartMax(comparisonResourceSeries, ['avg_cpu_usage_percent', 'max_cpu_usage_percent']) || 'auto',
    ],
  };

  return (
    <main>
      <header className="topbar">
        <div>
          <h1>Streaming Benchmark Dashboard</h1>
          <p>Spark and Flink run summaries from Postgres.</p>
        </div>
        <div className="status">{loading ? 'Loading' : refreshSeconds === '0' ? 'Ready' : `Auto ${refreshSeconds}s`}</div>
      </header>

      <section className="controls">
        {viewMode === 'run' && (
          <Select label="Framework" value={framework} onChange={setFramework}>
            {FRAMEWORKS.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </Select>
        )}
        <Select label="Scenario" value={scenario} onChange={setScenario}>
          {scenarioOptions.map((item) => (
            <option key={item} value={item}>
              {formatScenario(item)}
            </option>
          ))}
        </Select>
        <Select label="Mode" value={mode} onChange={setMode}>
          {MODES.map((item) => (
            <option key={item} value={item}>
              {item}
            </option>
          ))}
        </Select>
        {mode === 'fault' && (
          <>
            {viewMode === 'run' && (
              <Select label="Service" value={faultService} onChange={setFaultService}>
                {faultServiceOptions.map((item) => (
                  <option key={item} value={item}>
                    {item}
                  </option>
                ))}
              </Select>
            )}
            <Select label="Action" value={faultAction} onChange={setFaultAction}>
              {faultActionOptions.map((item) => (
                <option key={item || 'all'} value={item}>
                  {item || 'all'}
                </option>
              ))}
            </Select>
          </>
        )}
        <Select label="View" value={viewMode} onChange={setViewMode}>
          <option value="aggregate">average of matching runs</option>
          <option value="run">specific run</option>
        </Select>
        <NumberInput label="Warmup" value={warmupSeconds} onChange={setWarmupSeconds} />
        {viewMode === 'run' && (
          <NumberInput label="Bucket" value={bucketSeconds} onChange={setBucketSeconds} min={0.1} />
        )}
        {viewMode === 'aggregate' && (
          <NumberInput label="Bucket" value={bucketSeconds} onChange={setBucketSeconds} min={0.1} />
        )}
        <Select label="Refresh" value={refreshSeconds} onChange={setRefreshSeconds}>
          {REFRESH_INTERVALS.map(([value, label]) => (
            <option key={value} value={value}>
              {label}
            </option>
          ))}
        </Select>
      </section>

      <ExperimentLauncher
        jobs={experimentJobs}
        launching={launchingExperiment}
        message={launchMessage}
        onStart={handleStartExperiment}
      />

      {viewMode === 'run' && (
        <section className="run-selector">
          <Select label="Run" value={selectedRun} onChange={setSelectedRun}>
            {runs.map((run) => (
              <option key={run.run_id} value={run.run_id}>
                {run.run_id} - {formatNumber(run.messages, 0)} messages
              </option>
            ))}
          </Select>
          <span>{runs.length} matching runs</span>
          <button
            className="danger-button"
            disabled={!selectedRun || loading}
            type="button"
            onClick={handleDeleteSelectedRun}
          >
            Delete run
          </button>
        </section>
      )}

      {error && <div className="error">{error}</div>}

      {viewMode === 'aggregate' ? (
        <section className="comparison-grid">
          {FRAMEWORKS.map((item) => (
            <ComparisonPanel key={item} framework={item} data={comparison[item]} domains={comparisonDomains} />
          ))}
        </section>
      ) : (
        <>
          <SummaryCards data={activeSummary} mode={viewMode} />

          {chartData.length > 0 && (
            <>
              <LatencyChart data={chartData} />
              <ThroughputChart data={chartData} />
              {resourceChartData.length > 0 && (
                <>
                  <MemoryChart data={resourceChartData} />
                  <CpuChart data={resourceChartData} />
                </>
              )}

              <FaultList
                faults={faults}
                runStartMs={activeSummary?.first_sent_at_ms ?? activeSummary?.first_collected_at_ms}
              />
            </>
          )}
        </>
      )}
    </main>
  );
}
