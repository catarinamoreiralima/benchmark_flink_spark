const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '';

/**
 * @brief Executa uma requisição à API e converte a resposta para JSON.
 * @param {*} path Valor de `path`.
 * @param {*} options Valor de `options`.
 * @return {*} Resultado produzido pela função.
 */
async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Request failed: ${response.status}`);
  }
  return response.json();
}

/**
 * @brief Converte filtros em uma query string.
 * @param {*} query Valor de `query`.
 * @return {*} Resultado produzido pela função.
 */
function params(query) {
  const search = new URLSearchParams();
  Object.entries(query).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.forEach((item) => {
        if (item !== undefined && item !== null && item !== '') {
          search.append(key, item);
        }
      });
    } else if (value !== undefined && value !== null && value !== '') {
      search.set(key, value);
    }
  });
  const text = search.toString();
  return text ? `?${text}` : '';
}

/**
 * @brief Consulta as execuções disponíveis.
 * @param {*} filters Valor de `filters`.
 * @return {*} Resultado produzido pela função.
 */
export function getRuns(filters = {}) {
  return request(`/api/runs${params(filters)}`);
}

/**
 * @brief Consulta o resumo agregado das execuções.
 * @param {*} filters Valor de `filters`.
 * @return {*} Resultado produzido pela função.
 */
export function getAggregate(filters = {}) {
  return request(`/api/aggregate${params(filters)}`);
}

/**
 * @brief Consulta séries agregadas de latência e throughput.
 * @param {*} filters Valor de `filters`.
 * @return {*} Resultado produzido pela função.
 */
export function getAggregateLatencySeries(filters = {}) {
  return request(`/api/aggregate/latency-series${params(filters)}`);
}

/**
 * @brief Consulta séries agregadas de CPU e memória.
 * @param {*} filters Valor de `filters`.
 * @return {*} Resultado produzido pela função.
 */
export function getAggregateResourceSeries(filters = {}) {
  return request(`/api/aggregate/resource-series${params(filters)}`);
}

/**
 * @brief Consulta o resumo de uma execução.
 * @param {*} runId Valor de `runId`.
 * @param {*} options Valor de `options`.
 * @return {*} Resultado produzido pela função.
 */
export function getRunSummary(runId, options = {}) {
  return request(`/api/runs/${runId}/summary${params(options)}`);
}

/**
 * @brief Consulta a série de latência de uma execução.
 * @param {*} runId Valor de `runId`.
 * @param {*} options Valor de `options`.
 * @return {*} Resultado produzido pela função.
 */
export function getLatencySeries(runId, options = {}) {
  return request(`/api/runs/${runId}/latency-series${params(options)}`);
}

/**
 * @brief Consulta a série de recursos de uma execução.
 * @param {*} runId Valor de `runId`.
 * @param {*} options Valor de `options`.
 * @return {*} Resultado produzido pela função.
 */
export function getResourceSeries(runId, options = {}) {
  return request(`/api/runs/${runId}/resource-series${params(options)}`);
}

/**
 * @brief Consulta os eventos de falha de uma execução.
 * @param {*} runId Valor de `runId`.
 * @return {*} Resultado produzido pela função.
 */
export function getFaults(runId) {
  return request(`/api/runs/${runId}/faults`);
}

/**
 * @brief Remove uma execução do banco de dados.
 * @param {*} runId Valor de `runId`.
 * @return {*} Resultado produzido pela função.
 */
export function deleteRun(runId) {
  return request(`/api/runs/${runId}`, {
    method: 'DELETE',
  });
}

/**
 * @brief Solicita o início de um experimento.
 * @param {*} payload Valor de `payload`.
 * @return {*} Resultado produzido pela função.
 */
export function startExperiment(payload) {
  return request('/api/experiments/start', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });
}

/**
 * @brief Consulta os experimentos iniciados pelo dashboard.
 * @return {*} Resultado produzido pela função.
 */
export function getExperimentJobs() {
  return request('/api/experiments');
}
