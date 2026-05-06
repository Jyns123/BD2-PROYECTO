const BASE = '/api';

async function request(url, options = {}) {
  const res = await fetch(`${BASE}${url}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(err.detail || 'Request failed');
  }
  return res.json();
}

// -- Tables --
export const getTables = () => request('/tables');
export const getTable = (name) => request(`/tables/${name}`);
export const dropTable = (name) => request(`/tables/${name}`, { method: 'DELETE' });

// -- CSV inference --
export const inferCsv = (path, sampleRows = 50) =>
  request('/infer-csv', {
    method: 'POST',
    body: JSON.stringify({ path, sample_rows: sampleRows }),
  });

// -- Query --
export const runQuery = (sql, columnSizes = null, basePath = '') =>
  request('/query', {
    method: 'POST',
    body: JSON.stringify({
      sql,
      column_sizes: columnSizes,
      base_path: basePath,
    }),
  });
