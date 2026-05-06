import { useState } from 'react';
import { X, Plus, Trash2, Play, Wand2 } from 'lucide-react';
import { inferCsv } from '../services/api';

const INDEX_OPTIONS = ['BPLUSTREE', 'HASH', 'SEQUENTIAL', 'RTREE', 'HEAP'];
const TYPE_OPTIONS = ['INT', 'FLOAT', 'TEXT'];

export default function CreateTableModal({ onClose, onExecute }) {
  const [tableName, setTableName] = useState('');
  const [columns, setColumns] = useState([
    { name: '', type: 'INT', index: '' },
  ]);
  const [csvPath, setCsvPath] = useState('');
  const [columnSizes, setColumnSizes] = useState({});
  const [inferring, setInferring] = useState(false);
  const [inferError, setInferError] = useState('');

  const addColumn = () =>
    setColumns([...columns, { name: '', type: 'TEXT', index: '' }]);

  const removeColumn = (i) =>
    setColumns(columns.filter((_, idx) => idx !== i));

  const updateColumn = (i, field, val) => {
    const updated = [...columns];
    updated[i] = { ...updated[i], [field]: val };
    setColumns(updated);
  };

  const updateSize = (colName, size) => {
    setColumnSizes({ ...columnSizes, [colName]: parseInt(size) || 32 });
  };

  const detectFromCsv = async () => {
    setInferError('');
    if (!csvPath.trim()) {
      setInferError('Ingresa la ruta del CSV primero');
      return;
    }
    setInferring(true);
    try {
      const res = await inferCsv(csvPath.trim());
      if (!res.columns || res.columns.length === 0) {
        setInferError('CSV sin columnas detectables');
        return;
      }
      // Importar columnas detectadas (preserva nombre, tipo; index vacío por default)
      const detected = res.columns.map((c) => ({
        name: c.name,
        type: c.type,
        index: '',
      }));
      setColumns(detected);
      const sizes = {};
      res.columns.forEach((c) => {
        if (c.type === 'TEXT' && c.size) sizes[c.name] = c.size;
      });
      setColumnSizes(sizes);
    } catch (e) {
      setInferError(e.message || 'Error detectando columnas');
    } finally {
      setInferring(false);
    }
  };

  const buildSQL = () => {
    const colDefs = columns
      .filter((c) => c.name.trim())
      .map((c) => {
        let def = `${c.name} ${c.type}`;
        if (c.index) def += ` INDEX ${c.index}`;
        return def;
      })
      .join(', ');

    let sql = `CREATE TABLE ${tableName} (${colDefs})`;
    if (csvPath.trim()) {
      sql += ` FROM FILE '${csvPath.trim()}'`;
    }
    return sql;
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!tableName.trim() || columns.every((c) => !c.name.trim())) return;

    const sizes = {};
    columns.forEach((c) => {
      if (c.type === 'TEXT' && columnSizes[c.name]) {
        sizes[c.name] = columnSizes[c.name];
      }
    });

    onExecute(buildSQL(), Object.keys(sizes).length > 0 ? sizes : null);
  };

  const previewSQL = tableName.trim() && columns.some((c) => c.name.trim())
    ? buildSQL()
    : '';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      <div className="bg-bg-secondary border border-border rounded-xl w-full max-w-lg mx-4 shadow-2xl animate-fade-in">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h2 className="text-sm font-semibold text-text-primary">Create Table</h2>
          <button onClick={onClose} className="p-1 text-text-muted hover:text-text-primary transition-colors">
            <X size={16} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-5 space-y-4">
          {/* Table name */}
          <div>
            <label className="block text-[11px] text-text-muted mb-1 font-medium">Table Name</label>
            <input
              type="text"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              placeholder="students"
              className="w-full bg-bg-primary border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder-text-muted outline-none focus:border-border-focus transition-colors"
            />
          </div>

          {/* Columns */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-[11px] text-text-muted font-medium">Columns</label>
              <button
                type="button"
                onClick={addColumn}
                className="flex items-center gap-1 text-[10px] text-accent hover:text-accent-hover transition-colors"
              >
                <Plus size={11} /> Add Column
              </button>
            </div>

            <div className="space-y-1.5 max-h-48 overflow-y-auto">
              {columns.map((col, i) => (
                <div key={i} className="flex items-center gap-1.5">
                  <input
                    type="text"
                    value={col.name}
                    onChange={(e) => updateColumn(i, 'name', e.target.value)}
                    placeholder="column_name"
                    className="flex-1 bg-bg-primary border border-border rounded px-2 py-1 text-xs text-text-primary placeholder-text-muted outline-none focus:border-border-focus transition-colors font-mono"
                  />
                  <select
                    value={col.type}
                    onChange={(e) => updateColumn(i, 'type', e.target.value)}
                    className="bg-bg-primary border border-border rounded px-2 py-1 text-xs text-text-primary outline-none focus:border-border-focus transition-colors"
                  >
                    {TYPE_OPTIONS.map((t) => <option key={t} value={t}>{t}</option>)}
                  </select>
                  <select
                    value={col.index}
                    onChange={(e) => updateColumn(i, 'index', e.target.value)}
                    className="bg-bg-primary border border-border rounded px-2 py-1 text-xs text-text-secondary outline-none focus:border-border-focus transition-colors"
                  >
                    <option value="">No Index</option>
                    {INDEX_OPTIONS.map((t) => <option key={t} value={t}>{t}</option>)}
                  </select>
                  {col.type === 'TEXT' && (
                    <input
                      type="number"
                      value={columnSizes[col.name] || ''}
                      onChange={(e) => updateSize(col.name, e.target.value)}
                      placeholder="32"
                      className="w-14 bg-bg-primary border border-border rounded px-2 py-1 text-xs text-text-primary placeholder-text-muted outline-none focus:border-border-focus transition-colors"
                      title="Size in bytes"
                    />
                  )}
                  <button
                    type="button"
                    onClick={() => removeColumn(i)}
                    disabled={columns.length <= 1}
                    className="p-1 text-text-muted hover:text-error disabled:opacity-30 transition-colors"
                  >
                    <Trash2 size={12} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          {/* CSV path */}
          <div>
            <div className="flex items-center justify-between mb-1">
              <label className="text-[11px] text-text-muted font-medium">
                Load from CSV (optional)
              </label>
              <button
                type="button"
                onClick={detectFromCsv}
                disabled={inferring || !csvPath.trim()}
                className="flex items-center gap-1 text-[10px] text-accent hover:text-accent-hover disabled:opacity-40 transition-colors"
                title="Auto-detectar columnas desde el header del CSV"
              >
                <Wand2 size={11} /> {inferring ? 'Detecting…' : 'Auto-detect columns'}
              </button>
            </div>
            <input
              type="text"
              value={csvPath}
              onChange={(e) => { setCsvPath(e.target.value); setInferError(''); }}
              placeholder="data/students.csv"
              className="w-full bg-bg-primary border border-border rounded-md px-3 py-1.5 text-sm text-text-primary placeholder-text-muted outline-none focus:border-border-focus transition-colors font-mono"
            />
            {inferError && (
              <div className="text-[11px] text-error mt-1">{inferError}</div>
            )}
          </div>

          {/* SQL preview */}
          {previewSQL && (
            <div className="bg-bg-primary border border-border rounded-md p-2.5 text-[11px] font-mono text-text-secondary break-all">
              {previewSQL}
            </div>
          )}

          {/* Actions */}
          <div className="flex justify-end gap-2 pt-1">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-1.5 text-xs text-text-secondary hover:text-text-primary bg-bg-primary border border-border rounded-md hover:bg-bg-hover transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={!tableName.trim() || columns.every((c) => !c.name.trim())}
              className="flex items-center gap-1.5 px-4 py-1.5 text-xs font-medium text-white bg-accent hover:bg-accent-hover disabled:opacity-40 rounded-md transition-colors"
            >
              <Play size={11} /> Create
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
