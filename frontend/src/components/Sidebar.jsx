import { useState, useEffect } from 'react';
import {
  Database, ChevronRight, ChevronDown, Table2, Trash2,
  Plus, RefreshCw, Search, Columns3, Key, Hash
} from 'lucide-react';
import { getTable } from '../services/api';

export default function Sidebar({
  tables, selectedTable, tableInfo, onSelectTable, onDropTable, onCreateTable, onRefresh
}) {
  const [expanded, setExpanded] = useState({});
  const [filter, setFilter] = useState('');
  const [tableSchemas, setTableSchemas] = useState({});

  // Fetch schema when a table is expanded
  const toggle = async (name) => {
    const next = !expanded[name];
    setExpanded((prev) => ({ ...prev, [name]: next }));
    if (next && !tableSchemas[name]) {
      try {
        const info = await getTable(name);
        setTableSchemas((prev) => ({ ...prev, [name]: info }));
      } catch {
        // ignore
      }
    }
  };

  // Keep schema in sync when tableInfo updates
  useEffect(() => {
    if (selectedTable && tableInfo) {
      setTableSchemas((prev) => ({ ...prev, [selectedTable]: tableInfo }));
    }
  }, [selectedTable, tableInfo]);

  const filtered = tables.filter((t) =>
    t.toLowerCase().includes(filter.toLowerCase())
  );

  return (
    <aside className="w-72 min-w-60 bg-bg-secondary border-r border-border flex flex-col">
      {/* Header */}
      <div className="flex items-center gap-2 px-4 py-3 border-b border-border">
        <Database size={18} className="text-accent" />
        <span className="text-sm font-semibold tracking-wide">DB Manager</span>
        <div className="ml-auto flex gap-1">
          <button
            onClick={onRefresh}
            className="p-1.5 rounded hover:bg-bg-hover text-text-secondary hover:text-text-primary transition-colors"
            title="Refresh"
          >
            <RefreshCw size={14} />
          </button>
          <button
            onClick={onCreateTable}
            className="p-1.5 rounded hover:bg-bg-hover text-text-secondary hover:text-text-primary transition-colors"
            title="Create Table"
          >
            <Plus size={14} />
          </button>
        </div>
      </div>

      {/* Search */}
      <div className="px-3 py-2 border-b border-border">
        <div className="flex items-center gap-2 bg-bg-primary rounded-md px-2.5 py-1.5">
          <Search size={14} className="text-text-muted" />
          <input
            type="text"
            placeholder="Filter tables..."
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="bg-transparent text-sm text-text-primary placeholder-text-muted outline-none flex-1"
          />
        </div>
      </div>

      {/* Table list */}
      <div className="flex-1 overflow-y-auto py-1">
        {filtered.length === 0 && (
          <p className="px-4 py-6 text-xs text-text-muted text-center">
            No tables found
          </p>
        )}
        {filtered.map((name) => {
          const schema = tableSchemas[name];
          const columns = schema?.columns || [];
          const indexType = schema?.index || null;
          const keyCol = schema?.key_column || null;

          return (
            <div key={name} className="animate-slide-in">
              {/* Table row */}
              <div
                className={`flex items-center gap-1.5 px-3 py-2 cursor-pointer transition-colors group ${
                  selectedTable === name
                    ? 'bg-accent-subtle text-accent'
                    : 'hover:bg-bg-hover text-text-secondary hover:text-text-primary'
                }`}
              >
                <button onClick={() => toggle(name)} className="p-0.5">
                  {expanded[name]
                    ? <ChevronDown size={14} />
                    : <ChevronRight size={14} />
                  }
                </button>
                <Table2 size={14} className="shrink-0" />
                <span
                  onClick={() => onSelectTable(name)}
                  className="text-sm font-medium truncate flex-1"
                >
                  {name}
                </span>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    if (window.confirm(`Drop table "${name}"? This deletes all data.`)) {
                      onDropTable(name);
                    }
                  }}
                  className="p-1 rounded opacity-60 hover:opacity-100 hover:bg-error-subtle hover:text-error transition-all"
                  title="Drop table"
                >
                  <Trash2 size={12} />
                </button>
              </div>

              {/* Expanded: Columns + Indexes */}
              {expanded[name] && (
                <div className="mt-0.5 mb-2 animate-fade-in">
                  {/* Columns section */}
                  <div className="flex flex-col">
                    <div className="flex items-center gap-2 text-sm text-text-muted py-1 pl-8 hover:bg-bg-hover cursor-pointer transition-colors">
                      <Columns3 size={14} className="text-text-secondary shrink-0" />
                      <span className="font-medium text-text-primary text-sm">Columns</span>
                    </div>
                    
                    <div className="flex flex-col mt-0.5">
                      {columns.map((col, i) => (
                        <div key={i} className="flex items-center gap-2 py-1 pl-[52px] text-sm text-text-secondary hover:bg-bg-hover cursor-pointer transition-colors">
                          {col.name === keyCol
                            ? <Key size={13} className="text-warning shrink-0" />
                            : <span className="w-3.5 shrink-0" />
                          }
                          <span className="text-text-primary">{col.name}</span>
                          <span className="text-text-muted">({col.type || 'TEXT'})</span>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Indexes section */}
                  <div className="flex flex-col mt-1">
                    <div className="flex items-center gap-2 text-sm text-text-muted py-1 pl-8 hover:bg-bg-hover cursor-pointer transition-colors">
                      <Hash size={14} className="text-text-secondary shrink-0" />
                      <span className="font-medium text-text-primary text-sm">Indexes</span>
                    </div>
                    
                    <div className="flex flex-col mt-0.5">
                      {indexType && (
                        <div className="flex items-center gap-2 py-1 pl-[52px] text-sm text-text-secondary hover:bg-bg-hover cursor-pointer transition-colors">
                          <span className="w-3.5 shrink-0" />
                          <span className="text-text-primary capitalize">{indexType.toLowerCase()}</span>
                          <span className="text-text-muted">on</span>
                          <span className="text-text-primary">{keyCol || '?'}</span>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Footer */}
      <div className="px-4 py-2 border-t border-border text-xs text-text-muted">
        {tables.length} table{tables.length !== 1 ? 's' : ''}
      </div>
    </aside>
  );
}
