import { useState, useEffect, useCallback, useRef } from 'react';
import Sidebar from './components/Sidebar';
import QueryEditor from './components/QueryEditor';
import ResultsPanel from './components/ResultsPanel';
import StatsBar from './components/StatsBar';
import TableInspector from './components/TableInspector';
import CreateTableModal from './components/CreateTableModal';
import { getTables, getTable, runQuery, dropTable } from './services/api';

export default function App() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableInfo, setTableInfo] = useState(null);
  const [results, setResults] = useState(null);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);
  const [successMsg, setSuccessMsg] = useState(null);
  const [loading, setLoading] = useState(false);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [activeTab, setActiveTab] = useState('results');

  // -- Resizable panel --
  const [editorHeight, setEditorHeight] = useState(200);
  const dragging = useRef(false);
  const containerRef = useRef(null);

  const onMouseDown = (e) => {
    e.preventDefault();
    dragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  };

  useEffect(() => {
    const onMouseMove = (e) => {
      if (!dragging.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const newHeight = Math.max(80, Math.min(e.clientY - rect.top, rect.height - 120));
      setEditorHeight(newHeight);
    };
    const onMouseUp = () => {
      dragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => {
      window.removeEventListener('mousemove', onMouseMove);
      window.removeEventListener('mouseup', onMouseUp);
    };
  }, []);

  // -- Fetch tables --
  const refreshTables = useCallback(async () => {
    try {
      const data = await getTables();
      setTables(data.tables || []);
    } catch {
      setTables([]);
    }
  }, []);

  useEffect(() => { refreshTables(); }, [refreshTables]);

  // -- Select table --
  const handleSelectTable = async (name) => {
    setSelectedTable(name);
    try {
      const info = await getTable(name);
      setTableInfo(info);
    } catch {
      setTableInfo(null);
    }
  };

  // -- Drop table --
  const handleDropTable = async (name) => {
    try {
      await dropTable(name);
      if (selectedTable === name) {
        setSelectedTable(null);
        setTableInfo(null);
      }
      refreshTables();
    } catch (e) {
      setError(e.message);
    }
  };

  // -- Execute SQL --
  const handleExecute = async (sql, columnSizes) => {
    setError(null);
    setSuccessMsg(null);
    setLoading(true);
    try {
      const data = await runQuery(sql, columnSizes);
      if (data.rows && data.rows.length > 0) {
        setResults(data.rows);
        setStats(data.stats || null);
        setSuccessMsg(null);
        setActiveTab('results');
      } else {
        setResults(null);
        setStats(data.stats || null);

        // Build a useful success message
        const upper = sql.trim().toUpperCase();
        if (upper.startsWith('INSERT')) {
          setSuccessMsg('Row inserted successfully.');
        } else if (upper.startsWith('DELETE')) {
          setSuccessMsg('Delete executed successfully.');
        } else if (upper.startsWith('CREATE')) {
          setSuccessMsg(`Table "${data.table}" created successfully.`);
        } else {
          setSuccessMsg('Query executed successfully. 0 rows returned.');
        }
        setActiveTab('results');
        if (data.table) refreshTables();
      }
    } catch (e) {
      setError(e.message);
      setResults(null);
      setStats(null);
      setSuccessMsg(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex h-screen overflow-hidden bg-bg-primary" ref={containerRef}>
      {/* Sidebar */}
      <Sidebar
        tables={tables}
        selectedTable={selectedTable}
        tableInfo={tableInfo}
        onSelectTable={handleSelectTable}
        onDropTable={handleDropTable}
        onCreateTable={() => setShowCreateModal(true)}
        onRefresh={refreshTables}
      />

      {/* Main area */}
      <div className="flex flex-col flex-1 min-w-0">
        {/* Query editor */}
        <div style={{ height: editorHeight, minHeight: 80, flexShrink: 0 }}>
          <QueryEditor
            onExecute={handleExecute}
            loading={loading}
            selectedTable={selectedTable}
          />
        </div>

        {/* Resize handle */}
        <div
          onMouseDown={onMouseDown}
          className="h-1.5 bg-bg-secondary border-y border-border cursor-row-resize hover:bg-accent/20 active:bg-accent/30 transition-colors flex items-center justify-center"
        >
          <div className="w-10 h-0.5 rounded bg-border" />
        </div>

        {/* Bottom panels */}
        <div className="flex flex-col flex-1 min-h-0">
          {/* Tab bar */}
          <div className="flex items-center gap-6 bg-bg-primary border-b border-border px-4">
            {['results', 'inspector', 'messages'].map((tab) => (
              <button
                key={tab}
                onClick={() => setActiveTab(tab)}
                className={`px-1 py-2.5 text-[14px] font-medium tracking-wide transition-colors relative ${
                  activeTab === tab
                    ? 'text-accent'
                    : 'text-text-secondary hover:text-text-primary'
                }`}
              >
                {tab === 'results' ? 'Data output' : tab === 'inspector' ? 'Properties' : 'Messages'}
                {activeTab === tab && (
                  <span className="absolute bottom-0 left-0 right-0 h-0.5 bg-accent" />
                )}
              </button>
            ))}

            {stats && <StatsBar stats={stats} />}
          </div>

          {/* Tab content */}
          <div className="flex-1 overflow-auto">
            {activeTab === 'results' && (
              <ResultsPanel
                results={results}
                error={error}
                loading={loading}
                successMsg={successMsg}
              />
            )}
            {activeTab === 'inspector' && (
              <TableInspector
                tableInfo={tableInfo}
                tableName={selectedTable}
              />
            )}
            {activeTab === 'messages' && (
              <div className="p-4 animate-fade-in">
                {error ? (
                  <div className="bg-error-subtle border border-error/20 rounded-lg p-3 text-error text-sm font-mono">
                    {error}
                  </div>
                ) : (
                  <p className="text-text-muted text-sm">No messages.</p>
                )}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* Create table modal */}
      {showCreateModal && (
        <CreateTableModal
          onClose={() => setShowCreateModal(false)}
          onExecute={(sql, sizes) => {
            handleExecute(sql, sizes);
            setShowCreateModal(false);
          }}
        />
      )}
    </div>
  );
}
