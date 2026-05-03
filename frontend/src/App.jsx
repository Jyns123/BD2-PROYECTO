import { useState, useEffect, useCallback, useRef } from 'react';
import { Maximize2, Minimize2, AlertCircle, CheckCircle2, Info } from 'lucide-react';
import Sidebar from './components/Sidebar';
import QueryEditor from './components/QueryEditor';
import ResultsPanel from './components/ResultsPanel';
import StatsBar from './components/StatsBar';
import CreateTableModal from './components/CreateTableModal';
import { getTables, getTable, runQuery, dropTable } from './services/api';

const BOTTOM_TABS = [
  { id: 'results', label: 'Data Output' },
  { id: 'messages', label: 'Messages' },
  { id: 'notifications', label: 'Notifications' },
];

export default function App() {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableInfo, setTableInfo] = useState(null);
  const [results, setResults] = useState(null);
  const [stats, setStats] = useState(null);
  const [error, setError] = useState(null);
  const [successMsg, setSuccessMsg] = useState(null);
  const [notifications, setNotifications] = useState([]);
  const [loading, setLoading] = useState(false);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [activeTab, setActiveTab] = useState('results');
  const [cursor, setCursor] = useState({ line: 1, col: 1 });
  const [maximized, setMaximized] = useState(null); // 'editor' | 'bottom' | null

  // -- Resizable panel --
  const [editorHeight, setEditorHeight] = useState(220);
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

  const handleSelectTable = async (name) => {
    setSelectedTable(name);
    try {
      const info = await getTable(name);
      setTableInfo(info);
    } catch {
      setTableInfo(null);
    }
  };

  const handleDropTable = async (name) => {
    try {
      await dropTable(name);
      if (selectedTable === name) {
        setSelectedTable(null);
        setTableInfo(null);
      }
      pushNotification('success', `Table "${name}" dropped.`);
      refreshTables();
    } catch (e) {
      setError(e.message);
      pushNotification('error', e.message);
    }
  };

  const pushNotification = (kind, text) => {
    setNotifications((prev) => [
      { kind, text, time: new Date().toLocaleTimeString() },
      ...prev,
    ].slice(0, 50));
  };

  const handleExecute = async (sqlText, columnSizes) => {
    setError(null);
    setSuccessMsg(null);
    setLoading(true);
    try {
      const data = await runQuery(sqlText, columnSizes);
      if (data.rows && data.rows.length > 0) {
        setResults(data.rows);
        setStats(data.stats || null);
        setSuccessMsg(null);
        setActiveTab('results');
      } else {
        setResults(null);
        setStats(data.stats || null);

        const upper = sqlText.trim().toUpperCase();
        let msg;
        if (upper.startsWith('INSERT')) msg = 'Row inserted successfully.';
        else if (upper.startsWith('DELETE')) msg = 'Delete executed successfully.';
        else if (upper.startsWith('CREATE')) msg = `Table "${data.table}" created successfully.`;
        else msg = 'Query executed successfully. 0 rows returned.';

        setSuccessMsg(msg);
        pushNotification('success', msg);
        setActiveTab('messages');
        if (data.table) refreshTables();
      }
    } catch (e) {
      setError(e.message);
      setResults(null);
      setStats(null);
      setSuccessMsg(null);
      pushNotification('error', e.message);
      setActiveTab('messages');
    } finally {
      setLoading(false);
    }
  };

  const toggleMaximize = (which) => {
    setMaximized((prev) => (prev === which ? null : which));
  };

  const showEditor = maximized !== 'bottom';
  const showBottom = maximized !== 'editor';
  const showResize = showEditor && showBottom;
  const totalRows = results?.length ?? 0;

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-bg-primary">
      <div className="flex flex-1 min-h-0 overflow-hidden" ref={containerRef}>
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
          {showEditor && (
            <div
              style={{
                height: maximized === 'editor' ? '100%' : editorHeight,
                minHeight: 80,
                flexShrink: 0,
              }}
            >
              <QueryEditor
                onExecute={handleExecute}
                loading={loading}
                onCursorChange={setCursor}
                onToggleMaximize={() => toggleMaximize('editor')}
                isMaximized={maximized === 'editor'}
              />
            </div>
          )}

          {/* Resize handle */}
          {showResize && (
            <div
              onMouseDown={onMouseDown}
              className="h-1.5 bg-bg-secondary border-y border-border cursor-row-resize hover:bg-accent/20 active:bg-accent/30 transition-colors flex items-center justify-center shrink-0"
            >
              <div className="w-10 h-0.5 rounded bg-border" />
            </div>
          )}

          {/* Bottom panel */}
          {showBottom && (
            <div className="flex flex-col flex-1 min-h-0">
              {/* Tab bar */}
              <div className="flex items-stretch bg-bg-primary border-b border-border h-9 shrink-0">
                {BOTTOM_TABS.map((tab) => (
                  <button
                    key={tab.id}
                    onClick={() => setActiveTab(tab.id)}
                    className={`relative px-4 text-[13px] transition-colors ${
                      activeTab === tab.id
                        ? 'text-text-primary'
                        : 'text-text-secondary hover:text-text-primary'
                    }`}
                  >
                    {tab.label}
                    {activeTab === tab.id && (
                      <span className="absolute bottom-0 left-4 right-4 h-px bg-text-primary" />
                    )}
                  </button>
                ))}
                <div className="ml-auto flex items-center pr-2">
                  <button
                    onClick={() => toggleMaximize('bottom')}
                    className="p-1.5 text-text-secondary hover:text-text-primary transition-colors"
                    title={maximized === 'bottom' ? 'Restore' : 'Maximize'}
                  >
                    {maximized === 'bottom' ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
                  </button>
                </div>
              </div>

              {/* Tab content */}
              <div className="flex-1 overflow-auto">
                {activeTab === 'results' && (
                  <ResultsPanel results={results} loading={loading} />
                )}
                {activeTab === 'messages' && (
                  <MessagesPanel error={error} successMsg={successMsg} stats={stats} />
                )}
                {activeTab === 'notifications' && (
                  <NotificationsPanel notifications={notifications} />
                )}
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Bottom status bar */}
      <div className="flex items-center bg-bg-secondary border-t border-border px-3 text-[11px] text-text-muted shrink-0 h-6 select-none">
        <span>Total rows:{results ? ` ${totalRows}` : ''}</span>
        <div className="ml-auto flex items-center gap-4">
          <span>LF</span>
          <span>Ln {cursor.line}, Col {cursor.col}</span>
        </div>
      </div>

      {/* Create table modal */}
      {showCreateModal && (
        <CreateTableModal
          onClose={() => setShowCreateModal(false)}
          onExecute={(sqlText, sizes) => {
            handleExecute(sqlText, sizes);
            setShowCreateModal(false);
          }}
        />
      )}
    </div>
  );
}

// -- Inline panels -----------------------------------------------------------

function MessagesPanel({ error, successMsg, stats }) {
  if (!error && !successMsg && !stats) {
    return <p className="p-3 text-xs text-text-muted">No messages.</p>;
  }
  return (
    <div className="p-3 space-y-2 animate-fade-in">
      {error && (
        <div className="flex items-start gap-2 bg-error-subtle border border-error/20 rounded p-2.5">
          <AlertCircle size={14} className="text-error mt-0.5 shrink-0" />
          <span className="text-xs text-error font-mono break-all">{error}</span>
        </div>
      )}
      {successMsg && (
        <div className="flex items-start gap-2 bg-success-subtle border border-success/20 rounded p-2.5">
          <CheckCircle2 size={14} className="text-success mt-0.5 shrink-0" />
          <span className="text-xs text-success">{successMsg}</span>
        </div>
      )}
      {stats && (
        <div className="bg-bg-secondary border border-border/40 rounded p-2.5">
          <StatsBar stats={stats} />
        </div>
      )}
    </div>
  );
}

function NotificationsPanel({ notifications }) {
  if (notifications.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full py-12 text-text-muted">
        <Info size={24} className="mb-2 opacity-30" />
        <p className="text-xs">No notifications.</p>
      </div>
    );
  }
  return (
    <div className="animate-fade-in">
      {notifications.map((n, i) => (
        <div
          key={i}
          className="flex items-start gap-2 px-3 py-2 border-b border-border/40"
        >
          {n.kind === 'error' ? (
            <AlertCircle size={13} className="text-error mt-0.5 shrink-0" />
          ) : (
            <CheckCircle2 size={13} className="text-success mt-0.5 shrink-0" />
          )}
          <span className="text-xs text-text-muted shrink-0 font-mono">{n.time}</span>
          <span className="text-xs text-text-primary break-all">{n.text}</span>
        </div>
      ))}
    </div>
  );
}
