import { useState, useCallback } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { Play, Loader2, History, X } from 'lucide-react';

const EXAMPLE_QUERIES = [
  "SELECT * FROM students WHERE id = 101;",
  "SELECT * FROM students WHERE id BETWEEN 100 AND 200;",
  "INSERT INTO students VALUES (101, 'Ana', 20);",
  "DELETE FROM students WHERE id = 101;",
  "CREATE TABLE students (id INT INDEX BPLUSTREE, name TEXT, age INT) FROM FILE 'data/students.csv';",
];

export default function QueryEditor({ onExecute, loading, selectedTable }) {
  const [value, setValue] = useState('');
  const [history, setHistory] = useState([]);
  const [showHistory, setShowHistory] = useState(false);

  const handleExecute = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed) return;
    setHistory((prev) => [trimmed, ...prev.filter((q) => q !== trimmed)].slice(0, 30));
    onExecute(trimmed);
  }, [value, onExecute]);

  const handleKeyDown = useCallback((e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  }, [handleExecute]);

  return (
    <div className="flex flex-col h-full bg-bg-primary" onKeyDownCapture={handleKeyDown}>
      {/* Toolbar */}
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border shrink-0">
        <button
          onClick={handleExecute}
          disabled={loading || !value.trim()}
          className="flex items-center gap-1.5 px-3.5 py-1.5 bg-accent hover:bg-accent-hover disabled:opacity-40 text-white text-sm font-medium rounded transition-colors"
        >
          {loading
            ? <Loader2 size={14} className="animate-spin" />
            : <Play size={14} />
          }
          Execute
        </button>

        <span className="text-xs text-text-muted">Ctrl+Enter</span>

        <div className="ml-auto flex items-center gap-2">
          {selectedTable && (
            <span className="text-xs text-text-muted bg-bg-primary px-2 py-0.5 rounded">
              {selectedTable}
            </span>
          )}
          <button
            onClick={() => setShowHistory(!showHistory)}
            className={`p-1.5 rounded transition-colors ${
              showHistory ? 'bg-accent-subtle text-accent' : 'text-text-secondary hover:text-text-primary hover:bg-bg-hover'
            }`}
            title="Query History"
          >
            <History size={14} />
          </button>
        </div>
      </div>

      {/* Editor area - fills remaining space */}
      <div className="flex flex-1 min-h-0 overflow-hidden">
        <div className="flex-1 overflow-auto">
          <CodeMirror
            value={value}
            onChange={setValue}
            extensions={[sql()]}
            theme={oneDark}
            placeholder="-- Write your SQL query here..."
            basicSetup={{
              lineNumbers: true,
              foldGutter: false,
              highlightActiveLine: true,
              autocompletion: true,
            }}
          />
        </div>

        {/* History panel */}
        {showHistory && (
          <div className="w-80 border-l border-border bg-bg-primary overflow-y-auto animate-fade-in">
            <div className="flex items-center justify-between px-3 py-2 border-b border-border">
              <span className="text-xs font-medium text-text-secondary">History</span>
              <button
                onClick={() => setShowHistory(false)}
                className="p-0.5 text-text-muted hover:text-text-primary"
              >
                <X size={14} />
              </button>
            </div>
            {history.length === 0 ? (
              <p className="p-3 text-xs text-text-muted">No queries yet</p>
            ) : (
              history.map((q, i) => (
                <button
                  key={i}
                  onClick={() => { setValue(q); setShowHistory(false); }}
                  className="w-full text-left px-3 py-2 text-xs font-mono text-text-secondary hover:bg-bg-hover hover:text-text-primary border-b border-border/50 transition-colors truncate"
                >
                  {q}
                </button>
              ))
            )}
          </div>
        )}
      </div>

      {/* Quick queries */}
      <div className="flex items-center gap-2 px-3 py-1 overflow-x-auto shrink-0 pb-2">
        <span className="text-xs text-text-muted shrink-0">Quick:</span>
        {['SELECT *', 'INSERT', 'CREATE', 'DELETE'].map((label) => (
          <button
            key={label}
            onClick={() => {
              const tpl = EXAMPLE_QUERIES.find((q) => q.startsWith(label)) || '';
              setValue(tpl);
            }}
            className="text-xs px-2.5 py-1 rounded bg-bg-primary text-text-muted hover:text-accent hover:bg-accent-subtle transition-colors shrink-0"
          >
            {label}
          </button>
        ))}
      </div>
    </div>
  );
}
