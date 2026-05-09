import { useState, useCallback, useMemo } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql } from '@codemirror/lang-sql';
import { oneDark } from '@codemirror/theme-one-dark';
import { EditorView } from '@codemirror/view';
import { Maximize2, Minimize2, Play, Loader2 } from 'lucide-react';

const TABS = [
  { id: 'query', label: 'Query' },
  { id: 'history', label: 'Query History' },
];

export default function QueryEditor({ onExecute, loading, onCursorChange, onToggleMaximize, isMaximized }) {
  const [value, setValue] = useState('');
  const [history, setHistory] = useState([]);
  const [activeTab, setActiveTab] = useState('query');

  const handleExecute = useCallback(() => {
    const trimmed = value.trim();
    if (!trimmed || loading) return;
    setHistory((prev) => [trimmed, ...prev.filter((q) => q !== trimmed)].slice(0, 30));
    onExecute(trimmed);
  }, [value, onExecute, loading]);

  const handleKeyDown = useCallback((e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  }, [handleExecute]);

  const cursorExt = useMemo(
    () =>
      EditorView.updateListener.of((update) => {
        if (update.selectionSet || update.docChanged || update.focusChanged) {
          const pos = update.state.selection.main.head;
          const line = update.state.doc.lineAt(pos);
          const col = pos - line.from + 1;
          onCursorChange?.({ line: line.number, col });
        }
      }),
    [onCursorChange]
  );

  return (
    <div className="flex flex-col h-full bg-bg-primary" onKeyDownCapture={handleKeyDown}>
      {/* Top tab bar */}
      <div className="flex items-stretch border-b border-border bg-bg-primary shrink-0 h-9">
        {TABS.map((tab) => (
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
        <div className="ml-auto flex items-center gap-1.5 pr-2">
          <button
            onClick={handleExecute}
            disabled={loading || !value.trim()}
            className="flex items-center gap-1.5 px-2.5 py-1 bg-accent hover:bg-accent-hover disabled:opacity-40 disabled:cursor-not-allowed text-white text-[12px] font-medium rounded transition-colors"
            title="Execute (Ctrl+Enter)"
          >
            {loading
              ? <Loader2 size={12} className="animate-spin" />
              : <Play size={12} />
            }
            Execute
          </button>
          <button
            onClick={onToggleMaximize}
            className="p-1.5 text-text-secondary hover:text-text-primary transition-colors"
            title={isMaximized ? 'Restore' : 'Maximize'}
          >
            {isMaximized ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
          </button>
        </div>
      </div>

      {/* Body */}
      <div className="flex-1 min-h-0 overflow-hidden relative">
        <div
          className={`absolute inset-0 ${activeTab === 'query' ? 'block' : 'hidden'}`}
        >
          <CodeMirror
            value={value}
            onChange={setValue}
            extensions={[sql(), cursorExt]}
            theme={oneDark}
            height="100%"
            basicSetup={{
              lineNumbers: true,
              foldGutter: false,
              highlightActiveLine: true,
              autocompletion: true,
            }}
          />
        </div>

        {activeTab === 'history' && (
          <div className="absolute inset-0 overflow-y-auto bg-bg-primary">
            {history.length === 0 ? (
              <p className="px-3 py-2 text-xs text-text-muted">No queries yet. Press Ctrl+Enter on the Query tab to run.</p>
            ) : (
              history.map((q, i) => (
                <button
                  key={i}
                  onClick={() => { setValue(q); setActiveTab('query'); }}
                  className="w-full text-left px-3 py-2 text-xs font-mono text-text-secondary hover:bg-bg-hover hover:text-text-primary border-b border-border/50 transition-colors truncate block"
                  title={q}
                >
                  {q}
                </button>
              ))
            )}
          </div>
        )}
      </div>
    </div>
  );
}
