import { TableIcon, AlertCircle, Loader2, CheckCircle2 } from 'lucide-react';

export default function ResultsPanel({ results, error, loading, successMsg }) {
  if (loading) {
    return (
      <div className="flex items-center justify-center h-full py-16">
        <Loader2 size={22} className="animate-spin text-accent mr-2" />
        <span className="text-sm text-text-secondary">Executing query...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="p-4 animate-fade-in">
        <div className="flex items-start gap-2 bg-error-subtle border border-error/20 rounded-lg p-3">
          <AlertCircle size={16} className="text-error mt-0.5 shrink-0" />
          <span className="text-sm text-error font-mono">{error}</span>
        </div>
      </div>
    );
  }

  // Success message (INSERT, DELETE, CREATE)
  if (successMsg && (!results || results.length === 0)) {
    return (
      <div className="p-4 animate-fade-in">
        <div className="flex items-start gap-2 bg-success-subtle border border-success/20 rounded-lg p-3">
          <CheckCircle2 size={16} className="text-success mt-0.5 shrink-0" />
          <span className="text-sm text-success">{successMsg}</span>
        </div>
      </div>
    );
  }

  if (!results || results.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full py-16 text-text-muted">
        <TableIcon size={32} className="mb-2 opacity-30" />
        <p className="text-sm">No data output. Execute a query to get output.</p>
      </div>
    );
  }

  const columns = Object.keys(results[0]);

  return (
    <div className="overflow-auto h-full animate-fade-in">
      <table className="w-full text-sm border-collapse">
        <thead className="sticky top-0 z-10">
          <tr className="bg-bg-tertiary border-b border-border">
            <th className="px-3 py-2 text-left font-medium text-text-muted w-12 border-r border-border/50">
              #
            </th>
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-medium text-text-secondary border-r border-border/50 whitespace-nowrap"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {results.map((row, i) => (
            <tr
              key={i}
              className="border-b border-border/30 hover:bg-bg-hover transition-colors"
            >
              <td className="px-3 py-1.5 text-text-muted font-mono border-r border-border/30">
                {i + 1}
              </td>
              {columns.map((col) => (
                <td
                  key={col}
                  className="px-3 py-1.5 text-text-primary font-mono border-r border-border/30 whitespace-nowrap"
                >
                  {row[col] !== null && row[col] !== undefined
                    ? String(row[col])
                    : <span className="text-text-muted italic">NULL</span>
                  }
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>

      {/* Row count footer */}
      <div className="sticky bottom-0 bg-bg-secondary border-t border-border px-3 py-1.5 text-xs text-text-muted">
        {results.length} row{results.length !== 1 ? 's' : ''} returned
      </div>
    </div>
  );
}
