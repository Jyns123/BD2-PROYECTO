import { Loader2 } from 'lucide-react';

export default function ResultsPanel({ results, loading }) {
  if (loading) {
    return (
      <div className="flex items-center justify-center h-full py-16">
        <Loader2 size={22} className="animate-spin text-accent mr-2" />
        <span className="text-sm text-text-secondary">Executing query...</span>
      </div>
    );
  }

  if (!results || results.length === 0) {
    return <div className="h-full" />;
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

    </div>
  );
}
