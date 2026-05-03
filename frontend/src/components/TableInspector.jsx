import { Info, Columns3, Key, Database, HardDrive } from 'lucide-react';

const INDEX_BADGES = {
  BPLUSTREE: { label: 'B+ Tree', cls: 'bg-accent-subtle text-accent' },
  HASH: { label: 'Ext. Hash', cls: 'bg-warning-subtle text-warning' },
  SEQUENTIAL: { label: 'Sequential', cls: 'bg-success-subtle text-success' },
  RTREE: { label: 'R-Tree', cls: 'bg-error-subtle text-error' },
};

export default function TableInspector({ tableInfo, tableName }) {
  if (!tableName || !tableInfo) {
    return (
      <div className="flex flex-col items-center justify-center h-full py-16 text-text-muted">
        <Info size={28} className="mb-2 opacity-30" />
        <p className="text-sm">Select a table to view its properties.</p>
      </div>
    );
  }

  const columns = tableInfo.columns || [];
  const indexType = (tableInfo.index || 'BPLUSTREE').toUpperCase();
  const badge = INDEX_BADGES[indexType] || INDEX_BADGES.BPLUSTREE;

  return (
    <div className="p-4 space-y-4 animate-fade-in max-w-2xl">
      {/* General info */}
      <Section title="General" icon={<Database size={13} />}>
        <Row label="Table Name" value={tableName} />
        <Row label="Index Structure">
          <span className={`text-[11px] px-2 py-0.5 rounded font-medium ${badge.cls}`}>
            {badge.label}
          </span>
        </Row>
        <Row label="Record Size" value={`${tableInfo.record_size ?? '?'} bytes`} />
        <Row label="Key Column" value={tableInfo.key_column || '-'} />
      </Section>

      {/* Columns */}
      <Section title="Columns" icon={<Columns3 size={13} />}>
        <table className="w-full text-xs">
          <thead>
            <tr className="text-text-muted border-b border-border">
              <th className="text-left py-1.5 px-2 font-medium">#</th>
              <th className="text-left py-1.5 px-2 font-medium">Name</th>
              <th className="text-left py-1.5 px-2 font-medium">Type</th>
              <th className="text-left py-1.5 px-2 font-medium">Index</th>
            </tr>
          </thead>
          <tbody>
            {columns.map((col, i) => (
              <tr key={i} className="border-b border-border/30 hover:bg-bg-hover transition-colors">
                <td className="py-1.5 px-2 text-text-muted">{i + 1}</td>
                <td className="py-1.5 px-2 font-mono text-text-primary flex items-center gap-1.5">
                  {col.name}
                  {col.name === tableInfo.key_column && (
                    <Key size={10} className="text-warning" />
                  )}
                </td>
                <td className="py-1.5 px-2 text-text-secondary">{col.type || 'TEXT'}</td>
                <td className="py-1.5 px-2 text-text-muted">{col.index || '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Section>

      {/* Column sizes */}
      {tableInfo.column_sizes && Object.keys(tableInfo.column_sizes).length > 0 && (
        <Section title="Column Sizes" icon={<HardDrive size={13} />}>
          {Object.entries(tableInfo.column_sizes).map(([name, size]) => (
            <Row key={name} label={name} value={`${size} bytes`} />
          ))}
        </Section>
      )}
    </div>
  );
}

// -- Helpers --
function Section({ title, icon, children }) {
  return (
    <div className="bg-bg-secondary rounded-lg border border-border overflow-hidden">
      <div className="flex items-center gap-2 px-3 py-2 border-b border-border bg-bg-tertiary/50">
        <span className="text-accent">{icon}</span>
        <h3 className="text-xs font-semibold text-text-secondary uppercase tracking-wide">{title}</h3>
      </div>
      <div className="px-3 py-2">{children}</div>
    </div>
  );
}

function Row({ label, value, children }) {
  return (
    <div className="flex items-center justify-between py-1 text-xs">
      <span className="text-text-muted">{label}</span>
      {children || <span className="text-text-primary font-mono">{value}</span>}
    </div>
  );
}
