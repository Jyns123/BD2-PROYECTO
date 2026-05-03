import { HardDrive, Clock, ArrowDownToLine, ArrowUpFromLine } from 'lucide-react';

export default function StatsBar({ stats }) {
  if (!stats) return null;

  const items = [
    {
      icon: <ArrowDownToLine size={13} />,
      label: 'Reads',
      value: stats.reads ?? 0,
      color: 'text-accent',
    },
    {
      icon: <ArrowUpFromLine size={13} />,
      label: 'Writes',
      value: stats.writes ?? 0,
      color: 'text-warning',
    },
    {
      icon: <HardDrive size={13} />,
      label: 'Disk',
      value: stats.disk_accesses ?? (stats.reads ?? 0) + (stats.writes ?? 0),
      color: 'text-success',
    },
    {
      icon: <Clock size={13} />,
      label: 'Time',
      value: `${(stats.time_ms ?? 0).toFixed(2)}ms`,
      color: 'text-error',
    },
  ];

  return (
    <div className="ml-auto flex items-center gap-4 pr-2">
      {items.map((item) => (
        <div key={item.label} className="flex items-center gap-1 text-xs">
          <span className={item.color}>{item.icon}</span>
          <span className="text-text-muted">{item.label}:</span>
          <span className="text-text-primary font-mono font-medium">{item.value}</span>
        </div>
      ))}
    </div>
  );
}
