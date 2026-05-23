interface HeaderProps {
  healthy: boolean | null;
  checkedAt: string | null;
  onRefresh: () => void;
  refreshing: boolean;
  onLogout: () => void;
}

export function Header({ healthy, checkedAt, onRefresh, refreshing, onLogout }: HeaderProps) {
  const badge =
    healthy === null
      ? { label: "checking…", cls: "bg-gray-700 text-gray-300" }
      : healthy
      ? { label: "healthy", cls: "bg-emerald-900 text-emerald-300" }
      : { label: "degraded", cls: "bg-red-900 text-red-300" };

  const ts = checkedAt
    ? new Date(checkedAt).toLocaleTimeString()
    : null;

  return (
    <header className="flex items-center justify-between px-6 py-4 border-b border-gray-800">
      <div className="flex items-center gap-3">
        <span className="text-lg font-medium tracking-tight text-white">
          StreamForge <span className="text-gray-400">/ control plane</span>
        </span>
        <span className={`text-xs px-2 py-0.5 rounded-full font-medium ${badge.cls}`}>
          {badge.label}
        </span>
      </div>
      <div className="flex items-center gap-4 text-xs text-gray-500">
        {ts && <span>updated {ts}</span>}
        <button
          onClick={onRefresh}
          disabled={refreshing}
          className="px-3 py-1.5 rounded bg-gray-800 hover:bg-gray-700 text-gray-300 disabled:opacity-40 transition-colors"
        >
          {refreshing ? "refreshing…" : "↺ refresh"}
        </button>
        <button
          onClick={onLogout}
          className="px-3 py-1.5 rounded bg-gray-800 hover:bg-gray-700 text-gray-400 hover:text-red-400 transition-colors"
        >
          sign out
        </button>
      </div>
    </header>
  );
}
