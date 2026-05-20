import { useEffect, useRef, useState } from "react";
import { api, type LogEntry } from "../api/client";

interface LogPanelProps {
  service: string;
}

export function LogPanel({ service }: LogPanelProps) {
  const [lines, setLines] = useState<LogEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [tail, setTail] = useState(100);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);
    api
      .logs(service, tail)
      .then((r) => { if (!cancelled) setLines(r.lines); })
      .catch((e: Error) => { if (!cancelled) setError(e.message); })
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [service, tail]);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [lines]);

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-800 bg-gray-900/60">
        <span className="text-xs text-gray-400">
          logs / <span className="text-indigo-400">{service}</span>
        </span>
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span>tail</span>
          {[50, 100, 200, 500].map((n) => (
            <button
              key={n}
              onClick={() => setTail(n)}
              className={`px-1.5 py-0.5 rounded transition-colors ${
                tail === n ? "bg-indigo-700 text-white" : "hover:bg-gray-700 text-gray-400"
              }`}
            >
              {n}
            </button>
          ))}
        </div>
      </div>

      <div className="flex-1 overflow-y-auto p-4 text-xs leading-relaxed">
        {loading && (
          <p className="text-gray-500 animate-pulse">loading logs…</p>
        )}
        {error && (
          <p className="text-red-400">error: {error}</p>
        )}
        {!loading && !error && lines.length === 0 && (
          <p className="text-gray-600">no log output</p>
        )}
        {lines.map((entry, i) => (
          <div key={i} className="flex gap-3 hover:bg-gray-800/50 px-1 py-0.5 rounded">
            {entry.timestamp && (
              <span className="shrink-0 text-gray-600 select-none">
                {entry.timestamp.slice(11, 23)}
              </span>
            )}
            <span className="text-gray-300 break-all whitespace-pre-wrap">{entry.line}</span>
          </div>
        ))}
        <div ref={bottomRef} />
      </div>
    </div>
  );
}
