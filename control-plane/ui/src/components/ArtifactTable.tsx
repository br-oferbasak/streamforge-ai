import { useEffect, useState } from "react";
import { api, type Artifact } from "../api/client";

function humanSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / 1048576).toFixed(1)} MB`;
}

export function ArtifactTable() {
  const [artifacts, setArtifacts] = useState<Artifact[]>([]);
  const [bucket, setBucket] = useState("processed");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  function load() {
    setLoading(true);
    setError(null);
    api
      .artifacts(bucket)
      .then((r) => setArtifacts(r.artifacts))
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }

  useEffect(() => { load(); }, [bucket]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between px-4 py-2 border-b border-gray-800 bg-gray-900/60">
        <div className="flex items-center gap-2 text-xs text-gray-400">
          <span>bucket</span>
          <input
            value={bucket}
            onChange={(e) => setBucket(e.target.value)}
            className="bg-gray-800 border border-gray-700 rounded px-2 py-0.5 text-gray-200 focus:outline-none focus:border-indigo-500 w-36"
          />
        </div>
        <button
          onClick={load}
          disabled={loading}
          className="text-xs px-2 py-0.5 rounded bg-gray-800 hover:bg-gray-700 text-gray-400 disabled:opacity-40"
        >
          {loading ? "loading…" : "↺"}
        </button>
      </div>

      <div className="flex-1 overflow-auto">
        {error && <p className="p-4 text-xs text-red-400">error: {error}</p>}
        {!error && artifacts.length === 0 && !loading && (
          <p className="p-4 text-xs text-gray-600">no artifacts found in bucket "{bucket}"</p>
        )}
        {artifacts.length > 0 && (
          <table className="w-full text-xs text-left">
            <thead className="sticky top-0 bg-gray-900 border-b border-gray-800">
              <tr>
                <th className="px-4 py-2 text-gray-500 font-medium">key</th>
                <th className="px-4 py-2 text-gray-500 font-medium text-right">size</th>
                <th className="px-4 py-2 text-gray-500 font-medium text-right">modified</th>
              </tr>
            </thead>
            <tbody>
              {artifacts.map((a) => (
                <tr key={a.key} className="border-b border-gray-800/50 hover:bg-gray-800/40">
                  <td className="px-4 py-2 text-gray-300 font-mono break-all">{a.key}</td>
                  <td className="px-4 py-2 text-gray-500 text-right whitespace-nowrap">
                    {humanSize(a.size_bytes)}
                  </td>
                  <td className="px-4 py-2 text-gray-500 text-right whitespace-nowrap">
                    {new Date(a.last_modified).toLocaleString()}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
