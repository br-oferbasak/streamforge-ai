import { FormEvent, useState } from "react";
import { setToken } from "./auth";

interface LoginPageProps {
  onSuccess: () => void;
}

export function LoginPage({ onSuccess }: LoginPageProps) {
  const [secret, setSecret] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: FormEvent) {
    e.preventDefault();
    setError(null);
    setLoading(true);
    try {
      const res = await fetch("/api/v1/status", {
        headers: { Authorization: `Bearer ${secret}` },
      });
      if (res.ok) {
        setToken(secret);
        onSuccess();
      } else if (res.status === 401) {
        setError("incorrect secret");
      } else {
        setError(`server error: ${res.status}`);
      }
    } catch {
      setError("could not reach the control-plane API");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex h-screen items-center justify-center bg-gray-950">
      <div className="w-full max-w-sm rounded-xl border border-gray-800 bg-gray-900 p-8 shadow-xl">
        <h1 className="mb-1 text-lg font-medium text-white tracking-tight">
          StreamForge <span className="text-gray-400">/ control plane</span>
        </h1>
        <p className="mb-6 text-xs text-gray-500">enter the shared demo secret to continue</p>

        <form onSubmit={handleSubmit} className="flex flex-col gap-3">
          <input
            type="password"
            placeholder="shared secret"
            value={secret}
            onChange={(e) => setSecret(e.target.value)}
            autoFocus
            required
            className="rounded-lg border border-gray-700 bg-gray-800 px-4 py-2.5 text-sm text-white placeholder-gray-600 outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500"
          />
          {error && <p className="text-xs text-red-400">{error}</p>}
          <button
            type="submit"
            disabled={loading || !secret}
            className="rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-medium text-white hover:bg-indigo-500 disabled:opacity-40 transition-colors"
          >
            {loading ? "connecting…" : "connect"}
          </button>
        </form>
      </div>
    </div>
  );
}
