import { getToken } from "../auth";

const BASE = "/api/v1";

export class AuthError extends Error {}

export interface ServiceStatus {
  name: string;
  container_id: string | null;
  status: "running" | "stopped" | "restarting" | "unknown";
  started_at: string | null;
  image: string | null;
}

export interface SystemStatus {
  healthy: boolean;
  services: ServiceStatus[];
  checked_at: string;
}

export interface LogEntry {
  timestamp: string;
  line: string;
}

export interface LogResponse {
  service: string;
  lines: LogEntry[];
}

export interface Artifact {
  key: string;
  bucket: string;
  size_bytes: number;
  last_modified: string;
  etag: string | null;
}

export interface ArtifactResponse {
  bucket: string;
  artifacts: Artifact[];
  total: number;
}

async function get<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = new URL(BASE + path, window.location.origin);
  if (params) {
    Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, String(v)));
  }
  const token = getToken();
  const headers: Record<string, string> = token ? { Authorization: `Bearer ${token}` } : {};
  const res = await fetch(url.toString(), { headers });
  if (res.status === 401) throw new AuthError("session expired");
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return res.json() as Promise<T>;
}

export const api = {
  status: () => get<SystemStatus>("/status"),
  logs: (service: string, tail = 100) => get<LogResponse>(`/logs/${service}`, { tail }),
  artifacts: (bucket?: string, prefix?: string, limit = 50) =>
    get<ArtifactResponse>("/artifacts", {
      ...(bucket ? { bucket } : {}),
      ...(prefix ? { prefix } : {}),
      limit,
    }),
};
