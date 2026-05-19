import { useCallback, useEffect, useState } from "react";
import { api, type SystemStatus } from "./api/client";
import { ArtifactTable } from "./components/ArtifactTable";
import { Header } from "./components/Header";
import { LogPanel } from "./components/LogPanel";
import { ServiceGrid } from "./components/ServiceGrid";

type Tab = "logs" | "artifacts";

export default function App() {
  const [status, setStatus] = useState<SystemStatus | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [selectedService, setSelectedService] = useState<string | null>(null);
  const [tab, setTab] = useState<Tab>("logs");

  const refresh = useCallback(() => {
    setRefreshing(true);
    api
      .status()
      .then((s) => {
        setStatus(s);
        setSelectedService((prev) => prev ?? s.services[0]?.name ?? null);
      })
      .catch(console.error)
      .finally(() => setRefreshing(false));
  }, []);

  useEffect(() => {
    refresh();
    const id = setInterval(refresh, 15_000);
    return () => clearInterval(id);
  }, [refresh]);

  return (
    <div className="flex flex-col h-screen overflow-hidden">
      <Header
        healthy={status?.healthy ?? null}
        checkedAt={status?.checked_at ?? null}
        onRefresh={refresh}
        refreshing={refreshing}
      />

      <main className="flex-1 overflow-hidden flex flex-col gap-4 p-6">
        {/* service status grid */}
        <section>
          <h2 className="text-xs uppercase tracking-widest text-gray-600 mb-3">services</h2>
          {status ? (
            <ServiceGrid
              services={status.services}
              selectedService={selectedService}
              onSelect={setSelectedService}
            />
          ) : (
            <p className="text-xs text-gray-600 animate-pulse">connecting to control-plane API…</p>
          )}
        </section>

        {/* tabs */}
        <section className="flex-1 flex flex-col min-h-0 rounded-lg border border-gray-800 bg-gray-900 overflow-hidden">
          <div className="flex border-b border-gray-800">
            {(["logs", "artifacts"] as Tab[]).map((t) => (
              <button
                key={t}
                onClick={() => setTab(t)}
                className={`px-5 py-2.5 text-xs font-medium transition-colors ${
                  tab === t
                    ? "border-b-2 border-indigo-500 text-indigo-300"
                    : "text-gray-500 hover:text-gray-300"
                }`}
              >
                {t}
              </button>
            ))}
          </div>

          <div className="flex-1 min-h-0">
            {tab === "logs" && selectedService ? (
              <LogPanel service={selectedService} />
            ) : tab === "logs" ? (
              <p className="p-4 text-xs text-gray-600">select a service above to view logs</p>
            ) : (
              <ArtifactTable />
            )}
          </div>
        </section>
      </main>
    </div>
  );
}
