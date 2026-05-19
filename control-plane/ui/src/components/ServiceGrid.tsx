import type { ServiceStatus } from "../api/client";

const STATUS_CONFIG = {
  running:    { dot: "bg-emerald-400 shadow-[0_0_6px_1px_rgba(52,211,153,0.5)]", label: "running",    text: "text-emerald-400" },
  stopped:    { dot: "bg-red-500",                                                label: "stopped",    text: "text-red-400"     },
  restarting: { dot: "bg-yellow-400 animate-pulse",                               label: "restarting", text: "text-yellow-400"  },
  unknown:    { dot: "bg-gray-500",                                               label: "unknown",    text: "text-gray-400"    },
};

// Friendly labels for Docker Compose service names
const SERVICE_LABELS: Record<string, string> = {
  zookeeper:     "Zookeeper",
  kafka:         "Kafka",
  mysql:         "MySQL",
  connect:       "Debezium Connect",
  jobmanager:    "Flink JobManager",
  taskmanager:   "Flink TaskManager",
  minio:         "MinIO",
  "feature-sink": "Feature Sink",
};

function uptime(startedAt: string | null): string {
  if (!startedAt) return "—";
  const secs = Math.floor((Date.now() - new Date(startedAt).getTime()) / 1000);
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

interface ServiceCardProps {
  service: ServiceStatus;
  selected: boolean;
  onClick: () => void;
}

function ServiceCard({ service, selected, onClick }: ServiceCardProps) {
  const cfg = STATUS_CONFIG[service.status] ?? STATUS_CONFIG.unknown;
  return (
    <button
      onClick={onClick}
      className={`text-left p-4 rounded-lg border transition-colors ${
        selected
          ? "border-indigo-500 bg-indigo-950/40"
          : "border-gray-800 bg-gray-900 hover:border-gray-600"
      }`}
    >
      <div className="flex items-start justify-between mb-2">
        <span className="text-white font-medium">
          {SERVICE_LABELS[service.name] ?? service.name}
        </span>
        <span className={`flex items-center gap-1.5 text-xs ${cfg.text}`}>
          <span className={`inline-block w-2 h-2 rounded-full ${cfg.dot}`} />
          {cfg.label}
        </span>
      </div>
      <div className="text-xs text-gray-500 space-y-0.5">
        {service.container_id && (
          <div>
            <span className="text-gray-600">id </span>
            {service.container_id}
          </div>
        )}
        {service.image && (
          <div className="truncate">
            <span className="text-gray-600">image </span>
            {service.image}
          </div>
        )}
        {service.started_at && (
          <div>
            <span className="text-gray-600">up </span>
            {uptime(service.started_at)}
          </div>
        )}
      </div>
    </button>
  );
}

interface ServiceGridProps {
  services: ServiceStatus[];
  selectedService: string | null;
  onSelect: (name: string) => void;
}

export function ServiceGrid({ services, selectedService, onSelect }: ServiceGridProps) {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
      {services.map((s) => (
        <ServiceCard
          key={s.name}
          service={s}
          selected={selectedService === s.name}
          onClick={() => onSelect(s.name)}
        />
      ))}
    </div>
  );
}
