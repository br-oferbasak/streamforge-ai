"""
RAG Engine Benchmark — measures ingestion throughput and retrieval latency
across varying corpus sizes and top-k values.
"""
import time
import statistics
import uuid
from dataclasses import dataclass
from typing import List

from rag import Document, RAGEngine

# ---------------------------------------------------------------------------
# Scenario definitions
# ---------------------------------------------------------------------------

INGESTION_SIZES = [10, 50, 100, 250]   # number of documents to ingest
TOPK_VALUES = [1, 3, 5, 10]           # top-k retrieval values to benchmark
QUERY_REPEAT = 5                        # queries per top-k measurement


SAMPLE_DOCUMENTS = [
    ("StreamForge uses Apache Flink for stateful stream processing with exactly-once semantics.",
     {"source": "architecture", "topic": "flink"}),
    ("The prefetch engine caches hot MinIO objects locally, cutting ML job latency by up to 50%.",
     {"source": "prefetch_manual", "topic": "caching"}),
    ("CDC (Change Data Capture) captures row-level database changes and publishes them to Kafka topics.",
     {"source": "cdc_guide", "topic": "cdc"}),
    ("ChromaDB stores vector embeddings and supports ANN search for the RAG retrieval step.",
     {"source": "rag_doc", "topic": "chromadb"}),
    ("Prometheus metrics expose cache hit ratios, prefetch duration, and MinIO upload throughput.",
     {"source": "observability", "topic": "metrics"}),
    ("Flink checkpointing persists operator state to MinIO every 30 seconds for fault tolerance.",
     {"source": "reliability", "topic": "checkpointing"}),
    ("The agent workflow engine orchestrates multi-agent pipelines with dependency-aware execution.",
     {"source": "agents_doc", "topic": "orchestration"}),
    ("Debezium connectors convert MySQL binlog events into Kafka messages in Avro/JSON format.",
     {"source": "debezium_guide", "topic": "debezium"}),
    ("Tumbling event-time windows aggregate user events within configurable 60-second buckets.",
     {"source": "windowing_doc", "topic": "windowing"}),
    ("The feature sink writes processed Kafka events to MinIO as Parquet files for ML training.",
     {"source": "feature_store", "topic": "feature_engineering"}),
]

BENCHMARK_QUERIES = [
    "How does StreamForge handle stream processing?",
    "What is the purpose of the prefetch engine?",
    "How are database changes captured and sent to Kafka?",
    "What vector database does the RAG engine use?",
    "How are performance metrics exposed?",
]


def _make_documents(count: int) -> List[Document]:
    docs = []
    for i in range(count):
        text, meta = SAMPLE_DOCUMENTS[i % len(SAMPLE_DOCUMENTS)]
        # Vary content slightly to avoid duplicate-id collisions in ChromaDB
        docs.append(Document(
            id=f"doc_{uuid.uuid4().hex[:8]}",
            content=f"{text} (variant {i})",
            metadata=meta,
        ))
    return docs


# ---------------------------------------------------------------------------
# Benchmark routines
# ---------------------------------------------------------------------------

@dataclass
class IngestionResult:
    doc_count: int
    duration_s: float
    throughput_docs_per_s: float


@dataclass
class RetrievalResult:
    top_k: int
    avg_latency_s: float
    p95_latency_s: float
    min_latency_s: float


def benchmark_ingestion() -> List[IngestionResult]:
    results = []
    for n in INGESTION_SIZES:
        engine = RAGEngine(
            collection_name=f"bench_ingest_{n}_{uuid.uuid4().hex[:6]}",
            persist_directory=f"./rag_bench_tmp_{n}",
        )
        docs = _make_documents(n)
        start = time.time()
        engine.ingest_documents(docs)
        elapsed = time.time() - start
        results.append(IngestionResult(
            doc_count=n,
            duration_s=elapsed,
            throughput_docs_per_s=n / elapsed if elapsed > 0 else float("inf"),
        ))
        _cleanup(engine, f"./rag_bench_tmp_{n}")
    return results


def benchmark_retrieval(corpus_size: int = 50) -> List[RetrievalResult]:
    engine = RAGEngine(
        collection_name=f"bench_retrieval_{uuid.uuid4().hex[:6]}",
        persist_directory="./rag_bench_retrieval_tmp",
    )
    engine.ingest_documents(_make_documents(corpus_size))

    results = []
    for k in TOPK_VALUES:
        latencies = []
        for i in range(QUERY_REPEAT):
            query = BENCHMARK_QUERIES[i % len(BENCHMARK_QUERIES)]
            start = time.time()
            engine.retrieve(query, top_k=k)
            latencies.append(time.time() - start)
        latencies.sort()
        p95_idx = max(0, int(len(latencies) * 0.95) - 1)
        results.append(RetrievalResult(
            top_k=k,
            avg_latency_s=statistics.mean(latencies),
            p95_latency_s=latencies[p95_idx],
            min_latency_s=min(latencies),
        ))

    _cleanup(engine, "./rag_bench_retrieval_tmp")
    return results


def benchmark_end_to_end(corpus_size: int = 50) -> float:
    """Measure full pipeline: ingest → retrieve → generate (simulated LLM)."""
    engine = RAGEngine(
        collection_name=f"bench_e2e_{uuid.uuid4().hex[:6]}",
        persist_directory="./rag_bench_e2e_tmp",
    )
    docs = _make_documents(corpus_size)

    start = time.time()
    engine.ingest_documents(docs)
    context = engine.retrieve("How does the prefetch engine reduce ML job latency?", top_k=3)
    engine.generate_response("How does the prefetch engine reduce ML job latency?", context)
    elapsed = time.time() - start

    _cleanup(engine, "./rag_bench_e2e_tmp")
    return elapsed


def _cleanup(engine: RAGEngine, path: str):
    try:
        if not engine.is_mock:
            engine.client.delete_collection(engine.collection_name)
    except Exception:
        pass
    import shutil, os
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_ingestion_results(results: List[IngestionResult]):
    print("\n--- Ingestion Throughput ---")
    print(f"  {'Docs':>6}  {'Duration':>10}  {'Throughput':>15}")
    print("  " + "-" * 35)
    for r in results:
        print(f"  {r.doc_count:>6}  {r.duration_s:>9.3f}s  {r.throughput_docs_per_s:>12.1f} docs/s")


def print_retrieval_results(results: List[RetrievalResult]):
    print("\n--- Retrieval Latency (corpus=50 docs) ---")
    print(f"  {'top-k':>6}  {'Avg':>10}  {'P95':>10}  {'Min':>10}")
    print("  " + "-" * 44)
    for r in results:
        print(
            f"  {r.top_k:>6}  {r.avg_latency_s*1000:>8.2f}ms  "
            f"{r.p95_latency_s*1000:>8.2f}ms  {r.min_latency_s*1000:>8.2f}ms"
        )


def run_benchmark():
    print("=" * 60)
    print("   StreamForge AI: RAG Engine Benchmark")
    print("=" * 60)

    print("\n[1/3] Ingestion throughput across corpus sizes...")
    ingestion = benchmark_ingestion()
    print_ingestion_results(ingestion)

    print("\n[2/3] Retrieval latency across top-k values...")
    retrieval = benchmark_retrieval(corpus_size=50)
    print_retrieval_results(retrieval)

    print("\n[3/3] End-to-end pipeline latency (ingest + retrieve + generate)...")
    e2e = benchmark_end_to_end(corpus_size=50)
    print(f"\n  End-to-end latency: {e2e:.3f}s")

    print("\n" + "=" * 60)
    return {"ingestion": ingestion, "retrieval": retrieval, "e2e_s": e2e}


if __name__ == "__main__":
    run_benchmark()
