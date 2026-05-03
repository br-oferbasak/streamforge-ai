import os
import time
import statistics
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple

from prefetch import FileStat, prefetch_files, select_hot_files

REMOTE_LATENCY = 0.2  # 200ms per file access (simulating S3/MinIO)
LOCAL_LATENCY = 0.01  # 10ms per file access (simulating local disk/cache)


@dataclass
class ScenarioConfig:
    name: str
    num_files: int
    file_size_kb: int
    hot_ratio: float  # fraction of files that are "hot" (frequently accessed)
    repeat: int = 1   # number of job runs to average over


@dataclass
class BenchmarkResult:
    scenario: str
    no_prefetch_s: float
    prefetch_total_s: float
    prefetch_phase_s: float
    job_phase_s: float
    cache_hits: int
    total_files: int
    latency_reduction_pct: float


SCENARIOS = [
    ScenarioConfig("Small Dataset  (5 files,  100KB, 100% hot)", num_files=5,  file_size_kb=100, hot_ratio=1.0),
    ScenarioConfig("Medium Dataset (20 files, 100KB,  80% hot)", num_files=20, file_size_kb=100, hot_ratio=0.8),
    ScenarioConfig("Large Dataset  (50 files, 100KB,  60% hot)", num_files=50, file_size_kb=100, hot_ratio=0.6),
    ScenarioConfig("Mixed Access   (10 files, 500KB,  50% hot)", num_files=10, file_size_kb=500, hot_ratio=0.5),
    ScenarioConfig("Cache Miss     (10 files, 100KB,   0% hot)", num_files=10, file_size_kb=100, hot_ratio=0.0),
    ScenarioConfig("Full Cache     (10 files, 100KB, 100% hot)", num_files=10, file_size_kb=100, hot_ratio=1.0),
]


def create_mock_files(base_dir: Path, count: int, size_kb: int) -> List[FileStat]:
    base_dir.mkdir(parents=True, exist_ok=True)
    stats = []
    for i in range(count):
        file_path = base_dir / f"data_part_{i}.bin"
        with open(file_path, "wb") as f:
            f.write(os.urandom(size_kb * 1024))
        stats.append(FileStat(
            uri=f"file://{file_path.absolute()}",
            recent_access_count=10,
            last_access_epoch=time.time(),
        ))
    return stats


def split_hot_cold(files: List[FileStat], hot_ratio: float) -> Tuple[List[FileStat], List[FileStat]]:
    n_hot = max(0, int(len(files) * hot_ratio))
    return files[:n_hot], files[n_hot:]


def simulate_direct_access(files: List[FileStat]) -> float:
    start = time.time()
    for _ in files:
        time.sleep(REMOTE_LATENCY)
        time.sleep(LOCAL_LATENCY)
    return time.time() - start


def simulate_prefetch_access(
    cache_dir: Path,
    hot_files: List[FileStat],
    cold_files: List[FileStat],
) -> Tuple[float, float, int]:
    # Prefetch phase: stage hot files to local cache
    prefetch_start = time.time()
    if hot_files:
        prefetch_files(hot_files, cache_dir, simulate_latency_s=REMOTE_LATENCY)
    prefetch_dur = time.time() - prefetch_start

    # Job phase: access hot files from cache, cold files from remote
    job_start = time.time()
    hits = 0
    for f in hot_files:
        src = Path(f.uri.replace("file://", ""))
        if (cache_dir / src.name).exists():
            hits += 1
            time.sleep(LOCAL_LATENCY)
        else:
            time.sleep(REMOTE_LATENCY + LOCAL_LATENCY)
    for _ in cold_files:
        time.sleep(REMOTE_LATENCY + LOCAL_LATENCY)

    job_dur = time.time() - job_start
    return prefetch_dur, job_dur, hits


def run_scenario(cfg: ScenarioConfig, tmp_path: Path) -> BenchmarkResult:
    source_dir = tmp_path / "source" / cfg.name.replace(" ", "_")
    cache_dir = tmp_path / "cache" / cfg.name.replace(" ", "_")

    all_files = create_mock_files(source_dir, cfg.num_files, cfg.file_size_kb)
    hot_files, cold_files = split_hot_cold(all_files, cfg.hot_ratio)

    no_prefetch_times = []
    prefetch_total_times = []
    prefetch_phases = []
    job_phases = []
    all_hits = []

    for _ in range(cfg.repeat):
        no_prefetch_times.append(simulate_direct_access(all_files))
        # Clear cache between runs
        if cache_dir.exists():
            import shutil
            shutil.rmtree(cache_dir)
        p_dur, j_dur, hits = simulate_prefetch_access(cache_dir, hot_files, cold_files)
        prefetch_phases.append(p_dur)
        job_phases.append(j_dur)
        prefetch_total_times.append(p_dur + j_dur)
        all_hits.append(hits)

    avg_no_prefetch = statistics.mean(no_prefetch_times)
    avg_job = statistics.mean(job_phases)
    reduction = ((avg_no_prefetch - avg_job) / avg_no_prefetch) * 100 if avg_no_prefetch > 0 else 0.0

    return BenchmarkResult(
        scenario=cfg.name,
        no_prefetch_s=avg_no_prefetch,
        prefetch_total_s=statistics.mean(prefetch_total_times),
        prefetch_phase_s=statistics.mean(prefetch_phases),
        job_phase_s=avg_job,
        cache_hits=round(statistics.mean(all_hits)),
        total_files=cfg.num_files,
        latency_reduction_pct=reduction,
    )


def print_results(results: List[BenchmarkResult]):
    print("\n" + "=" * 78)
    print("           StreamForge AI: Prefetch Engine Benchmark Results")
    print("=" * 78)
    header = f"{'Scenario':<45} {'No-Prefetch':>11} {'Job Phase':>9} {'Hits':>8} {'Reduction':>10}"
    print(header)
    print("-" * 78)
    for r in results:
        print(
            f"{r.scenario:<45} {r.no_prefetch_s:>10.3f}s {r.job_phase_s:>8.3f}s"
            f" {r.cache_hits:>3}/{r.total_files:<4} {r.latency_reduction_pct:>8.1f}%"
        )
    print("=" * 78)
    print("Note: 'Job Phase' is the ML job execution time after prefetching.")
    print("      In production, prefetch runs in parallel with upstream stages.")
    print("=" * 78)


def run_benchmark():
    print("=" * 78)
    print("      StreamForge AI: Prefetch Engine — Multi-Scenario Benchmark")
    print("=" * 78)
    print(f"  Remote latency: {REMOTE_LATENCY*1000:.0f}ms/file  |  Local latency: {LOCAL_LATENCY*1000:.0f}ms/file")
    print(f"  Running {len(SCENARIOS)} scenarios...\n")

    results = []
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        for i, cfg in enumerate(SCENARIOS, 1):
            print(f"  [{i}/{len(SCENARIOS)}] {cfg.name} ...", end=" ", flush=True)
            result = run_scenario(cfg, tmp_path)
            results.append(result)
            print(f"done  (reduction: {result.latency_reduction_pct:.1f}%)")

    print_results(results)
    return results


if __name__ == "__main__":
    run_benchmark()
