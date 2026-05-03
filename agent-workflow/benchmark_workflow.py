"""
Agent Workflow Benchmark — measures orchestration overhead, agent communication
latency, and sequential vs parallel (simulated) execution time.
"""
import sys
import time
import statistics
import logging
from dataclasses import dataclass
from typing import List, Dict, Any

# Suppress INFO logs during benchmarking
logging.disable(logging.CRITICAL)

sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent / "src"))

from agent import Agent, DataIngestionAgent, StreamProcessorAgent, StorageSinkAgent
from communication import CommunicationManager


# ---------------------------------------------------------------------------
# Scenario helpers
# ---------------------------------------------------------------------------

def make_pipeline(agent_count: int) -> tuple[List[Agent], CommunicationManager]:
    """Create a linear pipeline of alternating agent types."""
    types = ["data_ingestion", "stream_processor", "storage_sink"]
    agents = [
        Agent.create_agent(types[i % len(types)], f"agent_{i}", {})
        for i in range(agent_count)
    ]
    comm = CommunicationManager()
    for a in agents:
        comm.register_agent(a.name)
    return agents, comm


ACTIONS = {
    "data_ingestion": "start_ingestion",
    "stream_processor": "process_stream",
    "storage_sink": "write_to_storage",
}

def agent_action(a: Agent) -> str:
    return ACTIONS.get(type(a).__name__.replace("Agent", "").lower().replace("data", "data_").replace("stream", "stream_").replace("storage", "storage_"), "start_ingestion")


_TYPE_ACTION = {
    DataIngestionAgent: "start_ingestion",
    StreamProcessorAgent: "process_stream",
    StorageSinkAgent: "write_to_storage",
}


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

@dataclass
class OrchestrationResult:
    agent_count: int
    sequential_s: float
    overhead_per_agent_ms: float


@dataclass
class CommunicationResult:
    message_count: int
    total_s: float
    avg_per_msg_us: float


@dataclass
class DependencyResult:
    pipeline_depth: int
    resolution_s: float


def benchmark_orchestration(sizes: List[int], repeat: int = 3) -> List[OrchestrationResult]:
    """Measure agent initialization + sequential execution overhead."""
    results = []
    for n in sizes:
        times = []
        for _ in range(repeat):
            start = time.perf_counter()
            agents, comm = make_pipeline(n)
            for a in agents:
                action = _TYPE_ACTION[type(a)]
                a.execute_action(action, {})
            times.append(time.perf_counter() - start)
        avg = statistics.mean(times)
        results.append(OrchestrationResult(
            agent_count=n,
            sequential_s=avg,
            overhead_per_agent_ms=(avg / n) * 1000,
        ))
    return results


def benchmark_communication(msg_counts: List[int], repeat: int = 3) -> List[CommunicationResult]:
    """Measure message send + receive throughput."""
    results = []
    for n in msg_counts:
        times = []
        for _ in range(repeat):
            comm = CommunicationManager()
            comm.register_agent("sender")
            comm.register_agent("receiver")
            start = time.perf_counter()
            for i in range(n):
                comm.send_message("sender", "receiver", {"seq": i, "payload": "x" * 64})
            comm.get_messages("receiver")
            times.append(time.perf_counter() - start)
        avg = statistics.mean(times)
        results.append(CommunicationResult(
            message_count=n,
            total_s=avg,
            avg_per_msg_us=(avg / n) * 1e6,
        ))
    return results


def benchmark_dependency_resolution(depths: List[int], repeat: int = 3) -> List[DependencyResult]:
    """
    Simulate a chain where each agent passes its result to the next via
    the CommunicationManager, measuring end-to-end dependency resolution time.
    """
    results = []
    for depth in depths:
        times = []
        for _ in range(repeat):
            agents, comm = make_pipeline(depth)
            start = time.perf_counter()
            prev_result: Dict[str, Any] = {}
            for a in agents:
                # Inject upstream messages
                if prev_result:
                    comm.send_message("upstream", a.name, prev_result)
                msgs = comm.get_messages(a.name)
                input_data = {"messages": msgs} if msgs else {}
                action = _TYPE_ACTION[type(a)]
                prev_result = a.execute_action(action, input_data)
            times.append(time.perf_counter() - start)
        avg = statistics.mean(times)
        results.append(DependencyResult(pipeline_depth=depth, resolution_s=avg))
    return results


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_orchestration(results: List[OrchestrationResult]):
    print("\n--- Agent Orchestration Overhead ---")
    print(f"  {'Agents':>8}  {'Total':>10}  {'Per-Agent':>12}")
    print("  " + "-" * 36)
    for r in results:
        print(f"  {r.agent_count:>8}  {r.sequential_s*1000:>8.2f}ms  {r.overhead_per_agent_ms:>10.3f}ms")


def print_communication(results: List[CommunicationResult]):
    print("\n--- Inter-Agent Communication Throughput ---")
    print(f"  {'Messages':>9}  {'Total':>10}  {'Avg/Msg':>12}")
    print("  " + "-" * 37)
    for r in results:
        print(f"  {r.message_count:>9}  {r.total_s*1000:>8.2f}ms  {r.avg_per_msg_us:>10.2f}µs")


def print_dependency(results: List[DependencyResult]):
    print("\n--- Dependency Chain Resolution Time ---")
    print(f"  {'Depth':>7}  {'Total':>10}  {'Per-Step':>12}")
    print("  " + "-" * 35)
    for r in results:
        per_step = (r.resolution_s / r.pipeline_depth) * 1000
        print(f"  {r.pipeline_depth:>7}  {r.resolution_s*1000:>8.2f}ms  {per_step:>10.3f}ms")


def run_benchmark():
    print("=" * 60)
    print("   StreamForge AI: Agent Workflow Benchmark")
    print("=" * 60)

    print("\n[1/3] Orchestration overhead vs pipeline size...")
    orch = benchmark_orchestration([1, 5, 10, 20, 50])
    print_orchestration(orch)

    print("\n[2/3] Communication throughput vs message volume...")
    comm_results = benchmark_communication([10, 100, 500, 1000])
    print_communication(comm_results)

    print("\n[3/3] Dependency chain resolution time vs depth...")
    dep = benchmark_dependency_resolution([2, 5, 10, 20])
    print_dependency(dep)

    print("\n" + "=" * 60)
    return {"orchestration": orch, "communication": comm_results, "dependency": dep}


if __name__ == "__main__":
    run_benchmark()
