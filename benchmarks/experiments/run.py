#!/usr/bin/env python3
"""
Meshflow benchmark suite — runs end-to-end experiments for research paper evaluation.

Usage:
  python -m benchmarks.experiments.run [--output results/] [--nodes 3] [--iterations 50]
"""

import json
import os
import subprocess
import sys
import time
import statistics
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests


@dataclass
class BenchmarkResult:
    experiment: str
    num_nodes: int
    workflow: str
    iterations: int
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    throughput_wf_per_sec: float
    total_time_ms: float
    success_rate: float
    failures: int
    timestamp: str
    details: dict = field(default_factory=dict)


class MeshflowBenchmark:
    BASE_URL_TEMPLATE = "http://localhost:{port}"

    def __init__(self, api_port: int = 8080, workflows_dir: str = "benchmarks/workflows"):
        self.api_port = api_port
        self.base_url = self.BASE_URL_TEMPLATE.format(port=api_port)
        self.workflows_dir = Path(workflows_dir)
        self.results: list[BenchmarkResult] = []

    def _api(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        resp = requests.request(method, url, timeout=30, **kwargs)
        return resp

    def deploy_workflow(self, name: str) -> bool:
        wf_path = self.workflows_dir / f"{name}.yaml"
        if not wf_path.exists():
            print(f"  [SKIP] Workflow {name} not found at {wf_path}")
            return False

        with open(wf_path, "rb") as f:
            resp = self._api("POST", "/deploy", data=f)
        if resp.status_code == 200:
            print(f"  Deployed: {name}")
            return True
        print(f"  [FAIL] Deploy {name}: {resp.status_code} {resp.text}")
        return False

    def trigger_workflow(self, name: str, timeout: float = 60) -> Optional[dict]:
        start = time.time()
        try:
            resp = self._api("POST", f"/trigger/{name}", timeout=timeout)
        except requests.RequestException as e:
            return {"error": str(e)}

        elapsed = (time.time() - start) * 1000

        if resp.status_code == 200:
            data = resp.json()
            data["trigger_latency_ms"] = elapsed
            return data
        return {"error": f"HTTP {resp.status_code}: {resp.text}"}

    def get_run_status(self, run_id: str) -> Optional[dict]:
        try:
            resp = self._api("GET", f"/runs/{run_id}")
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            pass
        return None

    def wait_for_completion(self, run_id: str, timeout: float = 30) -> Optional[dict]:
        start = time.time()
        poll_interval = 0.1

        while time.time() - start < timeout:
            status = self.get_run_status(run_id)
            if status is None:
                time.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, 2.0)
                continue

            if status.get("status") in ("completed", "failed"):
                return status

            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 2.0)

        return None

    def get_mesh_members(self) -> list[str]:
        try:
            resp = self._api("GET", "/mesh")
            if resp.status_code == 200:
                return resp.json().get("members", [])
        except requests.RequestException:
            pass
        return []

    def is_ready(self) -> bool:
        try:
            resp = self._api("GET", "/health")
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def run_throughput_experiment(self, workflow: str, iterations: int = 50) -> BenchmarkResult:
        print(f"\n{'='*60}")
        print(f"Throughput: {workflow} ({iterations} iterations)")
        print(f"{'='*60}")

        if not self.deploy_workflow(workflow):
            return BenchmarkResult(
                experiment="throughput",
                num_nodes=len(self.get_mesh_members()),
                workflow=workflow,
                iterations=iterations,
                avg_latency_ms=0, p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                throughput_wf_per_sec=0, total_time_ms=0,
                success_rate=0, failures=iterations,
                timestamp=datetime.now().isoformat(),
            )

        latencies = []
        failures = 0
        total_start = time.time()

        for i in range(iterations):
            start = time.time()
            result = self.trigger_workflow(workflow)
            elapsed = (time.time() - start) * 1000

            if result and "error" not in result:
                latencies.append(elapsed)
            else:
                failures += 1

            if (i + 1) % 10 == 0:
                print(f"  [{i+1}/{iterations}] avg={statistics.mean(latencies):.1f}ms" if latencies else f"  [{i+1}/{iterations}] running...")

        total_time = (time.time() - total_start) * 1000

        if latencies:
            latencies.sort()
            avg_lat = statistics.mean(latencies)
            p50 = latencies[len(latencies) // 2]
            p95_idx = int(len(latencies) * 0.95)
            p99_idx = int(len(latencies) * 0.99)
            p95 = latencies[min(p95_idx, len(latencies) - 1)]
            p99 = latencies[min(p99_idx, len(latencies) - 1)]
            throughput = len(latencies) / (total_time / 1000) if total_time > 0 else 0
        else:
            avg_lat = p50 = p95 = p99 = throughput = 0

        result = BenchmarkResult(
            experiment="throughput",
            num_nodes=len(self.get_mesh_members()),
            workflow=workflow,
            iterations=iterations,
            avg_latency_ms=avg_lat,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p99,
            throughput_wf_per_sec=throughput,
            total_time_ms=total_time,
            success_rate=(iterations - failures) / iterations if iterations else 0,
            failures=failures,
            timestamp=datetime.now().isoformat(),
            details={"latencies": latencies if len(latencies) < 200 else None},
        )

        print(f"  Success: {iterations - failures}/{iterations}")
        print(f"  Throughput: {throughput:.2f} wf/s")
        print(f"  P50: {p50:.1f}ms, P95: {p95:.1f}ms, P99: {p99:.1f}ms")
        print(f"  Avg: {avg_lat:.1f}ms")

        self.results.append(result)
        return result

    def run_latency_experiment(self, workflow: str, iterations: int = 30) -> BenchmarkResult:
        print(f"\n{'='*60}")
        print(f"End-to-end Latency: {workflow} ({iterations} iterations)")
        print(f"{'='*60}")

        if not self.deploy_workflow(workflow):
            return BenchmarkResult(
                experiment="latency",
                num_nodes=len(self.get_mesh_members()),
                workflow=workflow,
                iterations=iterations,
                avg_latency_ms=0, p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                throughput_wf_per_sec=0, total_time_ms=0,
                success_rate=0, failures=iterations,
                timestamp=datetime.now().isoformat(),
            )

        e2e_latencies = []
        failures = 0

        for i in range(iterations):
            start = time.time()
            result = self.trigger_workflow(workflow, timeout=30)
            trigger_latency = (time.time() - start) * 1000

            if result and "error" not in result:
                run_id = result.get("id")
                if run_id:
                    complete = self.wait_for_completion(run_id, timeout=30)
                    e2e_latency = (time.time() - start) * 1000
                    if complete and complete.get("status") == "completed":
                        e2e_latencies.append(e2e_latency)
                    else:
                        failures += 1
                else:
                    e2e_latencies.append(trigger_latency)
            else:
                failures += 1

            if (i + 1) % 10 == 0:
                if e2e_latencies:
                    print(f"  [{i+1}/{iterations}] avg e2e={statistics.mean(e2e_latencies):.1f}ms")
                else:
                    print(f"  [{i+1}/{iterations}] running...")

        total_time = sum(e2e_latencies)

        if e2e_latencies:
            e2e_latencies.sort()
            avg = statistics.mean(e2e_latencies)
            p50 = e2e_latencies[len(e2e_latencies) // 2]
            p95_idx = int(len(e2e_latencies) * 0.95)
            p99_idx = int(len(e2e_latencies) * 0.99)
            p95 = e2e_latencies[min(p95_idx, len(e2e_latencies) - 1)]
            p99 = e2e_latencies[min(p99_idx, len(e2e_latencies) - 1)]
            throughput = len(e2e_latencies) / (total_time / 1000) if total_time > 0 else 0
        else:
            avg = p50 = p95 = p99 = throughput = 0

        result = BenchmarkResult(
            experiment="latency",
            num_nodes=len(self.get_mesh_members()),
            workflow=workflow,
            iterations=iterations,
            avg_latency_ms=avg,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p99,
            throughput_wf_per_sec=throughput,
            total_time_ms=total_time,
            success_rate=(iterations - failures) / iterations if iterations else 0,
            failures=failures,
            timestamp=datetime.now().isoformat(),
        )

        print(f"  E2E P50: {p50:.1f}ms, P95: {p95:.1f}ms, P99: {p99:.1f}ms")
        print(f"  E2E Avg: {avg:.1f}ms")

        self.results.append(result)
        return result

    def run_fault_tolerance(self, workflow: str, kill_node: str, iterations: int = 10) -> BenchmarkResult:
        print(f"\n{'='*60}")
        print(f"Fault Tolerance: {workflow} (kill {kill_node}, {iterations} trials)")
        print(f"{'='*60}")

        if not self.deploy_workflow(workflow):
            return BenchmarkResult(
                experiment="fault_tolerance",
                num_nodes=len(self.get_mesh_members()),
                workflow=workflow,
                iterations=iterations,
                avg_latency_ms=0, p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                throughput_wf_per_sec=0, total_time_ms=0,
                success_rate=0, failures=iterations,
                timestamp=datetime.now().isoformat(),
            )

        recovery_times = []
        successes = 0
        failures = 0

        for i in range(iterations):
            start = time.time()
            result = self.trigger_workflow(workflow, timeout=30)

            if result and "error" not in result:
                run_id = result.get("id")
                if run_id:
                    time.sleep(0.5)
                    print(f"  [{i+1}] Killing {kill_node}...")
                    try:
                        subprocess.run(
                            ["docker", "kill", kill_node],
                            capture_output=True, timeout=10,
                        )
                    except Exception as e:
                        print(f"  [WARN] Kill failed: {e}")

                    complete = self.wait_for_completion(run_id, timeout=60)
                    recovery_time = (time.time() - start) * 1000

                    if complete and complete.get("status") == "completed":
                        recovery_times.append(recovery_time)
                        successes += 1
                        print(f"  [{i+1}] Recovered in {recovery_time:.0f}ms")
                    else:
                        failures += 1
                        print(f"  [{i+1}] Failed to recover")

                    subprocess.run(
                        ["docker", "restart", kill_node],
                        capture_output=True, timeout=30,
                    )
                    time.sleep(3)
                else:
                    failures += 1
            else:
                failures += 1

        if recovery_times:
            recovery_times.sort()
            avg = statistics.mean(recovery_times)
            p50 = recovery_times[len(recovery_times) // 2]
            p95_idx = int(len(recovery_times) * 0.95)
            p95 = recovery_times[min(p95_idx, len(recovery_times) - 1)]
        else:
            avg = p50 = p95 = 0

        result = BenchmarkResult(
            experiment="fault_tolerance",
            num_nodes=len(self.get_mesh_members()) + 1,
            workflow=workflow,
            iterations=iterations,
            avg_latency_ms=avg,
            p50_latency_ms=p50,
            p95_latency_ms=p95,
            p99_latency_ms=p95,
            throughput_wf_per_sec=0,
            total_time_ms=sum(recovery_times) if recovery_times else 0,
            success_rate=successes / iterations if iterations else 0,
            failures=failures,
            timestamp=datetime.now().isoformat(),
            details={"killed_node": kill_node, "recovery_count": successes, "recovery_times_ms": recovery_times},
        )

        print(f"  Recovery success: {successes}/{iterations}")
        print(f"  Avg recovery: {avg:.0f}ms, P50: {p50:.0f}ms")

        self.results.append(result)
        return result

    def save_results(self, output_dir: str = "benchmarks/experiments"):
        path = Path(output_dir)
        path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_file = path / f"results_{timestamp}.json"

        data = {
            "metadata": {
                "timestamp": timestamp,
                "meshflow_version": "0.1.0",
            },
            "results": [asdict(r) for r in self.results],
        }

        with open(out_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

        print(f"\nSaved results to {out_file}")
        return out_file


def find_mesh_nodes() -> list[str]:
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=meshflow", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    )
    return [name for name in result.stdout.strip().split("\n") if name]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Meshflow benchmark suite")
    parser.add_argument("--output", default="benchmarks/experiments", help="Output directory")
    parser.add_argument("--port", type=int, default=8080, help="API port of node1")
    parser.add_argument("--iterations", type=int, default=50, help="Iterations per experiment")
    parser.add_argument("--skip-fault-tolerance", action="store_true", help="Skip fault tolerance tests")
    parser.add_argument("--skip-latency", action="store_true", help="Skip e2e latency tests")
    args = parser.parse_args()

    bench = MeshflowBenchmark(api_port=args.port, workflows_dir="benchmarks/workflows")

    if not bench.is_ready():
        print(f"ERROR: No meshflow node at {bench.base_url}")
        print("Start with: docker compose up -d")
        sys.exit(1)

    nodes = find_mesh_nodes()
    print(f"Connected to {len(nodes)} mesh nodes: {nodes}")

    workflows = ["linear-3", "fanout-10", "diamond"]

    for wf in workflows:
        bench.run_throughput_experiment(wf, iterations=args.iterations)

    if not args.skip_latency:
        for wf in workflows:
            bench.run_latency_experiment(wf, iterations=min(args.iterations, 30))

    if not args.skip_fault_tolerance and len(nodes) >= 3:
        bench.run_fault_tolerance("linear-3", nodes[1], iterations=min(args.iterations, 10))

    bench.save_results(args.output)

    print("\n" + "=" * 60)
    print("Benchmark Summary")
    print("=" * 60)
    for r in bench.results:
        status = "PASS" if r.success_rate > 0.9 else "FAIL"
        print(f"  [{status}] {r.experiment:20s} {r.workflow:15s} "
              f"avg={r.avg_latency_ms:8.1f}ms p95={r.p95_latency_ms:8.1f}ms "
              f"tp={r.throughput_wf_per_sec:.2f}wf/s")


if __name__ == "__main__":
    main()
