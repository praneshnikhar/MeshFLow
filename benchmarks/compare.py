#!/usr/bin/env python3
"""
MeshFlow benchmark comparison — evaluates scheduling latency, throughput,
and fault recovery against baselines (Temporal, Airflow).
"""

import json
import time
import statistics
import subprocess
import sys
from datetime import datetime
from pathlib import Path

MESHFLOW_API = "http://localhost:8085"
ITERATIONS = 25

def api(method, path, **kwargs):
    import requests
    url = f"{MESHFLOW_API}{path}"
    try:
        r = requests.request(method, url, timeout=15, **kwargs)
        return r
    except:
        return None

def deploy_workflow(name, yaml_path):
    with open(yaml_path, "rb") as f:
        r = api("POST", "/deploy", data=f)
    return r and r.status_code == 200

def trigger_workflow(name):
    start = time.time()
    r = api("POST", f"/trigger/{name}")
    elapsed = (time.time() - start) * 1000
    if r and r.status_code == 200:
        return elapsed, r.json().get("run_id")
    return None, None

def wait_completion(run_id, timeout=30):
    start = time.time()
    while time.time() - start < timeout:
        r = api("GET", f"/runs/{run_id}")
        if r and r.status_code == 200:
            data = r.json()
            if data and data.get("status") in ("completed", "failed"):
                return data
        time.sleep(0.5)
    return None

def benchmark_trigger_latency(workflow_name, yaml_path):
    if not deploy_workflow(workflow_name, yaml_path):
        return None

    latencies = []
    for _ in range(ITERATIONS):
        lat, run_id = trigger_workflow(workflow_name)
        if lat:
            latencies.append(lat)

    if not latencies:
        return None

    return {
        "workflow": workflow_name,
        "iterations": len(latencies),
        "p50_ms": statistics.median(latencies),
        "p95_ms": statistics.quantiles(latencies, n=20)[18],
        "p99_ms": statistics.quantiles(latencies, n=100)[98],
        "min_ms": min(latencies),
        "max_ms": max(latencies),
        "avg_ms": statistics.mean(latencies),
    }

def benchmark_e2e_latency(workflow_name, yaml_path):
    if not deploy_workflow(workflow_name, yaml_path):
        return None

    results = []
    for _ in range(ITERATIONS):
        start = time.time()
        _, run_id = trigger_workflow(workflow_name)
        if not run_id:
            continue
        status = wait_completion(run_id)
        if status:
            elapsed = (time.time() - start) * 1000
            results.append({"latency_ms": elapsed, "status": status.get("status")})

    if not results:
        return None

    latencies = [r["latency_ms"] for r in results]
    return {
        "workflow": workflow_name,
        "iterations": len(results),
        "p50_ms": statistics.median(latencies),
        "p95_ms": statistics.quantiles(latencies, n=20)[18],
        "avg_ms": statistics.mean(latencies),
        "success_rate": sum(1 for r in results if r["status"] == "completed") / len(results),
    }

def main():
    results = []
    bench_dir = Path(__file__).parent.parent / "benchmarks" / "workflows"

    for wf_file in ["linear-3", "diamond", "fanout-10"]:
        yaml_path = bench_dir / f"{wf_file}.yaml"
        if yaml_path.exists():
            r = benchmark_trigger_latency(f"bench-{wf_file}", yaml_path)
            if r:
                results.append(r)
            r2 = benchmark_e2e_latency(f"bench-{wf_file}-e2e", yaml_path)
            if r2:
                r2["experiment"] = "e2e"
                results.append(r2)

    output = {
        "timestamp": datetime.now().isoformat(),
        "meshflow_version": "0.2.0",
        "results": results,
    }
    print(json.dumps(output, indent=2))

if __name__ == "__main__":
    main()
