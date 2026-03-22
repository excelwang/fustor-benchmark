import json
import os
import statistics
from pathlib import Path


def calculate_stats(latencies, total_time, count):
    if not latencies:
        return {
            "qps": 0,
            "avg": 0,
            "min": 0,
            "max": 0,
            "stddev": 0,
            "p50": 0,
            "p75": 0,
            "p90": 0,
            "p95": 0,
            "p99": 0,
            "raw": [],
        }

    l_ms = sorted([lat * 1000 for lat in latencies])
    qps = (count / total_time) if total_time > 0 else 0

    if len(l_ms) >= 2:
        quantiles = statistics.quantiles(l_ms, n=100)
        p75 = quantiles[74]
        p90 = quantiles[89]
        p95 = quantiles[94]
        p99 = quantiles[98]
        stddev = statistics.stdev(l_ms)
    else:
        p75 = l_ms[0]
        p90 = l_ms[0]
        p95 = l_ms[0]
        p99 = l_ms[0]
        stddev = 0

    return {
        "qps": qps,
        "avg": statistics.mean(l_ms),
        "min": min(l_ms),
        "max": max(l_ms),
        "stddev": stddev,
        "p50": statistics.median(l_ms),
        "p75": p75,
        "p90": p90,
        "p95": p95,
        "p99": p99,
        "raw": l_ms,
    }


def calculate_outcome_stats(latencies, total_time, attempted_count, not_ready_count, other_error_count):
    success_count = len(latencies)
    stats = calculate_stats(latencies, total_time, success_count)
    stats["attempted_count"] = attempted_count
    stats["success_count"] = success_count
    stats["not_ready_count"] = not_ready_count
    stats["other_error_count"] = other_error_count
    stats["success_rate"] = (success_count / attempted_count) if attempted_count > 0 else 0
    return stats


def generate_html_report(results, output_path):
    template_path = Path(__file__).parent / "query_template.html"
    with open(template_path, "r", encoding="utf-8") as file:
        template = file.read()

    integrity_avg = results["os_integrity"]["avg"]
    tree_avg = results["tree_materialized"]["avg"]
    ondemand_avg = results["find_on_demand_success"]["avg"]

    integrity_qps = results["os_integrity"]["qps"]
    tree_qps = results["tree_materialized"]["qps"]
    ondemand_qps = results["find_on_demand_success"]["qps"]

    tree_gain_latency = (integrity_avg / tree_avg) if tree_avg > 0 else 0
    tree_gain_qps = (tree_qps / integrity_qps) if integrity_qps > 0 else 0
    ondemand_gain_latency = (integrity_avg / ondemand_avg) if ondemand_avg > 0 else 0
    ondemand_gain_qps = (ondemand_qps / integrity_qps) if integrity_qps > 0 else 0

    summary = {
        "timestamp": results["timestamp"],
        "total_files": f"{results['metadata']['total_files_in_scope']:,}",
        "total_dirs": f"{results['metadata']['total_directories_in_scope']:,}",
        "depth": str(results["depth"]),
        "reqs": str(results["requests"]),
        "concurrency": str(results["concurrency"]),
        "integrity_interval": str(results["metadata"].get("integrity_interval", 60.0)),
        "os_avg": f"{results['os_baseline']['avg']:.2f}",
        "os_integrity_avg": f"{results['os_integrity']['avg']:.2f}",
        "tree_avg": f"{results['tree_materialized']['avg']:.2f}",
        "ondemand_avg": f"{results['find_on_demand_success']['avg']:.2f}",
        "gain_latency": f"{tree_gain_latency:.1f}x",
        "ondemand_gain_latency": f"{ondemand_gain_latency:.1f}x",
        "os_qps": f"{results['os_baseline']['qps']:.1f}",
        "os_integrity_qps": f"{results['os_integrity']['qps']:.1f}",
        "tree_qps": f"{results['tree_materialized']['qps']:.1f}",
        "ondemand_qps": f"{results['find_on_demand_success']['qps']:.1f}",
        "gain_qps": f"{tree_gain_qps:.1f}x",
        "ondemand_gain_qps": f"{ondemand_gain_qps:.1f}x",
        "ondemand_success_count": str(results["find_on_demand_success"]["success_count"]),
        "ondemand_execution_mode": str(results["find_on_demand_success"].get("execution_mode", "")),
        "ondemand_effective_concurrency": str(
            results["find_on_demand_success"].get("effective_concurrency", "")
        ),
        "ondemand_targeted_group_count": str(
            results["find_on_demand_success"].get("targeted_group_count", "")
        ),
        "ondemand_qps_semantics": str(results["find_on_demand_success"].get("qps_semantics", "")),
        "ondemand_contention_success_count": str(results["find_on_demand_contention"]["success_count"]),
        "ondemand_contention_not_ready_count": str(results["find_on_demand_contention"]["not_ready_count"]),
        "ondemand_contention_error_count": str(results["find_on_demand_contention"]["other_error_count"]),
    }

    html = template
    for key, value in summary.items():
        html = html.replace(f"{{{{{key}}}}}", value)

    html = html.replace("/* RESULTS_JSON_DATA */", json.dumps(results))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(html)
