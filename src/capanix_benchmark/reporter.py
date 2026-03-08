import json
import os
import statistics
from pathlib import Path


def calculate_stats(latencies, total_time, count):
    if not latencies:
        return {"qps": 0, "avg": 0, "min": 0, "max": 0, "stddev": 0, "p50": 0, "p95": 0, "p99": 0, "raw": []}

    l_ms = sorted([lat * 1000 for lat in latencies])
    qps = (count / total_time) if total_time > 0 else 0

    if len(l_ms) >= 2:
        quantiles = statistics.quantiles(l_ms, n=100)
        p95 = quantiles[94]
        p99 = quantiles[98]
        stddev = statistics.stdev(l_ms)
    else:
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
        "p95": p95,
        "p99": p99,
        "raw": l_ms,
    }


def generate_html_report(results, output_path):
    template_path = Path(__file__).parent / "query_template.html"
    with open(template_path, "r", encoding="utf-8") as file:
        template = file.read()

    integrity_avg = results["os_integrity"]["avg"]
    tree_avg = results["tree_materialized"]["avg"]
    ondemand_avg = results["find_on_demand"]["avg"]

    tree_gain = (integrity_avg / tree_avg) if tree_avg > 0 else 0
    ondemand_gain = (integrity_avg / ondemand_avg) if ondemand_avg > 0 else 0

    summary = {
        "timestamp": results["timestamp"],
        "total_files": f"{results['metadata']['total_files_in_scope']:,}",
        "total_dirs": f"{results['metadata']['total_directories_in_scope']:,}",
        "depth": str(results["depth"]),
        "reqs": str(results["requests"]),
        "concurrency": str(results["concurrency"]),
        "integrity_interval": str(results["metadata"].get("integrity_interval", 60.0)),
        "os_baseline_avg": f"{results['os_baseline']['avg']:.2f}",
        "os_integrity_avg": f"{results['os_integrity']['avg']:.2f}",
        "tree_avg": f"{results['tree_materialized']['avg']:.2f}",
        "ondemand_avg": f"{results['find_on_demand']['avg']:.2f}",
        "tree_gain": f"{tree_gain:.1f}x",
        "ondemand_gain": f"{ondemand_gain:.1f}x",
    }

    html = template
    for key, value in summary.items():
        html = html.replace(f"{{{{{key}}}}}", value)

    html = html.replace("/* RESULTS_JSON_DATA */", json.dumps(results))

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(html)
