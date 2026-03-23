import json
import os
import time
from pathlib import Path


PRIMARY_GATE_KEYS = (
    "discovery_p95_ok",
    "discovery_qps_ok",
    "integrity_p95_ok",
    "integrity_qps_ok",
    "ondemand_success_ok",
    "contention_not_ready_ok",
)


def _safe_number(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_ratio(numerator, denominator):
    num = _safe_number(numerator)
    den = _safe_number(denominator)
    if num is None or den in (None, 0):
        return None
    return num / den


def _safe_rate(numerator, denominator):
    num = _safe_number(numerator)
    den = _safe_number(denominator)
    if num is None or den in (None, 0):
        return None
    return num / den


def _bucket(payload, name):
    bucket = payload.get(name, {})
    return bucket if isinstance(bucket, dict) else {}


def _bool_gate(predicate_result):
    return bool(predicate_result)


def evaluate_scale_result(payload, result_path, thresholds):
    metadata = payload.get("metadata", {}) if isinstance(payload, dict) else {}
    total_files = int(metadata.get("total_files_in_scope", 0) or 0)

    os_baseline = _bucket(payload, "os_baseline")
    os_integrity = _bucket(payload, "os_integrity")
    tree_materialized = _bucket(payload, "tree_materialized")
    ondemand_success = _bucket(payload, "find_on_demand_success")
    ondemand_contention = _bucket(payload, "find_on_demand_contention")

    metrics = {
        "discovery_p95_ratio": _safe_ratio(
            tree_materialized.get("p95"), os_baseline.get("p95")
        ),
        "discovery_qps_ratio": _safe_ratio(
            tree_materialized.get("qps"), os_baseline.get("qps")
        ),
        "integrity_p95_ratio": _safe_ratio(
            ondemand_success.get("p95"), os_integrity.get("p95")
        ),
        "integrity_qps_ratio": _safe_ratio(
            ondemand_success.get("qps"), os_integrity.get("qps")
        ),
        "ondemand_success_rate": _safe_number(ondemand_success.get("success_rate")),
        "ondemand_contention_not_ready_rate": _safe_rate(
            ondemand_contention.get("not_ready_count"),
            ondemand_contention.get("attempted_count"),
        ),
        "os_integrity_stable_rate": _safe_number(os_integrity.get("stable_rate")),
    }

    gates = {
        "discovery_p95_ok": _bool_gate(
            metrics["discovery_p95_ratio"] is not None
            and metrics["discovery_p95_ratio"]
            <= (1.0 + thresholds["latency_regression_tolerance"])
        ),
        "discovery_qps_ok": _bool_gate(
            metrics["discovery_qps_ratio"] is not None
            and metrics["discovery_qps_ratio"]
            >= (1.0 - thresholds["qps_regression_tolerance"])
        ),
        "integrity_p95_ok": _bool_gate(
            metrics["integrity_p95_ratio"] is not None
            and metrics["integrity_p95_ratio"]
            <= (1.0 + thresholds["latency_regression_tolerance"])
        ),
        "integrity_qps_ok": _bool_gate(
            metrics["integrity_qps_ratio"] is not None
            and metrics["integrity_qps_ratio"]
            >= (1.0 - thresholds["qps_regression_tolerance"])
        ),
        "ondemand_success_ok": _bool_gate(
            metrics["ondemand_success_rate"] is not None
            and metrics["ondemand_success_rate"] >= thresholds["min_success_rate"]
        ),
        "contention_not_ready_ok": _bool_gate(
            metrics["ondemand_contention_not_ready_rate"] is not None
            and metrics["ondemand_contention_not_ready_rate"]
            <= thresholds["max_contention_not_ready_rate"]
        ),
        "environment_stable_ok": _bool_gate(
            metrics["os_integrity_stable_rate"] is not None
            and metrics["os_integrity_stable_rate"]
            >= thresholds["min_integrity_stable_rate"]
        ),
    }

    not_worse_than_baseline = all(gates[key] for key in PRIMARY_GATE_KEYS)
    conclusion_ready = not_worse_than_baseline and gates["environment_stable_ok"]

    return {
        "result_path": os.path.abspath(result_path),
        "timestamp": payload.get("timestamp"),
        "total_files": total_files,
        "concurrency": payload.get("concurrency"),
        "requests": payload.get("requests"),
        "metrics": metrics,
        "gates": gates,
        "not_worse_than_baseline": not_worse_than_baseline,
        "conclusion_ready": conclusion_ready,
    }


def _find_anchor_entry(scales, anchor_files):
    for scale in scales:
        if scale["total_files"] == anchor_files:
            return scale
    return None


def _find_consecutive_breakpoint(scales, metric_key, predicate, consecutive_points):
    streak = []
    for scale in scales:
        value = scale["metrics"].get(metric_key)
        if value is not None and predicate(value):
            streak.append(scale)
            if len(streak) >= consecutive_points:
                return {
                    "metric": metric_key,
                    "first_violation_files": streak[0]["total_files"],
                    "confirmed_at_files": scale["total_files"],
                    "first_violation_path": streak[0]["result_path"],
                    "confirmed_at_path": scale["result_path"],
                    "observed_values": [item["metrics"].get(metric_key) for item in streak],
                }
        else:
            streak = []
    return None


def analyze_scale_breakpoint(result_paths, thresholds):
    scales = []
    for result_path in result_paths:
        with open(result_path, "r", encoding="utf-8") as file:
            payload = json.load(file)
        scales.append(evaluate_scale_result(payload, result_path, thresholds))

    scales.sort(key=lambda item: item["total_files"])

    anchor_files = thresholds["anchor_files"]
    anchor_entry = _find_anchor_entry(scales, anchor_files)
    anchor_or_below = [scale for scale in scales if scale["total_files"] <= anchor_files]
    above_anchor = [scale for scale in scales if scale["total_files"] > anchor_files]

    metric_rules = {
        "discovery_p95_ratio": lambda value: value > thresholds["breakpoint_latency_ratio"],
        "discovery_qps_ratio": lambda value: value < thresholds["breakpoint_qps_ratio"],
        "integrity_p95_ratio": lambda value: value > thresholds["breakpoint_latency_ratio"],
        "integrity_qps_ratio": lambda value: value < thresholds["breakpoint_qps_ratio"],
        "ondemand_contention_not_ready_rate": lambda value: value
        > thresholds["breakpoint_not_ready_rate"],
    }

    breakpoint_hits = []
    for metric_key, predicate in metric_rules.items():
        hit = _find_consecutive_breakpoint(
            above_anchor,
            metric_key,
            predicate,
            thresholds["consecutive_points"],
        )
        if hit:
            breakpoint_hits.append(hit)

    breakpoint_hits.sort(
        key=lambda item: (item["first_violation_files"], item["confirmed_at_files"], item["metric"])
    )
    overall_breakpoint = breakpoint_hits[0] if breakpoint_hits else None

    return {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "thresholds": thresholds,
        "scale_count": len(scales),
        "scales": scales,
        "anchor_verdict": {
            "anchor_files": anchor_files,
            "anchor_present": anchor_entry is not None,
            "anchor_scale_not_worse_than_baseline": anchor_entry["not_worse_than_baseline"]
            if anchor_entry
            else False,
            "anchor_scale_conclusion_ready": anchor_entry["conclusion_ready"] if anchor_entry else False,
            "all_scales_up_to_anchor_not_worse_than_baseline": bool(anchor_or_below)
            and all(scale["not_worse_than_baseline"] for scale in anchor_or_below),
            "all_scales_up_to_anchor_conclusion_ready": bool(anchor_or_below)
            and all(scale["conclusion_ready"] for scale in anchor_or_below),
        },
        "breakpoint_verdict": {
            "consecutive_points_required": thresholds["consecutive_points"],
            "breakpoint_detected": overall_breakpoint is not None,
            "overall_breakpoint": overall_breakpoint,
            "metric_breakpoints": breakpoint_hits,
        },
    }


def _fmt_ratio(value):
    return "n/a" if value is None else f"{value:.2f}"


def _fmt_rate(value):
    return "n/a" if value is None else f"{value * 100:.1f}%"


def render_markdown_summary(analysis):
    thresholds = analysis["thresholds"]
    anchor = analysis["anchor_verdict"]
    breakpoint = analysis["breakpoint_verdict"]

    lines = [
        "# Scale Breakpoint Evaluation",
        "",
        f"- Generated at: `{analysis['timestamp']}`",
        f"- Anchor scale: `{thresholds['anchor_files']:,}` files",
        f"- Loaded scale points: `{analysis['scale_count']}`",
        "",
        "## Thresholds",
        "",
        f"- Not-worse latency tolerance: `+{thresholds['latency_regression_tolerance'] * 100:.0f}%`",
        f"- Not-worse QPS tolerance: `-{thresholds['qps_regression_tolerance'] * 100:.0f}%`",
        f"- Minimum on-demand success rate: `{thresholds['min_success_rate'] * 100:.1f}%`",
        f"- Maximum contention NOT_READY rate: `{thresholds['max_contention_not_ready_rate'] * 100:.1f}%`",
        f"- Minimum OS integrity stable rate: `{thresholds['min_integrity_stable_rate'] * 100:.1f}%`",
        f"- Breakpoint latency ratio: `>{thresholds['breakpoint_latency_ratio']:.2f}`",
        f"- Breakpoint QPS ratio: `<{thresholds['breakpoint_qps_ratio']:.2f}`",
        f"- Breakpoint NOT_READY rate: `>{thresholds['breakpoint_not_ready_rate'] * 100:.1f}%`",
        f"- Consecutive scale points required: `{thresholds['consecutive_points']}`",
        "",
        "## Scale Table",
        "",
        "| Files | Discovery P95 Ratio | Discovery QPS Ratio | Integrity P95 Ratio | Integrity QPS Ratio | Success Rate | Contention NOT_READY | Stable Rate | Verdict |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]

    for scale in analysis["scales"]:
        verdict = "pass" if scale["conclusion_ready"] else "fail"
        metrics = scale["metrics"]
        lines.append(
            "| "
            + f"{scale['total_files']:,}"
            + " | "
            + _fmt_ratio(metrics["discovery_p95_ratio"])
            + " | "
            + _fmt_ratio(metrics["discovery_qps_ratio"])
            + " | "
            + _fmt_ratio(metrics["integrity_p95_ratio"])
            + " | "
            + _fmt_ratio(metrics["integrity_qps_ratio"])
            + " | "
            + _fmt_rate(metrics["ondemand_success_rate"])
            + " | "
            + _fmt_rate(metrics["ondemand_contention_not_ready_rate"])
            + " | "
            + _fmt_rate(metrics["os_integrity_stable_rate"])
            + " | "
            + verdict
            + " |"
        )

    lines.extend(
        [
            "",
            "## Verdict",
            "",
            f"- Anchor scale present: `{anchor['anchor_present']}`",
            f"- Anchor scale not worse than baseline: `{anchor['anchor_scale_not_worse_than_baseline']}`",
            f"- Anchor scale conclusion ready: `{anchor['anchor_scale_conclusion_ready']}`",
            f"- All scales up to anchor not worse than baseline: `{anchor['all_scales_up_to_anchor_not_worse_than_baseline']}`",
            f"- All scales up to anchor conclusion ready: `{anchor['all_scales_up_to_anchor_conclusion_ready']}`",
        ]
    )

    if breakpoint["breakpoint_detected"]:
        overall = breakpoint["overall_breakpoint"]
        lines.append(
            f"- Breakpoint detected after anchor: `true`, metric=`{overall['metric']}`, "
            f"first_violation=`{overall['first_violation_files']:,}`, confirmed_at=`{overall['confirmed_at_files']:,}`"
        )
    else:
        lines.append("- Breakpoint detected after anchor: `false`")

    lines.extend(
        [
            "",
            "## Metric Breakpoints",
            "",
        ]
    )

    if breakpoint["metric_breakpoints"]:
        for item in breakpoint["metric_breakpoints"]:
            lines.append(
                f"- `{item['metric']}` first violated at `{item['first_violation_files']:,}` files and was confirmed by "
                f"`{item['confirmed_at_files']:,}` files."
            )
    else:
        lines.append("- No metric crossed the configured breakpoint rule.")

    return "\n".join(lines) + "\n"


def write_analysis_outputs(analysis, output_json=None, output_md=None):
    markdown = render_markdown_summary(analysis)

    if output_json:
        output_json = os.path.abspath(output_json)
        parent = os.path.dirname(output_json)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(output_json, "w", encoding="utf-8") as file:
            json.dump(analysis, file, indent=2)

    if output_md:
        output_md = os.path.abspath(output_md)
        parent = os.path.dirname(output_md)
        if parent:
            os.makedirs(parent, exist_ok=True)
        Path(output_md).write_text(markdown, encoding="utf-8")

    return markdown
