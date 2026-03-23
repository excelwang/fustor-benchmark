import json

from click.testing import CliRunner

from capanix_benchmark.cli import cli
from capanix_benchmark.scale_breakpoint import analyze_scale_breakpoint


def _result_payload(
    total_files,
    *,
    tree_p95,
    tree_qps,
    ondemand_p95,
    ondemand_qps,
    ondemand_success_rate=1.0,
    contention_attempted=100,
    contention_not_ready=0,
    integrity_stable_rate=1.0,
):
    return {
        "timestamp": "2026-03-23 00:00:00",
        "metadata": {
            "total_files_in_scope": total_files,
        },
        "concurrency": 20,
        "requests": 200,
        "os_baseline": {
            "p95": 100.0,
            "qps": 100.0,
        },
        "os_integrity": {
            "p95": 200.0,
            "qps": 50.0,
            "stable_rate": integrity_stable_rate,
        },
        "tree_materialized": {
            "p95": tree_p95,
            "qps": tree_qps,
        },
        "find_on_demand_success": {
            "p95": ondemand_p95,
            "qps": ondemand_qps,
            "success_rate": ondemand_success_rate,
        },
        "find_on_demand_contention": {
            "attempted_count": contention_attempted,
            "not_ready_count": contention_not_ready,
            "other_error_count": 0,
        },
    }


def _write_result(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def test_analyze_scale_breakpoint_detects_anchor_and_breakpoint(tmp_path):
    result_paths = [
        _write_result(
            tmp_path / "300m.json",
            _result_payload(300_000_000, tree_p95=105.0, tree_qps=95.0, ondemand_p95=210.0, ondemand_qps=46.0),
        ),
        _write_result(
            tmp_path / "500m.json",
            _result_payload(500_000_000, tree_p95=108.0, tree_qps=92.0, ondemand_p95=215.0, ondemand_qps=45.0),
        ),
        _write_result(
            tmp_path / "700m.json",
            _result_payload(700_000_000, tree_p95=130.0, tree_qps=78.0, ondemand_p95=218.0, ondemand_qps=45.0),
        ),
        _write_result(
            tmp_path / "1b.json",
            _result_payload(1_000_000_000, tree_p95=140.0, tree_qps=75.0, ondemand_p95=220.0, ondemand_qps=44.0),
        ),
    ]

    analysis = analyze_scale_breakpoint(
        result_paths,
        {
            "anchor_files": 500_000_000,
            "latency_regression_tolerance": 0.10,
            "qps_regression_tolerance": 0.10,
            "min_success_rate": 0.99,
            "max_contention_not_ready_rate": 0.05,
            "min_integrity_stable_rate": 0.95,
            "breakpoint_latency_ratio": 1.25,
            "breakpoint_qps_ratio": 0.80,
            "breakpoint_not_ready_rate": 0.15,
            "consecutive_points": 2,
        },
    )

    assert analysis["anchor_verdict"]["anchor_present"] is True
    assert analysis["anchor_verdict"]["anchor_scale_not_worse_than_baseline"] is True
    assert analysis["anchor_verdict"]["anchor_scale_conclusion_ready"] is True
    assert analysis["breakpoint_verdict"]["breakpoint_detected"] is True
    assert analysis["breakpoint_verdict"]["overall_breakpoint"]["metric"] == "discovery_p95_ratio"
    assert analysis["breakpoint_verdict"]["overall_breakpoint"]["first_violation_files"] == 700_000_000
    assert analysis["breakpoint_verdict"]["overall_breakpoint"]["confirmed_at_files"] == 1_000_000_000


def test_scale_breakpoint_cli_writes_outputs(tmp_path):
    result_a = _write_result(
        tmp_path / "500m.json",
        _result_payload(500_000_000, tree_p95=108.0, tree_qps=92.0, ondemand_p95=215.0, ondemand_qps=45.0),
    )
    result_b = _write_result(
        tmp_path / "1b.json",
        _result_payload(1_000_000_000, tree_p95=140.0, tree_qps=75.0, ondemand_p95=230.0, ondemand_qps=40.0),
    )
    output_json = tmp_path / "analysis.json"
    output_md = tmp_path / "analysis.md"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "scale-breakpoint",
            result_a,
            result_b,
            "--output-json",
            str(output_json),
            "--output-md",
            str(output_md),
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(output_json.read_text(encoding="utf-8"))
    assert payload["anchor_verdict"]["anchor_present"] is True
    assert payload["breakpoint_verdict"]["breakpoint_detected"] is False
    markdown = output_md.read_text(encoding="utf-8")
    assert "# Scale Breakpoint Evaluation" in markdown
    assert "500,000,000" in markdown
