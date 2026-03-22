import json
import os
from unittest.mock import patch

import pytest

from capanix_benchmark.generator import DataGenerator
from capanix_benchmark.reporter import calculate_outcome_stats
from capanix_benchmark import runner as runner_module
from capanix_benchmark.runner import BenchmarkRunner, BenchmarkTarget, FsMetaClient
from capanix_benchmark.tasks import (
    build_fs_meta_request_params,
    classify_fs_meta_error,
    run_find_sampling_phase,
    run_multi_nfs_submission_baseline_task,
    run_multi_nfs_submission_sampling_phase,
    run_multi_nfs_submission_validation_phase,
    run_single_fs_meta_req,
)


def test_generator_rejects_unsafe_path(tmp_path):
    unsafe_dir = tmp_path / "prod-data"
    unsafe_dir.mkdir(parents=True)
    (unsafe_dir / "keep.txt").write_text("keep", encoding="utf-8")

    generator = DataGenerator(str(unsafe_dir / "data"))

    with patch("click.echo") as echo_mock:
        generator.generate(num_dirs=1, num_subdirs=1, files_per_subdir=1)

    assert any("FATAL: Operation denied" in call.args[0] for call in echo_mock.call_args_list)
    assert (unsafe_dir / "keep.txt").exists()


def test_generator_allows_safe_path_and_blocks_non_empty(tmp_path):
    safe_root = tmp_path / "example-capanix-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"

    generator = DataGenerator(str(data_dir))
    generator.generate(num_dirs=1, num_subdirs=1, files_per_subdir=1)

    assert data_dir.exists()
    assert any(data_dir.iterdir())

    with patch("click.echo") as echo_mock:
        generator.generate(num_dirs=1, num_subdirs=1, files_per_subdir=1)

    assert any("is NOT empty" in call.args[0] for call in echo_mock.call_args_list)


def test_generator_allows_named_roots_with_nested_safe_target(tmp_path):
    safe_root = tmp_path / "example-capanix-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"

    generator = DataGenerator(str(data_dir))
    generator.generate(
        num_dirs=1,
        num_subdirs=1,
        files_per_subdir=1,
        root_layout="named-roots",
        root_ids=["nfs1", "nfs2"],
    )

    assert (data_dir / "nfs1").exists()
    assert (data_dir / "nfs2").exists()


def test_generator_named_roots_keeps_total_dir_count_semantics(tmp_path):
    safe_root = tmp_path / "example-capanix-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"

    generator = DataGenerator(str(data_dir))
    generator.generate(
        num_dirs=5,
        num_subdirs=1,
        files_per_subdir=1,
        root_layout="named-roots",
        root_ids=["nfs1", "nfs2"],
    )

    nfs1_dirs = list((data_dir / "nfs1" / "upload" / "submit").glob("*/*/*"))
    nfs2_dirs = list((data_dir / "nfs2" / "upload" / "submit").glob("*/*/*"))

    assert len(nfs1_dirs) + len(nfs2_dirs) == 5
    assert abs(len(nfs1_dirs) - len(nfs2_dirs)) <= 1


def test_generator_named_roots_creates_empty_root_dirs_when_count_is_zero(tmp_path):
    safe_root = tmp_path / "capanix-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"

    generator = DataGenerator(str(data_dir))
    generator.generate(
        num_dirs=2,
        num_subdirs=1,
        files_per_subdir=1,
        root_layout="named-roots",
        root_ids=["nfs1", "nfs2", "nfs3"],
    )

    assert (data_dir / "nfs1").is_dir()
    assert (data_dir / "nfs2").is_dir()
    assert (data_dir / "nfs3").is_dir()


def test_run_find_sampling_phase_accepts_absolute_target_path(tmp_path):
    data_dir = tmp_path / "data"
    file_dir = data_dir / "upload" / "submit" / "a" / "b" / "submission-1" / "sub_0"
    file_dir.mkdir(parents=True)
    file_path = file_dir / "data_0000.dat"
    file_path.write_text("payload", encoding="utf-8")

    _latency, metadata = run_find_sampling_phase((str(data_dir), str(file_dir.parent)))

    assert str(file_path) in metadata


def test_multi_nfs_submission_baseline_task_scans_all_roots(tmp_path):
    root1 = tmp_path / "nfs1"
    root2 = tmp_path / "nfs2"
    (root1 / "upload" / "submit").mkdir(parents=True)
    (root2 / "upload" / "submit").mkdir(parents=True)

    submission_id = "submission-123"
    submission_dir = root1 / "upload" / "submit" / "s" / "1" / submission_id
    nested_dir = submission_dir / "sub_0"
    nested_dir.mkdir(parents=True)
    (nested_dir / "data_0000.dat").write_text("x", encoding="utf-8")

    metrics = run_multi_nfs_submission_baseline_task(
        {"submission_id": submission_id, "root_dirs": [str(root1), str(root2)]}
    )

    assert metrics["roots_scanned"] == 2
    assert metrics["roots_with_search_path"] == 2
    assert metrics["discovery_find_calls"] == 2
    assert metrics["candidate_count"] == 1
    assert metrics["metadata_find_calls"] == 1
    assert metrics["file_count"] == 1
    assert metrics["dir_count"] == 2
    assert metrics["metadata_lines_parsed"] == 3


def test_multi_nfs_submission_validation_phase_detects_unstable_snapshot(tmp_path):
    root1 = tmp_path / "nfs1"
    root2 = tmp_path / "nfs2"
    (root1 / "upload" / "submit").mkdir(parents=True)
    (root2 / "upload" / "submit").mkdir(parents=True)

    submission_id = "submission-456"
    submission_dir = root1 / "upload" / "submit" / "s" / "4" / submission_id
    nested_dir = submission_dir / "sub_0"
    nested_dir.mkdir(parents=True)
    file_path = nested_dir / "data_0000.dat"
    file_path.write_text("x", encoding="utf-8")

    spec = {"submission_id": submission_id, "root_dirs": [str(root1), str(root2)]}
    _latency, snapshot, _metrics = run_multi_nfs_submission_sampling_phase(spec)

    file_path.write_text("xx", encoding="utf-8")

    validation = run_multi_nfs_submission_validation_phase((spec, snapshot))

    assert validation["stable"] is False


def test_local_mode_requires_start_cmd(tmp_path):
    safe_root = tmp_path / "work-capanix-benchmark-run"
    safe_root.mkdir()
    data_dir = safe_root / "data"
    data_dir.mkdir()
    (data_dir / "dummy.txt").write_text("x", encoding="utf-8")

    runner = BenchmarkRunner(
        run_dir=str(safe_root),
        target_dir=str(data_dir),
        base_url="http://127.0.0.1:18102",
        token="token",
        mode="local",
        start_cmd=None,
    )

    with pytest.raises(RuntimeError, match="requires --start-cmd"):
        runner.run(concurrency=1, reqs=1, target_depth=0, integrity_interval=0.0)


def test_extract_scope_counts_from_group_envelopes(tmp_path):
    runner = BenchmarkRunner(
        run_dir=str(tmp_path),
        target_dir=str(tmp_path),
        base_url="http://127.0.0.1:18102",
        token="token",
    )

    payload = {
        "groups": {
            "g1": {"status": "ok", "data": {"total_files": 2, "total_dirs": 3}},
            "g2": {"status": "error", "message": "not ready"},
            "g3": {"status": "ok", "data": {"total_files": 5, "total_dirs": 7}},
        }
    }

    assert runner._extract_scope_counts(payload) == (7, 10)


def test_to_api_path_uses_root_relative_query_paths(tmp_path):
    data_dir = tmp_path / "data"
    nested = data_dir / "a" / "b"
    nested.mkdir(parents=True)

    runner = BenchmarkRunner(
        run_dir=str(tmp_path),
        target_dir=str(data_dir),
        base_url="http://127.0.0.1:18102",
        token="token",
    )

    assert runner._to_api_path(str(data_dir)) == "/"
    assert runner._to_api_path(str(nested)) == "/a/b"


def test_to_api_path_named_roots_strips_root_id_prefix(tmp_path):
    data_dir = tmp_path / "data"
    nested = data_dir / "nfs1" / "upload" / "submit"
    nested.mkdir(parents=True)

    runner = BenchmarkRunner(
        run_dir=str(tmp_path),
        target_dir=str(data_dir),
        base_url="http://127.0.0.1:18102",
        token="token",
        root_layout="named-roots",
        root_ids=["nfs1", "nfs2", "nfs3"],
    )

    assert runner._to_api_path(str(nested), str(data_dir / "nfs1")) == "/upload/submit"


def test_build_fs_meta_request_params_uses_current_contract_axes():
    params = build_fs_meta_request_params(
        endpoint="tree",
        path="/bench/data",
        recursive=True,
        group_order="file-age",
        group_page_size=1,
        entry_page_size=256,
        pit_id="pit-1",
        entry_after="cursor-1",
    )

    assert params == {
        "path": "/bench/data",
        "recursive": "true",
        "group_order": "file-age",
        "group_page_size": "1",
        "entry_page_size": "256",
        "stability_mode": "none",
        "metadata_mode": "full",
        "pit_id": "pit-1",
        "entry_after": "cursor-1",
    }
    assert "limit" not in params
    assert "best" not in params
    assert "best_strategy" not in params


def test_status_diagnostic_uses_materialized_readiness_signals():
    client = FsMetaClient(
        base_url="http://127.0.0.1:18102",
        query_api_key="query-key",
        management_token="mgmt-token",
    )

    diagnostic = client._status_diagnostic(
        {
            "source": {"degraded_roots": [{"root_id": "root-a"}]},
            "sink": {
                "groups": [
                    {
                        "group_id": "group-a",
                        "initial_audit_completed": False,
                        "overflow_pending_audit": True,
                    }
                ]
            },
            "facade": {"pending": {"reason": "awaiting-runtime-exposure"}},
        }
    )

    assert diagnostic == (
        "degraded_roots=root-a; "
        "pending_initial_audit=group-a; "
        "overflow_pending_audit=group-a; "
        "facade.pending=awaiting-runtime-exposure"
    )


def test_calculate_outcome_stats_reports_contention_counts():
    stats = calculate_outcome_stats(
        [0.010, 0.020],
        total_time=2.0,
        attempted_count=5,
        not_ready_count=2,
        other_error_count=1,
    )

    assert stats["success_count"] == 2
    assert stats["attempted_count"] == 5
    assert stats["not_ready_count"] == 2
    assert stats["other_error_count"] == 1
    assert stats["success_rate"] == pytest.approx(0.4)


def test_force_find_success_runs_per_group_parallel_without_harness_cooldown(tmp_path, monkeypatch):
    runner = BenchmarkRunner(
        run_dir=str(tmp_path),
        target_dir=str(tmp_path),
        base_url="http://127.0.0.1:18102",
        token="token",
        root_layout="named-roots",
        root_ids=["nfs1", "nfs2"],
    )

    now = {"value": 0.0}

    def fake_time():
        return now["value"]

    def fake_run_single(*args, **kwargs):
        now["value"] += 0.1
        return {"outcome": "ok", "latency_seconds": 0.1}

    monkeypatch.setattr(runner_module.time, "time", fake_time)
    monkeypatch.setattr(runner_module, "run_single_fs_meta_req", fake_run_single)

    stats = runner.run_force_find_success(
        [
            BenchmarkTarget(local_path="/tmp/a", api_path="/a", group_id="nfs1"),
            BenchmarkTarget(local_path="/tmp/b", api_path="/b", group_id="nfs1"),
            BenchmarkTarget(local_path="/tmp/c", api_path="/c", group_id="nfs2"),
            BenchmarkTarget(local_path="/tmp/d", api_path="/d", group_id="nfs2"),
        ],
        concurrency=8,
        requests_count=4,
        recursive=True,
    )

    assert stats["avg"] == pytest.approx(100.0)
    assert stats["qps"] == pytest.approx(10.0)
    assert stats["execution_mode"] == "per_group_parallel"
    assert stats["requested_concurrency"] == 8
    assert stats["effective_concurrency"] == 2
    assert stats["targeted_group_count"] == 2
    assert stats["qps_semantics"] == "wall_clock_success_path"


def test_classify_fs_meta_error_marks_not_ready_conflicts():
    response = type(
        "Response",
        (),
        {
            "status_code": 429,
            "text": "",
            "json": staticmethod(
                lambda: {
                    "code": "FORCE_FIND_INFLIGHT_CONFLICT",
                    "message": "force-find inflight conflict: force-find already running for group: nfs1",
                }
            ),
        },
    )

    assert classify_fs_meta_error(response) == "not_ready"


def test_classify_fs_meta_error_marks_pit_capacity_exceeded_not_ready():
    response = type(
        "Response",
        (),
        {
            "status_code": 503,
            "text": "",
            "json": staticmethod(
                lambda: {
                    "code": "PIT_CAPACITY_EXCEEDED",
                    "error": "not ready: pit capacity exceeded; retry after existing queries expire",
                }
            ),
        },
    )

    assert classify_fs_meta_error(response) == "not_ready"


def test_run_single_fs_meta_req_consumes_group_and_entry_pagination(monkeypatch):
    requests_seen = []
    responses = [
        {
            "pit": {"id": "pit-1"},
            "group_page": {
                "next_cursor": "group-cursor-1",
                "next_entry_after": "entry-cursor-1",
            },
        },
        {
            "pit": {"id": "pit-1"},
            "group_page": {
                "next_cursor": "group-cursor-1",
                "next_entry_after": "entry-cursor-2",
            },
        },
        {
            "pit": {"id": "pit-1"},
            "group_page": {
                "next_cursor": "group-cursor-2",
                "next_entry_after": None,
            },
        },
        {
            "pit": {"id": "pit-1"},
            "group_page": {
                "next_cursor": None,
                "next_entry_after": None,
            },
        },
    ]

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, params, headers, timeout):
        requests_seen.append(params.copy())
        return FakeResponse(responses[len(requests_seen) - 1])

    monkeypatch.setattr("capanix_benchmark.tasks.requests.get", fake_get)

    result = run_single_fs_meta_req(
        "http://127.0.0.1:18102",
        {"Authorization": "Bearer query-key"},
        "tree",
        "/upload/submit/demo",
        True,
        "group-key",
        1,
        1000,
    )

    assert result["outcome"] == "ok"
    assert len(requests_seen) == 4
    assert requests_seen[0]["path"] == "/upload/submit/demo"
    assert "pit_id" not in requests_seen[0]
    assert "group_after" not in requests_seen[0]
    assert "entry_after" not in requests_seen[0]
    assert requests_seen[1]["pit_id"] == "pit-1"
    assert "group_after" not in requests_seen[1]
    assert requests_seen[1]["entry_after"] == "entry-cursor-1"
    assert requests_seen[2]["pit_id"] == "pit-1"
    assert "group_after" not in requests_seen[2]
    assert requests_seen[2]["entry_after"] == "entry-cursor-2"
    assert requests_seen[3]["pit_id"] == "pit-1"
    assert requests_seen[3]["group_after"] == "group-cursor-1"
    assert "entry_after" not in requests_seen[3]


def test_run_single_fs_meta_req_keeps_current_group_cursor_while_draining_last_group_entries(monkeypatch):
    requests_seen = []
    responses = [
        {
            "pit": {"id": "pit-1"},
            "groups": [{"group": "nfs1"}],
            "group_page": {
                "next_cursor": "group-cursor-1",
                "next_entry_after": None,
            },
        },
        {
            "pit": {"id": "pit-1"},
            "groups": [{"group": "nfs2"}],
            "group_page": {
                "next_cursor": "group-cursor-2",
                "next_entry_after": None,
            },
        },
        {
            "pit": {"id": "pit-1"},
            "groups": [{"group": "nfs3"}],
            "group_page": {
                "next_cursor": None,
                "next_entry_after": "entry-cursor-last-group",
            },
        },
        {
            "pit": {"id": "pit-1"},
            "groups": [{"group": "nfs3"}],
            "group_page": {
                "next_cursor": None,
                "next_entry_after": None,
            },
        },
    ]

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def fake_get(url, params, headers, timeout):
        requests_seen.append(params.copy())
        return FakeResponse(responses[len(requests_seen) - 1])

    monkeypatch.setattr("capanix_benchmark.tasks.requests.get", fake_get)

    result = run_single_fs_meta_req(
        "http://127.0.0.1:18102",
        {"Authorization": "Bearer query-key"},
        "tree",
        "/upload/submit/demo",
        True,
        "group-key",
        1,
        1000,
    )

    assert result["outcome"] == "ok"
    assert len(requests_seen) == 4
    assert "group_after" not in requests_seen[0]
    assert requests_seen[1]["group_after"] == "group-cursor-1"
    assert requests_seen[2]["group_after"] == "group-cursor-2"
    assert requests_seen[3]["group_after"] == "group-cursor-2"
    assert requests_seen[3]["entry_after"] == "entry-cursor-last-group"


def test_run_single_fs_meta_req_restarts_tree_after_pit_scope_mismatch(monkeypatch):
    requests_seen = []
    responses = [
        {
            "status_code": 200,
            "payload": {
                "pit": {"id": "pit-1"},
                "group_page": {
                    "next_cursor": "group-cursor-1",
                    "next_entry_after": None,
                },
            },
        },
        {
            "status_code": 400,
            "payload": {
                "code": "INVALID_INPUT",
                "error": "invalid input: pit_id does not match the requested tree scope",
            },
        },
        {
            "status_code": 200,
            "payload": {
                "pit": {"id": "pit-2"},
                "group_page": {
                    "next_cursor": None,
                    "next_entry_after": None,
                },
            },
        },
    ]

    class FakeResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload
            self.text = json.dumps(payload)

        def json(self):
            return self._payload

    def fake_get(url, params, headers, timeout):
        requests_seen.append(params.copy())
        response = responses[len(requests_seen) - 1]
        return FakeResponse(response["status_code"], response["payload"])

    monkeypatch.setattr("capanix_benchmark.tasks.requests.get", fake_get)

    result = run_single_fs_meta_req(
        "http://127.0.0.1:18102",
        {"Authorization": "Bearer query-key"},
        "tree",
        "/upload/submit/demo",
        True,
        "group-key",
        1,
        1000,
    )

    assert result["outcome"] == "ok"
    assert len(requests_seen) == 3
    assert "pit_id" not in requests_seen[0]
    assert requests_seen[1]["pit_id"] == "pit-1"
    assert requests_seen[1]["group_after"] == "group-cursor-1"
    assert "pit_id" not in requests_seen[2]
