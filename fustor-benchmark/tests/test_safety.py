import os
from unittest.mock import patch

import pytest

from capanix_benchmark.generator import DataGenerator
from capanix_benchmark.runner import BenchmarkRunner


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
