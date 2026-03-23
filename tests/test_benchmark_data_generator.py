import pytest

from capanix_benchmark.benchmark_data_generator import (
    HostShard,
    WorkerConfig,
    build_ssh_command,
    build_scp_command,
    build_worker_command,
    chunk_id,
    deterministic_uuid_for_index,
    parse_hosts,
    plan_host_shards,
    run_worker,
    submission_count_for_files,
    submission_dir_for_index,
)


def test_parse_hosts_trims_and_filters_empty_entries():
    assert parse_hosts("10.0.0.1, 10.0.0.2 ,,") == ["10.0.0.1", "10.0.0.2"]


def test_submission_count_for_files_requires_exact_division():
    assert submission_count_for_files(5000, 4, 250) == 5
    with pytest.raises(ValueError, match="must be divisible"):
        submission_count_for_files(5001, 4, 250)


def test_plan_host_shards_evenly_distributes_remainder():
    shards = plan_host_shards(["h1", "h2", "h3"], total_submissions=10)
    assert shards == [
        HostShard(host="h1", start_submission=0, end_submission=4),
        HostShard(host="h2", start_submission=4, end_submission=7),
        HostShard(host="h3", start_submission=7, end_submission=10),
    ]


def test_plan_host_shards_supports_start_offset():
    shards = plan_host_shards(["h1", "h2"], total_submissions=5, start_submission_offset=10)
    assert shards == [
        HostShard(host="h1", start_submission=10, end_submission=13),
        HostShard(host="h2", start_submission=13, end_submission=15),
    ]


def test_deterministic_uuid_for_index_is_stable_and_distributed():
    first = deterministic_uuid_for_index(0)
    second = deterministic_uuid_for_index(0)
    assert first == second
    assert deterministic_uuid_for_index(1) != first
    prefixes = {deterministic_uuid_for_index(index)[:2] for index in range(64)}
    assert len(prefixes) > 8


def test_build_remote_commands_include_resume_and_paths(tmp_path):
    config = WorkerConfig(
        run_id="run-1",
        host="10.0.82.144",
        remote_base_dir="/data/fustor-nfs",
        remote_state_dir="/var/tmp/benchmark-data-generator/run-1",
        start_submission=0,
        end_submission=1000,
        num_subdirs=4,
        files_per_subdir=250,
        file_size_bytes=1024,
        chunk_size_submissions=1000,
        workers=16,
        uuid_namespace_seed="seed",
        append=True,
        resume=True,
    )

    worker_command = build_worker_command(config, "/var/tmp/benchmark-data-generator/run-1/worker.py")
    assert worker_command[:4] == [
        "python3",
        "/var/tmp/benchmark-data-generator/run-1/worker.py",
        "worker-run",
        "--run-id",
    ]
    assert "--append" in worker_command
    assert "--resume" in worker_command

    scp_command = build_scp_command(tmp_path / "worker.py", "10.0.82.144", "/var/tmp/worker.py", ssh_user="bench")
    assert scp_command == [
        "scp",
        str(tmp_path / "worker.py"),
        "bench@10.0.82.144:/var/tmp/worker.py",
    ]

    ssh_command = build_ssh_command(
        "10.0.82.144",
        ["python3", "-c", "print('a;b')", "/tmp/data path"],
        ssh_user="bench",
    )
    assert ssh_command[0:2] == ["ssh", "bench@10.0.82.144"]
    assert ssh_command[2] == "python3 -c 'print('\"'\"'a;b'\"'\"')' '/tmp/data path'"


def test_worker_run_generates_expected_tree(tmp_path):
    base_dir = tmp_path / "data"
    state_dir = tmp_path / "state"
    config = WorkerConfig(
        run_id="run-1",
        host="host-1",
        remote_base_dir=str(base_dir),
        remote_state_dir=str(state_dir),
        start_submission=0,
        end_submission=2,
        num_subdirs=2,
        files_per_subdir=3,
        file_size_bytes=8,
        chunk_size_submissions=1,
        workers=2,
        uuid_namespace_seed="seed",
        append=False,
        resume=False,
    )

    manifest = run_worker(config)

    assert manifest["state"] == "completed"
    assert manifest["created_files"] == 12
    assert manifest["completed_chunk_count"] == 2

    first_submission_dir = submission_dir_for_index(base_dir, 0, "seed")
    files = sorted(first_submission_dir.glob("sub_*/*"))
    assert len(files) == 6
    assert all(path.stat().st_size == 8 for path in files)
    assert (state_dir / "chunks" / f"{chunk_id(0, 1)}.json").exists()
    assert (state_dir / "chunks" / f"{chunk_id(1, 2)}.json").exists()


def test_worker_run_resume_skips_completed_chunks(tmp_path):
    base_dir = tmp_path / "data"
    state_dir = tmp_path / "state"
    initial_config = WorkerConfig(
        run_id="run-1",
        host="host-1",
        remote_base_dir=str(base_dir),
        remote_state_dir=str(state_dir),
        start_submission=0,
        end_submission=2,
        num_subdirs=1,
        files_per_subdir=2,
        file_size_bytes=4,
        chunk_size_submissions=1,
        workers=1,
        uuid_namespace_seed="seed",
        append=False,
        resume=False,
    )
    run_worker(initial_config)

    tracked_file = submission_dir_for_index(base_dir, 0, "seed") / "sub_0" / "data_0000.dat"
    first_mtime = tracked_file.stat().st_mtime_ns

    resume_config = WorkerConfig(**{**initial_config.__dict__, "resume": True})
    manifest = run_worker(resume_config)

    assert manifest["state"] == "completed"
    assert manifest["created_files"] == 4
    assert tracked_file.stat().st_mtime_ns == first_mtime


def test_worker_run_resume_cleans_incomplete_chunk(tmp_path):
    base_dir = tmp_path / "data"
    state_dir = tmp_path / "state"
    submission_dir = submission_dir_for_index(base_dir, 0, "seed")
    partial_subdir = submission_dir / "sub_0"
    partial_subdir.mkdir(parents=True)
    (partial_subdir / "data_0000.dat").write_bytes(b"x")

    manifest_path = state_dir / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{}", encoding="utf-8")

    config = WorkerConfig(
        run_id="run-1",
        host="host-1",
        remote_base_dir=str(base_dir),
        remote_state_dir=str(state_dir),
        start_submission=0,
        end_submission=1,
        num_subdirs=1,
        files_per_subdir=2,
        file_size_bytes=4,
        chunk_size_submissions=1,
        workers=1,
        uuid_namespace_seed="seed",
        append=False,
        resume=True,
    )

    manifest = run_worker(config)

    files = sorted(submission_dir.glob("sub_*/*"))
    assert manifest["state"] == "completed"
    assert len(files) == 2
    assert all(path.stat().st_size == 4 for path in files)


def test_worker_run_append_allows_non_empty_base_dir(tmp_path):
    base_dir = tmp_path / "data"
    existing_path = base_dir / "upload" / "submit" / "existing-marker"
    existing_path.mkdir(parents=True)
    state_dir = tmp_path / "state-append"

    config = WorkerConfig(
        run_id="run-append",
        host="host-1",
        remote_base_dir=str(base_dir),
        remote_state_dir=str(state_dir),
        start_submission=2,
        end_submission=4,
        num_subdirs=1,
        files_per_subdir=2,
        file_size_bytes=4,
        chunk_size_submissions=1,
        workers=1,
        uuid_namespace_seed="seed",
        append=True,
        resume=False,
    )

    manifest = run_worker(config)

    assert manifest["state"] == "completed"
    assert manifest["created_files"] == 4
    assert existing_path.is_dir()
    assert submission_dir_for_index(base_dir, 2, "seed").is_dir()
    assert submission_dir_for_index(base_dir, 3, "seed").is_dir()
