from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import hashlib
import json
import os
import pathlib
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from typing import Any


DEFAULT_HOSTS = [
    "10.0.82.144",
    "10.0.82.145",
    "10.0.82.146",
    "10.0.82.147",
    "10.0.82.148",
]
DEFAULT_REMOTE_BASE_DIR = "/data/fustor-nfs"
DEFAULT_REMOTE_STATE_ROOT = "/var/tmp/benchmark-data-generator"
DEFAULT_LOCAL_STATE_ROOT = "benchmark-data-generator-runs"
DEFAULT_TOTAL_FILES = 500_000_000
DEFAULT_NUM_SUBDIRS = 4
DEFAULT_FILES_PER_SUBDIR = 250
DEFAULT_FILE_SIZE_BYTES = 1024
DEFAULT_CHUNK_SIZE_SUBMISSIONS = 1000
DEFAULT_UUID_NAMESPACE_SEED = "fustor-benchmark"
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
MANIFEST_VERSION = 1


@dataclass(frozen=True)
class HostShard:
    host: str
    start_submission: int
    end_submission: int

    @property
    def submission_count(self) -> int:
        return self.end_submission - self.start_submission


@dataclass(frozen=True)
class WorkerConfig:
    run_id: str
    host: str
    remote_base_dir: str
    remote_state_dir: str
    start_submission: int
    end_submission: int
    num_subdirs: int
    files_per_subdir: int
    file_size_bytes: int
    chunk_size_submissions: int
    workers: int
    uuid_namespace_seed: str
    append: bool
    resume: bool

    @property
    def files_per_submission(self) -> int:
        return files_per_submission(self.num_subdirs, self.files_per_subdir)

    @property
    def expected_files(self) -> int:
        return self.submission_count * self.files_per_submission

    @property
    def submission_count(self) -> int:
        return self.end_submission - self.start_submission


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def default_run_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_parent(path: pathlib.Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json_atomic(path: pathlib.Path, payload: dict[str, Any]) -> None:
    ensure_parent(path)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = pathlib.Path(handle.name)
    temp_path.replace(path)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def files_per_submission(num_subdirs: int, files_per_subdir: int) -> int:
    return num_subdirs * files_per_subdir


def submission_count_for_files(total_files: int, num_subdirs: int, files_per_subdir: int) -> int:
    per_submission = files_per_submission(num_subdirs, files_per_subdir)
    if total_files <= 0:
        raise ValueError("total_files must be positive")
    if per_submission <= 0:
        raise ValueError("num_subdirs * files_per_subdir must be positive")
    if total_files % per_submission != 0:
        raise ValueError(
            f"total_files={total_files} must be divisible by num_subdirs*files_per_subdir={per_submission}"
        )
    return total_files // per_submission


def parse_hosts(hosts_value: str) -> list[str]:
    hosts = [item.strip() for item in hosts_value.split(",") if item.strip()]
    if not hosts:
        raise ValueError("at least one host is required")
    return hosts


def plan_host_shards(hosts: list[str], total_submissions: int, start_submission_offset: int = 0) -> list[HostShard]:
    host_count = len(hosts)
    base = total_submissions // host_count
    remainder = total_submissions % host_count
    shards: list[HostShard] = []
    cursor = start_submission_offset
    for index, host in enumerate(hosts):
        shard_size = base + (1 if index < remainder else 0)
        next_cursor = cursor + shard_size
        shards.append(HostShard(host=host, start_submission=cursor, end_submission=next_cursor))
        cursor = next_cursor
    return shards


def iter_chunk_ranges(start_submission: int, end_submission: int, chunk_size_submissions: int):
    cursor = start_submission
    while cursor < end_submission:
        next_cursor = min(end_submission, cursor + chunk_size_submissions)
        yield cursor, next_cursor
        cursor = next_cursor


def deterministic_uuid_for_index(index: int, namespace_seed: str = DEFAULT_UUID_NAMESPACE_SEED) -> str:
    if index < 0:
        raise ValueError("submission index must be non-negative")
    digest = hashlib.md5(f"{namespace_seed}:{index}".encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest))


def submission_dir_for_index(
    base_dir: str | pathlib.Path,
    submission_index: int,
    namespace_seed: str = DEFAULT_UUID_NAMESPACE_SEED,
) -> pathlib.Path:
    submission_uuid = deterministic_uuid_for_index(submission_index, namespace_seed)
    return pathlib.Path(base_dir) / "upload" / "submit" / submission_uuid[0] / submission_uuid[1] / submission_uuid


def make_payload(file_size_bytes: int) -> bytes:
    if file_size_bytes < 0:
        raise ValueError("file_size_bytes must be non-negative")
    if file_size_bytes == 0:
        return b""
    seed = b"fustor-benchmark-payload-"
    copies = (file_size_bytes + len(seed) - 1) // len(seed)
    return (seed * copies)[:file_size_bytes]


def write_payload_file(path: pathlib.Path, payload: bytes) -> None:
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(fd, view)
            view = view[written:]
    finally:
        os.close(fd)


def generate_submission(
    base_dir: str,
    submission_index: int,
    num_subdirs: int,
    files_per_subdir: int,
    payload: bytes,
    namespace_seed: str,
) -> tuple[int, int]:
    submission_dir = submission_dir_for_index(base_dir, submission_index, namespace_seed)
    submission_dir.mkdir(parents=True, exist_ok=True)
    created_dirs = 1
    created_files = 0
    for subdir_index in range(num_subdirs):
        subdir_path = submission_dir / f"sub_{subdir_index}"
        subdir_path.mkdir(parents=True, exist_ok=True)
        created_dirs += 1
        for file_index in range(files_per_subdir):
            write_payload_file(subdir_path / f"data_{file_index:04d}.dat", payload)
            created_files += 1
    return created_files, created_dirs


def cleanup_submission_range(
    base_dir: str,
    start_submission: int,
    end_submission: int,
    namespace_seed: str,
) -> None:
    for submission_index in range(start_submission, end_submission):
        submission_dir = submission_dir_for_index(base_dir, submission_index, namespace_seed)
        if submission_dir.exists():
            shutil.rmtree(submission_dir)


def estimate_directory_count(submission_count: int, num_subdirs: int) -> int:
    root_dirs = 3
    max_hash_dirs = 16 + 256
    per_submission_dirs = submission_count * (1 + num_subdirs)
    return root_dirs + max_hash_dirs + per_submission_dirs


def chunk_id(start_submission: int, end_submission: int) -> str:
    return f"{start_submission:09d}-{end_submission:09d}"


def chunk_marker_path(state_dir: pathlib.Path, start_submission: int, end_submission: int) -> pathlib.Path:
    return state_dir / "chunks" / f"{chunk_id(start_submission, end_submission)}.json"


def list_chunk_markers(state_dir: pathlib.Path) -> list[pathlib.Path]:
    chunk_dir = state_dir / "chunks"
    if not chunk_dir.exists():
        return []
    return sorted(path for path in chunk_dir.iterdir() if path.suffix == ".json")


def aggregate_chunk_markers(state_dir: pathlib.Path) -> tuple[list[str], int, int]:
    completed_chunks: list[str] = []
    created_files = 0
    created_dirs = 0
    for marker_path in list_chunk_markers(state_dir):
        payload = read_json(marker_path)
        completed_chunks.append(payload["chunk_id"])
        created_files += int(payload.get("created_files", 0))
        created_dirs += int(payload.get("created_dirs", 0))
    return completed_chunks, created_files, created_dirs


def write_worker_manifest(
    manifest_path: pathlib.Path,
    config: WorkerConfig,
    state: str,
    completed_chunks: list[str],
    created_files: int,
    created_dirs: int,
    started_at: str,
    finished_at: str | None = None,
    last_error: str | None = None,
) -> None:
    write_json_atomic(
        manifest_path,
        {
            "manifest_version": MANIFEST_VERSION,
            "run_id": config.run_id,
            "host": config.host,
            "state": state,
            "remote_base_dir": config.remote_base_dir,
            "remote_state_dir": config.remote_state_dir,
            "start_submission": config.start_submission,
            "end_submission": config.end_submission,
            "expected_submissions": config.submission_count,
            "expected_files": config.expected_files,
            "chunk_size_submissions": config.chunk_size_submissions,
            "workers": config.workers,
            "file_size_bytes": config.file_size_bytes,
            "num_subdirs": config.num_subdirs,
            "files_per_subdir": config.files_per_subdir,
            "uuid_namespace_seed": config.uuid_namespace_seed,
            "append": config.append,
            "completed_chunks": completed_chunks,
            "completed_chunk_count": len(completed_chunks),
            "created_files": created_files,
            "created_dirs": created_dirs,
            "started_at": started_at,
            "finished_at": finished_at,
            "last_heartbeat": utc_now(),
            "last_error": last_error,
        },
    )


def run_worker(config: WorkerConfig) -> dict[str, Any]:
    base_dir = pathlib.Path(config.remote_base_dir)
    state_dir = pathlib.Path(config.remote_state_dir)
    manifest_path = state_dir / "manifest.json"
    started_at = utc_now()

    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "chunks").mkdir(parents=True, exist_ok=True)

    if config.resume:
        if not manifest_path.exists():
            raise RuntimeError(f"resume requested but remote manifest missing: {manifest_path}")
        completed_chunks, created_files, created_dirs = aggregate_chunk_markers(state_dir)
    else:
        if not config.append and base_dir.exists() and any(base_dir.iterdir()):
            raise RuntimeError(f"target directory must be empty for a fresh run: {base_dir}")
        base_dir.mkdir(parents=True, exist_ok=True)
        completed_chunks, created_files, created_dirs = [], 0, 0

    write_worker_manifest(
        manifest_path,
        config,
        state="running",
        completed_chunks=completed_chunks,
        created_files=created_files,
        created_dirs=created_dirs,
        started_at=started_at,
    )

    payload = make_payload(config.file_size_bytes)
    done_markers = {marker.stem for marker in list_chunk_markers(state_dir)}

    try:
        for start_submission, end_submission in iter_chunk_ranges(
            config.start_submission,
            config.end_submission,
            config.chunk_size_submissions,
        ):
            marker_path = chunk_marker_path(state_dir, start_submission, end_submission)
            current_chunk_id = chunk_id(start_submission, end_submission)
            if current_chunk_id in done_markers and marker_path.exists():
                continue

            cleanup_submission_range(
                config.remote_base_dir,
                start_submission,
                end_submission,
                config.uuid_namespace_seed,
            )

            chunk_started = time.time()
            chunk_created_files = 0
            chunk_created_dirs = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=config.workers) as executor:
                futures = [
                    executor.submit(
                        generate_submission,
                        config.remote_base_dir,
                        submission_index,
                        config.num_subdirs,
                        config.files_per_subdir,
                        payload,
                        config.uuid_namespace_seed,
                    )
                    for submission_index in range(start_submission, end_submission)
                ]
                for future in concurrent.futures.as_completed(futures):
                    generated_files, generated_dirs = future.result()
                    chunk_created_files += generated_files
                    chunk_created_dirs += generated_dirs

            marker_payload = {
                "chunk_id": current_chunk_id,
                "start_submission": start_submission,
                "end_submission": end_submission,
                "created_files": chunk_created_files,
                "created_dirs": chunk_created_dirs,
                "duration_seconds": round(time.time() - chunk_started, 3),
                "finished_at": utc_now(),
            }
            write_json_atomic(marker_path, marker_payload)

            completed_chunks.append(current_chunk_id)
            created_files += chunk_created_files
            created_dirs += chunk_created_dirs
            done_markers.add(current_chunk_id)
            write_worker_manifest(
                manifest_path,
                config,
                state="running",
                completed_chunks=completed_chunks,
                created_files=created_files,
                created_dirs=created_dirs,
                started_at=started_at,
            )
    except Exception as exc:
        write_worker_manifest(
            manifest_path,
            config,
            state="failed",
            completed_chunks=completed_chunks,
            created_files=created_files,
            created_dirs=created_dirs,
            started_at=started_at,
            finished_at=utc_now(),
            last_error=str(exc),
        )
        raise

    finished_at = utc_now()
    write_worker_manifest(
        manifest_path,
        config,
        state="completed",
        completed_chunks=completed_chunks,
        created_files=created_files,
        created_dirs=created_dirs,
        started_at=started_at,
        finished_at=finished_at,
    )
    return read_json(manifest_path)


def ssh_target(host: str, ssh_user: str | None = None) -> str:
    if ssh_user:
        return f"{ssh_user}@{host}"
    return host


def build_ssh_command(host: str, remote_args: list[str], ssh_user: str | None = None) -> list[str]:
    return ["ssh", ssh_target(host, ssh_user), shlex.join(remote_args)]


def build_scp_command(local_path: pathlib.Path, host: str, remote_path: str, ssh_user: str | None = None) -> list[str]:
    return ["scp", str(local_path), f"{ssh_target(host, ssh_user)}:{remote_path}"]


def build_worker_command(
    config: WorkerConfig,
    remote_script_path: str,
    remote_python: str = "python3",
) -> list[str]:
    command = [
        remote_python,
        remote_script_path,
        "worker-run",
        "--run-id",
        config.run_id,
        "--host",
        config.host,
        "--remote-base-dir",
        config.remote_base_dir,
        "--remote-state-dir",
        config.remote_state_dir,
        "--start-submission",
        str(config.start_submission),
        "--end-submission",
        str(config.end_submission),
        "--num-subdirs",
        str(config.num_subdirs),
        "--files-per-subdir",
        str(config.files_per_subdir),
        "--file-size-bytes",
        str(config.file_size_bytes),
        "--chunk-size-submissions",
        str(config.chunk_size_submissions),
        "--workers",
        str(config.workers),
        "--uuid-namespace-seed",
        config.uuid_namespace_seed,
    ]
    if config.append:
        command.append("--append")
    if config.resume:
        command.append("--resume")
    return command


def run_subprocess(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=check,
        text=True,
        capture_output=True,
    )


def fetch_remote_json(host: str, remote_path: str, ssh_user: str | None = None) -> dict[str, Any] | None:
    command = build_ssh_command(
        host,
        [
            "python3",
            "-c",
            (
                "import pathlib,sys;"
                "path=pathlib.Path(sys.argv[1]);"
                "print(path.read_text(encoding='utf-8') if path.exists() else '')"
            ),
            remote_path,
        ],
        ssh_user=ssh_user,
    )
    result = run_subprocess(command, check=True)
    content = result.stdout.strip()
    if not content:
        return None
    return json.loads(content)


def remote_precheck(
    host: str,
    base_dir: str,
    resume: bool,
    ssh_user: str | None = None,
) -> dict[str, Any]:
    script = (
        "import json, os, pathlib, sys;"
        "base_dir=pathlib.Path(sys.argv[1]);"
        "resume=(sys.argv[2]=='1');"
        "base_dir.mkdir(parents=True, exist_ok=True);"
        "entries=sorted(p.name for p in base_dir.iterdir());"
        "st=os.statvfs(base_dir);"
        "payload={"
        "'base_dir': str(base_dir),"
        "'existing_entries': entries,"
        "'empty': len(entries)==0,"
        "'free_bytes': st.f_bavail * st.f_frsize,"
        "'free_inodes': st.f_favail,"
        "'block_size': st.f_frsize or st.f_bsize,"
        "'resume': resume,"
        "};"
        "print(json.dumps(payload))"
    )
    result = run_subprocess(
        build_ssh_command(
            host,
            ["python3", "-c", script, base_dir, "1" if resume else "0"],
            ssh_user=ssh_user,
        ),
        check=True,
    )
    return json.loads(result.stdout)


def validate_precheck(
    precheck: dict[str, Any],
    expected_files: int,
    expected_submissions: int,
    num_subdirs: int,
    file_size_bytes: int,
    append: bool,
    resume: bool,
) -> None:
    block_size = int(precheck["block_size"])
    file_allocation = 0
    if file_size_bytes > 0:
        file_allocation = ((file_size_bytes + block_size - 1) // block_size) * block_size
    expected_bytes = expected_files * file_allocation
    expected_dirs = estimate_directory_count(expected_submissions, num_subdirs)
    safety_bytes = max(1 << 30, expected_bytes // 20)
    safety_inodes = max(100_000, (expected_files + expected_dirs) // 20)
    required_bytes = expected_bytes + safety_bytes
    required_inodes = expected_files + expected_dirs + safety_inodes

    free_bytes = int(precheck["free_bytes"])
    free_inodes = int(precheck["free_inodes"])
    existing_entries = list(precheck.get("existing_entries", []))

    if not resume and not append and existing_entries:
        raise RuntimeError(
            f"fresh run requires empty target directory on {precheck['base_dir']}, found entries: {existing_entries[:10]}"
        )
    if free_bytes < required_bytes:
        raise RuntimeError(
            f"insufficient free bytes on {precheck['base_dir']}: have={free_bytes} required={required_bytes}"
        )
    if free_inodes < required_inodes:
        raise RuntimeError(
            f"insufficient free inodes on {precheck['base_dir']}: have={free_inodes} required={required_inodes}"
        )


def load_existing_cluster_manifest(run_dir: pathlib.Path) -> dict[str, Any] | None:
    manifest_path = run_dir / "cluster-manifest.json"
    if not manifest_path.exists():
        return None
    return read_json(manifest_path)


def write_cluster_manifest(
    manifest_path: pathlib.Path,
    payload: dict[str, Any],
) -> None:
    write_json_atomic(manifest_path, payload)


def shard_map(shards: list[HostShard]) -> dict[str, dict[str, Any]]:
    return {
        shard.host: {
            "host": shard.host,
            "start_submission": shard.start_submission,
            "end_submission": shard.end_submission,
            "expected_submissions": shard.submission_count,
        }
        for shard in shards
    }


def sync_worker_script(host: str, remote_state_dir: str, ssh_user: str | None = None) -> str:
    remote_script_path = f"{remote_state_dir}/benchmark_data_generator.py"
    run_subprocess(
        build_ssh_command(
            host,
            [
                "python3",
                "-c",
                "import pathlib,sys; pathlib.Path(sys.argv[1]).mkdir(parents=True, exist_ok=True)",
                remote_state_dir,
            ],
            ssh_user=ssh_user,
        ),
        check=True,
    )
    run_subprocess(build_scp_command(pathlib.Path(__file__).resolve(), host, remote_script_path, ssh_user), check=True)
    return remote_script_path


def monitor_remote_workers(
    processes: dict[str, subprocess.Popen[str]],
    remote_manifest_paths: dict[str, str],
    manifest_payload: dict[str, Any],
    manifest_path: pathlib.Path,
    poll_interval_seconds: float,
    ssh_user: str | None = None,
) -> dict[str, int]:
    exit_codes: dict[str, int] = {}

    while len(exit_codes) < len(processes):
        for host, process in processes.items():
            if host in exit_codes:
                continue
            code = process.poll()
            if code is not None:
                exit_codes[host] = code

        host_status: dict[str, Any] = manifest_payload.setdefault("host_status", {})
        for host, remote_manifest_path in remote_manifest_paths.items():
            status = host_status.setdefault(host, {})
            try:
                remote_manifest = fetch_remote_json(host, remote_manifest_path, ssh_user=ssh_user)
            except Exception as exc:
                status["last_poll_error"] = str(exc)
                continue
            if remote_manifest:
                status.update(remote_manifest)
                status["last_poll_error"] = None

        manifest_payload["state"] = "running"
        manifest_payload["last_heartbeat"] = utc_now()
        manifest_payload["host_exit_codes"] = exit_codes.copy()
        write_cluster_manifest(manifest_path, manifest_payload)

        completed_hosts = len(exit_codes)
        progress_bits = []
        for host in sorted(host_status):
            status = host_status[host]
            completed_chunks = status.get("completed_chunk_count", 0)
            created_files = status.get("created_files", 0)
            state = status.get("state", "unknown")
            progress_bits.append(f"{host}:{state}:chunks={completed_chunks}:files={created_files}")
        if progress_bits:
            print(" | ".join(progress_bits), flush=True)

        if completed_hosts == len(processes):
            break
        time.sleep(poll_interval_seconds)

    return exit_codes


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Distributed benchmark data generator")
    subparsers = parser.add_subparsers(dest="command", required=True)

    cluster_parser = subparsers.add_parser("cluster-run", help="Run distributed generation across remote hosts.")
    cluster_parser.add_argument("--hosts", default=",".join(DEFAULT_HOSTS))
    cluster_parser.add_argument("--ssh-user", default=None)
    cluster_parser.add_argument("--remote-base-dir", default=DEFAULT_REMOTE_BASE_DIR)
    cluster_parser.add_argument("--remote-state-root", default=DEFAULT_REMOTE_STATE_ROOT)
    cluster_parser.add_argument("--state-root", default=DEFAULT_LOCAL_STATE_ROOT)
    cluster_parser.add_argument("--run-id", default=None)
    cluster_parser.add_argument("--resume", action="store_true")
    cluster_parser.add_argument("--total-files", type=int, default=DEFAULT_TOTAL_FILES)
    cluster_parser.add_argument("--start-submission-offset", type=int, default=0)
    cluster_parser.add_argument("--num-subdirs", type=int, default=DEFAULT_NUM_SUBDIRS)
    cluster_parser.add_argument("--files-per-subdir", type=int, default=DEFAULT_FILES_PER_SUBDIR)
    cluster_parser.add_argument("--file-size-bytes", type=int, default=DEFAULT_FILE_SIZE_BYTES)
    cluster_parser.add_argument("--chunk-size-submissions", type=int, default=DEFAULT_CHUNK_SIZE_SUBMISSIONS)
    cluster_parser.add_argument("--workers-per-host", type=int, default=0)
    cluster_parser.add_argument("--uuid-namespace-seed", default=DEFAULT_UUID_NAMESPACE_SEED)
    cluster_parser.add_argument("--poll-interval-seconds", type=float, default=DEFAULT_POLL_INTERVAL_SECONDS)
    cluster_parser.add_argument("--append", action="store_true")

    worker_parser = subparsers.add_parser("worker-run", help="Execute a local shard on a remote host.")
    worker_parser.add_argument("--run-id", required=True)
    worker_parser.add_argument("--host", required=True)
    worker_parser.add_argument("--remote-base-dir", required=True)
    worker_parser.add_argument("--remote-state-dir", required=True)
    worker_parser.add_argument("--start-submission", type=int, required=True)
    worker_parser.add_argument("--end-submission", type=int, required=True)
    worker_parser.add_argument("--num-subdirs", type=int, default=DEFAULT_NUM_SUBDIRS)
    worker_parser.add_argument("--files-per-subdir", type=int, default=DEFAULT_FILES_PER_SUBDIR)
    worker_parser.add_argument("--file-size-bytes", type=int, default=DEFAULT_FILE_SIZE_BYTES)
    worker_parser.add_argument("--chunk-size-submissions", type=int, default=DEFAULT_CHUNK_SIZE_SUBMISSIONS)
    worker_parser.add_argument("--workers", type=int, default=0)
    worker_parser.add_argument("--uuid-namespace-seed", default=DEFAULT_UUID_NAMESPACE_SEED)
    worker_parser.add_argument("--append", action="store_true")
    worker_parser.add_argument("--resume", action="store_true")

    return parser


def worker_config_from_args(args: argparse.Namespace) -> WorkerConfig:
    workers = args.workers or min(32, (os.cpu_count() or 1))
    if workers <= 0:
        raise ValueError("workers must be positive")
    return WorkerConfig(
        run_id=args.run_id,
        host=args.host,
        remote_base_dir=args.remote_base_dir,
        remote_state_dir=args.remote_state_dir,
        start_submission=args.start_submission,
        end_submission=args.end_submission,
        num_subdirs=args.num_subdirs,
        files_per_subdir=args.files_per_subdir,
        file_size_bytes=args.file_size_bytes,
        chunk_size_submissions=args.chunk_size_submissions,
        workers=workers,
        uuid_namespace_seed=args.uuid_namespace_seed,
        append=args.append,
        resume=args.resume,
    )


def run_cluster(args: argparse.Namespace) -> int:
    hosts = parse_hosts(args.hosts)
    if args.start_submission_offset < 0:
        raise ValueError("start_submission_offset must be non-negative")
    total_submissions = submission_count_for_files(args.total_files, args.num_subdirs, args.files_per_subdir)
    shards = plan_host_shards(hosts, total_submissions, start_submission_offset=args.start_submission_offset)
    workers_per_host = args.workers_per_host or min(32, (os.cpu_count() or 1))
    run_id = args.run_id or default_run_id()
    run_dir = pathlib.Path(args.state_root) / run_id
    manifest_path = run_dir / "cluster-manifest.json"
    existing_manifest = load_existing_cluster_manifest(run_dir)

    if args.resume:
        if existing_manifest is None:
            raise RuntimeError(f"resume requested but local manifest missing: {manifest_path}")
        immutable_fields = {
            "hosts": hosts,
            "remote_base_dir": args.remote_base_dir,
            "remote_state_root": args.remote_state_root,
            "total_files": args.total_files,
            "start_submission_offset": args.start_submission_offset,
            "num_subdirs": args.num_subdirs,
            "files_per_subdir": args.files_per_subdir,
            "file_size_bytes": args.file_size_bytes,
            "chunk_size_submissions": args.chunk_size_submissions,
            "uuid_namespace_seed": args.uuid_namespace_seed,
            "append": args.append,
        }
        for field_name, expected_value in immutable_fields.items():
            if existing_manifest.get(field_name) != expected_value:
                raise RuntimeError(f"resume arguments do not match existing {field_name}")
        manifest_payload = existing_manifest
    else:
        if existing_manifest is not None:
            raise RuntimeError(f"run directory already exists, use --resume with --run-id {run_id}")
        run_dir.mkdir(parents=True, exist_ok=False)
        (run_dir / "logs").mkdir(parents=True, exist_ok=True)
        manifest_payload = {
            "manifest_version": MANIFEST_VERSION,
            "run_id": run_id,
            "state": "planned",
            "started_at": utc_now(),
            "finished_at": None,
            "last_heartbeat": utc_now(),
            "hosts": hosts,
            "remote_base_dir": args.remote_base_dir,
            "remote_state_root": args.remote_state_root,
            "local_state_root": str(pathlib.Path(args.state_root).resolve()),
            "total_files": args.total_files,
            "total_submissions": total_submissions,
            "start_submission_offset": args.start_submission_offset,
            "num_subdirs": args.num_subdirs,
            "files_per_subdir": args.files_per_subdir,
            "file_size_bytes": args.file_size_bytes,
            "chunk_size_submissions": args.chunk_size_submissions,
            "workers_per_host": workers_per_host,
            "uuid_namespace_seed": args.uuid_namespace_seed,
            "append": args.append,
            "resume": False,
            "host_shards": shard_map(shards),
            "host_status": {},
            "host_exit_codes": {},
        }
        write_cluster_manifest(manifest_path, manifest_payload)

    print(f"Run ID: {run_id}")
    print(f"Hosts: {', '.join(hosts)}")
    print(f"Start submission offset: {args.start_submission_offset:,}")
    print(f"Total submissions: {total_submissions:,}")
    print(f"Files per host: {shards[0].submission_count * files_per_submission(args.num_subdirs, args.files_per_subdir):,}")

    shard_by_host = {shard.host: shard for shard in shards}
    for host in hosts:
        shard = shard_by_host[host]
        precheck = remote_precheck(host, args.remote_base_dir, args.resume, ssh_user=args.ssh_user)
        validate_precheck(
            precheck,
            expected_files=shard.submission_count * files_per_submission(args.num_subdirs, args.files_per_subdir),
            expected_submissions=shard.submission_count,
            num_subdirs=args.num_subdirs,
            file_size_bytes=args.file_size_bytes,
            append=args.append,
            resume=args.resume,
        )
        manifest_payload.setdefault("host_status", {}).setdefault(host, {})["precheck"] = precheck
        write_cluster_manifest(manifest_path, manifest_payload)

    processes: dict[str, subprocess.Popen[str]] = {}
    log_handles: list[Any] = []
    remote_manifest_paths: dict[str, str] = {}
    for host in hosts:
        shard = shard_by_host[host]
        remote_state_dir = f"{args.remote_state_root}/{run_id}"
        remote_script_path = sync_worker_script(host, remote_state_dir, ssh_user=args.ssh_user)
        remote_manifest_paths[host] = f"{remote_state_dir}/manifest.json"
        config = WorkerConfig(
            run_id=run_id,
            host=host,
            remote_base_dir=args.remote_base_dir,
            remote_state_dir=remote_state_dir,
            start_submission=shard.start_submission,
            end_submission=shard.end_submission,
            num_subdirs=args.num_subdirs,
            files_per_subdir=args.files_per_subdir,
            file_size_bytes=args.file_size_bytes,
            chunk_size_submissions=args.chunk_size_submissions,
            workers=workers_per_host,
            uuid_namespace_seed=args.uuid_namespace_seed,
            append=args.append,
            resume=args.resume,
        )
        worker_command = build_worker_command(config, remote_script_path)
        ssh_command = build_ssh_command(host, worker_command, ssh_user=args.ssh_user)
        stdout_handle = (run_dir / "logs" / f"{host}.stdout.log").open("a", encoding="utf-8")
        stderr_handle = (run_dir / "logs" / f"{host}.stderr.log").open("a", encoding="utf-8")
        log_handles.extend([stdout_handle, stderr_handle])
        processes[host] = subprocess.Popen(ssh_command, stdout=stdout_handle, stderr=stderr_handle, text=True)

    manifest_payload["state"] = "running"
    manifest_payload["last_heartbeat"] = utc_now()
    write_cluster_manifest(manifest_path, manifest_payload)

    exit_codes = monitor_remote_workers(
        processes=processes,
        remote_manifest_paths=remote_manifest_paths,
        manifest_payload=manifest_payload,
        manifest_path=manifest_path,
        poll_interval_seconds=args.poll_interval_seconds,
        ssh_user=args.ssh_user,
    )
    for handle in log_handles:
        handle.close()

    manifest_payload["host_exit_codes"] = exit_codes
    manifest_payload["finished_at"] = utc_now()
    manifest_payload["state"] = "completed" if all(code == 0 for code in exit_codes.values()) else "failed"
    write_cluster_manifest(manifest_path, manifest_payload)

    if manifest_payload["state"] == "completed":
        print(f"Distributed generation completed successfully. Manifest: {manifest_path}")
        return 0

    print(f"Distributed generation failed. Manifest: {manifest_path}", file=sys.stderr)
    return 1


def run_worker_from_args(args: argparse.Namespace) -> int:
    config = worker_config_from_args(args)
    manifest = run_worker(config)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.command == "cluster-run":
        return run_cluster(args)
    if args.command == "worker-run":
        return run_worker_from_args(args)
    raise RuntimeError(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
