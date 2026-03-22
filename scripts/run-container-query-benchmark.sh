#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
upstream_root="${UPSTREAM_ROOT:-$(cd "$repo_root/../fustor" && pwd)}"
run_dir="${RUN_DIR:-$repo_root/capanix-benchmark-run}"
data_dir="$run_dir/data"
state_dir="$run_dir/container-fsmeta-state"
logs_dir="$run_dir/container-logs"
results_dir="$run_dir/results"
recreate_data="${RECREATE_DATA:-0}"
run_status_path="$run_dir/run-status.json"
benchmark_stdout_log="$logs_dir/benchmark.stdout.log"
benchmark_stderr_log="$logs_dir/benchmark.stderr.log"
run_started_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
run_finished_at_utc=""
run_status="running"
run_error=""

bench_image="${BENCH_IMAGE:-fustor-benchmark:container-local}"
fsmeta_image="${FSMETA_IMAGE:-fs-meta-fixture:container-local}"
network_name="${NETWORK_NAME:-fustor-benchmark-net}"
fsmeta_container="${FSMETA_CONTAINER_NAME:-fustor-benchmark-fsmeta}"
bench_container="${BENCH_CONTAINER_NAME:-fustor-benchmark-runner}"
host_port="${HOST_PORT:-19184}"

num_dirs="${NUM_DIRS:-50}"
num_subdirs="${NUM_SUBDIRS:-4}"
files_per_subdir="${FILES_PER_SUBDIR:-250}"
concurrency="${CONCURRENCY:-20}"
num_requests="${NUM_REQUESTS:-200}"
target_depth="${TARGET_DEPTH:-5}"
integrity_interval="${INTEGRITY_INTERVAL:-1}"
ready_timeout="${READY_TIMEOUT:-300}"
group_order="${GROUP_ORDER:-group-key}"
group_page_size="${GROUP_PAGE_SIZE:-1}"
entry_page_size="${ENTRY_PAGE_SIZE:-1000}"
root_layout="${ROOT_LAYOUT:-named-roots}"
root_ids="${ROOT_IDS:-nfs1,nfs2,nfs3}"
pit_phase_batch_requests="${PIT_PHASE_BATCH_REQUESTS:-96}"
layout_manifest="$data_dir/.layout-manifest.json"
fixture_manifest="$run_dir/fixture-manifest.json"

fsmeta_binary="$upstream_root/target/debug/fs_meta_api_fixture"
docker_stage_dir="$run_dir/docker-stage/fsmeta-bin"

mkdir -p "$run_dir" "$state_dir" "$logs_dir" "$results_dir"
mkdir -p "$docker_stage_dir"

write_run_status() {
    python3 - "$run_status_path" "$run_status" "$run_started_at_utc" "${run_finished_at_utc:-}" "$run_error" "$results_dir/query-find.json" "$results_dir/query-find.html" <<'PY'
import json
import os
import sys

(
    manifest_path,
    status,
    started_at_utc,
    finished_at_utc,
    error,
    json_result_path,
    html_result_path,
) = sys.argv[1:]

payload = {
    "status": status,
    "started_at_utc": started_at_utc,
    "finished_at_utc": finished_at_utc or None,
    "error": error or None,
    "json_result_path": json_result_path,
    "json_result_exists": os.path.exists(json_result_path),
    "html_result_path": html_result_path,
    "html_result_exists": os.path.exists(html_result_path),
}
with open(manifest_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
PY
}

write_layout_manifest() {
    python3 - "$layout_manifest" "$root_layout" "$root_ids" "$num_dirs" "$num_subdirs" "$files_per_subdir" <<'PY'
import json
import sys

manifest_path, root_layout, root_ids_csv, num_dirs, num_subdirs, files_per_subdir = sys.argv[1:]
payload = {
    "root_layout": root_layout,
    "root_ids": [item.strip() for item in root_ids_csv.split(",") if item.strip()],
    "num_dirs_total": int(num_dirs),
    "num_subdirs": int(num_subdirs),
    "files_per_subdir": int(files_per_subdir),
}
with open(manifest_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
PY
}

write_fixture_manifest() {
    python3 - "$fixture_manifest" "$fsmeta_binary" <<'PY'
import hashlib
import json
import os
import sys

manifest_path, binary_path = sys.argv[1:]
st = os.stat(binary_path)
h = hashlib.sha256()
with open(binary_path, "rb") as fh:
    for chunk in iter(lambda: fh.read(1024 * 1024), b""):
        h.update(chunk)
payload = {
    "binary_path": binary_path,
    "sha256": h.hexdigest(),
    "mtime_epoch_s": int(st.st_mtime),
    "size_bytes": st.st_size,
}
with open(manifest_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2, sort_keys=True)
PY
}

validate_layout_manifest() {
    python3 - "$layout_manifest" "$root_layout" "$root_ids" "$num_dirs" "$num_subdirs" "$files_per_subdir" <<'PY'
import json
import sys

manifest_path, expected_layout, expected_root_ids_csv, expected_num_dirs, expected_num_subdirs, expected_files_per_subdir = sys.argv[1:]
expected = {
    "root_layout": expected_layout,
    "root_ids": [item.strip() for item in expected_root_ids_csv.split(",") if item.strip()],
    "num_dirs_total": int(expected_num_dirs),
    "num_subdirs": int(expected_num_subdirs),
    "files_per_subdir": int(expected_files_per_subdir),
}

try:
    with open(manifest_path, "r", encoding="utf-8") as fh:
        actual = json.load(fh)
except FileNotFoundError:
    print("missing layout manifest", file=sys.stderr)
    raise SystemExit(2)

mismatches = []
for key, expected_value in expected.items():
    if actual.get(key) != expected_value:
        mismatches.append(f"{key}: expected={expected_value!r} actual={actual.get(key)!r}")

if mismatches:
    print("layout mismatch", file=sys.stderr)
    for line in mismatches:
        print(line, file=sys.stderr)
    raise SystemExit(1)
PY
}

print_failure_context() {
    echo "Benchmark failed; collecting container diagnostics..." >&2
    if [[ -f "$logs_dir/server.stderr.log" ]]; then
        echo "--- server.stderr.log (tail) ---" >&2
        tail -n 80 "$logs_dir/server.stderr.log" >&2 || true
    fi
    if [[ -f "$logs_dir/server.stdout.log" ]]; then
        echo "--- server.stdout.log (tail) ---" >&2
        tail -n 40 "$logs_dir/server.stdout.log" >&2 || true
    fi
}

cleanup() {
    status=$?
    if [[ $status -ne 0 ]]; then
        run_status="failed"
        run_finished_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
        if [[ -z "$run_error" ]]; then
            run_error="container benchmark exited with status $status"
        fi
        write_run_status
        print_failure_context
    fi
    docker rm -f "$bench_container" >/dev/null 2>&1 || true
    docker rm -f "$fsmeta_container" >/dev/null 2>&1 || true
    return $status
}

trap cleanup EXIT

wait_for_fsmeta_login() {
    local deadline=$((SECONDS + ready_timeout + 30))
    while (( SECONDS < deadline )); do
        if docker run --rm -i \
            --network "$network_name" \
            --entrypoint python \
            "$bench_image" - "$fsmeta_container" <<'PY'
import sys
import requests

host = sys.argv[1]
try:
    response = requests.post(
        f"http://{host}:18102/api/fs-meta/v1/session/login",
        json={"username": "admin", "password": "admin"},
        timeout=2,
    )
    raise SystemExit(0 if response.status_code == 200 else 1)
except Exception:
    raise SystemExit(1)
PY
        then
            return 0
        fi
        sleep 1
    done

    if [[ -f "$logs_dir/server.stderr.log" ]]; then
        tail -n 40 "$logs_dir/server.stderr.log" >&2 || true
    fi
    echo "fs-meta fixture did not become ready" >&2
    return 1
}

start_fsmeta_container() {
    docker rm -f "$fsmeta_container" >/dev/null 2>&1 || true
    docker run -d \
        --name "$fsmeta_container" \
        --network "$network_name" \
        --entrypoint bash \
        -p "$host_port:18102" \
        -e FS_META_API_FACADE_RESOURCE_ID=fs-meta-fixture-http \
        -e FS_META_API_LISTENER_BIND_ADDR=0.0.0.0:18102 \
        -e FS_META_ROOTS_JSON="$roots_json" \
        -e FS_META_PASSWD_PATH=/state/fs-meta.passwd \
        -e FS_META_SHADOW_PATH=/state/fs-meta.shadow \
        -e FS_META_QUERY_KEYS_PATH=/state/fs-meta.query-keys.json \
        -v "$data_dir:/bench-data:ro" \
        -v "$state_dir:/state" \
        -v "$logs_dir:/logs" \
        "$fsmeta_image" \
        -lc '/usr/local/bin/fsmeta-fixture-entrypoint.sh >/logs/server.stdout.log 2>/logs/server.stderr.log'
    wait_for_fsmeta_login
}

run_phase_chunk() {
    local phase_name="$1"
    local phase_requests="$2"
    local output_path="$3"
    local container_output_path="${output_path/#$run_dir/\/work\/capanix-benchmark-run}"

    docker run --rm -i \
        --network "$network_name" \
        -v "$run_dir:/work/capanix-benchmark-run" \
        -v "$data_dir:/work/data:ro" \
        -e FS_META_FIXTURE_SHA256="$fixture_sha256" \
        -e FS_META_FIXTURE_MTIME_EPOCH_S="$fixture_mtime_epoch_s" \
        -e FS_META_FIXTURE_SIZE_BYTES="$fixture_size_bytes" \
        -e BENCH_PHASE="$phase_name" \
        -e PHASE_REQUESTS="$phase_requests" \
        -e PHASE_OUTPUT_PATH="$container_output_path" \
        -e BASE_URL="http://$fsmeta_container:18102" \
        -e TARGET_DEPTH="$target_depth" \
        -e CONCURRENCY="$concurrency" \
        -e INTEGRITY_INTERVAL="$integrity_interval" \
        -e READY_TIMEOUT="$ready_timeout" \
        -e GROUP_ORDER="$group_order" \
        -e GROUP_PAGE_SIZE="$group_page_size" \
        -e ENTRY_PAGE_SIZE="$entry_page_size" \
        -e ROOT_LAYOUT="$root_layout" \
        -e ROOT_IDS="$root_ids" \
        --entrypoint python \
        "$bench_image" - <<'PY'
import json
import os
import time

from capanix_benchmark.runner import BenchmarkRunner

phase = os.environ["BENCH_PHASE"]
output_path = os.environ["PHASE_OUTPUT_PATH"]
phase_requests = int(os.environ["PHASE_REQUESTS"])

runner = BenchmarkRunner(
    run_dir="/work/capanix-benchmark-run",
    target_dir="/work/data",
    base_url=os.environ["BASE_URL"],
    username="admin",
    password="admin",
    group_order=os.environ["GROUP_ORDER"],
    group_page_size=int(os.environ["GROUP_PAGE_SIZE"]),
    entry_page_size=int(os.environ["ENTRY_PAGE_SIZE"]),
    ready_timeout=float(os.environ["READY_TIMEOUT"]),
    root_layout=os.environ["ROOT_LAYOUT"],
    root_ids=[item.strip() for item in os.environ["ROOT_IDS"].split(",") if item.strip()],
)

elapsed_start = time.time()
metadata = None
revoke_error = None

try:
    if phase not in {"os_baseline", "os_integrity"}:
        runner.client.login_management("admin", "admin")
        key_label = f"phase-{phase}-{int(time.time())}"
        runner.created_query_api_key_id = runner.client.create_query_api_key(key_label)
        runner.client.wait_ready(runner.ready_timeout)
        stats_payload = runner.client.get_stats(path=runner.path, recursive=True, group=runner.stats_group)
        total_files, total_dirs = runner._extract_scope_counts(stats_payload)
        metadata = {
            "total_files_in_scope": total_files,
            "total_directories_in_scope": total_dirs,
        }

    targets = runner._discover_targets(int(os.environ["TARGET_DEPTH"]))
    worker_count = int(os.environ["CONCURRENCY"])

    if phase == "os_baseline":
        stats = runner.run_concurrent_os_baseline(targets, worker_count, phase_requests)
    elif phase == "os_integrity":
        stats = runner.run_concurrent_os_integrity(
            targets,
            worker_count,
            phase_requests,
            float(os.environ["INTEGRITY_INTERVAL"]),
        )
    elif phase == "tree":
        stats = runner.run_concurrent_fs_meta_endpoint(
            "tree",
            targets,
            worker_count,
            phase_requests,
            recursive=True,
            capture_outcomes=True,
        )
    elif phase == "find_success":
        stats = runner.run_force_find_success(targets, worker_count, phase_requests, recursive=True)
    elif phase == "find_contention":
        stats = runner.run_force_find_contention(targets, worker_count, phase_requests, recursive=True)
    else:
        raise RuntimeError(f"unsupported phase: {phase}")
finally:
    if runner.created_query_api_key_id:
        try:
            runner.client.revoke_query_api_key(runner.created_query_api_key_id)
        except Exception as exc:  # noqa: BLE001
            revoke_error = str(exc)

payload = {
    "phase": phase,
    "stats": stats,
    "elapsed_seconds": time.time() - elapsed_start,
    "attempted_count": phase_requests,
    "target_directory_count": len(targets),
    "metadata": metadata,
    "revoke_error": revoke_error,
}

os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2)
PY
}

merge_phase_chunks() {
    local phase_name="$1"
    local output_path="$2"
    shift 2

    PYTHONPATH="$repo_root/src" python3 - "$phase_name" "$output_path" "$@" <<'PY'
import json
import sys

from capanix_benchmark.reporter import calculate_outcome_stats, calculate_stats

phase_name = sys.argv[1]
output_path = sys.argv[2]
chunk_paths = sys.argv[3:]

chunks = []
for path in chunk_paths:
    with open(path, "r", encoding="utf-8") as fh:
        chunks.append(json.load(fh))

latencies = []
elapsed_seconds = 0.0
attempted_count = 0
target_directory_count = 0
metadata = None

for chunk in chunks:
    latencies.extend([(value / 1000.0) for value in chunk["stats"].get("raw", [])])
    elapsed_seconds += float(chunk.get("elapsed_seconds", 0.0))
    attempted_count += int(chunk.get("attempted_count", 0))
    target_directory_count = max(target_directory_count, int(chunk.get("target_directory_count", 0)))
    if metadata is None and chunk.get("metadata"):
        metadata = chunk["metadata"]

first_stats = chunks[0]["stats"]
if "success_count" in first_stats:
    not_ready_count = sum(int(chunk["stats"].get("not_ready_count", 0)) for chunk in chunks)
    other_error_count = sum(int(chunk["stats"].get("other_error_count", 0)) for chunk in chunks)
    merged_stats = calculate_outcome_stats(
        latencies,
        elapsed_seconds,
        attempted_count,
        not_ready_count,
        other_error_count,
    )
    for key in [
        "execution_mode",
        "requested_concurrency",
        "effective_concurrency",
        "qps_semantics",
        "targeted_group_count",
    ]:
        if key in first_stats:
            merged_stats[key] = first_stats[key]
else:
    merged_stats = calculate_stats(latencies, elapsed_seconds, attempted_count)

payload = {
    "phase": phase_name,
    "stats": merged_stats,
    "elapsed_seconds": elapsed_seconds,
    "attempted_count": attempted_count,
    "target_directory_count": target_directory_count,
    "metadata": metadata,
}

with open(output_path, "w", encoding="utf-8") as fh:
    json.dump(payload, fh, indent=2)
PY
}

rm -f "$results_dir/query-find.json" "$results_dir/query-find.html" "$benchmark_stdout_log" "$benchmark_stderr_log"
write_run_status

if [[ "$recreate_data" == "1" ]]; then
    rm -rf "$data_dir"
fi

need_generate=0
if [[ ! -d "$data_dir" ]] || [[ -z "$(find "$data_dir" -mindepth 1 -maxdepth 1 2>/dev/null)" ]]; then
    need_generate=1
else
    if ! validate_layout_manifest; then
        echo "Existing benchmark data does not match requested layout/size parameters." >&2
        echo "Set RECREATE_DATA=1 to rebuild $data_dir with ROOT_LAYOUT=$root_layout ROOT_IDS=$root_ids NUM_DIRS=$num_dirs." >&2
        exit 1
    fi
fi

if [[ ! -x "$fsmeta_binary" ]]; then
    cargo build -p capanix-app-fs-meta-worker-facade --bin fs_meta_api_fixture --manifest-path "$upstream_root/Cargo.toml"
fi

cp "$fsmeta_binary" "$docker_stage_dir/fs_meta_api_fixture"
write_fixture_manifest
readarray -t fixture_manifest_values < <(
    python3 - "$fixture_manifest" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], "r", encoding="utf-8"))
print(payload["sha256"])
print(payload["mtime_epoch_s"])
print(payload["size_bytes"])
PY
)
fixture_sha256="${fixture_manifest_values[0]}"
fixture_mtime_epoch_s="${fixture_manifest_values[1]}"
fixture_size_bytes="${fixture_manifest_values[2]}"

docker build -t "$bench_image" -f "$repo_root/docker/benchmark.Dockerfile" "$repo_root"
docker build -t "$fsmeta_image" -f "$repo_root/docker/fsmeta-fixture.Dockerfile" "$repo_root"

if ! docker network inspect "$network_name" >/dev/null 2>&1; then
    docker network create "$network_name" >/dev/null
fi

if [[ "$need_generate" == "1" ]]; then
    docker run --rm \
        --network "$network_name" \
        -v "$run_dir:/work/capanix-benchmark-run" \
        "$bench_image" generate /work/capanix-benchmark-run/data \
        --num-dirs "$num_dirs" \
        --num-subdirs "$num_subdirs" \
        --files-per-subdir "$files_per_subdir" \
        --root-layout "$root_layout" \
        --root-ids "$root_ids"
    write_layout_manifest
fi

echo "Benchmark data layout: ROOT_LAYOUT=$root_layout ROOT_IDS=$root_ids NUM_DIRS(total)=$num_dirs"
echo "Fixture binary: SHA256=$fixture_sha256 MTIME_EPOCH_S=$fixture_mtime_epoch_s SIZE_BYTES=$fixture_size_bytes"

docker rm -f "$fsmeta_container" >/dev/null 2>&1 || true
docker rm -f "$bench_container" >/dev/null 2>&1 || true

roots_json="$(python3 - "$root_layout" "$root_ids" <<'PY'
import json
import sys

root_layout = sys.argv[1]
root_ids = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
if root_layout == "named-roots":
    roots = [
        {
            "id": root_id,
            "selector": {"mount_point": f"/bench-data/{root_id}"},
            "subpath_scope": "/",
            "watch": True,
            "scan": True,
            "audit_interval_ms": 1000,
        }
        for root_id in root_ids
    ]
else:
    root_id = root_ids[0] if root_ids else "bench-root"
    roots = [
        {
            "id": root_id,
            "selector": {"mount_point": "/bench-data"},
            "subpath_scope": "/",
            "watch": True,
            "scan": True,
            "audit_interval_ms": 1000,
        }
    ]
print(json.dumps(roots, separators=(",", ":")))
PY
)"

start_fsmeta_container

phase_chunks_dir="$results_dir/phase-chunks"
rm -rf "$phase_chunks_dir"
mkdir -p "$phase_chunks_dir"

run_phase_chunk "os_baseline" "$num_requests" "$phase_chunks_dir/os_baseline.chunk1.json" \
    > >(tee "$benchmark_stdout_log") \
    2> >(tee "$benchmark_stderr_log" >&2)
run_phase_chunk "os_integrity" "$num_requests" "$phase_chunks_dir/os_integrity.chunk1.json" \
    >>"$benchmark_stdout_log" \
    2>>"$benchmark_stderr_log"

declare -a tree_chunks=()
remaining_tree="$num_requests"
tree_chunk_index=1
while (( remaining_tree > 0 )); do
    chunk_requests="$remaining_tree"
    if (( chunk_requests > pit_phase_batch_requests )); then
        chunk_requests="$pit_phase_batch_requests"
    fi
    if (( tree_chunk_index > 1 )); then
        start_fsmeta_container
    fi
    chunk_path="$phase_chunks_dir/tree.chunk${tree_chunk_index}.json"
    run_phase_chunk "tree" "$chunk_requests" "$chunk_path" >>"$benchmark_stdout_log" 2>>"$benchmark_stderr_log"
    tree_chunks+=("$chunk_path")
    remaining_tree=$((remaining_tree - chunk_requests))
    tree_chunk_index=$((tree_chunk_index + 1))
done
merge_phase_chunks "tree" "$phase_chunks_dir/tree.merged.json" "${tree_chunks[@]}"

start_fsmeta_container
declare -a find_success_chunks=()
remaining_find_success="$num_requests"
find_success_chunk_index=1
while (( remaining_find_success > 0 )); do
    chunk_requests="$remaining_find_success"
    if (( chunk_requests > pit_phase_batch_requests )); then
        chunk_requests="$pit_phase_batch_requests"
    fi
    if (( find_success_chunk_index > 1 )); then
        start_fsmeta_container
    fi
    chunk_path="$phase_chunks_dir/find_success.chunk${find_success_chunk_index}.json"
    run_phase_chunk "find_success" "$chunk_requests" "$chunk_path" >>"$benchmark_stdout_log" 2>>"$benchmark_stderr_log"
    find_success_chunks+=("$chunk_path")
    remaining_find_success=$((remaining_find_success - chunk_requests))
    find_success_chunk_index=$((find_success_chunk_index + 1))
done
merge_phase_chunks "find_success" "$phase_chunks_dir/find_success.merged.json" "${find_success_chunks[@]}"

start_fsmeta_container
run_phase_chunk "find_contention" "$num_requests" "$phase_chunks_dir/find_contention.chunk1.json" \
    >>"$benchmark_stdout_log" \
    2>>"$benchmark_stderr_log"

PYTHONPATH="$repo_root/src" python3 \
    > >(tee -a "$benchmark_stdout_log") \
    2> >(tee -a "$benchmark_stderr_log" >&2) \
    - \
    "$results_dir/query-find.json" \
    "$results_dir/query-find.html" \
    "$phase_chunks_dir/os_baseline.chunk1.json" \
    "$phase_chunks_dir/os_integrity.chunk1.json" \
    "$phase_chunks_dir/tree.merged.json" \
    "$phase_chunks_dir/find_success.merged.json" \
    "$phase_chunks_dir/find_contention.chunk1.json" \
    "$data_dir" \
    "$integrity_interval" \
    "$group_order" \
    "$group_page_size" \
    "$entry_page_size" \
    "$root_layout" \
    "$root_ids" \
    "$target_depth" \
    "$num_requests" \
    "$concurrency" \
    "$fixture_sha256" \
    "$fixture_mtime_epoch_s" \
    "$fixture_size_bytes" <<'PY'
import json
import os
import sys
import time

from capanix_benchmark.reporter import generate_html_report

(
    json_path,
    html_path,
    os_baseline_path,
    os_integrity_path,
    tree_path,
    find_success_path,
    find_contention_path,
    data_dir_arg,
    integrity_interval_arg,
    group_order_arg,
    group_page_size_arg,
    entry_page_size_arg,
    root_layout_arg,
    root_ids_arg,
    target_depth_arg,
    num_requests_arg,
    concurrency_arg,
    fixture_sha256_arg,
    fixture_mtime_epoch_s_arg,
    fixture_size_bytes_arg,
) = sys.argv[1:]

def load(path):
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)

os_baseline = load(os_baseline_path)
os_integrity = load(os_integrity_path)
tree = load(tree_path)
find_success = load(find_success_path)
find_contention = load(find_contention_path)
metadata = tree.get("metadata") or find_success.get("metadata") or {"total_files_in_scope": 0, "total_directories_in_scope": 0}

results = {
    "metadata": {
        "total_files_in_scope": int(metadata.get("total_files_in_scope", 0)),
        "total_directories_in_scope": int(metadata.get("total_directories_in_scope", 0)),
        "source_path": data_dir_arg,
        "api_endpoint": "container-phased",
        "integrity_interval": float(integrity_interval_arg),
        "group_order": group_order_arg,
        "group_page_size": int(group_page_size_arg),
        "entry_page_size": int(entry_page_size_arg),
        "stats_group": None,
        "root_layout": root_layout_arg,
        "root_ids": [item.strip() for item in root_ids_arg.split(",") if item.strip()],
        "fixture_binary_sha256": fixture_sha256_arg,
        "fixture_binary_mtime_epoch_s": fixture_mtime_epoch_s_arg,
        "fixture_binary_size_bytes": fixture_size_bytes_arg,
        "pit_phase_batch_requests": int(os.environ.get("PIT_PHASE_BATCH_REQUESTS", "96")),
    },
    "depth": int(target_depth_arg),
    "requests": int(num_requests_arg),
    "concurrency": int(concurrency_arg),
    "target_directory_count": max(
        int(os_baseline.get("target_directory_count", 0)),
        int(tree.get("target_directory_count", 0)),
        int(find_success.get("target_directory_count", 0)),
    ),
    "os_baseline": os_baseline["stats"],
    "os_integrity": os_integrity["stats"],
    "tree_materialized": tree["stats"],
    "find_on_demand_success": find_success["stats"],
    "find_on_demand_contention": find_contention["stats"],
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
}

with open(json_path, "w", encoding="utf-8") as fh:
    json.dump(results, fh, indent=2)
generate_html_report(results, html_path)

print("\n" + "=" * 120)
print(f"QUERY/FIND BENCHMARK (DEPTH {results['depth']}, INTERVAL {results['metadata']['integrity_interval']}s)")
print(f"Data Scale: {results['metadata']['total_files_in_scope']:,} files | Targets: {results['target_directory_count']}")
print("=" * 120)
print(f"{'Metric':<24} | {'OS Baseline':<18} | {'OS Integrity':<18} | {'tree':<18} | {'on-demand-find(success)':<18}")
print("-" * 120)
print(
    f"{'Avg Latency':<24} | {results['os_baseline']['avg']:10.2f} ms | {results['os_integrity']['avg']:10.2f} ms | "
    f"{results['tree_materialized']['avg']:10.2f} ms | {results['find_on_demand_success']['avg']:10.2f} ms"
)
print(
    f"{'P50 Latency':<24} | {results['os_baseline']['p50']:10.2f} ms | {results['os_integrity']['p50']:10.2f} ms | "
    f"{results['tree_materialized']['p50']:10.2f} ms | {results['find_on_demand_success']['p50']:10.2f} ms"
)
print(
    f"{'P99 Latency':<24} | {results['os_baseline']['p99']:10.2f} ms | {results['os_integrity']['p99']:10.2f} ms | "
    f"{results['tree_materialized']['p99']:10.2f} ms | {results['find_on_demand_success']['p99']:10.2f} ms"
)
print(
    f"{'Throughput (QPS)':<24} | {results['os_baseline']['qps']:16.1f} | {results['os_integrity']['qps']:16.1f} | "
    f"{results['tree_materialized']['qps']:16.1f} | {results['find_on_demand_success']['qps']:16.1f}"
)
print("-" * 120)
print(
    "on-demand contention: "
    f"success={results['find_on_demand_contention']['success_count']} "
    f"not_ready={results['find_on_demand_contention']['not_ready_count']} "
    f"other_error={results['find_on_demand_contention']['other_error_count']}"
)
print(f"JSON results saved to: {json_path}")
print(f"HTML report saved to: {html_path}")
PY

run_status="ok"
run_finished_at_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
write_run_status

echo "Container benchmark completed."
echo "Results: $results_dir/query-find.json"
echo "HTML:    $results_dir/query-find.html"
echo "Logs:    $logs_dir/server.stdout.log"
