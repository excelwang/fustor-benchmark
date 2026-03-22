import json
import os
import random
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import click
import requests

from .generator import DataGenerator
from .reporter import calculate_outcome_stats, calculate_stats, generate_html_report
from .tasks import (
    run_multi_nfs_submission_baseline_task,
    run_multi_nfs_submission_sampling_phase,
    run_multi_nfs_submission_validation_phase,
    run_single_fs_meta_req,
)

@dataclass(frozen=True)
class BenchmarkTarget:
    local_path: str
    api_path: str
    group_id: str


class LocalServiceRuntime:
    def __init__(self, start_cmd: str | None, stop_cmd: str | None):
        self.start_cmd = start_cmd
        self.stop_cmd = stop_cmd
        self.process = None

    def start(self):
        if not self.start_cmd:
            raise RuntimeError("local mode requires --start-cmd")
        self.process = subprocess.Popen(
            self.start_cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def stop(self):
        if self.stop_cmd:
            subprocess.run(self.stop_cmd, shell=True, check=False)
            return

        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            self.process = None


class FsMetaClient:
    def __init__(
        self,
        base_url: str,
        query_api_key: str | None = None,
        management_token: str | None = None,
        token: str | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.query_api_key = query_api_key or token
        self.management_token = management_token

    @property
    def query_headers(self):
        if not self.query_api_key:
            return {}
        return {"Authorization": f"Bearer {self.query_api_key}"}

    @property
    def management_headers(self):
        if not self.management_token:
            return {}
        return {"Authorization": f"Bearer {self.management_token}"}

    def login_management(self, username: str, password: str):
        response = requests.post(
            f"{self.base_url}/api/fs-meta/v1/session/login",
            json={"username": username, "password": password},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("token")
        if not token:
            raise RuntimeError("login succeeded but token is missing")
        self.management_token = token
        return token

    def create_query_api_key(self, label: str):
        if not self.management_token:
            raise RuntimeError("management token is required to create a query API key")

        response = requests.post(
            f"{self.base_url}/api/fs-meta/v1/query-api-keys",
            json={"label": label},
            headers=self.management_headers,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        api_key = payload.get("api_key")
        key = payload.get("key", {})
        key_id = key.get("key_id") if isinstance(key, dict) else None
        if not api_key or not key_id:
            raise RuntimeError("query API key creation succeeded but response is missing api_key or key_id")
        self.query_api_key = api_key
        return key_id

    def revoke_query_api_key(self, key_id: str):
        if not self.management_token:
            raise RuntimeError("management token is required to revoke a query API key")

        response = requests.delete(
            f"{self.base_url}/api/fs-meta/v1/query-api-keys/{key_id}",
            headers=self.management_headers,
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("revoked") is False:
            raise RuntimeError(f"query API key revocation was rejected for {key_id}")

    def _response_detail(self, response):
        try:
            payload = response.json()
        except ValueError:
            return response.text.strip() or None

        if isinstance(payload, dict):
            code = payload.get("code")
            message = payload.get("message") or payload.get("error")
            if code and message:
                return f"{code}: {message}"
            if code:
                return str(code)
            if message:
                return str(message)
        return json.dumps(payload, ensure_ascii=False)

    def _status_diagnostic(self, payload):
        if not isinstance(payload, dict):
            return None

        reasons = []

        source = payload.get("source", {})
        degraded_roots = source.get("degraded_roots", []) if isinstance(source, dict) else []
        if isinstance(degraded_roots, list) and degraded_roots:
            root_ids = []
            for root in degraded_roots:
                if not isinstance(root, dict):
                    continue
                root_id = root.get("root_id") or root.get("root_key") or root.get("id")
                if root_id:
                    root_ids.append(str(root_id))
            if root_ids:
                reasons.append(f"degraded_roots={','.join(root_ids)}")
            else:
                reasons.append(f"degraded_roots={len(degraded_roots)}")

        sink = payload.get("sink", {})
        groups = sink.get("groups", []) if isinstance(sink, dict) else []
        if isinstance(groups, list) and groups:
            pending_initial = []
            overflow_pending = []
            for group in groups:
                if not isinstance(group, dict):
                    continue
                group_id = str(group.get("group_id", "?"))
                if not group.get("initial_audit_completed"):
                    pending_initial.append(group_id)
                if group.get("overflow_pending_audit"):
                    overflow_pending.append(group_id)
            if pending_initial:
                reasons.append(f"pending_initial_audit={','.join(pending_initial)}")
            if overflow_pending:
                reasons.append(f"overflow_pending_audit={','.join(overflow_pending)}")

        facade = payload.get("facade", {})
        pending = facade.get("pending") if isinstance(facade, dict) else None
        if isinstance(pending, dict):
            reason = pending.get("reason")
            if reason:
                reasons.append(f"facade.pending={reason}")

        return "; ".join(reasons) if reasons else None

    def wait_ready(self, timeout_seconds: float):
        deadline = time.time() + timeout_seconds
        last_error = None
        last_status_payload = None
        while time.time() < deadline:
            try:
                stats = requests.get(
                    f"{self.base_url}/api/fs-meta/v1/stats",
                    params={"path": "/", "recursive": "true"},
                    headers=self.query_headers,
                    timeout=5,
                )
                if stats.status_code == 200:
                    return
                last_error = f"/stats status={stats.status_code}"
                detail = self._response_detail(stats)
                if detail:
                    last_error = f"{last_error}: {detail}"
            except requests.RequestException as exc:
                last_error = f"/stats request failed: {exc}"

            if self.management_token:
                try:
                    status = requests.get(
                        f"{self.base_url}/api/fs-meta/v1/status",
                        headers=self.management_headers,
                        timeout=5,
                    )
                    if status.status_code == 200:
                        last_status_payload = status.json()
                        diagnostic = self._status_diagnostic(last_status_payload)
                        if diagnostic:
                            last_error = f"{last_error}; {diagnostic}" if last_error else diagnostic
                    else:
                        detail = self._response_detail(status)
                        status_error = f"/status status={status.status_code}"
                        if detail:
                            status_error = f"{status_error}: {detail}"
                        last_error = f"{last_error}; {status_error}" if last_error else status_error
                except requests.RequestException as exc:
                    status_error = f"/status request failed: {exc}"
                    last_error = f"{last_error}; {status_error}" if last_error else status_error

            time.sleep(1)
        detail = f"fs-meta materialized query surfaces not ready within {timeout_seconds}s: {last_error}"
        if last_status_payload is not None:
            detail = f"{detail}; last_status={json.dumps(last_status_payload, ensure_ascii=False)}"
        raise RuntimeError(detail)

    def get_stats(self, path: str, recursive: bool, group: str | None):
        params = {
            "path": path,
            "recursive": "true" if recursive else "false",
        }
        if group:
            params["group"] = group

        response = requests.get(
            f"{self.base_url}/api/fs-meta/v1/stats",
            params=params,
            headers=self.query_headers,
            timeout=20,
        )
        if response.status_code != 200:
            return {}
        return response.json()


class BenchmarkRunner:
    def __init__(
        self,
        run_dir,
        target_dir,
        base_url,
        query_api_key=None,
        token=None,
        username=None,
        password=None,
        stats_group=None,
        group=None,
        path="/",
        group_order="group-key",
        group_page_size=1,
        entry_page_size=1000,
        mode="external",
        start_cmd=None,
        stop_cmd=None,
        ready_timeout=120.0,
        root_layout="single-root",
        root_ids=None,
        root_specs=None,
    ):
        self.run_dir = os.path.abspath(run_dir)
        self.data_dir = os.path.abspath(target_dir)
        self.stats_group = stats_group or group
        self.path = path
        self.group_order = group_order
        self.group_page_size = group_page_size
        self.entry_page_size = entry_page_size
        self.mode = mode
        self.ready_timeout = ready_timeout
        self.created_query_api_key_id = None
        self.root_layout = root_layout
        self.root_specs = [
            (str(group_id), os.path.abspath(root_dir))
            for group_id, root_dir in (root_specs or [])
        ]
        if self.root_layout == "explicit-roots":
            self.root_ids = [group_id for group_id, _root_dir in self.root_specs]
        else:
            self.root_ids = root_ids or ["nfs1", "nfs2", "nfs3"]

        self.client = FsMetaClient(base_url=base_url, query_api_key=query_api_key, token=token)
        self.username = username
        self.password = password

        self.local_runtime = LocalServiceRuntime(start_cmd=start_cmd, stop_cmd=stop_cmd)
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_under(self, root_dir: str, depth: int):
        base_depth = root_dir.rstrip("/").count(os.sep)
        targets = []

        for root, dirs, _files in os.walk(root_dir):
            current_depth = root.count(os.sep) - base_depth
            if current_depth == depth:
                targets.append(root)
                dirs[:] = []
            elif current_depth > depth:
                dirs[:] = []

        if not targets:
            return [root_dir]

        return targets

    def _root_group_dirs(self):
        if self.root_layout == "explicit-roots":
            return list(self.root_specs)
        if self.root_layout == "named-roots":
            return [
                (root_id, os.path.join(self.data_dir, root_id))
                for root_id in self.root_ids
            ]
        default_group = self.root_ids[0] if self.root_ids else "bench-root"
        return [(default_group, self.data_dir)]

    def _has_benchmark_data(self):
        for _group_id, root_dir in self._root_group_dirs():
            search_root = os.path.join(root_dir, "upload", "submit")
            if not os.path.isdir(search_root):
                continue
            with os.scandir(search_root) as entries:
                if any(True for _entry in entries):
                    return True
        return False

    def _to_api_path(self, path_value: str, group_root: str | None = None):
        apath = os.path.abspath(path_value)
        relative_root = group_root or self.data_dir
        try:
            common = os.path.commonpath([relative_root, apath])
        except ValueError:
            common = None
        if common == relative_root:
            relpath = os.path.relpath(apath, relative_root)
            if relpath in {"", "."}:
                return "/"
            return "/" + relpath.replace(os.sep, "/")
        return apath if apath.startswith("/") else f"/{apath}"

    def _discover_targets(self, depth: int):
        targets = []
        for group_id, root_dir in self._root_group_dirs():
            if not os.path.isdir(root_dir):
                continue
            for local_path in self._discover_leaf_targets_under(root_dir, depth):
                targets.append(
                    BenchmarkTarget(
                        local_path=local_path,
                        api_path=self._to_api_path(local_path, root_dir),
                        group_id=group_id,
                    )
                )
        if not targets:
            raise RuntimeError("Benchmark targets missing for configured root layout")
        return targets

    def _submission_id_for_target(self, target: BenchmarkTarget):
        root_dir_by_group = dict(self._root_group_dirs())
        source_root = root_dir_by_group.get(target.group_id, self.data_dir)
        absolute_target = os.path.abspath(target.local_path)
        try:
            relative_path = os.path.relpath(absolute_target, source_root)
        except ValueError:
            relative_path = os.path.basename(absolute_target.rstrip(os.sep))

        parts = relative_path.split(os.sep)
        if len(parts) >= 5 and parts[0] == "upload" and parts[1] == "submit":
            return parts[4]
        return os.path.basename(absolute_target.rstrip(os.sep))

    def _submission_baseline_spec(self, target: BenchmarkTarget):
        return {
            "submission_id": self._submission_id_for_target(target),
            "root_groups": [
                {"group_id": group_id, "root_dir": root_dir}
                for group_id, root_dir in self._root_group_dirs()
            ],
        }

    def _baseline_metric_average(self, metrics, key, divisor):
        if divisor <= 0:
            return 0.0
        return sum(metric.get(key, 0) for metric in metrics) / divisor

    def _attach_multi_nfs_baseline_fields(self, stats, metrics, requests_count, *, poll_rounds):
        divisor = requests_count if requests_count > 0 else 1
        stats["model"] = "multi_nfs_submission_discovery"
        stats["nfs_root_count"] = len(self._root_group_dirs())
        stats["poll_rounds_per_request"] = poll_rounds
        stats["total_roots_scanned_per_request"] = self._baseline_metric_average(
            metrics, "roots_scanned", divisor
        )
        stats["total_roots_with_search_path_per_request"] = self._baseline_metric_average(
            metrics, "roots_with_search_path", divisor
        )
        stats["total_discovery_find_calls_per_request"] = self._baseline_metric_average(
            metrics, "discovery_find_calls", divisor
        )
        stats["total_metadata_find_calls_per_request"] = self._baseline_metric_average(
            metrics, "metadata_find_calls", divisor
        )
        stats["candidate_count_per_poll"] = self._baseline_metric_average(
            metrics, "candidate_count", divisor * max(1, poll_rounds)
        )
        stats["metadata_lines_parsed_per_request"] = self._baseline_metric_average(
            metrics, "metadata_lines_parsed", divisor
        )
        stats["file_count_per_poll"] = self._baseline_metric_average(
            metrics, "file_count", divisor * max(1, poll_rounds)
        )
        stats["dir_count_per_poll"] = self._baseline_metric_average(
            metrics, "dir_count", divisor * max(1, poll_rounds)
        )

    def run_concurrent_os_baseline(self, targets, concurrency=20, requests_count=100):
        click.echo(f"Running OS baseline: {concurrency} workers, {requests_count} reqs...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_targets = [shuffled[i % len(shuffled)] for i in range(requests_count)]
        specs = [self._submission_baseline_spec(target) for target in sampled_targets]

        request_metrics = []
        start = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(run_multi_nfs_submission_baseline_task, spec)
                for spec in specs
            ]
            for future in as_completed(futures):
                request_metrics.append(future.result())

        latencies = [metric["latency_seconds"] for metric in request_metrics]
        stats = calculate_stats(latencies, time.time() - start, requests_count)
        self._attach_multi_nfs_baseline_fields(
            stats,
            request_metrics,
            requests_count,
            poll_rounds=1,
        )
        return stats

    def run_concurrent_os_integrity(self, targets, concurrency=20, requests_count=100, interval=60.0):
        click.echo(f"Running OS integrity: {requests_count} reqs, {concurrency} workers, wait={interval}s...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_targets = [shuffled[i % len(shuffled)] for i in range(requests_count)]
        specs = [self._submission_baseline_spec(target) for target in sampled_targets]

        start = time.time()
        sampling_results = [None] * requests_count

        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(run_multi_nfs_submission_sampling_phase, spec): idx
                for idx, spec in enumerate(specs)
            }
            for future in as_completed(futures):
                sampling_results[futures[future]] = future.result()

        time.sleep(interval)

        validation_results = [None] * requests_count
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(
                    run_multi_nfs_submission_validation_phase,
                    (spec, sampling_results[idx][1]),
                ): idx
                for idx, spec in enumerate(specs)
            }
            for future in as_completed(futures):
                validation_results[futures[future]] = future.result()

        total_wall = time.time() - start
        combined_metrics = []
        total_latencies = []
        stable_count = 0
        for idx in range(requests_count):
            sampling_latency, _snapshot, sampling_metrics = sampling_results[idx]
            validation_metrics = validation_results[idx]
            total_latencies.append(
                sampling_latency + interval + validation_metrics["latency_seconds"]
            )
            stable_count += int(bool(validation_metrics["stable"]))
            combined_metrics.append(
                {
                    "roots_scanned": sampling_metrics["roots_scanned"]
                    + validation_metrics["roots_scanned"],
                    "roots_with_search_path": sampling_metrics["roots_with_search_path"]
                    + validation_metrics["roots_with_search_path"],
                    "discovery_find_calls": sampling_metrics["discovery_find_calls"]
                    + validation_metrics["discovery_find_calls"],
                    "metadata_find_calls": sampling_metrics["metadata_find_calls"]
                    + validation_metrics["metadata_find_calls"],
                    "candidate_count": sampling_metrics["candidate_count"]
                    + validation_metrics["candidate_count"],
                    "metadata_lines_parsed": sampling_metrics["metadata_lines_parsed"]
                    + validation_metrics["metadata_lines_parsed"],
                    "file_count": sampling_metrics["file_count"]
                    + validation_metrics["file_count"],
                    "dir_count": sampling_metrics["dir_count"]
                    + validation_metrics["dir_count"],
                }
            )

        stats = calculate_stats(total_latencies, total_wall, requests_count)
        stats["poll_interval_seconds"] = interval
        stats["stable_snapshot_count"] = stable_count
        stats["unstable_snapshot_count"] = requests_count - stable_count
        stats["stable_rate"] = (stable_count / requests_count) if requests_count > 0 else 0
        self._attach_multi_nfs_baseline_fields(
            stats,
            combined_metrics,
            requests_count,
            poll_rounds=2,
        )
        return stats

    def run_concurrent_fs_meta_endpoint(
        self,
        endpoint: str,
        targets,
        concurrency=20,
        requests_count=100,
        recursive=True,
        serialize=False,
        capture_outcomes=False,
    ):
        click.echo(f"Running fs-meta endpoint {endpoint}: {concurrency} workers, {requests_count} reqs...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_targets = [shuffled[i % len(shuffled)] for i in range(requests_count)]

        latencies = []
        not_ready_count = 0
        other_error_count = 0
        start = time.time()
        max_workers = 1 if serialize else concurrency
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(
                    run_single_fs_meta_req,
                    self.client.base_url,
                    self.client.query_headers,
                    endpoint,
                    target.api_path,
                    recursive,
                    self.group_order,
                    self.group_page_size,
                    self.entry_page_size,
                    target.group_id if endpoint == "on-demand-force-find" else None,
                )
                for target in sampled_targets
            ]
            for future in as_completed(futures):
                result = future.result()
                if not isinstance(result, dict):
                    other_error_count += 1
                    continue
                outcome = result.get("outcome")
                latency = result.get("latency_seconds")
                if outcome == "ok" and latency is not None:
                    latencies.append(latency)
                elif outcome == "not_ready":
                    not_ready_count += 1
                else:
                    other_error_count += 1

        total_time = time.time() - start
        if capture_outcomes:
            return calculate_outcome_stats(
                latencies,
                total_time,
                requests_count,
                not_ready_count,
                other_error_count,
            )
        return calculate_stats(latencies, total_time, requests_count)

    def _sample_force_find_success_targets(self, targets, requests_count):
        grouped = {}
        for target in targets:
            grouped.setdefault(target.group_id, []).append(target)
        for items in grouped.values():
            random.shuffle(items)

        ordered_groups = [group_id for group_id in self.root_ids if group_id in grouped]
        for group_id in grouped:
            if group_id not in ordered_groups:
                ordered_groups.append(group_id)
        if not ordered_groups:
            return {}

        sampled = {group_id: [] for group_id in ordered_groups}
        for idx in range(requests_count):
            group_id = ordered_groups[idx % len(ordered_groups)]
            items = grouped[group_id]
            sampled[group_id].append(items[len(sampled[group_id]) % len(items)])
        return sampled

    def run_force_find_success(self, targets, concurrency=20, requests_count=100, recursive=True):
        grouped_targets = self._sample_force_find_success_targets(targets, requests_count)
        ordered_groups = [group_id for group_id in self.root_ids if group_id in grouped_targets]
        for group_id in grouped_targets:
            if group_id not in ordered_groups:
                ordered_groups.append(group_id)

        if not ordered_groups:
            return calculate_outcome_stats([], 0, requests_count, 0, requests_count)

        effective_concurrency = max(1, min(concurrency, len(ordered_groups)))
        click.echo(
            "Running fs-meta endpoint on-demand-force-find success path: "
            f"{effective_concurrency} effective workers across {len(ordered_groups)} groups, "
            f"requested_concurrency={concurrency}, {requests_count} reqs..."
        )

        latencies = []
        not_ready_count = 0
        other_error_count = 0
        start = time.time()
        while True:
            batch = []
            for group_id in ordered_groups:
                items = grouped_targets.get(group_id)
                if items:
                    batch.append(items.pop(0))
                if len(batch) >= effective_concurrency:
                    break
            if not batch:
                break

            with ThreadPoolExecutor(max_workers=effective_concurrency) as executor:
                futures = [
                    executor.submit(
                        run_single_fs_meta_req,
                        self.client.base_url,
                        self.client.query_headers,
                        "on-demand-force-find",
                        target.api_path,
                        recursive,
                        self.group_order,
                        self.group_page_size,
                        self.entry_page_size,
                        target.group_id,
                    )
                    for target in batch
                ]
                for future in as_completed(futures):
                    result = future.result()
                    if not isinstance(result, dict):
                        other_error_count += 1
                        continue
                    if result.get("outcome") == "ok" and result.get("latency_seconds") is not None:
                        latencies.append(result["latency_seconds"])
                    elif result.get("outcome") == "not_ready":
                        not_ready_count += 1
                    else:
                        other_error_count += 1

        stats = calculate_outcome_stats(
            latencies,
            time.time() - start,
            requests_count,
            not_ready_count,
            other_error_count,
        )
        stats["execution_mode"] = "per_group_parallel"
        stats["requested_concurrency"] = concurrency
        stats["effective_concurrency"] = effective_concurrency
        stats["qps_semantics"] = "wall_clock_success_path"
        stats["targeted_group_count"] = len(ordered_groups)
        return stats

    def run_force_find_contention(self, targets, concurrency=20, requests_count=100, recursive=True):
        grouped = {}
        for target in targets:
            grouped.setdefault(target.group_id, []).append(target)
        if not grouped:
            return calculate_outcome_stats([], 0, requests_count, 0, requests_count)
        preferred_group = next((group_id for group_id in self.root_ids if group_id in grouped), None)
        contention_group = preferred_group or sorted(grouped)[0]
        return self.run_concurrent_fs_meta_endpoint(
            "on-demand-force-find",
            grouped[contention_group],
            concurrency,
            requests_count,
            recursive=recursive,
            capture_outcomes=True,
        )

    def _extract_scope_counts(self, stats_payload):
        groups = stats_payload.get("groups", {}) if isinstance(stats_payload, dict) else {}
        if not isinstance(groups, dict):
            return 0, 0

        total_files = 0
        total_dirs = 0

        for group_payload in groups.values():
            if not isinstance(group_payload, dict):
                continue
            if group_payload.get("status") != "ok":
                continue
            data = group_payload.get("data", {})
            if not isinstance(data, dict):
                continue
            total_files += int(data.get("total_files", 0) or 0)
            total_dirs += int(data.get("total_dirs", 0) or 0)

        return total_files, total_dirs

    def run(self, concurrency=20, reqs=200, target_depth=5, integrity_interval=60.0):
        if not self._has_benchmark_data():
            raise RuntimeError("Benchmark data missing")

        if self.root_layout == "explicit-roots":
            click.echo(
                "Using explicit benchmark roots: "
                + ", ".join(
                    f"{group_id}={root_dir}" for group_id, root_dir in self._root_group_dirs()
                )
            )
        else:
            click.echo(f"Using data directory: {self.data_dir}")

        try:
            if self.mode == "local":
                self.local_runtime.start()

            if not self.client.query_api_key:
                if not self.username or not self.password:
                    raise RuntimeError("missing credentials: provide --query-api-key or management --username/--password")
                self.client.login_management(self.username, self.password)
                key_label = f"fustor-benchmark-{int(time.time())}"
                self.created_query_api_key_id = self.client.create_query_api_key(key_label)

            self.client.wait_ready(self.ready_timeout)

            targets = self._discover_targets(target_depth)

            os_baseline_stats = self.run_concurrent_os_baseline(targets, concurrency, reqs)
            os_integrity_stats = self.run_concurrent_os_integrity(targets, concurrency, reqs, integrity_interval)
            tree_stats = self.run_concurrent_fs_meta_endpoint("tree", targets, concurrency, reqs, recursive=True)
            find_success_stats = self.run_force_find_success(targets, concurrency, reqs, recursive=True)
            find_contention_stats = self.run_force_find_contention(targets, concurrency, reqs, recursive=True)

            stats_payload = self.client.get_stats(path=self.path, recursive=True, group=self.stats_group)
            total_files, total_dirs = self._extract_scope_counts(stats_payload)

            results = {
                "metadata": {
                    "total_files_in_scope": total_files,
                    "total_directories_in_scope": total_dirs,
                    "source_path": self.data_dir,
                    "source_roots": [
                        {"group_id": group_id, "root_dir": root_dir}
                        for group_id, root_dir in self._root_group_dirs()
                    ],
                    "api_endpoint": self.client.base_url,
                    "integrity_interval": integrity_interval,
                    "baseline_model": "multi_nfs_submission_discovery",
                    "baseline_nfs_root_count": len(self._root_group_dirs()),
                    "group_order": self.group_order,
                    "group_page_size": self.group_page_size,
                    "entry_page_size": self.entry_page_size,
                    "stats_group": self.stats_group,
                    "root_layout": self.root_layout,
                    "root_ids": self.root_ids,
                    "fixture_binary_sha256": os.environ.get("FS_META_FIXTURE_SHA256"),
                    "fixture_binary_mtime_epoch_s": os.environ.get("FS_META_FIXTURE_MTIME_EPOCH_S"),
                    "fixture_binary_size_bytes": os.environ.get("FS_META_FIXTURE_SIZE_BYTES"),
                },
                "depth": target_depth,
                "requests": reqs,
                "concurrency": concurrency,
                "target_directory_count": len(targets),
                "os_baseline": os_baseline_stats,
                "os_integrity": os_integrity_stats,
                "tree_materialized": tree_stats,
                "find_on_demand_success": find_success_stats,
                "find_on_demand_contention": find_contention_stats,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            }

            results_dir = os.path.join(self.run_dir, "results")
            os.makedirs(results_dir, exist_ok=True)

            json_path = os.path.join(results_dir, "query-find.json")
            html_path = os.path.join(results_dir, "query-find.html")
            with open(json_path, "w", encoding="utf-8") as file:
                json.dump(results, file, indent=2)

            generate_html_report(results, html_path)

            click.echo("\n" + "=" * 120)
            click.echo(f"QUERY/FIND BENCHMARK (DEPTH {target_depth}, INTERVAL {integrity_interval}s)")
            click.echo(f"Data Scale: {total_files:,} files | Targets: {len(targets)}")
            click.echo("=" * 120)
            click.echo(
                f"{'Metric':<24} | {'OS Baseline':<18} | {'OS Integrity':<18} | {'tree':<18} | {'on-demand-find(success)':<18}"
            )
            click.echo("-" * 120)

            def fmt_latency(bucket):
                return f"{bucket['avg']:10.2f} ms"

            def fmt_qps(bucket):
                return f"{bucket['qps']:10.1f}"

            click.echo(
                f"{'Avg Latency':<24} | {fmt_latency(os_baseline_stats)} | {fmt_latency(os_integrity_stats)} | {fmt_latency(tree_stats)} | {fmt_latency(find_success_stats)}"
            )
            click.echo(
                f"{'P50 Latency':<24} | {os_baseline_stats['p50']:10.2f} ms | {os_integrity_stats['p50']:10.2f} ms | {tree_stats['p50']:10.2f} ms | {find_success_stats['p50']:10.2f} ms"
            )
            click.echo(
                f"{'P99 Latency':<24} | {os_baseline_stats['p99']:10.2f} ms | {os_integrity_stats['p99']:10.2f} ms | {tree_stats['p99']:10.2f} ms | {find_success_stats['p99']:10.2f} ms"
            )
            click.echo(
                f"{'Throughput (QPS)':<24} | {fmt_qps(os_baseline_stats):>16} | {fmt_qps(os_integrity_stats):>16} | {fmt_qps(tree_stats):>16} | {fmt_qps(find_success_stats):>16}"
            )
            click.echo("-" * 120)
            click.echo(
                "on-demand contention: "
                f"success={find_contention_stats['success_count']} "
                f"not_ready={find_contention_stats['not_ready_count']} "
                f"other_error={find_contention_stats['other_error_count']}"
            )
            click.echo(click.style(f"JSON results saved to: {json_path}", fg="cyan"))
            click.echo(click.style(f"HTML report saved to: {html_path}", fg="green", bold=True))
        finally:
            revoke_error = None
            if self.created_query_api_key_id:
                try:
                    self.client.revoke_query_api_key(self.created_query_api_key_id)
                except Exception as exc:  # noqa: BLE001
                    revoke_error = exc
            if self.mode == "local":
                self.local_runtime.stop()
            if revoke_error is not None:
                click.echo(
                    f"WARNING: failed to revoke temporary query API key {self.created_query_api_key_id}: {revoke_error}",
                    err=True,
                )
