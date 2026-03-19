import json
import os
import random
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed

import click
import requests

from .generator import DataGenerator
from .reporter import calculate_stats, generate_html_report
from .tasks import (
    run_find_recursive_metadata_task,
    run_find_sampling_phase,
    run_find_validation_phase,
    run_single_fs_meta_req,
)


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
    def __init__(self, base_url: str, token: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.token = token

    @property
    def headers(self):
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}

    def login(self, username: str, password: str):
        response = requests.post(
            f"{self.base_url}/api/fs-meta/v1/auth/login",
            json={"username": username, "password": password},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
        token = payload.get("token")
        if not token:
            raise RuntimeError("login succeeded but token is missing")
        self.token = token
        return token

    def wait_ready(self, timeout_seconds: float):
        deadline = time.time() + timeout_seconds
        last_error = None
        while time.time() < deadline:
            try:
                health = requests.get(
                    f"{self.base_url}/api/fs-meta/v1/health",
                    headers=self.headers,
                    timeout=5,
                )
                if health.status_code == 200:
                    return
                last_error = f"status={health.status_code}"
            except requests.RequestException as exc:
                last_error = str(exc)
            time.sleep(1)
        raise RuntimeError(f"fs-meta service not ready within {timeout_seconds}s: {last_error}")

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
            headers=self.headers,
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
        token=None,
        username=None,
        password=None,
        group=None,
        path="/",
        limit=500000,
        mode="external",
        start_cmd=None,
        stop_cmd=None,
        ready_timeout=120.0,
    ):
        self.run_dir = os.path.abspath(run_dir)
        self.data_dir = os.path.abspath(target_dir)
        self.group = group
        self.path = path
        self.limit = limit
        self.mode = mode
        self.ready_timeout = ready_timeout

        self.client = FsMetaClient(base_url=base_url, token=token)
        self.username = username
        self.password = password

        self.local_runtime = LocalServiceRuntime(start_cmd=start_cmd, stop_cmd=stop_cmd)
        self.generator = DataGenerator(self.data_dir)

    def _discover_leaf_targets_local(self, depth: int):
        base_depth = self.data_dir.rstrip("/").count(os.sep)
        targets = []

        for root, dirs, _files in os.walk(self.data_dir):
            current_depth = root.count(os.sep) - base_depth
            if current_depth == depth:
                targets.append(root)
                dirs[:] = []
            elif current_depth > depth:
                dirs[:] = []

        if not targets:
            return [self.data_dir]

        return targets

    def _to_api_path(self, path_value: str):
        apath = os.path.abspath(path_value)
        return apath if apath.startswith("/") else f"/{apath}"

    def run_concurrent_os_baseline(self, targets, concurrency=20, requests_count=100):
        click.echo(f"Running OS baseline: {concurrency} workers, {requests_count} reqs...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_paths = [shuffled[i % len(shuffled)] for i in range(requests_count)]
        tasks = [(self.data_dir, path) for path in sampled_paths]

        latencies = []
        start = time.time()
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_recursive_metadata_task, task) for task in tasks]
            for future in as_completed(futures):
                latencies.append(future.result())

        return calculate_stats(latencies, time.time() - start, requests_count)

    def run_concurrent_os_integrity(self, targets, concurrency=20, requests_count=100, interval=60.0):
        click.echo(f"Running OS integrity: {requests_count} reqs, {concurrency} workers, wait={interval}s...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_paths = [shuffled[i % len(shuffled)] for i in range(requests_count)]

        start = time.time()
        sampling_latencies = []
        metadata_dicts = []

        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_sampling_phase, (self.data_dir, path)) for path in sampled_paths]
            for future in as_completed(futures):
                latency, metadata = future.result()
                sampling_latencies.append(latency)
                metadata_dicts.append(metadata)

        time.sleep(interval)

        validation_latencies = []
        with ProcessPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(run_find_validation_phase, (metadata, interval)) for metadata in metadata_dicts]
            for future in as_completed(futures):
                validation_latencies.append(future.result())

        total_wall = time.time() - start
        total_latencies = [s + interval + v for s, v in zip(sampling_latencies, validation_latencies)]
        return calculate_stats(total_latencies, total_wall, requests_count)

    def run_concurrent_fs_meta_endpoint(
        self,
        endpoint: str,
        targets,
        concurrency=20,
        requests_count=100,
        recursive=True,
        best=False,
        best_strategy=None,
    ):
        click.echo(f"Running fs-meta endpoint {endpoint}: {concurrency} workers, {requests_count} reqs...")
        shuffled = list(targets)
        random.shuffle(shuffled)
        sampled_paths = [self._to_api_path(shuffled[i % len(shuffled)]) for i in range(requests_count)]

        latencies = []
        start = time.time()
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(
                    run_single_fs_meta_req,
                    self.client.base_url,
                    self.client.headers,
                    endpoint,
                    path_value,
                    self.group,
                    recursive,
                    self.limit,
                    best,
                    best_strategy,
                )
                for path_value in sampled_paths
            ]
            for future in as_completed(futures):
                result = future.result()
                if result is not None:
                    latencies.append(result)

        return calculate_stats(latencies, time.time() - start, requests_count)

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
        data_exists = os.path.exists(self.data_dir) and len(os.listdir(self.data_dir)) > 0
        if not data_exists:
            raise RuntimeError("Benchmark data missing")

        click.echo(f"Using data directory: {self.data_dir}")

        try:
            if self.mode == "local":
                self.local_runtime.start()

            if not self.client.token:
                if not self.username or not self.password:
                    raise RuntimeError("missing credentials: provide --token or --username/--password")
                self.client.login(self.username, self.password)

            self.client.wait_ready(self.ready_timeout)

            targets = self._discover_leaf_targets_local(target_depth)

            os_baseline_stats = self.run_concurrent_os_baseline(targets, concurrency, reqs)
            os_integrity_stats = self.run_concurrent_os_integrity(targets, concurrency, reqs, integrity_interval)
            tree_stats = self.run_concurrent_fs_meta_endpoint("tree", targets, concurrency, reqs, recursive=True)
            find_stats = self.run_concurrent_fs_meta_endpoint(
                "on-demand-force-find",
                targets,
                concurrency,
                reqs,
                recursive=True,
            )

            stats_payload = self.client.get_stats(path=self.path, recursive=True, group=self.group)
            total_files, total_dirs = self._extract_scope_counts(stats_payload)

            results = {
                "metadata": {
                    "total_files_in_scope": total_files,
                    "total_directories_in_scope": total_dirs,
                    "source_path": self.data_dir,
                    "api_endpoint": self.client.base_url,
                    "integrity_interval": integrity_interval,
                },
                "depth": target_depth,
                "requests": reqs,
                "concurrency": concurrency,
                "target_directory_count": len(targets),
                "os_baseline": os_baseline_stats,
                "os_integrity": os_integrity_stats,
                "tree_materialized": tree_stats,
                "find_on_demand": find_stats,
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
                f"{'Metric':<24} | {'OS Baseline':<18} | {'OS Integrity':<18} | {'tree':<18} | {'on-demand-find':<18}"
            )
            click.echo("-" * 120)

            def fmt_latency(bucket):
                return f"{bucket['avg']:10.2f} ms"

            def fmt_qps(bucket):
                return f"{bucket['qps']:10.1f}"

            click.echo(
                f"{'Avg Latency':<24} | {fmt_latency(os_baseline_stats)} | {fmt_latency(os_integrity_stats)} | {fmt_latency(tree_stats)} | {fmt_latency(find_stats)}"
            )
            click.echo(
                f"{'P50 Latency':<24} | {os_baseline_stats['p50']:10.2f} ms | {os_integrity_stats['p50']:10.2f} ms | {tree_stats['p50']:10.2f} ms | {find_stats['p50']:10.2f} ms"
            )
            click.echo(
                f"{'P99 Latency':<24} | {os_baseline_stats['p99']:10.2f} ms | {os_integrity_stats['p99']:10.2f} ms | {tree_stats['p99']:10.2f} ms | {find_stats['p99']:10.2f} ms"
            )
            click.echo(
                f"{'Throughput (QPS)':<24} | {fmt_qps(os_baseline_stats):>16} | {fmt_qps(os_integrity_stats):>16} | {fmt_qps(tree_stats):>16} | {fmt_qps(find_stats):>16}"
            )
            click.echo("-" * 120)
            click.echo(click.style(f"JSON results saved to: {json_path}", fg="cyan"))
            click.echo(click.style(f"HTML report saved to: {html_path}", fg="green", bold=True))
        finally:
            if self.mode == "local":
                self.local_runtime.stop()
