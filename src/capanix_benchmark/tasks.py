import os
import subprocess
import time

import requests

TREE_SCOPE_MISMATCH_RETRY_LIMIT = 1


def _resolve_local_target_path(data_dir, subdir):
    if os.path.isabs(subdir):
        return os.path.abspath(subdir)
    return os.path.abspath(os.path.join(data_dir, subdir.lstrip("/")))


def _run_find_command(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def _normalize_root_groups(spec):
    root_groups = spec.get("root_groups")
    if root_groups:
        normalized = []
        for item in root_groups:
            if isinstance(item, dict):
                group_id = item.get("group_id")
                root_dir = item.get("root_dir")
            else:
                group_id, root_dir = item
            if not group_id or not root_dir:
                continue
            normalized.append({"group_id": str(group_id), "root_dir": str(root_dir)})
        return normalized

    return [
        {
            "group_id": os.path.basename(os.path.normpath(root_dir)),
            "root_dir": str(root_dir),
        }
        for root_dir in spec.get("root_dirs", [])
        if root_dir
    ]


def _search_submission_dirs(root_groups, submission_id):
    discovered = []
    metrics = {
        "roots_scanned": len(root_groups),
        "roots_with_search_path": 0,
        "discovery_find_calls": 0,
        "discovery_match_lines": 0,
    }

    for root_group in root_groups:
        root_label = root_group["group_id"]
        root_dir = root_group["root_dir"]
        search_root = os.path.join(root_dir, "upload", "submit")
        if not os.path.isdir(search_root):
            continue
        metrics["roots_with_search_path"] += 1
        metrics["discovery_find_calls"] += 1
        result = _run_find_command(
            ["find", search_root, "-type", "d", "-name", submission_id, "-print"]
        )
        matches = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        metrics["discovery_match_lines"] += len(matches)
        for match in matches:
            discovered.append((root_label, match))

    return discovered, metrics


def _collect_submission_snapshot(spec):
    submission_id = spec["submission_id"]
    root_groups = _normalize_root_groups(spec)
    discovered, discovery_metrics = _search_submission_dirs(root_groups, submission_id)

    inventory = {}
    metrics = {
        **discovery_metrics,
        "candidate_count": len(discovered),
        "metadata_find_calls": 0,
        "metadata_lines_parsed": 0,
        "file_count": 0,
        "dir_count": 0,
    }

    for root_label, candidate_dir in discovered:
        metrics["metadata_find_calls"] += 1
        result = _run_find_command(
            ["find", candidate_dir, "-printf", "%P|%y|%s|%T@|%C@\\n"]
        )
        for line in result.stdout.splitlines():
            parts = line.split("|")
            if len(parts) != 5:
                continue
            relative_path, content_type, size, modified_time, created_time = parts
            normalized_relative_path = relative_path or "."
            inventory[f"{root_label}:{normalized_relative_path}"] = (
                content_type,
                size,
                modified_time,
                created_time,
            )
            metrics["metadata_lines_parsed"] += 1
            if content_type == "d":
                metrics["dir_count"] += 1
            else:
                metrics["file_count"] += 1

    return {
        "submission_id": submission_id,
        "inventory": inventory,
        "candidate_count": metrics["candidate_count"],
    }, metrics


def run_find_recursive_metadata_task(args):
    data_dir, subdir = args
    target = _resolve_local_target_path(data_dir, subdir)

    cmd = ["find", target, "-printf", "%p|%y|%s|%T@|%C@\\n"]
    start = time.time()
    result = _run_find_command(cmd)

    for line in result.stdout.splitlines():
        parts = line.split("|")
        if len(parts) != 5:
            continue
        _ = {
            "name": os.path.basename(parts[0]),
            "path": parts[0],
            "content_type": "directory" if parts[1] == "d" else "file",
            "size": int(parts[2]),
            "modified_time": float(parts[3]),
            "created_time": float(parts[4]),
        }

    return time.time() - start


def run_multi_nfs_submission_baseline_task(spec):
    start = time.time()
    _snapshot, metrics = _collect_submission_snapshot(spec)
    metrics["latency_seconds"] = time.time() - start
    return metrics


def build_fs_meta_request_params(
    endpoint,
    path,
    recursive,
    group_order,
    group_page_size,
    entry_page_size,
    group=None,
    pit_id=None,
    group_after=None,
    entry_after=None,
):
    params = {
        "path": path,
        "recursive": "true" if recursive else "false",
        "group_order": group_order,
        "group_page_size": str(group_page_size),
        "entry_page_size": str(entry_page_size),
        "stability_mode": "none",
        "metadata_mode": "full",
    }

    if pit_id:
        params["pit_id"] = pit_id
    if group_after:
        params["group_after"] = group_after
    if entry_after:
        params["entry_after"] = entry_after
    if group:
        params["group"] = group

    if endpoint not in {"tree", "on-demand-force-find"}:
        raise ValueError(f"unsupported fs-meta query endpoint: {endpoint}")

    return params


def run_single_fs_meta_req(
    base_url,
    headers,
    endpoint,
    path,
    recursive,
    group_order,
    group_page_size,
    entry_page_size,
    group=None,
):
    start = time.time()
    pit_id = None
    group_after = None
    entry_after = None
    pending_group_after = None
    current_group_after = None
    timeout = 60 if endpoint == "on-demand-force-find" else 30
    restart_count = 0

    while True:
        request_group_after = group_after
        params = build_fs_meta_request_params(
            endpoint=endpoint,
            path=path,
            recursive=recursive,
            group_order=group_order,
            group_page_size=group_page_size,
            entry_page_size=entry_page_size,
            group=group,
            pit_id=pit_id,
            group_after=group_after,
            entry_after=entry_after,
        )

        try:
            response = requests.get(
                f"{base_url}/api/fs-meta/v1/{endpoint}",
                params=params,
                headers=headers,
                timeout=timeout,
            )
            if response.status_code != 200:
                if (
                    endpoint == "tree"
                    and pit_id is not None
                    and restart_count < TREE_SCOPE_MISMATCH_RETRY_LIMIT
                    and is_tree_pit_scope_mismatch(response)
                ):
                    pit_id = None
                    group_after = None
                    entry_after = None
                    pending_group_after = None
                    current_group_after = None
                    restart_count += 1
                    continue
                return {
                    "outcome": classify_fs_meta_error(response),
                    "latency_seconds": None,
                }
            payload = response.json()
        except (requests.RequestException, ValueError):
            return {
                "outcome": "error",
                "latency_seconds": None,
            }

        if not isinstance(payload, dict):
            return {
                "outcome": "error",
                "latency_seconds": None,
            }

        if pit_id is None:
            pit = payload.get("pit", {})
            if not isinstance(pit, dict):
                return {
                    "outcome": "error",
                    "latency_seconds": None,
                }
            pit_id = pit.get("id")
            if not pit_id:
                return {
                    "outcome": "error",
                    "latency_seconds": None,
                }

        group_page = payload.get("group_page", {})
        if not isinstance(group_page, dict):
            return {
                "outcome": "error",
                "latency_seconds": None,
            }

        next_group_after = group_page.get("next_cursor")
        next_entry_after = group_page.get("next_entry_after")
        current_group_after = request_group_after

        # Entry pagination must stay within the current group page. The caller
        # must keep sending the same group page selector while draining
        # next_entry_after, then advance to the next group page only after the
        # current page's entries are exhausted.
        if next_entry_after:
            pending_group_after = next_group_after
            group_after = current_group_after
            entry_after = next_entry_after
            continue

        group_after = pending_group_after if pending_group_after else next_group_after
        pending_group_after = None
        entry_after = None
        if not group_after:
            break

    return {
        "outcome": "ok",
        "latency_seconds": time.time() - start,
    }


def classify_fs_meta_error(response):
    code = None
    message = ""
    try:
        payload = response.json()
    except ValueError:
        payload = None

    if isinstance(payload, dict):
        code = payload.get("code")
        message = str(payload.get("message") or payload.get("error") or "")
    elif response.text:
        message = response.text

    message = message.lower()
    if response.status_code in {409, 423, 429, 503}:
        if code in {"NOT_READY", "FORCE_FIND_INFLIGHT_CONFLICT", "PIT_CAPACITY_EXCEEDED"}:
            return "not_ready"
        if "not ready" in message or "inflight" in message or "conflict" in message:
            return "not_ready"
    return "error"


def is_tree_pit_scope_mismatch(response):
    if response.status_code != 400:
        return False

    try:
        payload = response.json()
    except ValueError:
        return False

    if not isinstance(payload, dict):
        return False

    code = payload.get("code")
    message = str(payload.get("message") or payload.get("error") or "").lower()
    return code == "INVALID_INPUT" and "pit_id does not match the requested tree scope" in message


def run_find_sampling_phase(args):
    data_dir, subdir = args
    target = _resolve_local_target_path(data_dir, subdir)

    start = time.time()
    cmd = ["find", target, "-type", "f", "-printf", "%p\\t%C@\\t%s\\n"]
    result = _run_find_command(cmd)

    metadata = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            metadata[parts[0]] = (parts[1], parts[2])

    return time.time() - start, metadata


def run_multi_nfs_submission_sampling_phase(spec):
    start = time.time()
    snapshot, metrics = _collect_submission_snapshot(spec)
    return time.time() - start, snapshot, metrics


def run_multi_nfs_submission_validation_phase(args):
    spec, previous_snapshot = args
    start = time.time()
    snapshot, metrics = _collect_submission_snapshot(spec)
    metrics["stable"] = snapshot["inventory"] == previous_snapshot["inventory"]
    metrics["latency_seconds"] = time.time() - start
    return metrics


def run_find_validation_phase(args):
    metadata, interval = args

    start = time.time()
    now = time.time()
    for path, (old_ctime, old_size) in metadata.items():
        try:
            if now - float(old_ctime) < interval:
                continue
            st = os.stat(path)
            _stable = str(st.st_size) == old_size and f"{st.st_ctime:.6f}" == old_ctime
        except (OSError, ValueError):
            continue

    return time.time() - start
