import os
import subprocess
import time
import requests


def run_find_recursive_metadata_task(args):
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip("/"))

    cmd = ["find", target, "-printf", "%p|%y|%s|%T@|%C@\\n"]
    start = time.time()
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

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


def run_single_fs_meta_req(
    base_url,
    headers,
    endpoint,
    path,
    group,
    recursive,
    limit,
    best,
    best_strategy,
):
    start = time.time()
    params = {
        "path": path,
        "recursive": "true" if recursive else "false",
        "limit": str(limit),
    }

    if endpoint in {"tree", "on-demand-force-find"}:
        params["best"] = "true" if best else "false"
        if best_strategy:
            params["best_strategy"] = best_strategy

    if group:
        params["group"] = group

    try:
        response = requests.get(
            f"{base_url}/api/fs-meta/v1/{endpoint}",
            params=params,
            headers=headers,
            timeout=30,
        )
        if response.status_code != 200:
            return None
    except requests.RequestException:
        return None

    return time.time() - start


def run_find_sampling_phase(args):
    data_dir, subdir = args
    target = os.path.join(data_dir, subdir.lstrip("/"))

    start = time.time()
    cmd = ["find", target, "-type", "f", "-printf", "%p\\t%C@\\t%s\\n"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    metadata = {}
    for line in result.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) == 3:
            metadata[parts[0]] = (parts[1], parts[2])

    return time.time() - start, metadata


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
