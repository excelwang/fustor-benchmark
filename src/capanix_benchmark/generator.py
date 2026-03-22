import os
import uuid
import time
import click
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


SAFE_RUN_SUFFIX = "capanix-benchmark-run"


class DataGenerator:
    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.submit_dir = os.path.join(self.base_dir, "upload/submit")

    def _create_batch(self, args):
        uuid_path, num_subdirs, files_per_subdir = args
        for subdir_idx in range(num_subdirs):
            sub_path = os.path.join(uuid_path, f"sub_{subdir_idx}")
            os.makedirs(sub_path, exist_ok=True)
            for file_idx in range(files_per_subdir):
                file_path = os.path.join(sub_path, f"data_{file_idx:04d}.dat")
                with open(file_path, "w", encoding="utf-8"):
                    pass

    def _is_within_safe_run_dir(self):
        path = Path(self.base_dir)
        return any(parent.name == SAFE_RUN_SUFFIX for parent in [path, *path.parents])

    def _generate_single_root(
        self,
        base_dir: str,
        num_dirs: int,
        num_subdirs: int,
        files_per_subdir: int,
    ):
        submit_dir = os.path.join(base_dir, "upload/submit")
        total_files = num_dirs * num_subdirs * files_per_subdir
        click.echo(f"Generating {total_files:,} files in {num_dirs} UUID directories...")
        click.echo(f"Structure: {submit_dir}/{{c1}}/{{c2}}/{{uuid}}/sub_X/{{files}}")

        tasks = []
        for _ in range(num_dirs):
            uid = str(uuid.uuid4())
            uuid_dir = os.path.join(submit_dir, uid[0], uid[1], uid)
            tasks.append((uuid_dir, num_subdirs, files_per_subdir))

        start = time.time()
        workers = max(1, (os.cpu_count() or 1) * 4)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            list(executor.map(self._create_batch, tasks))

        duration = time.time() - start
        throughput = (total_files / duration) if duration > 0 else 0
        click.echo(f"Generation Complete: {duration:.2f}s (Average: {throughput:.1f} files/sec)")
        return base_dir

    def _split_named_root_dir_counts(self, num_dirs: int, root_ids: list[str]) -> list[tuple[str, int]]:
        root_count = max(1, len(root_ids))
        base_count = num_dirs // root_count
        remainder = num_dirs % root_count
        counts = []
        for idx, root_id in enumerate(root_ids):
            counts.append((root_id, base_count + (1 if idx < remainder else 0)))
        return counts

    def generate(
        self,
        num_dirs: int = 1000,
        num_subdirs: int = 4,
        files_per_subdir: int = 250,
        root_layout: str = "single-root",
        root_ids: list[str] | None = None,
    ):
        if not self._is_within_safe_run_dir():
            click.echo(
                click.style(
                    "FATAL: Operation denied. Target path must be within a 'capanix-benchmark-run' directory.",
                    fg="red",
                    bold=True,
                )
            )
            return

        if os.path.exists(self.base_dir) and os.listdir(self.base_dir):
            click.echo(click.style(f"FATAL: Target directory '{self.base_dir}' is NOT empty.", fg="red", bold=True))
            click.echo(click.style("To prevent data loss, generate will not automatically delete existing content.", fg="yellow"))
            click.echo(click.style("Please manually clear the directory if you wish to re-generate.", fg="cyan"))
            return

        os.makedirs(self.base_dir, exist_ok=True)
        if root_layout == "named-roots":
            effective_root_ids = root_ids or ["nfs1", "nfs2", "nfs3"]
            root_counts = self._split_named_root_dir_counts(num_dirs, effective_root_ids)
            click.echo(
                f"Named-roots layout: distributing {num_dirs} total UUID directories across "
                f"{len(effective_root_ids)} roots"
            )
            for root_id, root_num_dirs in root_counts:
                os.makedirs(os.path.join(self.base_dir, root_id), exist_ok=True)
                self._generate_single_root(
                    os.path.join(self.base_dir, root_id),
                    num_dirs=root_num_dirs,
                    num_subdirs=num_subdirs,
                    files_per_subdir=files_per_subdir,
                )
            return self.base_dir

        return self._generate_single_root(
            self.base_dir,
            num_dirs=num_dirs,
            num_subdirs=num_subdirs,
            files_per_subdir=files_per_subdir,
        )
