import os

import click

from .generator import DataGenerator
from .runner import BenchmarkRunner


DEFAULT_RUN_DIR = "capanix-benchmark-run"


@click.group()
def cli():
    """Capanix benchmark toolkit."""


@cli.command()
@click.argument("target_dir", type=click.Path(exists=True))
@click.option("-c", "--concurrency", default=20, show_default=True, help="Number of concurrent workers.")
@click.option("-n", "--num-requests", default=200, show_default=True, help="Total number of requests.")
@click.option("-d", "--target-depth", default=5, show_default=True, help="Relative depth for target path sampling.")
@click.option("--integrity-interval", default=60.0, show_default=True, help="Silence window in seconds for OS integrity mode.")
@click.option("--base-url", default="http://127.0.0.1:18102", show_default=True, help="fs-meta HTTP base URL.")
@click.option("--username", default=None, help="fs-meta username used by /auth/login.")
@click.option("--password", default=None, help="fs-meta password used by /auth/login.")
@click.option("--token", default=None, help="Bearer token (skip login when provided).")
@click.option("--group", default=None, help="Optional group query parameter.")
@click.option("--path", default="/", show_default=True, help="Stats query path.")
@click.option("--limit", default=500000, show_default=True, type=click.IntRange(1, 500000), help="query/find limit parameter.")
@click.option("--mode", type=click.Choice(["external", "local"]), default="external", show_default=True, help="Runtime mode.")
@click.option("--start-cmd", default=None, help="Local mode startup command.")
@click.option("--stop-cmd", default=None, help="Local mode shutdown command.")
@click.option("--ready-timeout", default=120.0, show_default=True, help="Seconds to wait for service readiness.")
def query(
    target_dir,
    concurrency,
    num_requests,
    target_depth,
    integrity_interval,
    base_url,
    username,
    password,
    token,
    group,
    path,
    limit,
    mode,
    start_cmd,
    stop_cmd,
    ready_timeout,
):
    """Run query/find benchmark against fs-meta HTTP v1."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    runner = BenchmarkRunner(
        run_dir=run_dir,
        target_dir=target_dir,
        base_url=base_url,
        token=token,
        username=username,
        password=password,
        group=group,
        path=path,
        limit=limit,
        mode=mode,
        start_cmd=start_cmd,
        stop_cmd=stop_cmd,
        ready_timeout=ready_timeout,
    )

    runner.run(
        concurrency=concurrency,
        reqs=num_requests,
        target_depth=target_depth,
        integrity_interval=integrity_interval,
    )


@cli.command()
@click.argument("target_dir", type=click.Path(exists=False))
@click.option("--num-dirs", default=1000, show_default=True, help="Number of UUID directories.")
@click.option("--num-subdirs", default=4, show_default=True, help="Subdirectories per UUID directory.")
@click.option("--files-per-subdir", default=250, show_default=True, help="Files per subdirectory.")
def generate(target_dir, num_dirs, num_subdirs, files_per_subdir):
    """Generate benchmark dataset in TARGET_DIR."""
    generator = DataGenerator(os.path.abspath(target_dir))
    generator.generate(num_dirs=num_dirs, num_subdirs=num_subdirs, files_per_subdir=files_per_subdir)


if __name__ == "__main__":
    cli()
