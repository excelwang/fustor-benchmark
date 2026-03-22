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
@click.option(
    "--username",
    default=None,
    help="fs-meta management username used by /session/login when minting a temporary query API key.",
)
@click.option(
    "--password",
    default=None,
    help="fs-meta management password used by /session/login when minting a temporary query API key.",
)
@click.option("--query-api-key", default=None, help="Query API key used on /tree, /stats, and /on-demand-force-find.")
@click.option("--token", "legacy_query_api_key", default=None, hidden=True)
@click.option("--stats-group", default=None, help="Optional /stats group query parameter.")
@click.option("--group", "legacy_stats_group", default=None, hidden=True)
@click.option("--path", default="/", show_default=True, help="Stats query path.")
@click.option(
    "--group-order",
    type=click.Choice(["group-key", "file-count", "file-age"]),
    default="group-key",
    show_default=True,
    help="Group ordering for /tree and /on-demand-force-find.",
)
@click.option(
    "--group-page-size",
    default=1,
    show_default=True,
    type=click.IntRange(1, 1000),
    help="Number of groups fetched per query page.",
)
@click.option(
    "--entry-page-size",
    default=1000,
    show_default=True,
    type=click.IntRange(1, 10000),
    help="Number of metadata entries fetched per returned group page.",
)
@click.option("--mode", type=click.Choice(["external", "local"]), default="external", show_default=True, help="Runtime mode.")
@click.option("--start-cmd", default=None, help="Local mode startup command.")
@click.option("--stop-cmd", default=None, help="Local mode shutdown command.")
@click.option("--ready-timeout", default=120.0, show_default=True, help="Seconds to wait for service readiness.")
@click.option(
    "--root-layout",
    type=click.Choice(["single-root", "named-roots"]),
    default="single-root",
    show_default=True,
    help="How TARGET_DIR is interpreted for benchmark root/group layout.",
)
@click.option(
    "--root-ids",
    default="nfs1,nfs2,nfs3",
    show_default=True,
    help="Comma-separated root ids used when --root-layout=named-roots.",
)
def query(
    target_dir,
    concurrency,
    num_requests,
    target_depth,
    integrity_interval,
    base_url,
    username,
    password,
    query_api_key,
    legacy_query_api_key,
    stats_group,
    legacy_stats_group,
    path,
    group_order,
    group_page_size,
    entry_page_size,
    mode,
    start_cmd,
    stop_cmd,
    ready_timeout,
    root_layout,
    root_ids,
):
    """Run query/find benchmark against fs-meta HTTP v1."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    effective_query_api_key = query_api_key or legacy_query_api_key
    effective_stats_group = stats_group or legacy_stats_group
    runner = BenchmarkRunner(
        run_dir=run_dir,
        target_dir=target_dir,
        base_url=base_url,
        query_api_key=effective_query_api_key,
        username=username,
        password=password,
        stats_group=effective_stats_group,
        path=path,
        group_order=group_order,
        group_page_size=group_page_size,
        entry_page_size=entry_page_size,
        mode=mode,
        start_cmd=start_cmd,
        stop_cmd=stop_cmd,
        ready_timeout=ready_timeout,
        root_layout=root_layout,
        root_ids=[item.strip() for item in root_ids.split(",") if item.strip()],
    )

    runner.run(
        concurrency=concurrency,
        reqs=num_requests,
        target_depth=target_depth,
        integrity_interval=integrity_interval,
    )


@cli.command()
@click.argument("target_dir", type=click.Path(exists=False))
@click.option(
    "--num-dirs",
    default=1000,
    show_default=True,
    help="Total number of UUID directories across the whole benchmark dataset.",
)
@click.option("--num-subdirs", default=4, show_default=True, help="Subdirectories per UUID directory.")
@click.option("--files-per-subdir", default=250, show_default=True, help="Files per subdirectory.")
@click.option(
    "--root-layout",
    type=click.Choice(["single-root", "named-roots"]),
    default="single-root",
    show_default=True,
    help="How TARGET_DIR should be populated.",
)
@click.option(
    "--root-ids",
    default="nfs1,nfs2,nfs3",
    show_default=True,
    help="Comma-separated root ids used when --root-layout=named-roots.",
)
def generate(target_dir, num_dirs, num_subdirs, files_per_subdir, root_layout, root_ids):
    """Generate benchmark dataset in TARGET_DIR."""
    generator = DataGenerator(os.path.abspath(target_dir))
    generator.generate(
        num_dirs=num_dirs,
        num_subdirs=num_subdirs,
        files_per_subdir=files_per_subdir,
        root_layout=root_layout,
        root_ids=[item.strip() for item in root_ids.split(",") if item.strip()],
    )


if __name__ == "__main__":
    cli()
