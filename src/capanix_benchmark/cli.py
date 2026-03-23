import os

import click

from .generator import DataGenerator
from .runner import BenchmarkRunner
from .scale_breakpoint import analyze_scale_breakpoint, write_analysis_outputs


DEFAULT_RUN_DIR = "capanix-benchmark-run"


def parse_root_specs(root_specs_value):
    if not root_specs_value:
        return []

    root_specs = []
    seen_ids = set()
    for item in root_specs_value.split(","):
        spec = item.strip()
        if not spec:
            continue
        if "=" not in spec:
            raise click.BadParameter(
                f"invalid root spec '{spec}'; expected <group_id>=<absolute_or_relative_path>"
            )
        group_id, raw_path = spec.split("=", 1)
        group_id = group_id.strip()
        root_path = raw_path.strip()
        if not group_id or not root_path:
            raise click.BadParameter(
                f"invalid root spec '{spec}'; both group_id and path are required"
            )
        if group_id in seen_ids:
            raise click.BadParameter(f"duplicate group_id in --root-specs: {group_id}")
        seen_ids.add(group_id)
        root_specs.append((group_id, os.path.abspath(root_path)))

    if not root_specs:
        raise click.BadParameter("--root-specs did not contain any valid entries")

    return root_specs


@click.group()
def cli():
    """Capanix benchmark toolkit."""


@cli.command()
@click.argument("target_dir", type=click.Path(exists=False))
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
    type=click.Choice(["single-root", "named-roots", "explicit-roots"]),
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
@click.option(
    "--root-specs",
    default=None,
    help="Comma-separated explicit root specs in the form group_id=/abs/path, used when --root-layout=explicit-roots.",
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
    root_specs,
):
    """Run query/find benchmark against fs-meta HTTP v1."""
    run_dir = os.path.abspath(DEFAULT_RUN_DIR)
    effective_query_api_key = query_api_key or legacy_query_api_key
    effective_stats_group = stats_group or legacy_stats_group
    parsed_root_specs = parse_root_specs(root_specs)
    if root_layout == "explicit-roots" and not parsed_root_specs:
        raise click.BadParameter("--root-specs is required when --root-layout=explicit-roots")
    if root_layout != "explicit-roots" and parsed_root_specs:
        raise click.BadParameter("--root-specs can only be used when --root-layout=explicit-roots")
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
        root_specs=parsed_root_specs,
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


@cli.command("scale-breakpoint")
@click.argument("result_paths", nargs=-1, type=click.Path(exists=True, dir_okay=False))
@click.option("--anchor-files", default=500_000_000, show_default=True, type=click.IntRange(1))
@click.option(
    "--latency-regression-tolerance",
    default=0.10,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Allowed p95 slowdown versus baseline before a scale point fails not-worse judgement.",
)
@click.option(
    "--qps-regression-tolerance",
    default=0.10,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Allowed QPS drop versus baseline before a scale point fails not-worse judgement.",
)
@click.option(
    "--min-success-rate",
    default=0.99,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Minimum on-demand success rate required for not-worse judgement.",
)
@click.option(
    "--max-contention-not-ready-rate",
    default=0.05,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Maximum acceptable on-demand contention NOT_READY rate for not-worse judgement.",
)
@click.option(
    "--min-integrity-stable-rate",
    default=0.95,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Minimum OS integrity stable rate required before claiming the scale point is conclusion-ready.",
)
@click.option(
    "--breakpoint-latency-ratio",
    default=1.25,
    show_default=True,
    type=click.FloatRange(0.0, min_open=True),
    help="Breakpoint signal when p95 ratio rises above this value after the anchor scale.",
)
@click.option(
    "--breakpoint-qps-ratio",
    default=0.80,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Breakpoint signal when QPS ratio drops below this value after the anchor scale.",
)
@click.option(
    "--breakpoint-not-ready-rate",
    default=0.15,
    show_default=True,
    type=click.FloatRange(0.0, 1.0),
    help="Breakpoint signal when contention NOT_READY rate rises above this value after the anchor scale.",
)
@click.option(
    "--consecutive-points",
    default=2,
    show_default=True,
    type=click.IntRange(1),
    help="Number of consecutive scale points required to confirm a breakpoint.",
)
@click.option("--output-json", default=None, help="Optional output path for structured breakpoint analysis JSON.")
@click.option("--output-md", default=None, help="Optional output path for markdown summary.")
def scale_breakpoint(
    result_paths,
    anchor_files,
    latency_regression_tolerance,
    qps_regression_tolerance,
    min_success_rate,
    max_contention_not_ready_rate,
    min_integrity_stable_rate,
    breakpoint_latency_ratio,
    breakpoint_qps_ratio,
    breakpoint_not_ready_rate,
    consecutive_points,
    output_json,
    output_md,
):
    """Evaluate scale breakpoint conclusions across multiple query-find.json results."""
    if not result_paths:
        raise click.UsageError("at least one query-find.json result path is required")

    thresholds = {
        "anchor_files": anchor_files,
        "latency_regression_tolerance": latency_regression_tolerance,
        "qps_regression_tolerance": qps_regression_tolerance,
        "min_success_rate": min_success_rate,
        "max_contention_not_ready_rate": max_contention_not_ready_rate,
        "min_integrity_stable_rate": min_integrity_stable_rate,
        "breakpoint_latency_ratio": breakpoint_latency_ratio,
        "breakpoint_qps_ratio": breakpoint_qps_ratio,
        "breakpoint_not_ready_rate": breakpoint_not_ready_rate,
        "consecutive_points": consecutive_points,
    }

    analysis = analyze_scale_breakpoint(result_paths, thresholds)
    markdown = write_analysis_outputs(
        analysis,
        output_json=output_json,
        output_md=output_md,
    )

    anchor = analysis["anchor_verdict"]
    breakpoint = analysis["breakpoint_verdict"]

    click.echo(
        "Scale breakpoint evaluation: "
        f"anchor_present={anchor['anchor_present']} "
        f"anchor_not_worse={anchor['anchor_scale_not_worse_than_baseline']} "
        f"anchor_ready={anchor['anchor_scale_conclusion_ready']} "
        f"breakpoint_detected={breakpoint['breakpoint_detected']}"
    )
    if output_json:
        click.echo(click.style(f"Analysis JSON saved to: {os.path.abspath(output_json)}", fg="cyan"))
    if output_md:
        click.echo(click.style(f"Analysis markdown saved to: {os.path.abspath(output_md)}", fg="green"))
    if not output_md:
        click.echo()
        click.echo(markdown)


if __name__ == "__main__":
    cli()
