# capanix-benchmark

`capanix-benchmark` is a benchmark toolkit for current `fs-meta HTTP v1` surfaces.

## Scope

It compares:

- `os_baseline`: recursive `find` metadata walk
- `os_integrity`: sampled + silence-window validation
- `tree_materialized`: `GET /api/fs-meta/v1/tree`
- `find_on_demand`: `GET /api/fs-meta/v1/on-demand-force-find`

## Quick Start

```bash
uv run capanix-benchmark generate capanix-benchmark-run/data --num-dirs 200
uv run capanix-benchmark query capanix-benchmark-run/data \
  --base-url http://127.0.0.1:18102 \
  --username admin --password admin
```

## Runtime Modes

- `external` (default): connect to an already running fs-meta service.
- `local`: execute caller-provided `--start-cmd` / `--stop-cmd` lifecycle hooks.

## Notes

- All benchmark artifacts are written to `./capanix-benchmark-run/`.
- The data generator refuses to run unless target path is inside `capanix-benchmark-run`.
