# dem-terrain

One-time pipeline: GEDTM-30m global DEM → H3-indexed native Parquet 2.11+ with terrain derivatives.

## What this does

Reads a single global GEDTM-30m COG (~305 GB) via windowed reads, computes terrain derivatives, and writes single Parquet files per H3 resolution (1-10) with elevation, slope, aspect, TRI, TPI. Output goes to S3. Reduces DEM load from 20-40 min to ~5 sec in walkthru-weather-index.

## Key files

- `main.py` — the entire pipeline (windowed COG reads → terrain derivatives → H3 cells → DuckDB Parquet write)
- `infra/` — OpenTofu for Verda cloud (CPU Node: 360 vCPUs, 1440GB RAM)
- `.env` / `.env.example` — S3 credentials and config
- `infra/secrets.tfvars.example` — Verda + AWS credentials for cloud deploy

## Tech choices

- **DuckDB 1.5.0-dev** writes native Parquet GEOMETRY (not GeoParquet convention) — `ST_Point(lon, lat)::GEOMETRY('EPSG:4326')` gives per-row-group bbox stats and geometry shredding automatically
- **CPU Node** (no GPU) — terrain derivatives run fine on NumPy with 360 cores. CuPy/GPU fallback exists but isn't needed
- **Single file per resolution** — no Hive partitioning; DuckDB native geometry gives per-row-group bbox pushdown
- **Local COG auto-detection** — uses local file on NVMe if available, falls back to remote URL
- **Checkpointing** — `checkpoint.json` tracks completed windows, pipeline is resumable

## Output layout

```
s3://{bucket}/{prefix}/
  h3_res=1/data.parquet      # ~1 KB
  h3_res=2/data.parquet      # ~20 KB
  ...
  h3_res=10/data.parquet     # ~170 GB (single file, sorted by h3_index)
  _metadata.json
```

## Schema per file

`h3_index` (VARCHAR), `geometry` (GEOMETRY POINT EPSG:4326), `lat` `lon` `elev` `slope` `aspect` `tri` `tpi` (all FLOAT)

## Commands

```bash
uv sync                                              # install deps
uv run python main.py --dry-run                      # list windows without processing
uv run python main.py --resolutions 1,2,3 --scratch-dir ./scratch  # local test
uv run ruff check . && uv run ruff format .          # lint + format
```

## Cloud deploy

```bash
cd infra/
tofu init && tofu apply -var-file="secrets.tfvars"   # spin up
ssh root@<ip>                                         # connect
tmux attach -t dem                                    # watch pipeline
tail -f /data/scratch/pipeline.log                    # or tail logs
tofu destroy -var-file="secrets.tfvars"               # tear down after
```

## Conventions

- Python 3.12+, pathlib (no os.path), ruff for lint+format
- All deps pinned to exact versions in pyproject.toml
- Logging is verbose — every tile logs: load time, raster size, terrain derivative time, H3 cell count, memory usage
- `.env` for Python vars, `secrets.tfvars` for OpenTofu vars (both gitignored)
