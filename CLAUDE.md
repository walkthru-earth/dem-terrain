# dem-terrain

One-time pipeline: GEDTM-30m global DEM → H3-indexed native Parquet 2.11+ with terrain derivatives.

## What this does

Converts ~2,400 COG tiles from OpenLandMap STAC into partitioned Parquet files with elevation, slope, aspect, TRI, TPI per H3 cell (res 1-10). Output goes to S3. Reduces DEM load from 20-40 min to ~5 sec in walkthru-weather-index.

## Key files

- `main.py` — the entire pipeline (STAC discovery → load COG → terrain derivatives → H3 cells → DuckDB Parquet write)
- `infra/` — OpenTofu for Verda cloud (CPU Node: 360 vCPUs, 1440GB RAM)
- `.env` / `.env.example` — S3 credentials and config
- `infra/secrets.tfvars.example` — Verda + AWS credentials for cloud deploy

## Tech choices

- **DuckDB 1.5.0-dev** writes native Parquet GEOMETRY (not GeoParquet convention) — `ST_Point(lon, lat)::GEOMETRY('EPSG:4326')` gives per-row-group bbox stats and geometry shredding automatically
- **CPU Node** (no GPU) — terrain derivatives run fine on NumPy with 360 cores. CuPy/GPU fallback exists but isn't needed
- **H3 parent res 2** partitioning for res 6-10 (~5,882 partitions, 10-70MB each)
- **Checkpointing** — `checkpoint.json` tracks completed tiles, pipeline is resumable

## Output layout

```
s3://{bucket}/{prefix}/dem-terrain/
  h3_res=1/data.parquet                           # single file (tiny)
  ...
  h3_res=5/data.parquet                           # single file (~10MB)
  h3_res=6/h3_parent_2={id}/data.parquet          # hive-partitioned
  ...
  h3_res=10/h3_parent_2={id}/data.parquet         # ~170GB total
```

## Schema per file

`h3_index` (VARCHAR), `geometry` (GEOMETRY POINT EPSG:4326), `lat` `lon` `elev` `slope` `aspect` `tri` `tpi` (all FLOAT)

## Commands

```bash
uv sync                                              # install deps
uv run python main.py --dry-run                      # test STAC connectivity
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
