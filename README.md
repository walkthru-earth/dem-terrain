# dem-terrain

One-time pipeline that converts the [GEDTM-30m](https://stac.openlandmap.org/gedtm-30m/collection.json) global DEM into H3-indexed [native Parquet 2.11+](https://github.com/apache/parquet-format/blob/master/Geospatial.md) files with pre-computed terrain derivatives.

Replaces the 20-40 min on-the-fly DEM load in [walkthru-weather-index](../walkthru-weather-index/) with a ~5 second Parquet scan.

## What it produces

### v2 (recommended)

```
s3://{bucket}/{prefix}/
  v2/h3/
    h3_res=1/data.parquet      5.3 KB           223 cells
    h3_res=2/data.parquet     30.5 KB         1,546 cells
    h3_res=3/data.parquet      204 KB        10,851 cells
    h3_res=4/data.parquet      1.4 MB        76,135 cells
    h3_res=5/data.parquet      9.7 MB       533,062 cells
    h3_res=6/data.parquet     67.4 MB     3,730,922 cells
    h3_res=7/data.parquet      476 MB    26,115,785 cells
    h3_res=8/data.parquet      3.2 GB   182,814,924 cells
    h3_res=9/data.parquet     22.1 GB 1,279,700,961 cells
    h3_res=10/data.parquet   156.4 GB 8,957,910,337 cells
```

**Total: 10,450,894,746 cells (~10.5 billion) in ~183 GB.** Single file per resolution, sorted by `h3_index`. Schema: `h3_index` (BIGINT), `elev`, `slope`, `aspect`, `tri`, `tpi` (all FLOAT). No geometry/lat/lon columns — derive coordinates via DuckDB h3 extension (`h3_cell_to_lat`, `h3_cell_to_lng`, `h3_cell_to_boundary_wkt`). Sorted int64 `h3_index` enables better delta encoding compression and range-based spatial queries via Parquet row group min/max statistics.

### v1 (legacy)

```
s3://{bucket}/{prefix}/
  v1/h3/
    h3_res=1/data.parquet  ...  h3_res=10/data.parquet
```

Same cell counts and resolutions as v2. Schema: `h3_index` (VARCHAR), `geometry` (native Parquet GEOMETRY POINT EPSG:4326), `lat`, `lon`, `elev`, `slope`, `aspect`, `tri`, `tpi` (all FLOAT).

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- [OpenTofu](https://opentofu.org/) (for cloud deployment)
- AWS credentials with S3 write access
- Verda cloud account (for cloud instance)

## Quick start

```bash
uv sync

cp .env.example .env   # fill in S3 bucket, AWS keys

uv run python main.py --dry-run                                      # list windows
uv run python main.py --resolutions 1,2,3,4,5 --scratch-dir ./scratch  # local test
```

## Cloud deployment

The full pipeline (res 1-10) needs ~1440 GB RAM. Uses a Verda CPU Node (360 vCPUs).

```bash
cd infra/
cp secrets.tfvars.example secrets.tfvars   # fill in credentials
tofu init && tofu apply -var-file="secrets.tfvars"

ssh root@$(tofu output -raw instance_ip)
tmux attach -t dem                          # watch pipeline
tail -f /data/scratch/pipeline.log          # or tail logs

tofu destroy -var-file="secrets.tfvars"     # tear down after
```

## Resumability

The pipeline checkpoints to `/data/scratch/checkpoint.json` at two levels:

1. **Window level** — each completed or skipped window is recorded. On restart, these are skipped.
2. **Resolution merge level** — after DuckDB merges temp files to S3, it's recorded. Already-merged resolutions are skipped on restart.

Temp Parquet files in `/data/scratch/temp/{group}/` survive restarts. To resume after a crash or interruption, just rerun the same command — it picks up where it left off.

To force a full rerun: `rm /data/scratch/checkpoint.json`

## Query the output

### v2 (recommended)

```sql
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       elev, slope, aspect
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=5/data.parquet')
WHERE h3_index BETWEEN ? AND ?  -- row group statistics prune efficiently
ORDER BY elev DESC
LIMIT 5;
```

For deck.gl H3HexagonLayer (needs hex string):

```sql
SELECT h3_h3_to_string(h3_index) AS h3_hex, elev
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=5/data.parquet')
```

### v1 (legacy — requires `INSTALL spatial`)

```sql
INSTALL spatial; LOAD spatial;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index, elev, slope, aspect, tri, tpi
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v1/h3/h3_res=5/data.parquet')
WHERE lat BETWEEN 27.5 AND 28.5
  AND lon BETWEEN 86.5 AND 87.5
ORDER BY elev DESC
LIMIT 5;
```

## Development

```bash
uv sync --group dev
uv run pre-commit install
uv run ruff check . && uv run ruff format .
```

## Source

> Ho, Y., Grohmann, C. H., Lindsay, J., Reuter, H. I., Parente, L., Witjes, M., & Hengl, T. (2025). GEDTM30: global ensemble digital terrain model at 30 m and derived multiscale terrain variables. *PeerJ*, 13, e19673. [doi:10.7717/peerj.19673](https://doi.org/10.7717/peerj.19673)
>
> Dataset: [doi:10.5281/zenodo.14900181](https://doi.org/10.5281/zenodo.14900181)

## License

This project is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://github.com/walkthru-earth). See [LICENSE](LICENSE) for details. The source [GEDTM-30m](https://doi.org/10.5281/zenodo.14900181) is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [OpenGeoHub](https://opengeohub.org/).

Contact: [hi@walkthru.earth](mailto:hi@walkthru.earth)
