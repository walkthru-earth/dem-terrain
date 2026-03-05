# Global DEM Terrain Derivatives

H3-indexed terrain data from [GEDTM-30m](https://stac.openlandmap.org/gedtm-30m/collection.json) in Apache Parquet format. Ten H3 resolutions (1–10), one file per resolution, sorted by `h3_index`. Available in two versions: **v2 (recommended)** with BIGINT `h3_index` and no geometry columns, and **v1 (legacy)** with VARCHAR `h3_index` and [native Parquet 2.11+ GEOMETRY](https://github.com/apache/parquet-format/blob/master/Geospatial.md).

| | |
|---|---|
| **Source** | GEDTM-30m (OpenGeoHub), 30m resolution, global land coverage |
| **Format** | Apache Parquet — v2: BIGINT h3_index, no geometry; v1: VARCHAR h3_index, native GEOMETRY (DuckDB 1.5) |
| **CRS** | EPSG:4326 (WGS 84) |
| **License** | [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links) |
| **Code** | [walkthru-earth/dem-terrain](https://github.com/walkthru-earth/dem-terrain) |

## Quick Start

### v2 (recommended)

```sql
-- DuckDB — no spatial extension needed for v2
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
LIMIT 20;
```

For deck.gl H3HexagonLayer (needs hex string):

```sql
SELECT h3_h3_to_string(h3_index) AS h3_hex, elev
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=5/data.parquet')
```

```python
# Python (v2)
import duckdb

con = duckdb.connect()
con.install_extension("h3", repository="community"); con.load_extension("h3")
for ext in ("httpfs",):
    con.install_extension(ext); con.load_extension(ext)
con.sql("SET s3_region = 'us-west-2'")

df = con.sql("""
    SELECT h3_index,
           h3_cell_to_lat(h3_index) AS lat,
           h3_cell_to_lng(h3_index) AS lng,
           elev, slope, aspect, tri, tpi
    FROM read_parquet(
        's3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=5/data.parquet'
    )
    ORDER BY elev DESC
    LIMIT 100
""").fetchdf()
```

### v1 (legacy — requires `INSTALL spatial`)

```sql
-- DuckDB
INSTALL spatial; LOAD spatial;
INSTALL httpfs;  LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index, elev, slope, aspect, tri, tpi
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v1/h3/h3_res=5/data.parquet')
WHERE lat BETWEEN 35 AND 45
  AND lon BETWEEN -10 AND 5
ORDER BY elev DESC
LIMIT 20;
```

## Files

### v2 (recommended)

```
walkthru-earth/dem-terrain/
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

### v1 (legacy)

```
walkthru-earth/dem-terrain/
  v1/h3/
    h3_res=1/data.parquet  ...  h3_res=10/data.parquet
```

Same cell counts and resolutions as v2.

### Size comparison

| Res | Cells | v2 (BIGINT, 6 cols) | v1 (VARCHAR, 9 cols) | Reduction |
|-----|------:|--------------------:|---------------------:|----------:|
| 1 | 223 | 5.3 KB | 12.2 KB | 57% |
| 2 | 1,546 | 30.5 KB | 57.4 KB | 47% |
| 3 | 10,851 | 204 KB | 373 KB | 45% |
| 4 | 76,135 | 1.4 MB | 2.5 MB | 44% |
| 5 | 533,062 | 9.7 MB | 17 MB | 43% |
| 6 | 3,730,922 | 67.4 MB | 115 MB | 41% |
| 7 | 26,115,785 | 476 MB | 783 MB | 39% |
| 8 | 182,814,924 | 3.2 GB | 5.3 GB | 40% |
| 9 | 1,279,700,961 | 22.1 GB | 36.1 GB | 39% |
| 10 | 8,957,910,337 | 156.4 GB | 244.7 GB | 36% |
| **Total** | **10.5 B** | **~183 GB** | **~287 GB** | **36%** |

Compression: ZSTD level 3. Row groups: 1,000,000 rows.

## Schema

### v2 (recommended)

| Column | Type | Unit | Description |
|--------|------|------|-------------|
| `h3_index` | BIGINT | — | H3 cell ID (int64). Sorted for delta encoding compression and range-based queries. |
| `elev` | FLOAT | meters | Elevation above sea level |
| `slope` | FLOAT | degrees | Terrain slope (0 = flat, 90 = cliff) |
| `aspect` | FLOAT | degrees | Slope direction (0/360 = N, 90 = E, 180 = S, 270 = W) |
| `tri` | FLOAT | meters | Terrain Ruggedness Index — mean abs. elevation diff to 8 neighbors |
| `tpi` | FLOAT | meters | Topographic Position Index — elevation minus mean of 8 neighbors (+ridge, -valley) |

`geometry`, `lat`, and `lon` columns are removed in v2 — derive them via the DuckDB h3 extension: `h3_cell_to_lat(h3_index)`, `h3_cell_to_lng(h3_index)`, `h3_cell_to_boundary_wkt(h3_index)`.

### v1 (legacy)

| Column | Type | Unit | Description |
|--------|------|------|-------------|
| `h3_index` | VARCHAR | — | H3 cell ID (hex string) |
| `geometry` | GEOMETRY | — | Cell center point (native Parquet 2.11+ GEOMETRY, EPSG:4326) |
| `lat` | FLOAT | degrees | Cell center latitude |
| `lon` | FLOAT | degrees | Cell center longitude |
| `elev` | FLOAT | meters | Elevation above sea level |
| `slope` | FLOAT | degrees | Terrain slope (0 = flat, 90 = cliff) |
| `aspect` | FLOAT | degrees | Slope direction (0/360 = N, 90 = E, 180 = S, 270 = W) |
| `tri` | FLOAT | meters | Terrain Ruggedness Index — mean abs. elevation diff to 8 neighbors |
| `tpi` | FLOAT | meters | Topographic Position Index — elevation minus mean of 8 neighbors (+ridge, -valley) |

**Sample values** (res 5, Everest region):

| h3_index | elev | slope | aspect | tri | tpi |
|----------|------|-------|--------|-----|-----|
| 853c0317fffffff | 6,879 m | 40.8° | 134° | 327 m | 52 m |
| 853c03bbfffffff | 6,568 m | 23.0° | 221° | 223 m | 31 m |

## How It Works

Terrain derivatives are computed from the GEDTM-30m elevation raster:

| Derivative | Method | Reference |
|------------|--------|-----------|
| **Slope** | `arctan(√(dz/dx² + dz/dy²))` via central differences, pixel sizes converted to meters using latitude-dependent scale | Horn (1981) |
| **Aspect** | `atan2(-dz/dx, dz/dy)`, converted to 0–360° compass bearing | Horn (1981) |
| **TRI** | Mean absolute elevation difference across 3×3 neighborhood | Riley et al. (1999) |
| **TPI** | Center pixel minus mean of 3×3 neighborhood | Weiss (2001) |

Values at H3 cell centers are obtained by **bilinear interpolation** from the raster grid (scipy `RegularGridInterpolator`).

DEM is read at different resolutions depending on H3 level:

| H3 Res | DEM Sampling | Rationale |
|--------|-------------|-----------|
| 1–5 | ~500 m | Cells are 8–418 km; coarse DEM sufficient |
| 6–7 | ~110 m | Cells are 1.2–3.2 km; finer detail needed |
| 8–10 | ~30 m (native) | Cells are 66–461 m; full resolution |

## More Examples

### v2 queries

```sql
-- Range query on sorted BIGINT h3_index — row group min/max statistics prune efficiently
INSTALL h3 FROM community; LOAD h3;
INSTALL httpfs; LOAD httpfs;
SET s3_region = 'us-west-2';

SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       elev, slope
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=7/data.parquet')
WHERE h3_index BETWEEN ? AND ?
ORDER BY elev DESC
LIMIT 100;

-- Query across all resolutions (Hive partitioning)
SELECT h3_res, h3_index, elev
FROM read_parquet(
    's3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=*/data.parquet',
    hive_partitioning = true
)
WHERE h3_res = 5
ORDER BY elev DESC
LIMIT 20;

-- DuckDB-WASM (browser) — use HTTPS URL
SELECT h3_index,
       h3_cell_to_lat(h3_index) AS lat,
       h3_cell_to_lng(h3_index) AS lng,
       elev, slope
FROM read_parquet(
    'https://data.source.coop/walkthru-earth/dem-terrain/v2/h3/h3_res=5/data.parquet'
)
ORDER BY elev DESC
LIMIT 100;
```

### v1 legacy queries (requires `INSTALL spatial`)

```sql
-- Spatial pushdown using native geometry bbox stats (no lat/lon filter needed)
SELECT h3_index, elev, slope
FROM read_parquet('s3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v1/h3/h3_res=7/data.parquet')
WHERE geometry && ST_MakeEnvelope(86.5, 27.5, 87.5, 28.5);

-- lat/lon filtering (v1 only — these columns exist in v1)
SELECT *
FROM read_parquet(
    's3://us-west-2.opendata.source.coop/walkthru-earth/dem-terrain/v1/h3/h3_res=*/data.parquet',
    hive_partitioning = true
)
WHERE h3_res = 5
  AND lat BETWEEN 27 AND 29 AND lon BETWEEN 86 AND 88;
```

## Geometry Format

**v2** drops the `geometry` column entirely. Coordinates are derivable from the BIGINT `h3_index` via the DuckDB h3 extension (`h3_cell_to_lat`, `h3_cell_to_lng`, `h3_cell_to_boundary_wkt`). This eliminates the need for `INSTALL spatial` and reduces file size.

**v1** retains the `geometry` column, written with `GEOPARQUET_VERSION 'BOTH'`, providing dual compatibility:

- **Native Parquet 2.11+ GEOMETRY logical type** — per-row-group bounding box statistics for spatial filter pushdown. Supported by DuckDB 1.5+, Apache Arrow (Rust), Apache Iceberg, GDAL 3.12+.
- **GeoParquet 1.0 `geo` file-level metadata** — backwards compatibility with older tools (QGIS, pyarrow, GeoPandas).

## Source

[GEDTM-30m](https://stac.openlandmap.org/gedtm-30m/collection.json) — Global Ensemble Digital Terrain Model at 30m resolution. Cloud Optimized GeoTIFF, 1,440,010 × 600,010 px, Int32 (decimeters, scale 0.1), EPSG:4326, global land coverage 65°S–85°N.

> Ho, Y., Grohmann, C. H., Lindsay, J., Reuter, H. I., Parente, L., Witjes, M., & Hengl, T. (2025). GEDTM30: global ensemble digital terrain model at 30 m and derived multiscale terrain variables. *PeerJ*, 13, e19673. [doi:10.7717/peerj.19673](https://doi.org/10.7717/peerj.19673)
>
> Dataset: [doi:10.5281/zenodo.14900181](https://doi.org/10.5281/zenodo.14900181)

## License

This dataset is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [walkthru.earth](https://walkthru.earth/links). The source [GEDTM-30m](https://doi.org/10.5281/zenodo.14900181) is licensed under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) by [OpenGeoHub](https://opengeohub.org/).
