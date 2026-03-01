# GDAL Analysis for DEM-Terrain Pipeline

> Investigated 2026-03-01. Updated 2026-03-02.

## GDAL 3.12.2

**GDAL 3.12.2** (released 2026-02-09), following 3.12.0 "Chicoutimi" (2025-11-07).

## Key Features Relevant to This Project

### `gdal raster pipeline` (3.11+)

New unified CLI with streaming block-based evaluation (no full raster in memory):

```bash
# Example: compute slope from COG → native Parquet GEOMETRY
gdal raster pipeline \
  read dem.tif \
  ! slope --unit degree \
  ! as-features --geometry-type point --include-xy \
  ! write slope.parquet -f Parquet \
    -lco USE_PARQUET_GEO_TYPES=YES -lco COMPRESSION=ZSTD

# Chain with tee for multiple outputs
gdal raster pipeline \
  read dem.tif \
  ! tee \
    [ slope --unit degree ! as-features --geometry-type point --include-xy \
      ! write slope.parquet -f Parquet \
        -lco USE_PARQUET_GEO_TYPES=YES -lco COMPRESSION=ZSTD ] \
    [ aspect ! as-features --geometry-type point --include-xy \
      ! write aspect.parquet -f Parquet \
        -lco USE_PARQUET_GEO_TYPES=YES -lco COMPRESSION=ZSTD ]
```

Terrain derivative subcommands:

| Command | Description |
|---------|-------------|
| `gdal raster slope` | Slope map (degrees or percent, Horn or ZevenbergenThorne) |
| `gdal raster aspect` | Aspect map (azimuth or trigonometric) |
| `gdal raster tri` | Terrain Ruggedness Index (Riley or Wilson) |
| `gdal raster tpi` | Topographic Position Index |
| `gdal raster roughness` | Roughness map |
| `gdal raster hillshade` | Shaded relief |

Other pipeline steps: `stack` (combine bands), `materialize` (flush to disk), `tee` (fan out), nested pipelines with `[ ]`.

### `gdal raster as-features` (3.12)

Raster-to-vector conversion — one feature per pixel:

```bash
gdal pipeline \
  read dem.tif \
  ! as-features --geometry-type point --include-xy \
  ! write output.parquet -f Parquet \
    -lco USE_PARQUET_GEO_TYPES=YES -lco COMPRESSION=ZSTD
```

### Native Parquet GEOMETRY Type (3.12)

GDAL 3.12 can write the native Parquet 2.11+ `GEOMETRY` logical type via:

```bash
ogr2ogr -f Parquet output.parquet input.fgb \
  -lco USE_PARQUET_GEO_TYPES=YES \
  -lco COMPRESSION=ZSTD
```

Writes per-row-group bounding box statistics automatically. Requires libarrow >= 21. All Parquet commands in this doc use `USE_PARQUET_GEO_TYPES=YES` for native geometry.

### Other Improvements

- **COG driver**: recomputed statistics in `STATISTICS=YES` mode
- **`gdal raster tile`**: C++ rewrite, 3-6x faster than old `gdal2tiles.py`
- **VRT pixel functions**: `mean`, `median`, `geometric_mean`, `mode` with NoData handling
- **Streaming evaluation**: pipeline steps are lazy/on-demand, not materialized unless requested

## Can GDAL CLI Replace Our Python Pipeline?

**No.** Here's the gap analysis:

| Requirement | GDAL 3.12 | Our Pipeline (Python) |
|-------------|-----------|----------------------|
| Compute slope/aspect/TRI/TPI | Yes — but each is a separate pass (4-5 reads of 305 GB COG) | Single windowed read, all derivatives computed in one pass via NumPy |
| H3 cell indexing | Not available | h3-py generates cells per window |
| Multi-resolution output (res 1-10) | No concept of this | Reads at 3 different DEM resolutions per group |
| Sort by H3 index | No | DuckDB ORDER BY h3_index |
| DuckDB native Parquet GEOMETRY with shredding | No (GDAL writes same logical type but no geometry shredding, different stats format) | DuckDB 1.5 writes with shredding + bbox stats |
| Checkpointing / resumability | No | checkpoint.json tracks completed windows |
| S3 upload with prefix layout | No (would need separate aws cli) | Built into pipeline |
| Single Parquet file per resolution | Awkward (as-features produces one huge file per derivative, not per resolution) | merge_temp_to_final() per resolution |

### Recommendation

**Keep the current Python pipeline.** The GDAL CLI approach would be slower (multiple passes), use more disk, and still require Python for H3 + DuckDB Parquet.

GDAL 3.12's `gdal raster pipeline ! as-features ! write Parquet` could be useful for simpler raster-to-Parquet conversions that don't need H3 indexing.

## Contour Line Generation

> Investigated 2026-03-02.

### Source Raster Metadata

```
File:      /data/scratch/gedtm30.tif (305 GB COG)
Size:      1,440,010 × 600,010 px
DataType:  Int32
NoData:    -2147483648
Scale:     0.1  (raw values = decimeters)
Offset:    0.0
Mean:      6501.9 raw = 650.2 m
StdDev:    8383.1 raw = 838.3 m
Block:     2048 × 2048
CRS:       EPSG:4326 (WGS 84)
Overviews: 10 levels (720005×300005 → 1406×585)
```

### Contour Interval Choice

For a 30m DEM with ~2-5m vertical accuracy:

| Interval | Use Case | Output Size (est.) |
|----------|----------|--------------------|
| 100 m | Standard (1:250k–1:1M maps) | ~50-100 GB |
| 50 m | Detailed (1:100k maps) | ~100-200 GB |
| 20 m | High detail (pushes DEM accuracy) | ~300-500 GB |

**Recommended: 100m** — best match for 30m DEM accuracy.

### Command

```bash
# Step 1: VRT that converts decimeters → meters (tiny file, lazy scaling)
gdal_translate -of VRT -unscale -ot Float32 \
  /data/scratch/gedtm30.tif /data/scratch/gedtm30_meters.vrt

# Step 2: Generate 100m contour lines → native Parquet GEOMETRY
#   - `-a elev`: elevation attribute in meters (from VRT)
#   - `-i 100`: 100m interval
#   - `-3d`: LineStringZ (elevation baked into vertices)
#   - NoData (-2147483648 raw) carried by VRT, auto-skipped
gdal_contour \
  -a elev \
  -i 100 \
  -3d \
  -nln contours \
  -f Parquet \
  -lco COMPRESSION=ZSTD \
  -lco USE_PARQUET_GEO_TYPES=YES \
  -lco ROW_GROUP_SIZE=100000 \
  /data/scratch/gedtm30_meters.vrt \
  /data/scratch/contours.parquet
```

### Performance Notes

- `gdal_contour` is **single-threaded** — expect 24-48h for the full 305 GB COG
- Output size: ~50-100 GB at 100m interval (hundreds of millions of LineStringZ features)

## References

- [GDAL 3.12.0 release notes](https://github.com/OSGeo/gdal/blob/v3.12.0/NEWS.md)
- [gdal raster pipeline docs](https://gdal.org/en/stable/programs/gdal_raster_pipeline.html)
- [gdal raster as-features docs](https://gdal.org/en/stable/programs/gdal_raster_as_features.html)
- [Parquet driver — GDAL](https://gdal.org/en/stable/drivers/vector/parquet.html)
- [Native Parquet GEOMETRY — Apache Parquet 2.11](https://parquet.apache.org/blog/2026/02/13/native-geospatial-types-in-apache-parquet/)
- [DuckDB v1.5 GEOMETRY rework](https://github.com/duckdb/duckdb/pull/19136)
