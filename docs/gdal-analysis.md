# GDAL Analysis for DEM-Terrain Pipeline

> Investigated 2026-03-01. VPS instance: 86.38.238.25 (Verda CPU.360V.1440G, FIN-02).

## VPS Installed Version

**GDAL 3.8.4** (released 2024-02-08) — shipped with Ubuntu 24.04 `gdal-bin` package.

- `gdaldem` available at `/usr/bin/gdaldem`
- Supports: slope, aspect, TRI, TPI, roughness, hillshade
- **No Parquet vector driver** (`ogr2ogr --formats` has no Parquet entry)
- No `gdal raster pipeline` CLI (added in 3.11)
- No native Parquet GEOMETRY type support (added in 3.12)

## Latest Stable Version

**GDAL 3.12.2** (released 2026-02-09), following 3.12.0 "Chicoutimi" (2025-11-07).

## Key New Features (3.11 → 3.12) Relevant to This Project

### `gdal raster pipeline` (3.11+)

New unified CLI with streaming block-based evaluation (no full raster in memory):

```bash
# Example: compute slope from COG
gdal raster pipeline \
  read dem.tif \
  ! slope --unit degree \
  ! write slope_output.tif

# Chain with tee for multiple outputs
gdal raster pipeline \
  read dem.tif \
  ! tee \
    [ slope --unit degree ! write slope.tif ] \
    [ aspect ! write aspect.tif ]
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
  ! write output.parquet -f Parquet
```

### Native Parquet GEOMETRY Type (3.12)

GDAL 3.12 can write the native Parquet 2.11+ `GEOMETRY` logical type via:

```
ogr2ogr -f Parquet output.parquet input.geojson \
  -lco USE_PARQUET_GEO_TYPES=YES
```

Writes per-row-group bounding box statistics automatically. Requires libarrow >= 21.

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

### What GDAL *Could* Help With

1. **Pre-computing derivative rasters** — run `gdaldem slope/aspect/TRI/TPI` once on the 305 GB COG to produce 4 derivative GeoTIFFs on NVMe. Then our Python pipeline reads 5 bands (elev + 4 derivatives) instead of computing derivatives in NumPy.
   - **Pro**: Simpler Python code, `gdaldem` is battle-tested and handles edge cases (nodata, poles)
   - **Con**: 4 × 305 GB = ~1.2 TB extra disk (we have 2 TB NVMe, COG is 305 GB, leaves ~1.4 TB — tight but possible). Also 4 sequential full reads of the COG.

2. **Validation** — use `gdaldem` to spot-check our NumPy terrain derivatives against GDAL's reference implementation.

### Hybrid Approach (Possible but Not Recommended)

```bash
# Step 1: Pre-compute derivatives (4 passes, ~2 hours each)
gdaldem slope  /data/scratch/gedtm30.tif /data/scratch/slope.tif  -compute_edges
gdaldem aspect /data/scratch/gedtm30.tif /data/scratch/aspect.tif -compute_edges
gdaldem TRI    /data/scratch/gedtm30.tif /data/scratch/tri.tif
gdaldem TPI    /data/scratch/gedtm30.tif /data/scratch/tpi.tif

# Step 2: Python reads 5 files via windowed reads, generates H3 cells, writes Parquet
# (Simpler compute_terrain_derivatives — just interpolate, no gradient computation)
```

**Why not recommended for us:**
- Uses ~1.5 TB disk (tight on 2 TB NVMe with COG + output)
- 4 sequential full reads of 305 GB = ~8 hours just for derivative computation
- Our NumPy approach computes all 4 derivatives in a single window read (~0.1 sec per window)
- The derivative computation is not the bottleneck — H3 cell generation and Parquet writing are

## Recommendation

**Keep the current Python pipeline.** The GDAL CLI approach would be slower (multiple passes), use more disk, and still require Python for H3 + DuckDB Parquet. The only reason to upgrade GDAL on the VPS would be for validation/debugging — not worth the effort for this one-time pipeline.

If we ever need to reprocess, GDAL 3.12's `gdal raster pipeline ! as-features ! write Parquet` could be useful for simpler raster-to-Parquet conversions that don't need H3 indexing.

## Version Comparison

| Feature | GDAL 3.8.4 (VPS) | GDAL 3.12.2 (Latest) |
|---------|-------------------|----------------------|
| `gdaldem` slope/aspect/TRI/TPI | Yes | Yes |
| `gdal raster pipeline` | No | Yes |
| `gdal raster as-features` | No | Yes |
| Parquet vector driver | No | Yes |
| Native Parquet GEOMETRY type | No | Yes (`USE_PARQUET_GEO_TYPES=YES`) |
| Geometry shredding | No | No (DuckDB 1.5 only) |
| COG streaming reads | Yes | Yes (improved) |
| H3 indexing | No | No |

## References

- [GDAL 3.12.0 release notes](https://github.com/OSGeo/gdal/blob/v3.12.0/NEWS.md)
- [gdal raster pipeline docs](https://gdal.org/en/stable/programs/gdal_raster_pipeline.html)
- [gdal raster as-features docs](https://gdal.org/en/stable/programs/gdal_raster_as_features.html)
- [Parquet driver — GDAL](https://gdal.org/en/stable/drivers/vector/parquet.html)
- [Native Parquet GEOMETRY — Apache Parquet 2.11](https://parquet.apache.org/blog/2026/02/13/native-geospatial-types-in-apache-parquet/)
- [DuckDB v1.5 GEOMETRY rework](https://github.com/duckdb/duckdb/pull/19136)
