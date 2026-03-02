"""Global DEM to native Parquet 2.11+ pipeline.

Converts GEDTM-30m global DEM to H3-indexed, partitioned native Parquet files
with terrain derivatives (elevation, slope, aspect, TRI, TPI).

DuckDB 1.5.0-dev writes native Parquet GEOMETRY logical type (GEOPARQUET_VERSION
'BOTH') with per-row-group geo_types stats AND GeoParquet 1.0 'geo' file-level
metadata for backwards compatibility with older tools (QGIS, pyarrow, etc).

The pipeline reads from a single global COG (Cloud Optimized GeoTIFF) using
windowed reads. If a local copy exists on NVMe, it uses that for speed;
otherwise it reads directly from the remote URL via GDAL vsicurl.

Usage:
    uv run main.py                          # Full global processing
    uv run main.py --resolutions 1,2,3,4,5  # Only low-res
    uv run main.py --dry-run                # List windows without processing
    uv run main.py --dem-path /data/scratch/gedtm30.tif  # Explicit local COG
"""

from __future__ import annotations

import json
import logging
import os
import resource
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import h3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from rasterio.windows import from_bounds
from scipy.interpolate import RegularGridInterpolator
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def _mem_gb() -> str:
    """Current RSS memory in GB (for log lines)."""
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS returns bytes, Linux returns KB
    if sys.platform == "darwin":
        return f"{rss / 1e9:.1f}GB"
    return f"{rss / 1e6:.1f}GB"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# GEDTM-30m: single global COG hosted on OpenGeoHub S3
# 1,440,010 x 600,010 px, int32, 2048x2048 blocks, 10 overviews (2-1024x)
DEM_COG_URL = "https://s3.opengeohub.org/global/edtm/legendtm_rf_30m_m_s_20000101_20231231_go_epsg.4326_v20250130.tif"

# Default local COG filename (looked up in SCRATCH_DIR)
LOCAL_COG_NAME = "gedtm30.tif"

# Processing window size (degrees) — each chunk is read independently
WINDOW_SIZE = 5.0

# S3 output
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
AWS_REGION = os.environ.get("AWS_REGION", "") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Local scratch directory (NVMe on cloud instance)
SCRATCH_DIR = Path(os.environ.get("SCRATCH_DIR", "/data/scratch"))
CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

# GEDTM-30m extent (lat -65 to 85, lon -180 to 180)
LAT_MIN, LAT_MAX = -65.0, 85.0
LON_MIN, LON_MAX = -180.0, 180.0


@dataclass
class ResolutionGroup:
    """DEM sampling strategy per H3 resolution group."""

    name: str
    h3_resolutions: list[int]
    dem_resolution: float  # degrees per pixel
    description: str


RESOLUTION_GROUPS = [
    ResolutionGroup(
        name="low",
        h3_resolutions=[1, 2, 3, 4, 5],
        dem_resolution=0.005,  # ~500m
        description="Cells 8-418 km; 500m DEM is more than sufficient",
    ),
    ResolutionGroup(
        name="medium",
        h3_resolutions=[6, 7],
        dem_resolution=0.001,  # ~110m
        description="Cells 1.2-3.2 km; need finer DEM detail",
    ),
    ResolutionGroup(
        name="high",
        h3_resolutions=[8, 9, 10],
        dem_resolution=0.00028,  # ~30m, native
        description="Cells 66-461m; need full DEM resolution",
    ),
]


# ---------------------------------------------------------------------------
# GPU / CPU terrain derivative computation
# ---------------------------------------------------------------------------


def _try_import_cupy():
    """Try to import CuPy for GPU-accelerated computation."""
    try:
        import cupy as cp

        # Verify a GPU is actually available
        _ = cp.cuda.Device(0).compute_capability
        log.info("CuPy available — using GPU for terrain derivatives")
        return cp
    except Exception:
        log.info("CuPy not available — using NumPy (CPU) for terrain derivatives")
        return None


cp = _try_import_cupy()
xp = cp if cp is not None else np  # array module (GPU or CPU)


def compute_terrain_derivatives(
    elevation: np.ndarray,
    pixel_size_x: float,
    pixel_size_y: float,
    lat_center: float,
) -> dict[str, np.ndarray]:
    """Compute slope, aspect, TRI, and TPI from an elevation grid.

    All inputs/outputs are in numpy arrays (transferred from/to GPU if CuPy).

    Args:
        elevation: 2D elevation array (rows=lat, cols=lon).
        pixel_size_x: Pixel width in degrees.
        pixel_size_y: Pixel height in degrees (positive).
        lat_center: Center latitude for metric conversion.

    Returns:
        Dictionary with keys: slope, aspect, tri, tpi (all same shape as input).
    """
    elev = xp.asarray(elevation, dtype=xp.float32)

    # Convert pixel size from degrees to meters for gradient computation
    meters_per_deg_lat = 111_320.0
    meters_per_deg_lon = 111_320.0 * float(np.cos(np.radians(lat_center)))
    cell_x = pixel_size_x * meters_per_deg_lon
    cell_y = pixel_size_y * meters_per_deg_lat

    # Gradient (central differences)
    dy, dx = xp.gradient(elev, cell_y, cell_x)

    # Slope (degrees)
    slope_rad = xp.arctan(xp.sqrt(dx**2 + dy**2))
    slope_deg = xp.degrees(slope_rad)

    # Aspect (compass degrees, 0=N, 90=E, 180=S, 270=W)
    aspect_rad = xp.arctan2(-dx, dy)
    aspect_deg = xp.degrees(aspect_rad)
    aspect_deg = xp.where(aspect_deg < 0, aspect_deg + 360.0, aspect_deg)

    # TRI and TPI via 3x3 neighborhood
    padded = xp.pad(elev, 1, mode="edge")
    tri_sum = xp.zeros_like(elev)
    neighbor_sum = xp.zeros_like(elev)
    for di in range(-1, 2):
        for dj in range(-1, 2):
            if di == 0 and dj == 0:
                continue
            neighbor = padded[1 + di : 1 + di + elev.shape[0], 1 + dj : 1 + dj + elev.shape[1]]
            tri_sum += xp.abs(neighbor - elev)
            neighbor_sum += neighbor
    tri = tri_sum / 8.0
    tpi = elev - neighbor_sum / 8.0

    # Transfer back to CPU if on GPU
    if cp is not None:
        slope_deg = cp.asnumpy(slope_deg)
        aspect_deg = cp.asnumpy(aspect_deg)
        tri = cp.asnumpy(tri)
        tpi = cp.asnumpy(tpi)

    return {
        "slope": slope_deg.astype(np.float32),
        "aspect": aspect_deg.astype(np.float32),
        "tri": tri.astype(np.float32),
        "tpi": tpi.astype(np.float32),
    }


# ---------------------------------------------------------------------------
# DEM source resolution
# ---------------------------------------------------------------------------


def resolve_dem_path(explicit_path: str | None) -> str:
    """Determine the DEM source: explicit path > local COG > remote URL."""
    if explicit_path:
        p = Path(explicit_path)
        if p.exists():
            log.info("Using explicit DEM path: %s (%.1f GB)", p, p.stat().st_size / 1e9)
            return str(p)
        log.warning("Explicit DEM path %s not found, trying defaults", p)

    local = SCRATCH_DIR / LOCAL_COG_NAME
    if local.exists():
        size_gb = local.stat().st_size / 1e9
        log.info("Using local COG: %s (%.1f GB)", local, size_gb)
        return str(local)

    log.info("No local COG found at %s — using remote URL (slower)", local)
    log.info(
        "  Tip: download first with aria2c -x16 -s16 -k50M -d %s -o %s '%s'",
        SCRATCH_DIR,
        LOCAL_COG_NAME,
        DEM_COG_URL,
    )
    return DEM_COG_URL


# ---------------------------------------------------------------------------
# Window generation
# ---------------------------------------------------------------------------


def generate_windows() -> list[dict]:
    """Generate non-overlapping geographic windows covering the DEM extent.

    Returns list of dicts with keys: id, bbox [west, south, east, north].
    Windows are 5x5 degrees by default (WINDOW_SIZE).
    """
    windows = []
    lon = LON_MIN
    while lon < LON_MAX:
        lon_end = min(lon + WINDOW_SIZE, LON_MAX)
        lat = LAT_MIN
        while lat < LAT_MAX:
            lat_end = min(lat + WINDOW_SIZE, LAT_MAX)
            win_id = f"w_{lon:+08.1f}_{lat:+07.1f}"
            windows.append(
                {
                    "id": win_id,
                    "bbox": [lon, lat, lon_end, lat_end],
                }
            )
            lat = lat_end
        lon = lon_end
    return windows


# ---------------------------------------------------------------------------
# Window processing
# ---------------------------------------------------------------------------


def load_dem_window(
    dem_path: str,
    bbox: list[float],
    target_resolution: float,
) -> dict | None:
    """Read a geographic window from the COG at target resolution.

    Uses rasterio windowed reads with automatic overview selection.
    Returns dict with keys: elevation (2D), lats (1D), lons (1D),
    pixel_size_x, pixel_size_y, or None if the window has no valid data.
    """
    west, south, east, north = bbox
    width = round((east - west) / target_resolution)
    height = round((north - south) / target_resolution)

    # Clamp to reasonable sizes
    width = max(2, min(width, 20_000))
    height = max(2, min(height, 20_000))

    t0 = time.time()
    try:
        with rasterio.open(dem_path) as src:
            window = from_bounds(west, south, east, north, src.transform)
            elevation = src.read(
                1,
                window=window,
                out_shape=(height, width),
                resampling=rasterio.enums.Resampling.bilinear,
            ).astype(np.float32)

            nodata = src.nodata
            if nodata is not None:
                elevation[elevation == nodata] = np.nan

            # Apply scale/offset from raster metadata (GEDTM-30m: scale=0.1, offset=0)
            scale = src.scales[0] if src.scales else 1.0
            offset = src.offsets[0] if src.offsets else 0.0
            if scale != 1.0 or offset != 0.0:
                elevation = elevation * scale + offset

            if np.all(np.isnan(elevation)):
                return None

            pixel_size_x = (east - west) / width
            pixel_size_y = (north - south) / height
            lons = np.linspace(west + pixel_size_x / 2, east - pixel_size_x / 2, width)
            lats = np.linspace(north - pixel_size_y / 2, south + pixel_size_y / 2, height)

            valid_pct = 100.0 * np.count_nonzero(np.isfinite(elevation)) / elevation.size
            log.info(
                "  DEM loaded: %dx%d (%.1f%% valid) in %.1fs",
                width,
                height,
                valid_pct,
                time.time() - t0,
            )

            return {
                "elevation": elevation,
                "lats": lats,
                "lons": lons,
                "pixel_size_x": pixel_size_x,
                "pixel_size_y": pixel_size_y,
            }
    except Exception as e:
        log.warning("  DEM window load FAILED (%.1fs): %s", time.time() - t0, e)
        return None


def generate_h3_cells_for_window(
    bbox: list[float],
    h3_res: int,
) -> list[str]:
    """Generate H3 cell IDs that have their center within the window bbox.

    Ownership rule: a cell belongs to a window if its center falls within
    the bounding box. This ensures each cell is processed exactly once
    across non-overlapping windows.
    """
    west, south, east, north = bbox

    # Buffer in degrees (convert from km)
    edge_km = h3.average_hexagon_edge_length(h3_res, unit="km")
    buffer = (edge_km / 111.32) * 1.5

    poly = h3.LatLngPoly(
        [
            (south - buffer, west - buffer),
            (south - buffer, east + buffer),
            (north + buffer, east + buffer),
            (north + buffer, west - buffer),
        ]
    )
    candidate_cells = h3.h3shape_to_cells(poly, h3_res)

    if not candidate_cells:
        return []

    # Vectorized ownership filter
    coords = np.array([h3.cell_to_latlng(c) for c in candidate_cells])
    cells_list = list(candidate_cells)
    mask = (coords[:, 0] >= south) & (coords[:, 0] < north) & (coords[:, 1] >= west) & (coords[:, 1] < east)
    return [cells_list[i] for i in np.nonzero(mask)[0]]


def interpolate_terrain_to_cells(
    cells: list[str],
    dem_data: dict,
    derivatives: dict[str, np.ndarray],
) -> dict[str, list]:
    """Interpolate terrain values to H3 cell centers.

    Returns dict of column lists: h3_index, lat, lon, elev, slope, aspect, tri, tpi.
    """
    if not cells:
        return {k: [] for k in ("h3_index", "lat", "lon", "elev", "slope", "aspect", "tri", "tpi")}

    lats = dem_data["lats"]
    lons = dem_data["lons"]
    elevation = dem_data["elevation"]

    # Build interpolators (lat axis is descending in raster, so flip)
    # RegularGridInterpolator expects strictly ascending axes
    if lats[0] > lats[-1]:
        lats_asc = lats[::-1]
        elevation_asc = elevation[::-1, :]
        derivs_asc = {k: v[::-1, :] for k, v in derivatives.items()}
    else:
        lats_asc = lats
        elevation_asc = elevation
        derivs_asc = derivatives

    interpolators = {}
    interpolators["elev"] = RegularGridInterpolator(
        (lats_asc, lons), elevation_asc, method="linear", bounds_error=False, fill_value=np.nan
    )
    for name in ("slope", "aspect", "tri", "tpi"):
        interpolators[name] = RegularGridInterpolator(
            (lats_asc, lons), derivs_asc[name], method="linear", bounds_error=False, fill_value=np.nan
        )

    # Get cell center coordinates
    cell_lats = []
    cell_lons = []
    for cell in cells:
        lat, lon = h3.cell_to_latlng(cell)
        cell_lats.append(lat)
        cell_lons.append(lon)

    points = np.column_stack([cell_lats, cell_lons])

    # Interpolate all fields
    result = {"h3_index": list(cells), "lat": cell_lats, "lon": cell_lons}
    for field, interp in interpolators.items():
        values = interp(points)
        result[field] = [float(v) if np.isfinite(v) else None for v in values]

    return result


# ---------------------------------------------------------------------------
# Parquet output via DuckDB
# ---------------------------------------------------------------------------


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with spatial and httpfs extensions."""
    log.info("Initializing DuckDB %s", duckdb.__version__)
    con = duckdb.connect()

    for ext in ("spatial", "httpfs"):
        con.install_extension(ext)
        con.load_extension(ext)
        log.info("  Extension '%s' loaded", ext)

    if S3_BUCKET:
        aws_key = os.environ.get("AWS_ACCESS_KEY_ID", "")
        aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
        if aws_key and aws_secret:
            con.sql(f"SET s3_region='{AWS_REGION}'")
            con.sql(f"SET s3_access_key_id='{aws_key}'")
            con.sql(f"SET s3_secret_access_key='{aws_secret}'")
            con.sql("SET s3_url_style='path'")
            log.info("  S3 configured: region=%s, bucket=%s, url_style=path", AWS_REGION, S3_BUCKET)
        else:
            log.warning("  S3_BUCKET set but AWS credentials missing!")
    else:
        log.info("  No S3_BUCKET — writing to local filesystem")

    return con


def merge_temp_to_final(
    con: duckdb.DuckDBPyConnection,
    temp_dir: Path,
    h3_res: int,
) -> int:
    """Merge temp Parquet files into a single output file per resolution.

    DuckDB 1.5 native Parquet GEOMETRY writes per-row-group bbox stats
    automatically, so a single sorted file supports spatial filter pushdown
    without Hive partitioning.

    Returns total row count.
    """
    res_temp_dir = temp_dir / f"h3_res={h3_res}"
    if not res_temp_dir.exists():
        log.warning("No temp data for H3 res %d — skipping", h3_res)
        return 0

    temp_glob = str(res_temp_dir / "*.parquet")

    # Count rows
    total_rows = con.sql(f"SELECT count(*) FROM read_parquet('{temp_glob}', hive_partitioning=false)").fetchone()[0]
    if total_rows == 0:
        log.warning("No rows for H3 res %d — skipping", h3_res)
        return 0

    log.info("Merging H3 res %d: %d cells from temp files", h3_res, total_rows)

    if S3_BUCKET:
        output_base = f"s3://{S3_BUCKET}/{S3_PREFIX}"
    else:
        output_base = str(SCRATCH_DIR / "output" / "dem-terrain")
        Path(output_base).mkdir(parents=True, exist_ok=True)

    output_path = f"{output_base}/h3_res={h3_res}/data.parquet"
    if not S3_BUCKET:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    con.sql(f"""
        COPY (
            SELECT h3_index,
                   ST_Point(lon, lat)::GEOMETRY('EPSG:4326') AS geometry,
                   lat, lon, elev, slope, aspect, tri, tpi
            FROM read_parquet('{temp_glob}', hive_partitioning=false)
            ORDER BY h3_index
        ) TO '{output_path}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE 1000000, GEOPARQUET_VERSION 'BOTH')
    """)
    log.info("  Wrote %s (%d rows) in %.1fs", output_path, total_rows, time.time() - t0)

    return total_rows


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------


def load_checkpoint() -> dict:
    """Load processing checkpoint (which windows are done)."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"completed_windows": {}, "completed_resolutions": []}


def save_checkpoint(state: dict) -> None:
    """Save processing checkpoint."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------


def write_metadata(total_cells: dict[int, int], elapsed_seconds: float) -> None:
    """Write _metadata.json with dataset documentation."""
    metadata = {
        "dataset": "dem-terrain",
        "source": "GEDTM-30m (OpenLandMap)",
        "source_url": DEM_COG_URL,
        "crs": "EPSG:4326",
        "geometry_type": "native_parquet_2.11_geometry",
        "geometry_encoding": "WKB with GEOMETRY logical type annotation",
        "h3_resolutions": list(range(1, 11)),
        "layout": "single Parquet file per resolution, sorted by h3_index",
        "columns": {
            "h3_index": "H3 cell ID (hex string)",
            "geometry": "Cell center as POINT, native Parquet 2.11+ GEOMETRY('EPSG:4326')",
            "lat": "Cell center latitude (float32)",
            "lon": "Cell center longitude (float32)",
            "elev": "Mean elevation in meters (float32)",
            "slope": "Mean slope in degrees (float32)",
            "aspect": "Mean aspect in compass degrees, 0=N (float32)",
            "tri": "Terrain Ruggedness Index in meters (float32)",
            "tpi": "Topographic Position Index in meters (float32)",
        },
        "compression": "ZSTD level 3",
        "cells_per_resolution": {str(k): v for k, v in sorted(total_cells.items())},
        "processing_time_seconds": round(elapsed_seconds, 1),
        "processing_date": time.strftime("%Y-%m-%d"),
    }

    if S3_BUCKET:
        import boto3

        s3 = boto3.client("s3", region_name=AWS_REGION)
        key = f"{S3_PREFIX}/_metadata.json" if S3_PREFIX else "_metadata.json"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=json.dumps(metadata, indent=2),
            ContentType="application/json",
        )
        log.info("Wrote metadata to s3://%s/%s", S3_BUCKET, key)
    else:
        meta_path = SCRATCH_DIR / "output" / "dem-terrain" / "_metadata.json"
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        meta_path.write_text(json.dumps(metadata, indent=2))
        log.info("Wrote metadata to %s", meta_path)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def process_resolution_group(
    group: ResolutionGroup,
    windows: list[dict],
    dem_path: str,
    checkpoint: dict,
    con: duckdb.DuckDBPyConnection,
) -> dict[int, int]:
    """Process all windows for a resolution group.

    For each window: read DEM, compute terrain, generate H3 cells, write temp
    Parquet. After all windows: merge temp files to final output via DuckDB.

    Returns dict mapping h3_res -> total cells written.
    """
    log.info(
        "=== Processing group '%s' (H3 res %s, DEM %.5f°) ===",
        group.name,
        group.h3_resolutions,
        group.dem_resolution,
    )

    temp_dir = SCRATCH_DIR / "temp" / group.name
    temp_dir.mkdir(parents=True, exist_ok=True)

    skipped = 0
    processed = 0

    for i, win in enumerate(tqdm(windows, desc=f"Windows ({group.name})", unit="win")):
        win_key = f"{group.name}:{win['id']}"
        if win_key in checkpoint["completed_windows"]:
            skipped += 1
            continue

        win_t0 = time.time()
        bbox = win["bbox"]
        log.info(
            "[%d/%d] Window %s bbox=[%.1f,%.1f,%.1f,%.1f] (mem=%s)",
            i + 1,
            len(windows),
            win["id"],
            *bbox,
            _mem_gb(),
        )

        # Load DEM at target resolution
        dem_data = load_dem_window(dem_path, bbox, group.dem_resolution)
        if dem_data is None:
            log.info("  Window all-NaN (ocean/nodata), skipping")
            checkpoint["completed_windows"][win_key] = "skipped_no_data"
            save_checkpoint(checkpoint)
            continue

        # Compute terrain derivatives
        lat_center = (bbox[1] + bbox[3]) / 2.0
        deriv_t0 = time.time()
        derivatives = compute_terrain_derivatives(
            dem_data["elevation"],
            dem_data["pixel_size_x"],
            dem_data["pixel_size_y"],
            lat_center,
        )
        log.info("  Terrain derivatives in %.1fs", time.time() - deriv_t0)

        # For each target H3 resolution
        win_cells = 0
        for h3_res in group.h3_resolutions:
            h3_t0 = time.time()
            cells = generate_h3_cells_for_window(bbox, h3_res)
            if not cells:
                continue

            cell_data = interpolate_terrain_to_cells(cells, dem_data, derivatives)

            # Filter out cells with no valid elevation (ocean/nodata)
            valid = [j for j, e in enumerate(cell_data["elev"]) if e is not None]
            if not valid:
                continue

            columns = {
                "h3_index": pa.array([cell_data["h3_index"][j] for j in valid], type=pa.string()),
                "lat": pa.array([cell_data["lat"][j] for j in valid], type=pa.float32()),
                "lon": pa.array([cell_data["lon"][j] for j in valid], type=pa.float32()),
                "elev": pa.array([cell_data["elev"][j] for j in valid], type=pa.float32()),
                "slope": pa.array([cell_data["slope"][j] for j in valid], type=pa.float32()),
                "aspect": pa.array([cell_data["aspect"][j] for j in valid], type=pa.float32()),
                "tri": pa.array([cell_data["tri"][j] for j in valid], type=pa.float32()),
                "tpi": pa.array([cell_data["tpi"][j] for j in valid], type=pa.float32()),
            }

            table = pa.table(columns)

            # Write temp Parquet file
            res_dir = temp_dir / f"h3_res={h3_res}"
            res_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(table, res_dir / f"{win['id']}.parquet", compression="zstd")

            win_cells += len(valid)
            log.info("  H3 res %d: %d cells (%.1fs)", h3_res, len(valid), time.time() - h3_t0)

        log.info(
            "  Window done: %d total cells in %.1fs",
            win_cells,
            time.time() - win_t0,
        )

        processed += 1
        checkpoint["completed_windows"][win_key] = "done"
        save_checkpoint(checkpoint)

    if skipped > 0:
        log.info("Skipped %d already-processed windows (from checkpoint)", skipped)
    log.info("Processed %d windows for group '%s'", processed, group.name)

    # Merge temp files to final output via DuckDB
    cells_written = {}
    for h3_res in group.h3_resolutions:
        res_key = f"res_{h3_res}"
        if res_key in checkpoint.get("completed_resolutions", []):
            log.info("Skipping already-written H3 res %d", h3_res)
            continue

        count = merge_temp_to_final(con, temp_dir, h3_res)
        cells_written[h3_res] = count

        checkpoint.setdefault("completed_resolutions", []).append(res_key)
        save_checkpoint(checkpoint)

    # Clean up temp files for this group
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
        log.info("Cleaned up temp files for group '%s'", group.name)

    return cells_written


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Global DEM to native Parquet 2.11+ pipeline")
    parser.add_argument(
        "--resolutions",
        type=str,
        default="1,2,3,4,5,6,7,8,9,10",
        help="Comma-separated H3 resolutions to process (default: 1-10)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List windows without processing",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Override scratch directory (default: /data/scratch or $SCRATCH_DIR)",
    )
    parser.add_argument(
        "--dem-path",
        type=str,
        default=None,
        help="Path to local DEM COG (default: auto-detect in scratch dir, fallback to remote URL)",
    )
    args = parser.parse_args()

    if args.scratch_dir:
        global SCRATCH_DIR, CHECKPOINT_FILE
        SCRATCH_DIR = Path(args.scratch_dir)
        CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

    target_resolutions = set(int(r) for r in args.resolutions.split(","))

    # Resolve DEM source (local file > remote URL)
    dem_path = resolve_dem_path(args.dem_path)

    # Generate geographic windows
    windows = generate_windows()

    start = time.time()
    log.info("=" * 60)
    log.info("Starting DEM to Parquet pipeline")
    log.info("  DEM source: %s", dem_path)
    log.info("  Windows: %d (%g° x %g°)", len(windows), WINDOW_SIZE, WINDOW_SIZE)
    log.info("  Target resolutions: %s", sorted(target_resolutions))
    log.info("  Output: %s", f"s3://{S3_BUCKET}/{S3_PREFIX}/" if S3_BUCKET else "local")
    log.info("  Scratch: %s", SCRATCH_DIR)
    log.info("  Memory: %s", _mem_gb())
    log.info("=" * 60)

    if args.dry_run:
        log.info("Dry run — %d windows generated. Examples:", len(windows))
        for w in windows[:5]:
            log.info("  %s bbox=%s", w["id"], w["bbox"])
        if len(windows) > 5:
            log.info("  ... and %d more", len(windows) - 5)
        return

    # Load checkpoint
    checkpoint = load_checkpoint()
    log.info(
        "Checkpoint: %d windows previously completed",
        len(checkpoint.get("completed_windows", {})),
    )

    # Setup DuckDB
    con = get_duckdb_connection()

    # Process each resolution group
    total_cells: dict[int, int] = {}
    for group in RESOLUTION_GROUPS:
        # Filter to only requested resolutions
        group_resolutions = [r for r in group.h3_resolutions if r in target_resolutions]
        if not group_resolutions:
            continue

        filtered_group = ResolutionGroup(
            name=group.name,
            h3_resolutions=group_resolutions,
            dem_resolution=group.dem_resolution,
            description=group.description,
        )
        cells = process_resolution_group(filtered_group, windows, dem_path, checkpoint, con)
        total_cells.update(cells)

    elapsed = time.time() - start

    # Write metadata
    write_metadata(total_cells, elapsed)

    # Summary
    log.info("=" * 60)
    log.info("Pipeline complete in %.1f minutes", elapsed / 60)
    for res in sorted(total_cells):
        log.info("  H3 res %2d: %12d cells", res, total_cells[res])
    total = sum(total_cells.values())
    log.info("  Total:     %12d cells", total)
    log.info("  Peak memory: %s", _mem_gb())
    log.info("=" * 60)

    # Write completion marker
    marker = SCRATCH_DIR / "COMPLETE"
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(f"Completed at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")


if __name__ == "__main__":
    main()
