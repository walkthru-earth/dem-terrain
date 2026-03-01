"""Global DEM to native Parquet 2.11+ pipeline.

Converts GEDTM-30m global DEM to H3-indexed, partitioned native Parquet files
with terrain derivatives (elevation, slope, aspect, TRI, TPI).

DuckDB 1.5.0-dev writes native Parquet GEOMETRY (first-class logical type with
per-row-group bounding box stats, geometry shredding, and spatial predicate
pushdown). This is NOT GeoParquet's metadata convention.

Usage:
    uv run main.py                          # Full global processing
    uv run main.py --resolutions 1,2,3,4,5  # Only low-res
    uv run main.py --dry-run                # List tiles without processing
"""

from __future__ import annotations

import json
import logging
import os
import resource
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import duckdb
import h3
import numpy as np
import pyarrow as pa
import rasterio
from pystac_client import Client as STACClient
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

STAC_URL = "https://stac.openlandmap.org"
COLLECTION = "gedtm-30m"

# S3 output
S3_BUCKET = os.environ.get("S3_BUCKET", "")
S3_PREFIX = os.environ.get("S3_PREFIX", "").strip("/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Local scratch directory (NVMe on cloud instance)
SCRATCH_DIR = Path(os.environ.get("SCRATCH_DIR", "/data/scratch"))
CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

# GEDTM-30m extent (lat -65 to 85, lon -180 to 180)
LAT_MIN, LAT_MAX = -65.0, 85.0
LON_MIN, LON_MAX = -180.0, 180.0

# H3 parent resolution for sub-partitioning res 6-10 files
H3_PARENT_RES = 2


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

    # TRI (Terrain Ruggedness Index): mean absolute difference from neighbors
    # Using a 3x3 kernel
    padded = xp.pad(elev, 1, mode="edge")
    tri_sum = xp.zeros_like(elev)
    for di in range(-1, 2):
        for dj in range(-1, 2):
            if di == 0 and dj == 0:
                continue
            neighbor = padded[1 + di : 1 + di + elev.shape[0], 1 + dj : 1 + dj + elev.shape[1]]
            tri_sum += xp.abs(neighbor - elev)
    tri = tri_sum / 8.0

    # TPI (Topographic Position Index): elevation minus mean of neighbors
    neighbor_sum = xp.zeros_like(elev)
    for di in range(-1, 2):
        for dj in range(-1, 2):
            if di == 0 and dj == 0:
                continue
            neighbor = padded[1 + di : 1 + di + elev.shape[0], 1 + dj : 1 + dj + elev.shape[1]]
            neighbor_sum += neighbor
    tpi = elev - neighbor_sum / 8.0

    # Transfer back to CPU if on GPU
    if cp is not None:
        slope_deg = cp.asnumpy(slope_deg)
        aspect_deg = cp.asnumpy(aspect_deg)
        tri = cp.asnumpy(tri)
        tpi = cp.asnumpy(tpi)
        elev = cp.asnumpy(elev)

    return {
        "slope": slope_deg.astype(np.float32),
        "aspect": aspect_deg.astype(np.float32),
        "tri": tri.astype(np.float32),
        "tpi": tpi.astype(np.float32),
    }


# ---------------------------------------------------------------------------
# STAC tile discovery
# ---------------------------------------------------------------------------


def discover_tiles() -> list[dict]:
    """Query GEDTM-30m STAC catalog for all available COG tile URLs.

    Returns list of dicts with keys: id, url, bbox (west, south, east, north).
    """
    log.info("Discovering GEDTM-30m tiles from STAC catalog...")
    client = STACClient.open(STAC_URL)

    tiles = []
    search = client.search(
        collections=[COLLECTION],
        bbox=[LON_MIN, LAT_MIN, LON_MAX, LAT_MAX],
        max_items=None,
    )
    for item in search.items():
        # GEDTM-30m items have a COG asset (typically "dtm" or "data")
        asset = None
        for key in ("dtm", "data", "image"):
            if key in item.assets:
                asset = item.assets[key]
                break
        if asset is None:
            # Fall back to first asset with a GeoTIFF media type
            for a in item.assets.values():
                if a.media_type and "tiff" in a.media_type.lower():
                    asset = a
                    break
        if asset is None:
            log.warning("Skipping item %s — no COG asset found", item.id)
            continue

        bbox = item.bbox  # [west, south, east, north]
        tiles.append({"id": item.id, "url": asset.href, "bbox": bbox})

    log.info("Discovered %d tiles", len(tiles))
    return tiles


# ---------------------------------------------------------------------------
# Tile processing
# ---------------------------------------------------------------------------


def load_dem_tile(url: str, target_resolution: float, bbox: list[float]) -> dict | None:
    """Load a DEM COG tile at the target resolution via rasterio.

    Returns dict with keys: elevation (2D ndarray), lats (1D), lons (1D),
    pixel_size_x, pixel_size_y, or None if the tile has no valid data.
    """
    west, south, east, north = bbox
    width = round((east - west) / target_resolution)
    height = round((north - south) / target_resolution)

    # Clamp to reasonable sizes
    width = max(2, min(width, 20_000))
    height = max(2, min(height, 20_000))

    t0 = time.time()
    try:
        with rasterio.open(url) as src:
            elevation = src.read(
                1,
                out_shape=(height, width),
                resampling=rasterio.enums.Resampling.bilinear,
            ).astype(np.float32)

            nodata = src.nodata
            if nodata is not None:
                elevation[elevation == nodata] = np.nan
            if np.all(np.isnan(elevation)):
                log.info("  DEM tile all-NaN, skipping")
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
        log.warning("  DEM load FAILED (%.1fs): %s", time.time() - t0, e)
        return None


def generate_h3_cells_for_tile(
    bbox: list[float],
    h3_res: int,
) -> list[str]:
    """Generate H3 cell IDs that have their center within the tile bbox.

    Ownership rule: a cell belongs to a tile if its center falls within the
    tile's bounding box. This ensures each cell is processed exactly once.
    """
    west, south, east, north = bbox

    # Use h3.h3shape_to_cells with a polygon covering the bbox
    # Add a small buffer to ensure edge cells are included
    buffer = h3.average_hexagon_edge_length(h3_res, unit="deg") * 1.5
    poly = h3.LatLngPoly(
        [
            (south - buffer, west - buffer),
            (south - buffer, east + buffer),
            (north + buffer, east + buffer),
            (north + buffer, west - buffer),
        ]
    )
    candidate_cells = h3.h3shape_to_cells(poly, h3_res)

    # Filter: keep only cells whose center falls strictly within the tile bbox
    owned_cells = []
    for cell in candidate_cells:
        lat, lon = h3.cell_to_latlng(cell)
        if south <= lat < north and west <= lon < east:
            owned_cells.append(cell)

    return owned_cells


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
            log.info("  S3 configured: region=%s, bucket=%s", AWS_REGION, S3_BUCKET)
        else:
            log.warning("  S3_BUCKET set but AWS credentials missing!")
    else:
        log.info("  No S3_BUCKET — writing to local filesystem")

    return con


def write_parquet_for_resolution(
    con: duckdb.DuckDBPyConnection,
    records: list[dict[str, list]],
    h3_res: int,
) -> int:
    """Write accumulated records for a single H3 resolution to Parquet.

    For res 1-5: single file.
    For res 6-10: Hive-partitioned by H3 parent at res 2.

    Returns the number of rows written.
    """
    # Merge all tile records into flat arrays
    merged = {k: [] for k in ("h3_index", "lat", "lon", "elev", "slope", "aspect", "tri", "tpi")}
    for rec in records:
        for k in merged:
            merged[k].extend(rec[k])

    if not merged["h3_index"]:
        log.warning("No data for H3 res %d — skipping", h3_res)
        return 0

    # Deduplicate by h3_index (shouldn't happen with ownership rule, but safety net)
    raw_count = len(merged["h3_index"])
    seen = set()
    unique_indices = []
    for i, idx in enumerate(merged["h3_index"]):
        if idx not in seen:
            seen.add(idx)
            unique_indices.append(i)

    dupes = raw_count - len(unique_indices)
    for k in merged:
        merged[k] = [merged[k][i] for i in unique_indices]

    total_rows = len(merged["h3_index"])
    if dupes > 0:
        log.info("Writing H3 res %d: %d cells (%d dupes removed)", h3_res, total_rows, dupes)
    else:
        log.info("Writing H3 res %d: %d cells", h3_res, total_rows)

    # Create Arrow table
    table = pa.table(
        {
            "h3_index": pa.array(merged["h3_index"], type=pa.string()),
            "lat": pa.array(merged["lat"], type=pa.float32()),
            "lon": pa.array(merged["lon"], type=pa.float32()),
            "elev": pa.array(merged["elev"], type=pa.float32()),
            "slope": pa.array(merged["slope"], type=pa.float32()),
            "aspect": pa.array(merged["aspect"], type=pa.float32()),
            "tri": pa.array(merged["tri"], type=pa.float32()),
            "tpi": pa.array(merged["tpi"], type=pa.float32()),
        }
    )

    con.register("tile_df", table)

    if S3_BUCKET:
        output_base = f"s3://{S3_BUCKET}/{S3_PREFIX}/dem-terrain"
    else:
        output_base = str(SCRATCH_DIR / "output" / "dem-terrain")
        Path(output_base).mkdir(parents=True, exist_ok=True)

    t0 = time.time()

    if h3_res <= 5:
        output_path = f"{output_base}/h3_res={h3_res}/data.parquet"
        if not S3_BUCKET:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)

        con.sql(f"""
            COPY (
                SELECT h3_index,
                       ST_Point(lon, lat)::GEOMETRY('EPSG:4326') AS geometry,
                       lat, lon, elev, slope, aspect, tri, tpi
                FROM tile_df
                ORDER BY h3_index
            ) TO '{output_path}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
             ROW_GROUP_SIZE 1000000)
        """)
        log.info("  Wrote %s in %.1fs", output_path, time.time() - t0)
    else:
        parents = [h3.cell_to_parent(cell, H3_PARENT_RES) for cell in merged["h3_index"]]
        unique_parents = set(parents)
        parent_col = pa.array(parents, type=pa.string())
        table_with_parent = table.append_column("h3_parent_2", parent_col)
        con.register("tile_df_partitioned", table_with_parent)

        output_dir = f"{output_base}/h3_res={h3_res}"
        if not S3_BUCKET:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

        con.sql(f"""
            COPY (
                SELECT h3_index,
                       ST_Point(lon, lat)::GEOMETRY('EPSG:4326') AS geometry,
                       lat, lon, elev, slope, aspect, tri, tpi,
                       h3_parent_2
                FROM tile_df_partitioned
                ORDER BY h3_index
            ) TO '{output_dir}'
            (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
             ROW_GROUP_SIZE 1000000, PARTITION_BY (h3_parent_2),
             OVERWRITE_OR_IGNORE)
        """)
        log.info(
            "  Wrote %d partitions to %s/ in %.1fs",
            len(unique_parents),
            output_dir,
            time.time() - t0,
        )

        con.unregister("tile_df_partitioned")

    con.unregister("tile_df")
    return total_rows


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------


def load_checkpoint() -> dict:
    """Load processing checkpoint (which tiles are done)."""
    if CHECKPOINT_FILE.exists():
        return json.loads(CHECKPOINT_FILE.read_text())
    return {"completed_tiles": {}, "completed_resolutions": []}


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
        "source_url": "https://stac.openlandmap.org/gedtm-30m/collection.json",
        "crs": "EPSG:4326",
        "geometry_type": "native_parquet_2.11_geometry",
        "geometry_encoding": "WKB with GEOMETRY logical type annotation",
        "h3_resolutions": list(range(1, 11)),
        "h3_parent_partition_res": H3_PARENT_RES,
        "partitioning": {
            "res_1_to_5": "single file per resolution",
            "res_6_to_10": f"hive-partitioned by h3_parent_{H3_PARENT_RES}",
        },
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
        key = f"{S3_PREFIX}/dem-terrain/_metadata.json" if S3_PREFIX else "dem-terrain/_metadata.json"
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
    tiles: list[dict],
    checkpoint: dict,
    con: duckdb.DuckDBPyConnection,
) -> dict[int, int]:
    """Process all tiles for a resolution group.

    Returns dict mapping h3_res -> total cells written.
    """
    log.info(
        "=== Processing group '%s' (H3 res %s, DEM %.5f°) ===",
        group.name,
        group.h3_resolutions,
        group.dem_resolution,
    )

    # Accumulate records per H3 resolution
    records: dict[int, list[dict[str, list]]] = {r: [] for r in group.h3_resolutions}

    skipped = 0
    for i, tile in enumerate(tqdm(tiles, desc=f"Tiles ({group.name})", unit="tile")):
        tile_key = f"{group.name}:{tile['id']}"
        if tile_key in checkpoint["completed_tiles"]:
            skipped += 1
            continue

        tile_t0 = time.time()
        log.info(
            "[%d/%d] Tile %s bbox=[%.1f,%.1f,%.1f,%.1f] (mem=%s)",
            i + 1,
            len(tiles),
            tile["id"],
            *tile["bbox"],
            _mem_gb(),
        )

        # Load DEM at target resolution
        dem_data = load_dem_tile(tile["url"], group.dem_resolution, tile["bbox"])
        if dem_data is None:
            checkpoint["completed_tiles"][tile_key] = "skipped_no_data"
            save_checkpoint(checkpoint)
            continue

        # Compute terrain derivatives
        lat_center = (tile["bbox"][1] + tile["bbox"][3]) / 2.0
        deriv_t0 = time.time()
        derivatives = compute_terrain_derivatives(
            dem_data["elevation"],
            dem_data["pixel_size_x"],
            dem_data["pixel_size_y"],
            lat_center,
        )
        log.info("  Terrain derivatives computed in %.1fs", time.time() - deriv_t0)

        # For each target H3 resolution
        tile_cells = 0
        for h3_res in group.h3_resolutions:
            cells = generate_h3_cells_for_tile(tile["bbox"], h3_res)
            if not cells:
                continue

            cell_data = interpolate_terrain_to_cells(cells, dem_data, derivatives)
            records[h3_res].append(cell_data)
            tile_cells += len(cells)
            log.info("  H3 res %d: %d cells", h3_res, len(cells))

        log.info(
            "  Tile done: %d total cells in %.1fs",
            tile_cells,
            time.time() - tile_t0,
        )

        checkpoint["completed_tiles"][tile_key] = "done"
        save_checkpoint(checkpoint)

    if skipped > 0:
        log.info("Skipped %d already-processed tiles (from checkpoint)", skipped)

    # Write Parquet for each resolution
    cells_written = {}
    for h3_res in group.h3_resolutions:
        res_key = f"res_{h3_res}"
        if res_key in checkpoint.get("completed_resolutions", []):
            log.info("Skipping already-written H3 res %d", h3_res)
            continue

        count = write_parquet_for_resolution(con, records[h3_res], h3_res)
        cells_written[h3_res] = count

        checkpoint.setdefault("completed_resolutions", []).append(res_key)
        save_checkpoint(checkpoint)

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
        help="Discover tiles and print count without processing",
    )
    parser.add_argument(
        "--scratch-dir",
        type=str,
        default=None,
        help="Override scratch directory (default: /data/scratch or $SCRATCH_DIR)",
    )
    args = parser.parse_args()

    if args.scratch_dir:
        global SCRATCH_DIR, CHECKPOINT_FILE
        SCRATCH_DIR = Path(args.scratch_dir)
        CHECKPOINT_FILE = SCRATCH_DIR / "checkpoint.json"

    target_resolutions = set(int(r) for r in args.resolutions.split(","))

    start = time.time()
    log.info("=" * 60)
    log.info("Starting DEM to Parquet pipeline")
    log.info("  Target resolutions: %s", sorted(target_resolutions))
    log.info("  Output: %s", f"s3://{S3_BUCKET}/{S3_PREFIX}/dem-terrain/" if S3_BUCKET else "local")
    log.info("  Scratch: %s", SCRATCH_DIR)
    log.info("  Memory: %s", _mem_gb())
    log.info("=" * 60)

    # Discover tiles
    tiles = discover_tiles()
    if not tiles:
        log.error("No tiles found — check STAC catalog connectivity")
        sys.exit(1)

    if args.dry_run:
        log.info("Dry run — %d tiles discovered. Exiting.", len(tiles))
        for t in tiles[:5]:
            log.info("  Example: %s bbox=%s", t["id"], t["bbox"])
        if len(tiles) > 5:
            log.info("  ... and %d more", len(tiles) - 5)
        return

    # Load checkpoint
    checkpoint = load_checkpoint()
    log.info(
        "Checkpoint: %d tiles previously completed",
        len(checkpoint.get("completed_tiles", {})),
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
        cells = process_resolution_group(filtered_group, tiles, checkpoint, con)
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
