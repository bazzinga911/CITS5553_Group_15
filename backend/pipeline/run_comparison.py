"""
Pipeline entrypoint: orchestrates reading input Parquet files,
gridding, assigning Grid_IDs, running comparisons, and writing outputs.

Usage:
    python -m backend.pipeline.run_comparison \
        --orig path/to/orig.parquet \
        --dl path/to/dl.parquet \
        --out results/ \
        --cell-km 100 \
        --method max
"""

import argparse
import os
import geopandas as gpd
from backend.comparisons.max_per_cell import compare
from backend.pipeline.grid import make_regular_grid, assign_grid_id
from backend.pipeline.io_s3 import read_points, write_grid


def main():
    # ------------------------------------------------------------------
    # 1. Parse CLI arguments
    # ------------------------------------------------------------------
    parser = argparse.ArgumentParser(description="Run comparison pipeline.")
    parser.add_argument("--orig", required=True, help="Path to Original dataset (Parquet/GeoParquet)")
    parser.add_argument("--dl", required=True, help="Path to DL dataset (Parquet/GeoParquet)")
    parser.add_argument("--out", required=True, help="Output folder (local or s3://)")
    parser.add_argument("--cell-km", type=int, default=100, help="Cell size in kilometres (default: 100 km)")
    parser.add_argument("--method", choices=["max"], default="max", help="Comparison method (default: max)")
    args = parser.parse_args()

    # ------------------------------------------------------------------
    # 2. Read inputs
    # ------------------------------------------------------------------
    orig = read_points(args.orig)   # GeoDataFrame with Te_ppm + geometry
    dl   = read_points(args.dl)

    # ------------------------------------------------------------------
    # 3. Build grid
    # ------------------------------------------------------------------
    cell_size_m = args.cell_km * 1000
    bounds = gpd.GeoSeries(pd.concat([orig.geometry, dl.geometry])).total_bounds
    grid = make_regular_grid(bounds=bounds, cell_size_m=cell_size_m, crs=orig.crs)

    # Derive grid dimensions (nx, ny)
    minx, miny, maxx, maxy = bounds
    nx = int((maxx - minx) // cell_size_m) + 1
    ny = int((maxy - miny) // cell_size_m) + 1

    # ------------------------------------------------------------------
    # 4. Assign Grid_IDs
    # ------------------------------------------------------------------
    orig_idx = assign_grid_id(orig, grid)
    dl_idx   = assign_grid_id(dl, grid)

    # ------------------------------------------------------------------
    # 5. Compare
    # ------------------------------------------------------------------
    arr_orig, arr_dl, arr_cmp = compare(orig_idx, dl_idx, nx=nx, ny=ny, method=args.method)

    # ------------------------------------------------------------------
    # 6. Write outputs
    # ------------------------------------------------------------------
    outdir = args.out
    os.makedirs(outdir, exist_ok=True)

    write_grid(os.path.join(outdir, "orig_grid.parquet"), arr_orig)
    write_grid(os.path.join(outdir, "dl_grid.parquet"), arr_dl)
    write_grid(os.path.join(outdir, "comp_grid.parquet"), arr_cmp)

    # Done flag (for UI)
    with open(os.path.join(outdir, "done.flag"), "w") as f:
        f.write("done")

    print("Pipeline finished")


if __name__ == "__main__":
    main()
