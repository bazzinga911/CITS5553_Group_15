"""
Grid construction and point assignment utilities.
"""

import geopandas as gpd
import pandas as pd
from shapely.geometry import box


def make_regular_grid(bounds, cell_size_m, crs):
    """
    Create a regular grid (rectangular cells) covering the given bounds.

    Parameters
    ----------
    bounds : tuple (minx, miny, maxx, maxy)
        Extent to cover (projected coordinates, e.g. meters).
    cell_size_m : int
        Grid cell size in meters.
    crs : str or CRS
        Coordinate reference system.

    Returns
    -------
    GeoDataFrame with polygon cells and Grid_ID.
    """
    minx, miny, maxx, maxy = bounds

    nx = int((maxx - minx) // cell_size_m) + 1
    ny = int((maxy - miny) // cell_size_m) + 1

    polys = []
    ids = []
    gid = 0
    for iy in range(ny):
        for ix in range(nx):
            x0 = minx + ix * cell_size_m
            x1 = x0 + cell_size_m
            y0 = miny + iy * cell_size_m
            y1 = y0 + cell_size_m
            polys.append(box(x0, y0, x1, y1))
            ids.append(gid)
            gid += 1

    gdf = gpd.GeoDataFrame({"Grid_ID": ids}, geometry=polys, crs=crs)
    return gdf


def assign_grid_id(points_gdf, grid_gdf):
    """
    Assign each point to a grid cell using spatial join.

    Parameters
    ----------
    points_gdf : GeoDataFrame
        Point samples with geometry.
    grid_gdf : GeoDataFrame
        Grid polygons with Grid_ID.

    Returns
    -------
    GeoDataFrame with added 'Grid_ID', 'grid_ix', 'grid_iy' columns.
    """
    joined = gpd.sjoin(points_gdf, grid_gdf[["Grid_ID", "geometry"]], how="left", predicate="within")
    joined = joined.drop(columns=["index_right"])

    # derive ix/iy from Grid_ID (row-major order)
    nx = int(grid_gdf["Grid_ID"].max() ** 0.5) + 1  # rough guess
    joined["grid_ix"] = joined["Grid_ID"] % nx
    joined["grid_iy"] = joined["Grid_ID"] // nx

    return joined
