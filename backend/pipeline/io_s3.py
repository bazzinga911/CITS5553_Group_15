"""
I/O helpers for reading/writing GeoParquet files (local or S3).
"""

import geopandas as gpd
import fsspec


def read_points(path):
    """
    Read a GeoParquet file into a GeoDataFrame.
    Works with local paths or s3:// URIs (requires s3fs).
    """
    with fsspec.open(path) as f:
        gdf = gpd.read_parquet(f)
    return gdf


def write_grid(path, gdf):
    """
    Write a GeoDataFrame to GeoParquet.
    Works with local paths or s3:// URIs (requires s3fs).
    """
    with fsspec.open(path, "wb") as f:
        gdf.to_parquet(f, index=False)
