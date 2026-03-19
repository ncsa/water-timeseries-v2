"""Spatial utilities for working with geospatial data."""

from typing import Optional

import geopandas as gpd


def filter_gdf_by_bbox(
    gdf: gpd.GeoDataFrame,
    bbox_west: Optional[float] = None,
    bbox_south: Optional[float] = None,
    bbox_east: Optional[float] = None,
    bbox_north: Optional[float] = None,
    id_column: str = "id_geohash",
) -> gpd.GeoDataFrame:
    """Filter a GeoDataFrame by bounding box coordinates.

    Filters features based on their centroid falling within the specified
    bounding box. At least one bbox boundary must be provided.

    Args:
        gdf: Input GeoDataFrame with geometry column.
        bbox_west: Minimum longitude (west) boundary.
        bbox_south: Minimum latitude (south) boundary.
        bbox_east: Maximum longitude (east) boundary.
        bbox_north: Maximum latitude (north) boundary.
        id_column: Name of the column containing unique identifiers.
            Defaults to "id_geohash".

    Returns:
        Filtered GeoDataFrame containing only features whose centroids
        fall within the bounding box.

    Raises:
        ValueError: If no bbox parameters are provided.

    Example:
        >>> import geopandas as gpd
        >>> gdf = gpd.read_file("lakes.gpkg")
        >>> filtered = filter_gdf_by_bbox(gdf, bbox_west=10, bbox_south=50, bbox_east=20, bbox_north=60)
    """
    # Check if at least one bbox parameter is provided
    if all(v is None for v in [bbox_west, bbox_south, bbox_east, bbox_north]):
        raise ValueError("At least one bbox parameter must be provided")

    # Calculate centroids
    cent = gdf.geometry.centroid

    # Build mask for filtering
    mask = True
    if bbox_west is not None:
        mask &= cent.x >= bbox_west
    if bbox_east is not None:
        mask &= cent.x <= bbox_east
    if bbox_south is not None:
        mask &= cent.y >= bbox_south
    if bbox_north is not None:
        mask &= cent.y <= bbox_north

    return gdf[mask]
