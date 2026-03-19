from pathlib import Path
from typing import Optional, Union

import geopandas as gpd
import numpy as np
import pandas as pd
from loguru import logger


def load_vector_dataset(
    file_path: Union[str, Path],
    logger: Optional[logger] = None,
) -> Optional[gpd.GeoDataFrame]:
    """Load a vector dataset from file based on file extension.

    Supports GeoPackage, Shapefile, GeoJSON, and Parquet formats.

    Args:
        file_path: Path to the vector dataset file.
        logger: Optional logger instance for logging messages.

    Returns:
        GeoDataFrame if successful, None otherwise.

    Raises:
        FileNotFoundError: If the file does not exist.
    """
    file_path = Path(file_path)

    if not file_path.exists():
        if logger:
            logger.warning(f"Vector dataset file not found: {file_path}")
        raise FileNotFoundError(f"Vector dataset file not found: {file_path}")

    suffix = file_path.suffix.lower()

    if logger:
        logger.info(f"Loading vector dataset from {file_path}")

    # GeoPackage, Shapefile, GeoJSON formats
    if suffix in [".gpkg", ".shp", ".geojson", ".gjson"]:
        vector_ds = gpd.read_file(file_path)
    elif suffix in [".parquet"]:
        vector_ds = gpd.read_parquet(file_path)
    else:
        if logger:
            logger.warning(f"Unsupported vector file format: {suffix}")
        return None

    return vector_ds


def calculate_water_area_after(
    df_water, break_date_after, water_column: str, stats=["mean", "median", "std", "min", "max"]
):
    after = df_water.loc[break_date_after:][water_column].agg(stats)
    cols_out = [f"post_break_{col}" for col in after.index]
    after.index = cols_out
    return after


def calculate_water_area_before(df_water, break_date, water_column: str, stats=["mean", "median", "std", "min", "max"]):
    before = df_water.loc[:break_date][water_column].agg(stats)
    cols_out = [f"pre_break_{col}" for col in before.index]
    before.index = cols_out
    return before


def get_water_dataset_type(input_ds) -> str:
    """Determine the water dataset type based on the presence of specific variables in the dataset."""
    if "area_water_permanent" in input_ds.data_vars:
        water_dataset_type = "jrc"
    elif "water" in input_ds.data_vars:
        water_dataset_type = "dynamic_world"
    else:
        raise ValueError("Unknown water dataset type")

    return water_dataset_type


def calculate_temporal_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate temporal statistics for a given DataFrame."""
    df[["pre_break_median", "post_break_median"]].replace(0, np.nan, inplace=True)
    # df.dropna(subset=["pre_break_median", "post_break_median"], inplace=True)
    breaks = pd.to_datetime(df["date_break"])
    df["date_break_year"] = breaks.dt.year
    df["date_break_month"] = breaks.dt.month
    # change area ha
    df["water_change_ha"] = df["pre_break_median"] - df["post_break_median"]
    # change area perc
    df["water_change_perc"] = df["water_change_ha"].div(df["pre_break_median"].replace(0, np.nan)) * 100
    return df
