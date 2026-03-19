"""
Test suite for EarthEngineDownloader class.

This module contains comprehensive tests for authentication, downloading
Dynamic World data, JRC data, and error handling.
"""

import pathlib
import unittest.mock as mock

import pytest

from water_timeseries.downloader import EarthEngineDownloader
from water_timeseries.utils.spatial import filter_gdf_by_bbox


# Path to test data
TEST_DATA_DIR = pathlib.Path(__file__).parent / "data"
VECTOR_DATASET = TEST_DATA_DIR / "lake_polygons.parquet"


class TestEarthEngineDownloaderAuthentication:
    """Test authentication and initialization of EarthEngineDownloader."""

    def test_init_with_valid_ee_project(self):
        """Test initialization with a valid ee_project argument."""
        downloader = EarthEngineDownloader(ee_project="my-valid-project")
        assert downloader.ee_project == "my-valid-project"

    def test_init_with_empty_string_ee_project(self):
        """Test that empty string ee_project raises ValueError."""
        with pytest.raises(ValueError, match="ee_project must be provided or set as EE_PROJECT environment variable"):
            EarthEngineDownloader(ee_project="")

    def test_init_with_none_and_env_var_set(self, monkeypatch):
        """Test initialization with None ee_project uses EE_PROJECT env var."""
        monkeypatch.setenv("EE_PROJECT", "env-project-id")
        downloader = EarthEngineDownloader(ee_project=None)
        assert downloader.ee_project == "env-project-id"

    def test_init_with_none_and_no_env_var(self, monkeypatch):
        """Test that None ee_project without env var raises ValueError."""
        monkeypatch.delenv("EE_PROJECT", raising=False)
        with pytest.raises(ValueError, match="ee_project must be provided"):
            EarthEngineDownloader(ee_project=None)

    def test_argument_overwrites_env_var(self, monkeypatch):
        """Test that ee_project argument takes precedence over EE_PROJECT env var."""
        monkeypatch.setenv("EE_PROJECT", "env-project-id")
        downloader = EarthEngineDownloader(ee_project="arg-project-id")
        assert downloader.ee_project == "arg-project-id"

    def test_init_with_none_uses_env_var_even_with_auth(self, monkeypatch):
        """Test that None ee_project works with ee_auth=True when env var is set."""
        monkeypatch.setenv("EE_PROJECT", "env-project-id")
        with mock.patch("geemap.ee_initialize"):
            downloader = EarthEngineDownloader(ee_project=None, ee_auth=True)
            assert downloader.ee_project == "env-project-id"
            assert downloader.ee_auth is True

    def test_invalid_env_var_type(self, monkeypatch):
        """Test that non-string EE_PROJECT env var is ignored."""
        monkeypatch.setenv("EE_PROJECT", "")
        with pytest.raises(ValueError, match="ee_project must be provided"):
            EarthEngineDownloader(ee_project=None)


class TestSpatialFiltering:
    """Test spatial filtering functionality."""

    def test_filter_gdf_by_bboxAlaska(self):
        """Test spatial bbox filtering on test dataset."""
        import geopandas as gpd

        # Load test dataset
        gdf = gpd.read_parquet(VECTOR_DATASET)

        # Verify initial count
        assert len(gdf) == 118, f"Expected 118 features, got {len(gdf)}"

        # Apply bbox filter for Alaska region
        filtered_gdf = filter_gdf_by_bbox(
            gdf,
            bbox_west=-164.2,
            bbox_east=-164,
            bbox_south=66.5,
            bbox_north=66.55,
        )

        # Verify filtered count
        assert len(filtered_gdf) == 17, f"Expected 17 features after filtering, got {len(filtered_gdf)}"

    def test_filter_gdf_by_bbox_all_params(self):
        """Test spatial bbox filtering with all parameters provided."""
        import geopandas as gpd

        gdf = gpd.read_parquet(VECTOR_DATASET)

        # Filter with all bbox parameters
        filtered = filter_gdf_by_bbox(
            gdf,
            bbox_west=-164.2,
            bbox_east=-164,
            bbox_south=66.5,
            bbox_north=66.55,
        )

        assert isinstance(filtered, gpd.GeoDataFrame)
        assert len(filtered) < len(gdf)

    def test_filter_gdf_by_bbox_partial_params(self):
        """Test spatial bbox filtering with only west and east parameters."""
        import geopandas as gpd

        gdf = gpd.read_parquet(VECTOR_DATASET)

        # Filter with only west and east
        filtered = filter_gdf_by_bbox(
            gdf,
            bbox_west=-164.2,
            bbox_east=-164,
        )

        assert len(filtered) < len(gdf)

    def test_filter_gdf_by_bbox_no_params_raises_error(self):
        """Test that filtering without any bbox params raises ValueError."""
        import geopandas as gpd

        gdf = gpd.read_parquet(VECTOR_DATASET)

        with pytest.raises(ValueError, match="At least one bbox parameter must be provided"):
            filter_gdf_by_bbox(gdf)


# Run tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
