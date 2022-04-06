import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.wind_cube_profile import Pipeline

parent = os.path.dirname(__file__)


def test_pipeline_rtd():
    set_dev_env()

    # Real-time data
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc_rtd.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/WLS71460_2022_02_01__00_00_00.rtd.7z", parent)
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc.wind_cube_profile_rtd.b0.20220201.000000.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)


def test_pipeline_sta():

    # 10-minute statistics
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc_sta.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/WLS71460_2022_02_02__00_01_00.sta.7z", parent)
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc.wind_cube_profile_sta.b0.20220202.000100.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)
