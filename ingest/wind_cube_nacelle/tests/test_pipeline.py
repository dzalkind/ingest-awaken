import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.wind_cube_nacelle import Pipeline

parent = os.path.dirname(__file__)


def test_pipeline_rtd():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_rtd.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand(
            "tests/data/input/WIPO0200010-(wi020200010)_real_time_data_2022-01-12_19-00-00.csv",
            parent,
        )
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc.wind_cube_nacelle_rtd.b0.20220112.181801.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)

def test_pipeline_sta():

    pipeline = Pipeline(
        expand("config/pipeline_config_sta.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand(
            "tests/data/input/WIPO0200010-(wi020200010)_average_data_2022-01-12_19-00-00.csv",
            parent,
        )
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc.wind_cube_nacelle_sta.b0.20220112.182000.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)
