import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.lidar_galion_g4000 import Pipeline

parent = os.path.dirname(__file__)


def test_lidar_galion_g4000_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/nwtc/Galion - NREL - 1750194_23071906_56.scn", parent)
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc/nwtc.lidar_galion_g4000.a0.20190723.060354.nc",
            parent,
        )
    )

    assert output.equals(expected)
