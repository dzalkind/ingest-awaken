import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.met_z03 import Pipeline

parent = os.path.dirname(__file__)


def test_met_z03_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    print(expand("config/storage_config.yml", parent))
    output = pipeline.run(expand("tests/data/input/met07.dat", parent))
    expected = xr.open_dataset(
        expand("tests/data/expected/awaken.met_z03.a0.20211030.060300.nc", parent)
    )
    xr.testing.assert_allclose(output, expected)
