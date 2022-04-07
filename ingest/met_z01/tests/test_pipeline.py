import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.met_z01 import Pipeline

parent = os.path.dirname(__file__)


def test_met_z01_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    print(expand("config/storage_config.yml", parent))
    output = pipeline.run(
        expand("tests/data/input/sa4.met.z01.00.20211207.130000.txt", parent)
    )
    expected = xr.open_dataset(
        expand("tests/data/expected/awaken.met_z01.b0.20211207.130000.nc", parent)
    )
    xr.testing.assert_allclose(output, expected)
