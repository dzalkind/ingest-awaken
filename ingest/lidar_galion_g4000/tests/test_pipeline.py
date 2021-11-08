import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.lidar_galion_g4000 import Pipeline

parent = os.path.dirname(__file__)


# TODO – Developer: Update paths to your input files here. Please add tests if needed.
def test_pipeline_at_nwtc():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(expand("tests/data/input/nwtc/data.txt", parent))
    expected = xr.open_dataset(expand("tests/data/expected/nwtc/data.txt", parent))

    assert output.equals(expected)
