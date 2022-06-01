import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.assist_nwtc import Pipeline

parent = os.path.dirname(__file__)


def test_pipeline_at_nwtc():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(expand("tests/data/input/short.nwtc.assist.z02.00.20220512.000124.assistsummary.cdf", parent))
    expected = xr.open_dataset(expand("tests/data/expected/nwtc.assist_nwtc.b0.20220512.000130.nc", parent))
    xr.testing.assert_allclose(output, expected)
