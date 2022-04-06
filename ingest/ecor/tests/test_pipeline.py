import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.ecor import Pipeline

parent = os.path.dirname(__file__)


def test_ecor_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand(
            "tests/data/input/sgpecorE14.00.20190910.010201.raw.2019_0910_0000_14.flx",
            parent,
        )
    )
    expected = xr.open_dataset(
        expand("tests/data/expected/awaken.ecor-30min.b0.20190910.000000.nc", parent)
    )
    xr.testing.assert_allclose(output, expected)
