import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.aml_ld_z01 import Pipeline

parent = os.path.dirname(__file__)


def test_aml_ld_z01_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/aml.ld.z01.00.20211222.204000.dat", parent)
    )
    expected = xr.open_dataset(
        expand("tests/data/expected/awaken.aml_ld_z01.a0.20211222.203010.nc", parent)
    )
    xr.testing.assert_allclose(output, expected)
