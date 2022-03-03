import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.flux_sonics_nwtc import Pipeline

parent = os.path.dirname(__file__)


def test_flux_sonics_nwtc_pipeline():
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand(
            "tests/data/input/nwtc/CR1000X_21544_Cellular_Sonics_2021_08_24_1600.dat",
            parent,
        )
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc/nwtc.flux_sonics_nwtc.a0.20210824.160002.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)
