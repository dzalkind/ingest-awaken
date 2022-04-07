import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.lidar_halo_xrp_nwtc import LidarHaloXrpPipeline

parent = os.path.dirname(__file__)


def test_lidar_halo_xrp_nwtc_pipeline():
    set_dev_env()
    pipeline = LidarHaloXrpPipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/nwtc/nwtc.lidar.z02.00.20211026.040000.stare.hpl", parent)
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc/nwtc.lidar-halo_xrp_stare.b0.20211026.040012.nc", parent
        )
    )
    xr.testing.assert_allclose(output, expected)
