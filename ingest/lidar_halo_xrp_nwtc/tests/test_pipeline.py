import os
import xarray as xr
from utils import expand, set_dev_env
from ingest.lidar_halo_xrp_nwtc import LidarHaloXrpPipeline

parent = os.path.dirname(__file__)


def test_lidar_halo_xrp_nwtc_dap_pipeline():
    set_dev_env()
    pipeline = LidarHaloXrpPipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand(
            "tests/data/input/nwtc/nwtc.lidar.z02.00.20211026.040000.stare.hpl", parent
        )
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc/nwtc.lidar-halo_xrp_stare_z02.b0.20211026.040012.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)


def test_lidar_halo_xrp_nwtc_local_pipeline():
    set_dev_env()
    pipeline = LidarHaloXrpPipeline(
        expand("config/pipeline_config_nwtc.yml", parent),
        expand("config/storage_config.yml", parent),
    )
    output = pipeline.run(
        expand("tests/data/input/nwtc/Stare_199_20220206_01.hpl", parent)
    )
    expected = xr.open_dataset(
        expand(
            "tests/data/expected/nwtc/nwtc.lidar-halo_xrp_stare_199.b0.20220206.010013.nc",
            parent,
        )
    )
    xr.testing.assert_allclose(output, expected)
