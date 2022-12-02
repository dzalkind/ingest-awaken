import xarray as xr
from pathlib import Path
from tsdat import PipelineConfig, assert_close


def test_lidar_halo_xrp_pipeline():
    config_path = Path("pipelines/lidar_halo_xrp/config/pipeline_nwtc.yaml")
    config = PipelineConfig.from_yaml(config_path)
    pipeline = config.instantiate_pipeline()

    test_file = "pipelines/lidar_halo_xrp/test/data/input/nwtc.lidar.z02.00.20211026.040000.stare.hpl"
    expected_file = "pipelines/lidar_halo_xrp/test/data/expected/nwtc.lidar-halo_xrp_stare_z02.a1.20211026.040012.nc"

    dataset = pipeline.run([test_file])
    expected: xr.Dataset = xr.open_dataset(expected_file)  # type: ignore
    assert_close(dataset, expected, check_attrs=False)
