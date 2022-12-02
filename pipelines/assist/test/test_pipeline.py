import xarray as xr
from pathlib import Path
from tsdat import PipelineConfig, assert_close


def test_assist_pipeline():
    config_path = Path("pipelines/assist/config/pipeline_nwtc.yaml")
    config = PipelineConfig.from_yaml(config_path)
    pipeline = config.instantiate_pipeline()

    test_file = "pipelines/assist/test/data/input/short.nwtc.assist.z02.00.20220512.000124.assistsummary.cdf"
    expected_file = (
        "pipelines/assist/test/data/expected/nwtc.assist_z02.a1.20220512.000130.nc"
    )

    dataset = pipeline.run([test_file])
    expected: xr.Dataset = xr.open_dataset(expected_file)  # type: ignore
    assert_close(dataset, expected, check_attrs=False)