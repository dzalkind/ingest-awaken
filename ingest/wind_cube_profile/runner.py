from ingest.wind_cube_profile import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc_rtd.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/WLS71460_2022_02_01__00_00_00.rtd.7z",
            __file__,
        )
    )
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc_sta.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/WLS71460_2022_02_02__00_01_00.sta.7z",
            __file__,
        )
    )
