from ingest.wind_cube_nacelle import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_rtd.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/WIPO0200010-(wi020200010)_real_time_data_2022-01-12_19-00-00.csv",
            __file__,
        )
    )

    pipeline = Pipeline(
        expand("config/pipeline_config_sta.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/WIPO0200010-(wi020200010)_average_data_2022-01-12_19-00-00.csv",
            __file__,
        )
    )
