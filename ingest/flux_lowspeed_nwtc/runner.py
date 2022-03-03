from ingest.flux_lowspeed_nwtc import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/CR1000X_21544_Cellular_LowSpeedData_2021_08_24_1600.dat",
            __file__,
        )
    )
