from ingest.ecor_richland import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_richland.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/sgpecorE14.00.20190910.010201.raw.2019_0910_0000_14.flx",
            __file__,
        )
    )
