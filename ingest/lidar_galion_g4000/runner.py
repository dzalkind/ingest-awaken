from ingest.lidar_galion_g4000 import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(
        expand(
            "tests/data/input/nwtc/Galion - NREL - 1750194_23071906_56.scn", __file__
        )
    )
