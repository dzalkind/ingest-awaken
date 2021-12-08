from ingest.met_z03 import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_rufus.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/met.z03.00.20160518.180000.txt", __file__))
