from ingest.met_z01 import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/sa4.met.z01.00.20211207.130000.txt", __file__))
