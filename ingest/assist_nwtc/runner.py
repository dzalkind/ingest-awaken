from ingest.assist_nwtc import Pipeline
from utils import expand, set_dev_env


# TODO – Developer: Update path to data and/or configuration files as needed.
if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/data.csv", __file__))
