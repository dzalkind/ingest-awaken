from ingest.assist_nwtc import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/short.nwtc.assist.z02.00.20220512.000124.assistsummary.cdf", __file__))
