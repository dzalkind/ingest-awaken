from ingest.assist_nwtc import Pipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = Pipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("/srv/data/sletizia/ingest-awaken/ingest/assist_nwtc/tests/data/assistsummary.20220612.000043.cdf", __file__))
