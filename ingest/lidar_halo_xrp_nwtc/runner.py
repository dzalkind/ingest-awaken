from ingest.lidar_halo_xrp_nwtc import LidarHaloXrpPipeline
from utils import expand, set_dev_env


if __name__ == "__main__":
    set_dev_env()
    pipeline = LidarHaloXrpPipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/nwtc/nwtc.lidar.z02.00.20211026.040000.stare.hpl", __file__))
