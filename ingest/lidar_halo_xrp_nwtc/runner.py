from ingest.lidar_halo_xrp_nwtc import LidarHaloXrpPipeline
from utils import expand, set_dev_env


# TODO – Developer: Update path to data and/or configuration files as needed.
if __name__ == "__main__":
    set_dev_env()
    pipeline = LidarHaloXrpPipeline(
        expand("config/pipeline_config_nwtc.yml", __file__),
        expand("config/storage_config.yml", __file__),
    )
    pipeline.run(expand("tests/data/input/nwtc/Stare_199_20210510_00.hpl", __file__))
