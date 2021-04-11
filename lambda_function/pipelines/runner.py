
import os
import re
import importlib
from typing import List, Union

from tsdat.io import S3Path


pipeline_map = {
    'a2e_waves_ingest': re.compile('.*waves\\.csv'),
    'a2e_imu_ingest':   re.compile('.*\\.bin'),
    'a2e_lidar_ingest': re.compile('.*\\.sta'),
    'a2e_buoy_ingest': re.compile('buoy\\..*\\.(?:csv|zip|tar|tar\\.gz)')
}

location_map = {
    'humboldt': re.compile('.*\\.z05\\..*'),
    'morro'   : re.compile('.*\\.z06\\..*')
}


def instantiate_pipeline(pipeline_dir, pipeline_config, storage_config):
    # Dynamically instantiate the pipeline class from the designated folder
    module_path = f'pipelines.{pipeline_dir}.pipeline'
    module = importlib.import_module(module_path)
    class_ = getattr(module, 'Pipeline')
    instance = class_(pipeline_config, storage_config)
    return instance


def run_pipeline(input_files: Union[List[S3Path], List[str]] = []):
    """-------------------------------------------------------------------
    Run the appropriate pipeline on the provided files.  This method
    determines the appropriate pipeline to call based upon the file name.
    It also determines the appropriate config files to use based upon
    the current deployment mode and the file name.

    Args:

        input_files (Union[List[S3Path], List[str]]):

            The list of files to run the pipeline against.  List will be
            either S3 paths (if running in AWS mode) or string paths (if
            running in local mode).  If multiple files are passed together,
            it is assumes that they must be co-processed in the same
            pipeline invocation.

    -------------------------------------------------------------------"""

    # If no files are provided, just return out
    if len(input_files) == 0:
        return

    # Since we assume all these files will be co-processed together,
    # they will all use the same pipeline.  So use the filename of
    # the first file to pick the correct pipeline and location to
    # use.
    query_file = os.path.basename(input_files[0])

    # Get the storage config file
    pipelines_dir = os.path.dirname(os.path.realpath(__file__))
    storage_config = os.path.join(pipelines_dir, 'config/storage_config.yml')

    # Look up the correct location for the given data file
    location = None
    for location_key, file_pattern in location_map.items():
        if file_pattern.match(query_file):
            location = location_key
            break

    # Look up the correct pipeline for the given file
    pipeline_dir = None
    for pipeline_key, file_pattern in pipeline_map.items():
        if file_pattern.match(query_file):
            pipeline_dir = pipeline_key
            break

    # Look up the correct pipeline config file
    pipeline_config = os.path.join(pipelines_dir, pipeline_dir, 'config', f'pipeline_config_{location}.yml')

    # Create and run pipeline
    pipeline = instantiate_pipeline(pipeline_dir, pipeline_config, storage_config)
    pipeline.run(input_files)
