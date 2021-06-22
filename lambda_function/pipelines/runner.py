
import importlib
import traceback

import json
import os
import re
import sys
from pipelines.utils.log_helper import logger
from typing import List, Union

from tsdat.io import S3Path

pipeline_map = {
    'a2e_tracker_ingest': re.compile('tracker\\..*\\.tar\\.gz'),
    'a2e_waves_ingest': re.compile('.*waves\\.csv'),
    'a2e_imu_ingest':   re.compile('.*\\.imu\\.bin'),
    'a2e_lidar_ingest': re.compile('.*\\.sta\\.7z'),
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


def get_log_message(pipeline_state, pipeline_name, location, input_files, exception=False):

    log_msg = {
        "Pipeline_Name": pipeline_name,
        "State": pipeline_state,
        "Location": location,
        "Input_Files": input_files
    }

    if exception:
        exception_type, exception_value, exception_traceback = sys.exc_info()
        traceback_string = traceback.format_exception(exception_type, exception_value, exception_traceback)
        log_msg["Error_Type"] = exception_type.__name__
        log_msg["Exception_Message"] = str(exception_value)
        log_msg["Stack_Trace"] = traceback_string

    return json.dumps(log_msg)



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
    # We must use the python equivalent toString() method since the
    # input file could be provided as an S3Path object or a string file path
    file_path = input_files[0].__str__()
    query_file = os.path.basename(file_path)

    logger.debug(f"Dynamically determining pipeline to use from input file: {query_file}")

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

    # If no pipeline is registered for this file, then skip it
    if location is None or pipeline_dir is None:
        logger.info(f'Skipping files: {input_files} since no pipeline is registered for file pattern: {query_file}.')

    else:
        # Look up the correct pipeline config file and instantiate pipeline
        pipeline_config = os.path.join(pipelines_dir, pipeline_dir, 'config', f'pipeline_config_{location}.yml')
        pipeline = instantiate_pipeline(pipeline_dir, pipeline_config, storage_config)

        try:
            # Run Pipeline
            logger.info(get_log_message('Start', pipeline_dir, location, input_files))
            pipeline.run(input_files)
            logger.info(get_log_message('Success', pipeline_dir, location, input_files))

        except Exception as e:
            logger.error(get_log_message('Error', pipeline_dir, location, input_files, exception=True))


def run_plots(start_time: str, end_time: str, pipeline: Union[str, List[str]] = None, location: Union[str, List[str]] = None):   
    """-------------------------------------------------------------------
    Use the pipeline to re-create plots across the given time period. This
    method determines the appropriate pipeline to call based upon the file
    name. It also determines the appropriate config files to use based 
    upon the current deployment mode and the file name.

    Args:
        
        start_time (str):

            The start time or date to start searching for data (inclusive). 
            Should be like "20210106" to plot data beginning on or after 
            January 6th, 2021.

        end_time (str):

            The end time or date to stop searching for data (exclusive). 
            Should be like "20210108" to plot data ending before January 
            8th, 2021.

        pipelines (Union[str, List[str]], optional):

            The name of the pipeline to instantiate and use to create and 
            store plots. If None, create plots using all pipelines. 
            Defaults to None.

        locations (Union[str, List[str]], optional):

            The name(s) of the locations where the pipeline(s) should be 
            run. If None, create plots at all available locations. 
            Defaults to None.
    -------------------------------------------------------------------"""
    # Get the storage config file
    pipelines_dir = os.path.dirname(os.path.realpath(__file__))
    storage_config = os.path.join(pipelines_dir, 'config/storage_config.yml')
    
    if pipeline is None:
        pipelines = list(pipeline_map.keys())
    elif isinstance(pipeline, str):
        pipelines = [pipeline]
    
    if location is None:
        locations = list(location_map.keys())
    elif isinstance(location, str):
        locations = [location]

    for pipeline_name in pipelines:

        # If provided pipeline is not in our mappings, then we aren't able
        # to continue.
        if pipeline_name not in pipeline_map:
            logger.info(f"Skipping at unrecognized pipeline: '{pipeline_name}'")
            break
        
        for location in locations:

            # If provided location is not in our mappings, then we aren't able
            # to continue.
            if location not in location_map:
                logger.info(f"Skipping {pipeline} pipeline at unrecognized location: {location}")
                break
            
            # Look up the correct pipeline config file and instantiate pipeline
            pipeline_config = os.path.join(pipelines_dir, pipeline_name, 'config', f'pipeline_config_{location}.yml')
            pipeline = instantiate_pipeline(pipeline, pipeline_config, storage_config)
            
            # Create plots
            try:
                date_range = [start_time, end_time]
                logger.info(get_log_message('Start', pipeline_name, location, date_range))
                pipeline.run_plots(start_time, end_time)
                logger.info(get_log_message('Success', pipeline_name, location, date_range))

            except Exception as e:
                logger.error(get_log_message('Error', pipeline_name, location, date_range, exception=True))
