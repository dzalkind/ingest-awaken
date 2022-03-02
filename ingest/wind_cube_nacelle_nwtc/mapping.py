import re

from typing import AnyStr, Dict
from utils import IngestSpec, expand
from . import Pipeline


# TODO – Developer: Update the "YOUR_REGEX-HERE" patterns to match files that should
# trigger your ingest pipeline.

# See https://regex101.com for information on setting up a regex pattern. Note that the
# full filepath will be passed to the compiled regex pattern, so you can optionally
# match the directory structure in addition to (or instead of) the file basename.
mapping: Dict["AnyStr@compile", IngestSpec] = {
    
    # Real-time data
    # Mapping for Raw Data -> Ingest
    re.compile(r".*_real_time_data_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_nacelle_rtd",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r"YOUR-REGEX-HERE"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_nacelle_nwtc",
    ),
    
    ## 10-min avg data
    # Mapping for Raw Data -> Ingest
    re.compile(r".*_average_data_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_nacelle_rtd",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)  
    # TODO
    re.compile(r"YOUR-REGEX-HERE"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_nacelle_nwtc",
    ),
}
