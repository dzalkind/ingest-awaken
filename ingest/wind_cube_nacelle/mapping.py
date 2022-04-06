import re

from typing import AnyStr, Dict
from utils import IngestSpec, expand
from . import Pipeline


# See https://regex101.com for information on setting up a regex pattern. Note that the
# full filepath will be passed to the compiled regex pattern, so you can optionally
# match the directory structure in addition to (or instead of) the file basename.
mapping: Dict["AnyStr@compile", IngestSpec] = {
    # Real-time data
    # Mapping for Raw Data -> Ingest
    re.compile(
        r".*_real_time_data_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv"
    ): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_nacelle_rtd",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r".*nwtc\.wind_cube_nacelle_rtd\.b0.\d{8}.\d{6}.nc"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_nacelle_rtd",
    ),
    ## 10-min avg data
    # Mapping for Raw Data -> Ingest
    re.compile(r".*_average_data_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_sta.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_nacelle_sta",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r".*nwtc\.wind_cube_nacelle_sta\.b0.\d{8}.\d{6}.nc"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_sta.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_nacelle_sta",
    ),
}
