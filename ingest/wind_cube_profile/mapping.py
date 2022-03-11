import re

from typing import AnyStr, Dict
from utils import IngestSpec, expand
from . import Pipeline


# See https://regex101.com for information on setting up a regex pattern. Note that the
# full filepath will be passed to the compiled regex pattern, so you can optionally
# match the directory structure in addition to (or instead of) the file basename.
mapping: Dict["AnyStr@compile", IngestSpec] = {
    # Mapping for Raw Data -> Ingest
    re.compile(r".*WLS71460_\d{4}_\d{2}_\d{2}__\d{2}_\d{2}_\d{2}.rtd.7z"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_rtd",
    ),
    re.compile(r".*WLS71460_\d{4}_\d{2}_\d{2}__\d{2}_\d{2}_\d{2}.sta.7z"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc_sta.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="wind_cube_sta",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r"nwtc.wind_cube_profile_rtd.b0.\d{8}.\d{6}.nc"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc_rtd.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_rtd",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r"nwtc.wind_cube_profile_sta.b0.\d{8}.\d{6}.nc"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc_sta.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_wind_cube_sta",
    ),
    # You can add as many {regex: IngestSpec} entries as you would like. This is useful
    # if you would like to reuse this ingest at other locations or possibly for other
    # similar instruments
}
