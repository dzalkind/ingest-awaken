import re

from typing import AnyStr, Dict
from utils import IngestSpec, expand
from . import Pipeline


# ingest pipeline.

# See http://www.pyregex.com for information on setting up a regex pattern. Note that
# the full filepath will be passed to the compiled regex pattern, so you can optionally
# match the directory structure in addition to (or instead of) the file basename.
mapping: Dict["AnyStr@compile", IngestSpec] = {
    # Mapping for Raw Data -> Ingest
    re.compile(r".*Galion - NREL - \d{7}_\d{8}_\d{2}"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="galion_ingest",
    ),
    # Mapping for Processed Data -> Ingest (so we can reprocess plots)
    re.compile(r".*nwtc\.lidar_galion_g4000\.a0.\d{8}.\d{6}.nc"): IngestSpec(
        pipeline=Pipeline,
        pipeline_config=expand("config/pipeline_config_nwtc.yml", __file__),
        storage_config=expand("config/storage_config.yml", __file__),
        name="plot_galion_ingest",
    ),
    # You can add as many {regex: IngestSpec} entries as you would like. This is useful
    # if you would like to reuse this ingest at other locations or possibly for other
    # similar instruments
}
