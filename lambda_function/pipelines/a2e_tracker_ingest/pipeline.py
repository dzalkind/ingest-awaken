import tarfile

import datetime
import logging
import os
import pandas as pd
import re
from typing import Union, List

from tsdat.io import DatastreamStorage, S3Path
from .track_summary import parse_json_tracks

logger = logging.getLogger()


class Pipeline:

    def __init__(self, pipeline_config, storage_config: Union[str, DatastreamStorage]) -> None:
        logger.info(f'Instantiating tracker Pipeline with pipeline config: {pipeline_config} and storage config {storage_config}')

        # Parse the location from the pipeline config file name
        pipeline_config_filename = os.path.basename(pipeline_config)
        pattern = re.compile('pipeline_config_(.*)\\.yml')
        match = pattern.match(pipeline_config_filename)
        groups = match.groups()
        self.location = groups[0]

        # We can pass either a DatastreamStorage object or the path to a config file
        storage = storage_config
        if isinstance(storage_config, str):
            storage = DatastreamStorage.from_config(storage_config)
        self.storage : DatastreamStorage = storage

    def run(self, filepath: Union[str, List[str]]) -> None:
        """Runs the Tracker ingest pipeline from start to finish.
        This will produce a csv file that will not be standardized with tsdat.

        :param filepath:
            The path or list of paths to the file(s) to run the pipeline on.
        :type filepath: Union[str, List[str]]
        """

        # Tracker ingests should only consist of a single zip file
        if isinstance(filepath, list):
            assert len(filepath) == 1
            filepath = filepath[0]

        # Fetch the zip file to local storage - it will be removed when this block goes out of scope
        with self.storage.tmp.fetch(filepath) as zip_path:
            local_dir = os.path.dirname(zip_path)

            # unzip the tar.gz file into the same directory
            with tarfile.open(filepath) as tar:
                tar.extractall(path=local_dir)

            # The zipped file contains a folder with the input files
            extracted_dir = None
            for filename in os.listdir(local_dir):
                filepath = os.path.join(local_dir, filename)
                if os.path.isdir(filepath):
                    extracted_dir = filepath
                    break

            # Just use local_dir if the zip file did not contain a folder
            if not extracted_dir:
                extracted_dir = local_dir

            csv_file = parse_json_tracks(extracted_dir)

            # Convert the time to our standard format
            df = pd.read_csv(csv_file)
            input_datetime_str = df['date_time'][0]  # 2021-05-11 14:15:11.552000
            input_datetime = datetime.datetime.strptime(input_datetime_str, '%Y-%m-%d %H:%M:%S.%f')
            output_datetime_str = input_datetime.strftime("%Y%m%d.%H%M%S")

            # Save the file to storage
            filename = f'{self.location}/tracker.a1/{self.location}.tracker.a1.{output_datetime_str}.csv'
            dest_path: S3Path = self.storage.root.join(filename)
            self.storage.tmp.upload(csv_file, dest_path)

            # Also save the raw data to storage
            raw_filename = os.path.basename(filepath)
            filename = f'{self.location}/tracker.00/{self.location}.tracker.00.{output_datetime_str}.raw.{raw_filename}'
            dest_path: S3Path = self.storage.root.join(filename)
            self.storage.tmp.upload(filepath, dest_path)
