import zipfile

import os
import re
from typing import Union, List

from tsdat.io import DatastreamStorage, S3Path


class Pipeline:

    def __init__(self, pipeline_config, storage_config: Union[str, DatastreamStorage]) -> None:

        # Parse the location from the pipeline config file name
        pattern = re.compile('pipeline_config_(.*)\\.yml')
        match = pattern.match(pipeline_config)
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

            # unzip the zip file into the same directory
            with zipfile.ZipFile(filepath, 'r') as zipped:
                zipped.extractall(local_dir)

            csv_file = self._run_tracker_ingest(local_dir)

            # TODO: We need to decide how to name these files - I'm thinking we get the time from the
            # csv file name directly, or we can read the csv into pandas and get it from there...
            time = '20210101.000000'
            filename = f'{self.location}/tracker.a1/{self.location}.tracker.a1.{time}.csv'
            dest_path: S3Path = self.storage.root.join(filename)

            # Save the file to storage
            self.storage.tmp.upload(csv_file, dest_path)

    def _run_tracker_ingest(self, local_dir) -> str:
        """
        Run the tracker ingest and return the file that was produced
        so it can be saved to storage.
        :param local_dir: Path to local directory where files were unzipeed
        :type local_dir:
        :return: Full path to the csv file that was created from this method.
        :rtype:
        """
        pass
