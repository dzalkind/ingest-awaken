import zipfile
from os import listdir

import datetime
import json
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

            csv_file = self._parse_json_tracks(local_dir)

            # TODO: We need to decide how to name these files - I'm thinking we get the time from the
            # csv file name directly, or we can read the csv into pandas and get it from there...
            time = '20210101.000000'
            filename = f'{self.location}/tracker.a1/{self.location}.tracker.a1.{time}.csv'
            dest_path: S3Path = self.storage.root.join(filename)

            # Save the file to storage
            self.storage.tmp.upload(csv_file, dest_path)

    def _generate_header_string(self):
        return (
            "json_file,date_time,duration_sec,num_blobs,size_min_pix,size_max_pix,x_min_m,x_max_m,y_min_m,y_max_m,z_min_m,z_max_m\n")

    def _parse_json_tracks(self, input_directory):
        """
        Run the tracker ingest and return the file that was produced
        so it can be saved to storage.
        :param local_dir: Path to local directory where files were unzipeed
        :type local_dir:
        :return: Full path to the csv file that was created from this method.
        :rtype:
        """
        print("Running track_summary over all .json files in: " + input_directory)
        ofilename = os.path.basename(input_directory) + '-tracks.csv'

        output_file_path = os.path.join(input_directory, ofilename)
        print(f"Opening output file: {output_file_path}")

        output_file = open(output_file_path, 'w')
        output_file.write(self.generate_header_string())

        # load the .json tracks3d files
        for f in listdir(input_directory):
            if (f.endswith(".json")):
                print(f)
                datafile = open(os.path.join(input_directory, f), 'r')

                # the json library parses the json files into lists for us
                data = json.load(datafile)

                # the timestamps are in milliseconds
                blob0_timestamp = data['blobs'][0]['cam1_blob']['timestamp'] / 1000
                blob0_time = datetime.datetime.fromtimestamp(blob0_timestamp).strftime('%Y-%m-%d %H:%M:%S.%f')
                # print(blob0_time)

                duration_sec = (data['endTime'] - data['startTime']) / 1000
                # print(duration_sec)

                num_blobs = len(data['blobs'])
                # print(num_blobs)

                size_min_pix = -999
                size_max_pix = -999
                x_min_m = -999
                y_min_m = -999
                z_min_m = -999
                x_max_m = -999
                y_max_m = -999
                z_max_m = -999

                for blob in data['blobs']:
                    # cam1 and cam2 keep track of their blobs separately
                    # use min and max from both for the overall min/max values
                    if (blob['cam1_blob']['image'] is not None):
                        cam1_pixels = sum(x > 0 for x in blob['cam1_blob']['image'])
                    else:
                        cam1_pixels = 0
                    if (blob['cam2_blob']['image'] is not None):
                        cam2_pixels = sum(x > 0 for x in blob['cam2_blob']['image'])
                    else:
                        cam2_pixels = 0
                    blob_min_pixels = min(cam1_pixels, cam2_pixels)
                    blob_max_pixels = max(cam1_pixels, cam2_pixels)

                    if (size_min_pix == -999 or blob_min_pixels < size_min_pix):
                        size_min_pix = blob_min_pixels
                    if (size_max_pix == -999 or blob_max_pixels > size_max_pix):
                        size_max_pix = blob_max_pixels
                    if (x_min_m == -999 or blob['x'] < x_min_m):
                        x_min_m = blob['x']
                    if (y_min_m == -999 or blob['y'] < y_min_m):
                        y_min_m = blob['y']
                    if (z_min_m == -999 or blob['z'] < z_min_m):
                        z_min_m = blob['z']
                    if (x_max_m == -999 or blob['x'] > x_max_m):
                        x_max_m = blob['x']
                    if (y_max_m == -999 or blob['y'] > y_max_m):
                        y_max_m = blob['y']
                    if (z_max_m == -999 or blob['z'] > z_max_m):
                        z_max_m = blob['z']

                # print(size_min_pix)
                # print(size_max_pix)
                # print(x_min_m)
                # print(x_max_m)
                # print(y_min_m)
                # print(y_max_m)
                # print(z_min_m)
                # print(z_max_m)

                output_file.write(f + "," +
                                  str(blob0_time) + "," +
                                  str(duration_sec) + "," +
                                  str(num_blobs) + "," +
                                  str(size_min_pix) + "," +
                                  str(size_max_pix) + "," +
                                  str(x_min_m) + "," +
                                  str(x_max_m) + "," +
                                  str(y_min_m) + "," +
                                  str(y_max_m) + "," +
                                  str(z_min_m) + "," +
                                  str(z_max_m) + "\n")

        output_file.close()
        return output_file_path
