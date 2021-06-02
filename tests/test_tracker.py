import tarfile

import datetime
import os
import pandas as pd
import sys
import unittest

# Add the project directory to the pythonpath
test_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(test_dir)
lambda_dir = os.path.join(project_dir, 'lambda_function')
data_dir = os.path.join(project_dir, 'data')
sys.path.insert(0, lambda_dir)

from pipelines.a2e_tracker_ingest.track_summary import parse_json_tracks


class TestTracker(unittest.TestCase):

    def test_tracker(self):
        filepath = os.path.join(project_dir, 'data/a2e_tracker_ingest/tracker.z05.00.20210511.210000.tar.gz')
        local_dir = os.path.dirname(filepath)
        location = 'humboldt'

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

        if not extracted_dir:
            extracted_dir = local_dir

        csv_file = parse_json_tracks(extracted_dir)

        df = pd.read_csv(csv_file)
        input_datetime_str = df['date_time'][0]  # 2021-05-11 14:15:11.552000

        # Convert the time to our standard format
        input_datetime = datetime.datetime.strptime(input_datetime_str, '%Y-%m-%d %H:%M:%S.%f')
        output_datetime_str = input_datetime.strftime("%Y%m%d.%H%M%S")

        storage_filepath = f'{location}/tracker.a1/{location}.tracker.a1.{output_datetime_str}.csv'
        print(storage_filepath)


if __name__ == '__main__':
    unittest.main()
