import logging
import os
import sys
import unittest

# Add the project directory to the pythonpath
test_dir = os.path.dirname(os.path.realpath(__file__))
project_dir = os.path.dirname(test_dir)
lambda_dir = os.path.join(project_dir, 'lambda_function')
data_dir = os.path.join(project_dir, 'data')
sys.path.insert(0, lambda_dir)

from pipelines.runner import run_plots
import pipelines.utils.log_helper as log


class TestPlots(unittest.TestCase):
    """-------------------------------------------------------------------
    Tests running the suite of a2e tsdat pipelines on the local
    filesystem.
    -------------------------------------------------------------------"""
    def setUp(self) -> None:
        # Set the environment variables for storage
        os.environ['STORAGE_CLASSNAME'] = 'tsdat.io.FilesystemStorage'
        os.environ['RETAIN_INPUT_FILES'] = 'True'
        os.environ['ROOT_DIR'] = os.path.join(data_dir, 'storage')

        # Configure logging
        log.logger.setLevel('INFO')
        log.logger.addHandler(logging.StreamHandler(sys.stdout))
        log.lambda_mode = False

    def tearDown(self) -> None:
        super().tearDown()

    def test_buoy(self):
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_buoy_ingest", location="humboldt")
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_buoy_ingest", location="morro")

    def test_imu(self):
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_imu_ingest", location="humboldt")
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_imu_ingest", location="morro")

    def test_lidar(self):
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_lidar_ingest", location="humboldt")
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_lidar_ingest", location="morro")

    def test_waves(self):
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_waves_ingest", location="humboldt")
        run_plots(start_time="20201201", end_time="20201202", pipeline="a2e_waves_ingest", location="morro")


if __name__ == '__main__':
    unittest.main()
