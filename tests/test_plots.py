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

from pipelines.runner import run_pipeline
import pipelines.utils.log_helper as log


# Configure logging
log.logger.setLevel('INFO')
log.logger.addHandler(logging.StreamHandler(sys.stdout))


class TestPlots(unittest.TestCase):
    """-------------------------------------------------------------------
    Tests re-running plots from a2e tsdat pipelines on the local
    filesystem.
    -------------------------------------------------------------------"""
    def setUp(self) -> None:
        # Set the environment variables for storage
        os.environ['STORAGE_CLASSNAME'] = 'tsdat.io.FilesystemStorage'
        os.environ['RETAIN_INPUT_FILES'] = 'True'
        os.environ['ROOT_DIR'] = os.path.join(data_dir, 'storage')

    def tearDown(self) -> None:
        super().tearDown()

    def test_buoy(self):
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z05.a0.20201201.000000.10m.a2e.nc')])
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z06.a0.20201201.000000.10m.a2e.nc')])

    def test_imu(self):
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z05.a0.20201201.000008.imu.a2e.nc')])
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z06.a0.20201201.000011.imu.a2e.nc')])

    def test_lidar(self):
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/lidar.z05.a0.20201201.001000.sta.a2e.nc')])
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/lidar.z06.a0.20201201.001000.sta.a2e.nc')])

    def test_waves(self):
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z05.a0.20201201.000000.waves.a2e.nc')])
        run_pipeline([os.path.join(data_dir, 'sample_a2e_output/buoy.z06.a0.20201201.000000.waves.a2e.nc')])


if __name__ == '__main__':
    unittest.main()
