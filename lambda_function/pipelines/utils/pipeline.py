import xarray as xr
from tsdat import IngestPipeline
from tsdat.utils import DSUtil
from tsdat.io import FileHandler

class A2ePipeline(IngestPipeline):

    def run_plots(self, start_time: str, end_time: str):
        """Runs the `hook_generate_and_persist_plots()` function for files in
        storage spanning the specified time range. 

        :param start_time: 
            The start time or date to start searching for data (inclusive). 
            Should be like "20210106" to search for data beginning on or after 
            January 6th, 2021.
        :type start_time: str
        :param end_time: 
            The end time or date to stop searching for data (exclusive). Should
            be like "20210108" to search for data ending before January 8th, 
            2021.
        :type end_time: str
        """
        datastream_name = self.config.pipeline_definition.output_datastream_name
        filetype = self.storage.default_file_type

        with self.storage.fetch(datastream_name, start_time, end_time, filetype=filetype) as tmp_files:
            
            for tmp_file in tmp_files:
                ds = FileHandler.read(tmp_file)
                self.hook_generate_and_persist_plots(ds)
