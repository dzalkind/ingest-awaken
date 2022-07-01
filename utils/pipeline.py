import xarray as xr
from tsdat import IngestPipeline, FileHandler
from tsdat.io import S3Path
from tsdat.qc import QualityManagement
from typing import Union, List, Dict


class A2ePipeline(IngestPipeline):
    def run_plots(self, files: Union[List[S3Path], List[str]]):
        """----------------------------------------------------------------------------
        Runs the `IngestPipeline.hook_generate_and_persist_plots()` function on the
        provided file or list of files. This is useful for re-running plots without the
        need to also reprocess the data.

        Args:
            files (Union[List[S3Path], str]): The file(s) to read in and produce plots
            for. Note that these files will be processed independent of one another.

        ----------------------------------------------------------------------------"""
        for _file in files:
            with self.storage.tmp.fetch(_file) as tmp_file:
                ds = FileHandler.read(tmp_file)
                self.hook_generate_and_persist_plots(ds)

    # Prevent tsdat from attempting to access previously stored data since it probably
    # already got moved.
    def get_previous_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        return None
