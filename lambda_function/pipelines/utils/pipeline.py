from tsdat import IngestPipeline
from tsdat.utils import DSUtil
from tsdat.io import FileHandler, S3Path
from typing import Union, List

class A2ePipeline(IngestPipeline):

    def run_plots(self, files: Union[List[S3Path], str]):
        """Runs the 'hook_generate_and_persist_plots()` function on the 
        provided file or list of files. 

        :param files: The files to read in and produce plots for.
        :type files: Union[List[S3Path], str]
        """            
        for _file in files:
            with self.storage.tmp.fetch(_file) as tmp_file:
                ds = FileHandler.read(tmp_file)
                self.hook_generate_and_persist_plots(ds)
