import lzma
import pandas as pd
import xarray as xr

from tsdat import Config
from tsdat.io import AbstractFileHandler


class StaFileHandler(AbstractFileHandler):

    def write(self, ds: xr.Dataset, filename: str, config: Config, **kwargs):
        raise NotImplementedError("Error: this file format should not be used to write to.")

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        lzma_file = lzma.open(filename, "rt", encoding="cp1252")  # Default encoding for Windows devices
        df = pd.read_csv(lzma_file, sep="\t", header=41, index_col=False)
        return df.to_xarray()
