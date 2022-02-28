import xarray as xr
import pandas as pd
from datetime import datetime
from tsdat import AbstractFileHandler


class AmlFileHandler(AbstractFileHandler):
    """--------------------------------------------------------------------------------
    Custom file handler for reading meteorological files from Campbell dataloggers
    for the A2E AWAKEN effort.

    See https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/io/index.html for more
    examples of FileHandler implementations.

    --------------------------------------------------------------------------------"""

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """----------------------------------------------------------------------------
        Method to read data in a custom format and convert it into an xarray Dataset.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: An xr.Dataset object
        ----------------------------------------------------------------------------"""
        read_params = self.parameters.get("read", {})
        read_csv_kwargs = read_params.get("read_csv", {})
        to_xarray_kwargs = read_params.get("to_xarray", {})

        df = pd.read_csv(filename, **read_csv_kwargs)
        df["time"] = df["Date"] + " " + df["Time"]

        return df.to_xarray(**to_xarray_kwargs)
