import tsdat
import xarray as xr
import pandas as pd


# TODO – Developer: Write your FileHandler and add documentation
class CustomFileHandler(tsdat.AbstractFileHandler):
    """--------------------------------------------------------------------------------
    Custom file handler for reading <some data type> files from a <instrument name>
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
        print("here")

        df = pd.read_csv(filename, sep=";")

        # replace column names ' ' with '_'
        rename_map = {}
        for col in df.columns:
            rename_map[col] = col.replace(" ", "_")
        df.rename(columns=rename_map, inplace=True)

        # Drop UTC info from Timestamp
        tt = pd.DatetimeIndex(df.Timestamp)
        df.Timestamp = tt.tz_convert(None)

        return df.to_xarray()
