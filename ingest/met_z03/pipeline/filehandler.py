import xarray as xr
import pandas as pd
from datetime import datetime
from tsdat import AbstractFileHandler


# TODO – Developer: Write your FileHandler and add documentation
class MetFileHandler(AbstractFileHandler):
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
        columns = [
            "id",
            "year",
            "day",
            "time",
            "S",
            "U",
            "U_dir",
            "U_dir_std",
            "T",
            "RH",
            "P",
            "E",
            "T_10X",
            "p_10X",
        ]
        df = pd.read_csv(
            filename, delimiter=",", header=None, index_col=False, names=columns
        )

        dt = [0] * len(df)
        for year, jd, t, i in zip(df["year"], df["day"], df["time"], df.index):
            if t < 100:
                time_str = str(year - 2000) + str(jd) + "00" + str(t)
            elif t < 1000:
                time_str = str(year - 2000) + str(jd) + "0" + str(t)
            else:
                time_str = str(year - 2000) + str(jd) + str(t)
            dt[i] = datetime.strptime(time_str, "%y%j%H%M")

        df["datetime"] = dt
        df.drop(columns=["year", "day", "time"], inplace=True)
        df.set_index("datetime", drop=True, inplace=True)

        return df.to_xarray()
