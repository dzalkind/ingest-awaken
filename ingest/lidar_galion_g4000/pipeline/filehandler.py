import tsdat
import xarray as xr
import pandas as pd
from tsdat import Config
import numpy as np


class ScnHandler(tsdat.AbstractFileHandler):
    """-------------------------------------------------------------------
    Custom file handler for reading *.scn files from a Galion G4000 lidar

    See https://tsdat.readthedocs.io/ for more file handler examples.
    -------------------------------------------------------------------"""

    # def write(self, ds: xr.Dataset, filename: str, config: Config, **kwargs):
    #     """-------------------------------------------------------------------
    #     Classes derived from the FileHandler class can implement this method
    #     to save to a custom file format.

    #     Args:
    #         ds (xr.Dataset): The dataset to save.
    #         filename (str): An absolute or relative path to the file including
    #                         filename.
    #         config (Config, optional):  Optional Config object. Defaults to
    #                                     None.
    #     -------------------------------------------------------------------"""
    #     raise NotImplementedError(
    #         "Error: this file format should not be used to write to."
    #     )

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class can implement this method.
        to read a custom file format into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: An xr.Dataset object
        -------------------------------------------------------------------"""

        df = pd.read_csv(filename, sep="\t", header=5, index_col=False)

        time = df["Ray time"].unique()
        azimuth = df[df["Range gate"] == 0]["Az"].to_numpy()
        elevation = df[df["Range gate"] == 0]["El"].to_numpy()
        pitch = df[df["Range gate"] == 0]["Pitch"].to_numpy()
        roll = df[df["Range gate"] == 0]["Roll"].to_numpy()
        range_gate = np.unique(df["Range gate"].to_numpy())

        wind_speed = np.full(
            (len(time), len(range_gate)), fill_value=-9999, dtype=np.float64
        )
        intensity = np.full(
            (len(time), len(range_gate)), fill_value=-9999, dtype=np.float64
        )

        for index, row in df.iterrows():
            i_range = row["Range gate"]
            i_time = np.where(row["Ray time"] == time)

            wind_speed[i_time, i_range] = row["Doppler"]
            intensity[i_time, i_range] = row["Intensity"]

        dataset = xr.Dataset(
            {
                "azimuth": (("time"), azimuth),
                "elevation": (("time"), elevation),
                "pitch": (("time"), pitch),
                "roll": (("time"), roll),
                "wind_speed": (("time", "range_gate"), wind_speed),
                "intensity": (("time", "range_gate"), intensity),
            },
            coords={"time": time, "range_gate": range_gate},
        )

        return dataset
