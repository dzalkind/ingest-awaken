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

        df_new = pd.DataFrame()

        df_new["Ray time"] = df["Ray time"].unique()
        df_new["Az"] = df[df["Range gate"] == 0]["Az"].reset_index(drop=True)
        df_new["El"] = df[df["Range gate"] == 0]["El"].reset_index(drop=True)
        df_new["Pitch"] = df[df["Range gate"] == 0]["Pitch"].reset_index(drop=True)
        df_new["Roll"] = df[df["Range gate"] == 0]["Roll"].reset_index(drop=True)

        unique_range_gates = df["Range gate"].unique()

        # Init columns
        for range_gate in unique_range_gates:
            df_new["Doppler_" + str(range_gate)] = np.nan
            df_new["Intensity_" + str(range_gate)] = np.nan

        for index, row in df.iterrows():
            new_index = df_new["Ray time"] == row["Ray time"]
            #     df_new.loc[new_index]['Doppler_'+str(row['Range gate'])] = row['Doppler']
            df_new["Doppler_" + str(row["Range gate"])].iloc[new_index] = row["Doppler"]
            df_new["Intensity_" + str(row["Range gate"])].iloc[new_index] = row[
                "Intensity"
            ]

        return df_new.to_xarray()
