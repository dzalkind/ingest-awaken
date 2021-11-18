import tsdat
import numpy as np
import xarray as xr
import pandas as pd


class FluxHandler(tsdat.AbstractFileHandler):
    """-------------------------------------------------------------------
    Custom file handler for reading *.hpl files from a Halo photonics lidar

    See https://tsdat.readthedocs.io/ for more file handler examples.
    -------------------------------------------------------------------"""

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class can implement this method.
        to read a custom file format into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: An xr.Dataset object
        -------------------------------------------------------------------"""

        # read csv options
        read_csv_kwargs = {}
        read_csv_kwargs["header"] = 1
        read_csv_kwargs["index_col"] = False
        read_csv_kwargs["skiprows"] = [2, 3]

        # read csv
        df = pd.read_csv(filename, **read_csv_kwargs)

        # combine into inputs defined by config file
        heights = [3, 7]
        inputs = ["U_ax", "V_ax", "W_ax", "Ts"]

        input_data = {}
        for inp in inputs:
            input_data[inp] = np.r_[[np.array(df[f"{inp}_{h}m"]) for h in heights]].T

        dataset = xr.Dataset(
            {
                "TIMESTAMP": (("time"), df["TIMESTAMP"]),
                "U_ax": (("time", "height"), input_data["U_ax"]),
                "V_ax": (("time", "height"), input_data["V_ax"]),
                "W_ax": (("time", "height"), input_data["W_ax"]),
                "Ts": (("time", "height"), input_data["Ts"]),
            },
            coords={
                "time": np.array(df["TIMESTAMP"]),
                "height": np.array(heights),
            },
        )

        return dataset
