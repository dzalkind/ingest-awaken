import tsdat
import xarray as xr
import pandas as pd


# class FluxHandler(tsdat.AbstractFileHandler):
#     """-------------------------------------------------------------------
#     Custom file handler for reading surface flux csv files

#     See https://tsdat.readthedocs.io/ for more file handler examples.
#     -------------------------------------------------------------------"""

#     def read(self, filename: str, **kwargs) -> xr.Dataset:
#         """-------------------------------------------------------------------
#         Classes derived from the FileHandler class can implement this method.
#         to read a custom file format into a xr.Dataset object.

#         Args:
#             filename (str): The path to the file to read in.

#         Returns:
#             xr.Dataset: An xr.Dataset object
#         -------------------------------------------------------------------"""

#         # read csv options
#         read_csv_kwargs = {}
#         read_csv_kwargs["header"] = 1
#         read_csv_kwargs["index_col"] = False
#         read_csv_kwargs["skiprows"] = [2, 3]

#         # read csv
#         df = pd.read_csv(filename, **read_csv_kwargs)

#         return df.to_xarray()
