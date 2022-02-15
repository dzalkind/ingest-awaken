import tsdat
import xarray as xr
import pandas as pd
import lzma


class RTD_FileHandler(tsdat.AbstractFileHandler):
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
        att_dict = {}
        with lzma.open(filename, "rt", encoding="cp1252") as f:
            header_len = int(f.readline().split("=")[1])

            for i_line in range(header_len - 1):
                header_line = f.readline()

                if "=" in header_line:
                    temp = header_line.split("=")

                att_string = temp[1].replace("\n", "")
                att_string = att_string.replace("°", "deg")

                att_key = temp[0].replace("°", "deg")
                att_key = att_key.replace("/", ",")
                att_dict[att_key] = att_string

            # start at 1 because we've already read the header
            df = pd.read_csv(f, sep="\t", header=1, index_col=False)

        # Get rid of '°'s
        rename_map = {}
        for col in df.columns:
            if "°" in col:
                rename_map[col] = col.replace("°", "deg")

        df.rename(columns=rename_map, inplace=True)

        # Change V to -1
        if "Position" in df:
            df["Position"][df["Position"] == "V"] = -1
            df["Position"] = df["Position"].astype(float)

        ds = df.to_xarray()
        ds.attrs = att_dict

        return ds
