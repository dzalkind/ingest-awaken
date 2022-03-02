import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, format_time_xticks


# TODO – Developer: Use hooks to add custom functionality to the pipeline including
# plots, as applicable. Remove any unused code.


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    WIND_CUBE_NACELLE INGESTION PIPELINE

    Ingest for nacelle-based wind cube

    --------------------------------------------------------------------------------"""

    def hook_customize_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        return raw_dataset_mapping

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")
        loc = dataset.attrs["location_meaning"]

        with plt.style.context(style_file):

            # Scan pattern plot
            filename = DSUtil.get_plot_filename(dataset, "scan_pattern", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots(3, 1)

                dataset["Distance"].plot(
                    ax=ax[0],
                    x="time",
                )
                ax[0].set_ylabel("Distance (m)")
                ax[0].set_xlabel("Time (UTC)")

                dataset["Tilt"].plot(
                    ax=ax[1],
                    x="time",
                )
                ax[1].set_ylabel("Tilt (deg)")
                ax[1].set_xlabel("Time (UTC)")

                dataset["Roll"].plot(
                    ax=ax[2],
                    x="time",
                )
                ax[2].set_ylabel("Roll (deg)")
                ax[2].set_xlabel("Time (UTC)")

                fig.suptitle(f"Scan pattern at {loc} on {date}")
                [a.set_title("") for a in ax]  # Remove bogus title created by xarray
                [format_time_xticks(a) for a in ax]

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)

            # Wind speed and cnr plot
            filename = DSUtil.get_plot_filename(dataset, "wind_cnr", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots(3, 1)

                dataset["RWS"].plot(
                    ax=ax[0],
                    x="time",
                )
                ax[0].set_ylabel("RWS (m/)")
                ax[0].set_xlabel("Time (UTC)")

                dataset["DRWS"].plot(
                    ax=ax[1],
                    x="time",
                )
                ax[1].set_ylabel("DRWS (m/s)")
                ax[1].set_xlabel("Time (UTC)")

                dataset["CNR"].plot(
                    ax=ax[2],
                    x="time",
                )
                ax[2].set_ylabel("CNR (dB)")
                ax[2].set_xlabel("Time (UTC)")

                fig.suptitle(f"Scan pattern at {loc} on {date}")
                [a.set_title("") for a in ax]  # Remove bogus title created by xarray
                [format_time_xticks(a) for a in ax]

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
