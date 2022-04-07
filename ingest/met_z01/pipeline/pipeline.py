import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, format_time_xticks


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    met_z01 INGESTION PIPELINE

    Example data ingest for the A2e AWAKEN project. Data doi: 10.21947/1328928

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

        ds = dataset
        date = pd.to_datetime(ds.time.values)

        with plt.style.context(style_file):

            filename = DSUtil.get_plot_filename(ds, "wind", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots(
                    nrows=2, ncols=1, figsize=(14, 8), constrained_layout=True
                )

                ax[0].plot(date, ds["wind_speed"], label="resultant")
                ax[0].plot(date, ds["wind_speed_mean"], label="mean")
                ax[0].legend(ncol=2, bbox_to_anchor=(1, -0.05))
                ax[0].set_ylabel("Wind Speed [m/s]")
                ax[0].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                ax[1].scatter(date, ds["wind_dir"])
                ax[1].set_ylabel("Wind Dir [deg]")
                ax[1].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)

            filename = DSUtil.get_plot_filename(ds, "met", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots(
                    nrows=4, ncols=1, figsize=(14, 8), constrained_layout=True
                )

                ax[0].plot(date, ds["temperature"])
                ax[0].set_ylabel("T [deg C]")
                ax[0].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                ax[1].plot(date, ds["pressure"])
                ax[1].set_ylabel("P [mb]")
                ax[1].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                ax[2].plot(date, ds["rel_humidity"])
                ax[2].set_ylabel("RH [%]")
                ax[2].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                ax[3].plot(date, ds["E_downwelling_shortwave"])
                ax[3].set_ylabel("E [W/m^2]")
                ax[3].set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
