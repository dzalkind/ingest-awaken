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
    FLUX_LOWSPEED_NWTC INGESTION PIPELINE

    Low speed data from surface flux station from NWTC

    --------------------------------------------------------------------------------"""

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")
        loc = dataset.attrs["location_meaning"]

        with plt.style.context(style_file):

            # Create the first plot - Lidar Wind Speeds at several elevations
            filename = DSUtil.get_plot_filename(dataset, "wind_speed_and_dir", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                # Create the figure and axes objects
                fig, ax = plt.subplots(
                    nrows=4, ncols=1, figsize=(14, 8), constrained_layout=True
                )
                fig.suptitle(
                    f"Wind Speed and Direction Time Series at {dataset.attrs['location_meaning']} on {date}"
                )

                dataset.wind_speed.plot(ax=ax[0], linewidth=2)
                dataset.surface_air_pressure.plot(ax=ax[1], linewidth=2)
                dataset.air_temperature.plot(ax=ax[2], linewidth=2)
                dataset.relative_humidity.plot(ax=ax[3], linewidth=2)

                # Set the labels and ticks

                [a.set_xticklabels("") for a in ax]
                [a.set_ylabel(a.get_ylabel(), rotation=45, labelpad=50) for a in ax]

                format_time_xticks(ax[-1])

                ax[-1].set_xlabel("Time (UTC)")

                # Save the figure
                fig.savefig(tmp_path, dpi=100)
                self.storage.save(tmp_path)
                plt.close()
