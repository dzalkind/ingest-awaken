import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, add_colorbar, format_time_xticks


class LidarHaloXrpPipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    LIDAR HALO XRP NWTC INGESTION PIPELINE

    Ingest for the XRP Halo Lidar at the NWTC site

    --------------------------------------------------------------------------------"""

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        dataset["distance"] = (
            "range_gate",
            dataset.coords["range_gate"].data * dataset.attrs["Range gate length (m)"],
        )
        dataset = dataset.swap_dims({"range_gate": "distance"})

        dataset["SNR"].data = 10 * np.log10(dataset.intensity.data - 1)

        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):

        ds = dataset.where(dataset.distance < 5000, drop=True)
        date = pd.to_datetime(ds.time.data[0]).strftime("%d-%b-%Y")
        location = ds.attrs["location_meaning"]

        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")
        with plt.style.context(style_file):

            filename = DSUtil.get_plot_filename(
                dataset, "wind_speed_v_dist_time", "png"
            )
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                fig, ax = plt.subplots()

                # csf = ds.doppler.plot.contourf(
                #     ax=axs,
                #     x="time",
                #     levels=30,
                #     cmap=cmocean.cm.deep_r,
                #     add_colorbar=False,
                #     # vmin=-5,
                #     # vmax=5,
                # )

                ds.doppler.plot(ax=ax, x="time", cmap=cmocean.cm.deep_r)

                fig.suptitle(f"Wind Speed at {location} on {date}")
                # add_colorbar(axs, csf, r"Wind Speed (ms$^{-1}$)")
                format_time_xticks(ax)
                ax.set_xlabel("Time (UTC)")
                ax.set_ylabel("Height (m)")

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)

            filename = DSUtil.get_plot_filename(ds, "SNR_v_dist_time", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                fig, ax = plt.subplots()

                csf = ds.SNR.plot.contourf(
                    ax=ax,
                    x="time",
                    levels=30,
                    cmap=cmocean.cm.deep_r,
                    add_colorbar=False,
                )

                fig.suptitle(f"Signal to Noise Ratio at {location} on {date}")
                add_colorbar(ax, csf, "SNR (dB)")
                format_time_xticks(ax)
                ax.set_xlabel("Time (UTC)")
                ax.set_ylabel("Height (m)")

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
