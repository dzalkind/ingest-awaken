import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from pathlib import Path

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
            dataset.coords["range_gate"].data * dataset.attrs["Range gate length (m)"]
            + dataset.attrs["Range gate length (m)"] / 2,
        )
        dataset["distance_overlapped"] = (
            "range_gate",
            dataset.coords["range_gate"].data * 1.5
            + dataset.attrs["Range gate length (m)"] / 2,
        )
        intensity = dataset.intensity.data.copy()
        intensity[intensity <= 1] = np.nan
        dataset["SNR"].data = 10 * np.log10(intensity - 1)

        # Dynamically add scan type and z-id (z02, z03, etc) to dataset metadata
        raw_filepath = list(raw_mapping.keys())[0]
        raw_basename = Path(raw_filepath).name
        if ".raw." in raw_basename:  # tsdat-renamed raw file
            to_trim = raw_basename.index(".raw.") + len(".raw.")
            raw_basename = raw_basename[to_trim:]
            # loc_id, instrument, z02/z03, data level, date, time, scan type, extension
            if ".z" in raw_basename:
                _, _, z_id, _, _, _, scan_type, _ = raw_basename.lower().split(".")
            else:  # local NREL tsdat-ing
                z_id = str(dataset.attrs["System ID"])
                scan_type = ""
                if "user" in raw_basename.lower():
                    scan_type = "user"
                elif "stare" in raw_basename.lower():
                    scan_type = "stare"
                elif "vad" in raw_basename.lower():
                    scan_type = "vad"
                elif "wind_profile" in raw_basename.lower():
                    scan_type = "wind_profile"

        qualifier = self.config.pipeline_definition.qualifier

        if scan_type not in ["user", "stare", "vad", "wind_profile"]:
            raise NameError(f"Scan type '{scan_type}' not supported.")

        new_qualifier = "_".join([qualifier, scan_type, z_id])

        dataset.attrs["datastream_name"] = dataset.attrs["datastream_name"].replace(
            qualifier, new_qualifier
        )

        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):

        ds = dataset
        date = pd.to_datetime(ds.time.data[0]).strftime("%d-%b-%Y")
        location = ds.attrs["location_meaning"]

        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")
        with plt.style.context(style_file):

            filename = DSUtil.get_plot_filename(
                dataset, "wind_speed_v_dist_time", "png"
            )
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                fig, ax = plt.subplots()

                ds.wind_speed.plot(
                    ax=ax,
                    x="time",
                    cmap=cmocean.cm.deep_r,
                    vmin=-15,
                    vmax=15,
                )

                fig.suptitle(
                    f"Wind Speed at {location} on {date} \n File: "
                    + dataset.attrs["Filename"]
                )
                # add_colorbar(axs, csf, r"Wind Speed (ms$^{-1}$)")
                format_time_xticks(ax)
                ax.set_xlabel("Time (UTC)")
                ax.set_ylabel("Range Gate")

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)

            filename = DSUtil.get_plot_filename(ds, "SNR_v_dist_time", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                fig, ax = plt.subplots()

                csf = ds.SNR.plot(
                    ax=ax,
                    x="time",
                    cmap=cmocean.cm.deep_r,
                    vmin=-30,
                    vmax=0,
                )

                fig.suptitle(
                    f"Signal to Noise Ratio at {location} on {date} \n File: "
                    + dataset.attrs["Filename"]
                )

                format_time_xticks(ax)
                ax.set_xlabel("Time (UTC)")
                ax.set_ylabel("Range Gate")

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
