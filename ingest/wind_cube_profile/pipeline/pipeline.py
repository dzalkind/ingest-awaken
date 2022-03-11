import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt
from matplotlib import cm
import numpy as np
from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, format_time_xticks


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    WIND_CUBE INGESTION PIPELINE

    Wind cube v2 ingest

    --------------------------------------------------------------------------------"""

    def hook_customize_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:

        for filename, ds in raw_dataset_mapping.items():
            ds["height"] = np.fromstring(
                ds.attrs["Altitudes AGL (m)"], sep="\t", dtype=int
            )

            if ".rtd" in filename:
                raw_output_map = {
                    "Wind Speed (m/s)": "wind_speed",
                    "Wind Direction (deg)": "wind_direction",
                    "CNR (dB)": "cnr",
                    "Radial Wind Speed (m/s)": "wind_speed_rad",
                    "Radial Wind Speed Dispersion (m/s)": "wind_speed_disp",
                    "X-wind (m/s)": "wind_speed_x",
                    "Y-wind (m/s)": "wind_speed_y",
                    "Z-wind (m/s)": "wind_speed_z",
                }
            else:  # .sta file
                raw_output_map = {
                    "Wind Speed (m/s)": "wind_speed",
                    "Wind Direction (deg)": "wind_direction",
                    "CNR (dB)": "cnr",
                    "CNR min (dB)": "cnr",
                    "Wind Speed min (m/s)": "wind_speed_min",
                    "Wind Speed max (m/s)": "wind_speed_max",
                    "Wind Speed Dispersion (m/s)": "wind_speed_disp",
                    "Z-wind (m/s)": "wind_speed_z",
                    "Z-wind Dispersion (m/s)": "wind_speed_z",
                    "Dopp Spect Broad (m/s)": "dopp_spect_broad",
                    "Data Availability (%)": "data_availability",
                }

            for category, output_name in raw_output_map.items():
                var_names = [f"{height}m {category}" for height in ds.height.data]
                var_data = [ds[name].data for name in var_names]
                var_data = np.array(var_data).transpose()
                ds[output_name] = (["time", "height"], var_data)

        return raw_dataset_mapping

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")
        loc = dataset.attrs["location_meaning"]

        with plt.style.context(style_file):

            # plot all wind speed and directions
            filename = DSUtil.get_plot_filename(dataset, "wind_speed_and_dir", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots(2, 1)

                cols = cm.coolwarm(np.linspace(0, 1, len(dataset.height)))
                for h, c in zip(dataset.height, cols):
                    ws_height = dataset.wind_speed.sel(height=h)
                    wd_height = dataset.wind_direction.sel(height=h)

                    ws_height.plot.line(
                        ax=ax[0],
                        x="time",
                        label=f"z = {float(h)}m",
                        color=c,
                        ylim=[0, 25],
                        linewidth=0.5,
                    )
                    wd_height.plot.line(
                        ax=ax[1],
                        x="time",
                        color=c,
                        add_legend=False,
                        ylim=[0, 360],
                        linewidth=0.5,
                    )

                [a.grid() for a in ax]
                fig.suptitle(f"Wind speed and direction at {loc} on {date}")
                [a.set_title("") for a in ax]  # Remove bogus title created by xarray
                ax[0].legend(ncol=2)
                # ax.set_ylabel("Example (m)")
                ax[-1].set_xlabel("Time (UTC)")
                [format_time_xticks(a) for a in ax]

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
