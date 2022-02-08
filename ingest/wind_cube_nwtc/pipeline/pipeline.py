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
                    "Wind Direction (°)": "wind_direction",
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
                    "Wind Direction (°)": "wind_direction",
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

            # Create an example plot with some noise added for fun
            filename = DSUtil.get_plot_filename(dataset, "example_noise", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig, ax = plt.subplots()

                noise = np.random.random(dataset["example_var"].data.shape) - 0.5
                noisy_example = dataset["example_var"] + noise

                dataset["example_var"].plot(
                    ax=ax,
                    x="time",
                    c=cmocean.cm.deep_r(0.75),
                    label="example_var",
                )
                noisy_example.plot(
                    ax=ax,
                    x="time",
                    c=cmocean.cm.deep_r(0.25),
                    label="noisy_example_var",
                )

                fig.suptitle(f"Example variable at {loc} on {date}")
                ax.set_title("")  # Remove bogus title created by xarray
                ax.legend(ncol=2, bbox_to_anchor=(1, -0.05))
                ax.set_ylabel("Example (m)")
                ax.set_xlabel("Time (UTC)")
                format_time_xticks(ax)

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
