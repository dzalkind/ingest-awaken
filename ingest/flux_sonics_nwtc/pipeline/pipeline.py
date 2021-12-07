import os
import xarray as xr
import matplotlib.pyplot as plt
import cmocean
import matplotlib as mpl

from tsdat.utils import DSUtil

from typing import Dict
from utils import A2ePipeline

import numpy as np
import pandas as pd


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    FLUX_SONICS INGESTION PIPELINE

    Surface flux station with sonic anemometers

    --------------------------------------------------------------------------------"""

    def hook_customize_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        """
        Do mapping here
        """
        for ds_name in raw_dataset_mapping:
            # combine into inputs defined by config file
            heights = [3, 7]
            inputs = ["U_ax", "V_ax", "W_ax", "Ts"]

            input_data = {}
            for inp in inputs:
                input_data[inp] = np.r_[
                    [
                        np.array(raw_dataset_mapping[ds_name][f"{inp}_{h}m"])
                        for h in heights
                    ]
                ].T

            raw_dataset_mapping[ds_name] = raw_dataset_mapping[ds_name].assign(
                {
                    "U_ax": (("time", "height"), input_data["U_ax"]),
                    "V_ax": (("time", "height"), input_data["V_ax"]),
                    "W_ax": (("time", "height"), input_data["W_ax"]),
                    "Ts": (("time", "height"), input_data["Ts"]),
                    "height": (("height"), np.array(heights)),
                }
            )

        return raw_dataset_mapping

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:

        # Standard UV directions assume U is west (270 deg.), V is south (180 deg.)
        met_direction = (
            dataset.orientation.data
        )  # - 270        # TODO: check this offset with conventions
        met_direction = np.where(met_direction < 0, met_direction + 360, met_direction)

        R = np.array(
            [
                [np.cos(np.deg2rad(met_direction)), -np.sin(np.deg2rad(met_direction))],
                [np.sin(np.deg2rad(met_direction)), np.cos(np.deg2rad(met_direction))],
            ]
        )

        # Flatten U, V arrays
        uv = np.array([dataset.U_ax.data.flatten(), dataset.V_ax.data.flatten()])

        # North, east vectors, apply rotation matrix
        ne = R @ uv

        dataset["northward_wind"].data = ne[0, :].reshape(dataset.U_ax.shape)
        dataset["eastward_wind"].data = ne[1, :].reshape(dataset.U_ax.shape)

        # Copy raw to standard categories that don't need rotation
        raw_names = ["W_ax", "Ts"]
        output_var_names = [
            "upward_air_velocity",
            "air_temperature",
        ]

        for outname, rawname in zip(output_var_names, raw_names):
            dataset[outname].data = dataset[rawname].data

        # wind speed and direction, relative to North
        dataset["wind_speed"] = np.sqrt(
            dataset.northward_wind ** 2 + dataset.eastward_wind ** 2
        )
        direction_raw = np.degrees(
            np.arctan2(dataset.eastward_wind, dataset.northward_wind)
        )
        dataset["wind_direction"] = (
            dataset.dims,
            np.where(direction_raw < 0, direction_raw + 360, direction_raw),
        )

        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        def format_time_xticks(ax, start=4, stop=21, step=4, date_format="%H-%M"):
            ax.xaxis.set_major_locator(
                mpl.dates.HourLocator(byhour=range(start, stop, step))
            )
            ax.xaxis.set_major_formatter(mpl.dates.DateFormatter(date_format))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=0, ha="center")

        def add_colorbar(ax, plot, label):
            cb = plt.colorbar(plot, ax=ax, pad=0.01)
            cb.ax.set_ylabel(label, fontsize=12)
            cb.outline.set_linewidth(1)
            cb.ax.tick_params(size=0)
            cb.ax.minorticks_off()
            return cb

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")

        # Colormaps to use
        wind_cmap = cmocean.cm.deep_r
        avail_cmap = cmocean.cm.amp_r

        with plt.style.context(style_file):

            # Create the figure and axes objects
            fig, ax = plt.subplots(
                nrows=2, ncols=1, figsize=(14, 8), constrained_layout=True
            )
            fig.suptitle(
                f"Wind Speed and Direction Time Series at {dataset.attrs['location_meaning']} on {date}"
            )

            # Select heights to plot
            heights = [3, 7]

            # Plot the wind speed
            for i, height in enumerate(heights):
                velocity = dataset.wind_speed.sel(height=height)
                velocity.plot(
                    ax=ax[0],
                    linewidth=2,
                    c=wind_cmap(i / len(heights)),
                    label=f"{height} m",
                )

            # Set the labels and ticks
            format_time_xticks(ax[0])
            ax[0].legend(
                facecolor="white", ncol=len(heights), bbox_to_anchor=(1, -0.05)
            )
            ax[0].set_title("")  # Remove bogus title created by xarray
            ax[0].set_xlabel("Time (UTC)")
            ax[0].set_ylabel(r"Wind Speed (ms$^{-1}$)")

            # Plot the wind direction
            for i, height in enumerate(heights):
                direction = dataset.wind_direction.sel(height=height)
                direction.plot(
                    ax=ax[1],
                    linewidth=2,
                    c=wind_cmap(i / len(heights)),
                    label=f"{height} m",
                )

            # Set the labels and ticks
            format_time_xticks(ax[1])
            ax[1].legend(
                facecolor="white", ncol=len(heights), bbox_to_anchor=(1, -0.05)
            )
            ax[1].set_title("")  # Remove bogus title created by xarray
            ax[1].set_xlabel("Time (UTC)")
            ax[1].set_ylabel(r"Wind Direction (deg. relative to U)")

            # Save the figure
            filename = DSUtil.get_plot_filename(dataset, "sonics", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                fig.savefig(tmp_path, dpi=100)
                self.storage.save(tmp_path)
            plt.close()
