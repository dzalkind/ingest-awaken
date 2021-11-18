import os
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from utils import A2ePipeline

import numpy as np


# TODO – Developer: Use hooks to add custom functionality to the pipeline including
# plots, as applicable. Remove any unused code.


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
        return raw_dataset_mapping

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:

        raw_categories = ["U_ax", "V_ax", "W_ax", "Ts"]
        output_var_names = [
            "eastward_wind",
            "northward_wind",
            "upward_air_velocity",
            "air_temperature",
        ]
        # orientation = dataset.orientation.data
        # R = np.array(
        #     [
        #         [np.cos(np.deg2rad(orientation)), -np.sin(np.deg2rad(orientation))],
        #         [np.sin(np.deg2rad(orientation)), np.cos(np.deg2rad(orientation))],
        #     ]
        # )

        # uv =
        # ew =

        for category, output_name in zip(raw_categories, output_var_names):
            var_names = [f"{category}_{height}m" for height in heights]
            var_data = [raw_dataset[name].data for name in var_names]
            var_data = np.array(var_data).transpose()
            dataset[output_name].data = var_data

        # wind speed and direction, relative to U direction
        dataset["wind_speed"] = np.sqrt(dataset.U_ax ** 2 + dataset.V_ax ** 2)
        direction_raw = np.degrees(np.arctan2(dataset.V_ax, dataset.U_ax))
        dataset["wind_direction"] = (
            dataset.dims,
            np.where(direction_raw < 0, direction_raw + 360, direction_raw).T,
        )

        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        return dataset

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")
        with plt.style.context(style_file):
            pass
