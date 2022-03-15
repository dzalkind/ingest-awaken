import os
import cmocean
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, format_time_xticks


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    WIND_CUBE_NACELLE INGESTION PIPELINE

    Ingest for nacelle-based wind cube

    --------------------------------------------------------------------------------"""

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")
        loc = dataset.attrs["location_meaning"]

        with plt.style.context(style_file):

            # Real-time plots
            if "rtd" in self.config.dataset_definition.attrs["datastream_name"]:
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
                    [
                        a.set_title("") for a in ax
                    ]  # Remove bogus title created by xarray
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
                    [
                        a.set_title("") for a in ax
                    ]  # Remove bogus title created by xarray
                    [format_time_xticks(a) for a in ax]

                    fig.savefig(tmp_path)
                    self.storage.save(tmp_path)
                    plt.close(fig)

            else:  # sta file
                # Wind speed and cnr plot
                filename = DSUtil.get_plot_filename(dataset, "wind_cnr", "png")
                with self.storage._tmp.get_temp_filepath(filename) as tmp_path:
                    fig, ax = plt.subplots(3, 1)

                    dataset["HWS_low"].plot(ax=ax[0], x="time", label="HWS_low")
                    dataset["HWS_hub"].plot(ax=ax[0], x="time", label="HWS_hub")
                    dataset["HWS_high"].plot(ax=ax[0], x="time", label="HWS_high")
                    ax[0].set_ylabel("HWS (m/s)")
                    ax[0].set_xlabel("Time (UTC)")
                    ax[0].legend()

                    dataset["TI_low"].plot(ax=ax[1], x="time", label="TI_low")
                    dataset["TI_hub"].plot(ax=ax[1], x="time", label="TI_hub")
                    dataset["TI_high"].plot(ax=ax[1], x="time", label="TI_high")
                    ax[1].set_ylabel("TI (-)")
                    ax[1].set_xlabel("Time (UTC)")
                    ax[1].legend()

                    dataset["CNR0"].plot(ax=ax[2], x="time", label="CNR0")
                    dataset["CNR1"].plot(ax=ax[2], x="time", label="CNR1")
                    dataset["CNR2"].plot(ax=ax[2], x="time", label="CNR2")
                    dataset["CNR3"].plot(ax=ax[2], x="time", label="CNR3")
                    ax[2].set_ylabel("CNR (dB)")
                    ax[2].set_xlabel("Time (UTC)")
                    ax[2].legend()

                    fig.suptitle(f"Wind speed and CNR at {loc} on {date}")
                    [
                        a.set_title("") for a in ax
                    ]  # Remove bogus title created by xarray
                    # [format_time_xticks(a) for a in ax]

                    fig.savefig(tmp_path)
                    self.storage.save(tmp_path)
                    plt.close(fig)
