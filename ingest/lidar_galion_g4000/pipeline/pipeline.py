import os
import cmocean
import xarray as xr
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from tsdat import DSUtil
from typing import Dict
from utils import A2ePipeline


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    GALION G4000 LIDAR INGESTION PIPELINE

    Ingest for Galion G4000 Lidar

    --------------------------------------------------------------------------------"""

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")
        with plt.style.context(style_file):

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

            ds = dataset
            date = pd.to_datetime(ds.time.data[0]).strftime("%d-%b-%Y")

            # Colormaps to use
            wind_cmap = cmocean.cm.deep_r
            avail_cmap = cmocean.cm.amp_r

            # Create the first plot - Az, El, Pitch and Roll
            filename = DSUtil.get_plot_filename(dataset, "position", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                # Create the figure and axes objects
                fig, ax = plt.subplots(
                    nrows=4, ncols=1, figsize=(14, 8), constrained_layout=True
                )
                fig.suptitle(
                    f"Position of lidar at {ds.attrs['location_meaning']} on {date}"
                )

                # Select heights to plot
                range_gates = [2, 22, 42, 62, 82]

                # Plot the data
                ds.azimuth.plot(ax=ax[0], linewidth=2)
                ds.elevation.plot(ax=ax[1], linewidth=2)
                ds.pitch.plot(ax=ax[2], linewidth=2)
                ds["roll"].plot(ax=ax[3], linewidth=2)

                # Set the labels and ticks
                [format_time_xticks(a) for a in ax]
                # ax.legend(facecolor="white", ncol=len(heights), bbox_to_anchor=(1, -0.05))
                ax[0].set_title("")  # Remove bogus title created by xarray
                [a.set_xlabel("") for a in ax[:-1]]
                ax[-1].set_xlabel("Time (UTC)")

                # Save the figure
                fig.savefig(tmp_path, dpi=100)
                self.storage.save(tmp_path)
                plt.close()

            # Create the second plot - Doppler, Intensity at several range gates
            filename = DSUtil.get_plot_filename(dataset, "wind_speed_intensity", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                # Create the figure and axes objects
                fig, ax = plt.subplots(
                    nrows=2, ncols=1, figsize=(14, 8), constrained_layout=True
                )
                fig.suptitle(
                    f"Lidar Time Series at {ds.attrs['location_meaning']} on {date}"
                )

                # Select range_gates to plot
                range_gates = [2, 22, 42, 62, 82]

                # Plot the data
                for i, range_gate in enumerate(range_gates):
                    wind_speed = ds.wind_speed.sel(range_gate=range_gate)
                    wind_speed.plot(
                        ax=ax[0],
                        linewidth=2,
                        c=wind_cmap(i / len(range_gates)),
                        label=f"RG_{range_gate}",
                    )

                    intensity = ds.intensity.sel(range_gate=range_gate)
                    intensity.plot(
                        ax=ax[1], linewidth=2, c=wind_cmap(i / len(range_gates))
                    )

                # # Set the labels and ticks
                # format_time_xticks(ax)
                ax[0].legend(facecolor="white", ncol=len(range_gates))
                [a.set_title("") for a in ax]  # Remove bogus title created by xarray
                # ax[-1].set_xlabel("Time (UTC)")
                # ax[0].set_ylabel(r"Wind Speed (ms$^{-1}$)")

                # Save the figure
                fig.savefig(tmp_path, dpi=100)
                self.storage.save(tmp_path)
                plt.close()
