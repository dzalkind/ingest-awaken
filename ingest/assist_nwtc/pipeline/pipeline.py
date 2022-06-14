import os
import cmocean
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

from typing import Dict
from tsdat import DSUtil
from utils import A2ePipeline, format_time_xticks
import datetime as dtm


def subplots(vert_size, hor_size, margin=0.05, spacing=0.02):

    w = hor_size / np.sum(hor_size) * (1 - 2 * margin - len(hor_size) * spacing)
    x = margin + np.append(0, np.cumsum(w[:-1] + spacing))

    h = vert_size / np.sum(vert_size) * (1 - 2 * margin - len(vert_size) * spacing)
    y = np.flip(margin + np.append(0, np.cumsum(np.flip(h[1:]) + spacing)))

    ax = []
    fig = plt.gcf()
    for j in range(len(hor_size)):
        for i in range(len(vert_size)):
            ax.append(fig.add_axes((x[j], y[i], w[j], h[i])))
    return ax


class Pipeline(A2ePipeline):
    """--------------------------------------------------------------------------------
    ASSIST INGESTION PIPELINE

    Ingest for summary ASSIST data

    --------------------------------------------------------------------------------"""

    def hook_generate_and_persist_plots(self, dataset: xr.Dataset):
        style_file = os.path.join(os.path.dirname(__file__), "styling.mplstyle")

        date = pd.to_datetime(dataset.time.data[0]).strftime("%d-%b-%Y")
        loc = dataset.attrs["location_meaning"]

        extract = {
            r"Tb at 675 cm$^{-1}$": "mean_Tb_675_680",
            r"Tb at 985 cm$^{-1}$": "mean_Tb_985_990",
            "ABB (apex)": "ABBapexTemp",
            "ABB (bottom)": "ABBbottomTemp",
            "ABB (top)": "ABBtopTemp",
            "HBB (apex)": "HBBapexTemp",
            "HBB (bottom)": "HBBbottomTemp",
            "HBB (top)": "HBBtopTemp",
            "Front End": "frontEndEnclosureTemp",
            "Interferometer": "interferometerTemp",
            "Cooling block thermistor": "detectorStirlingCoolerBlockTemp",
            "LW Responsivity": "LWresponsivity",
            "Imaginary radiance": "mean_imaginary_rad_985_990",
            "Interferometer humidity [%]": "interferometerHumidity",
            "Hatch status": "hatchOpen",
        }

        offset = {
            r"Tb at 675 cm$^{-1}$": -273.15,
            r"Tb at 985 cm$^{-1}$": -273.15,
            "ABB (apex)": 0,
            "ABB (bottom)": 0,
            "ABB (top)": 0,
            "HBB (apex)": 0,
            "HBB (bottom)": 0,
            "HBB (top)": 0,
            "Front End": 0,
            "Interferometer": 0,
            "Cooling block thermistor": 0,
            "LW Responsivity": 0,
            "Imaginary radiance": 0,
            "Interferometer humidity [%]": 0,
            "Hatch status": 0,
        }

        # graphics
        plot_split = [
            [
                r"Tb at 675 cm$^{-1}$",
                r"Tb at 985 cm$^{-1}$",
                "ABB (apex)",
                "Front End",
                "Interferometer",
                "Cooling block thermistor",
            ],
            [
                "ABB (apex)",
                "ABB (top)",
                "ABB (bottom)",
                "HBB (apex)",
                "HBB (top)",
                "HBB (bottom)",
            ],
            ["LW Responsivity"],
            ["Imaginary radiance"],
            ["Interferometer humidity [%]"],
        ]

        colors = [
            [0, 0, 0, 1],
            [1, 0, 0, 1],
            [0, 1, 0, 1],
            [0, 0, 1, 1],
            [1, 0, 1, 1],
            [0, 1, 1, 1],
        ]
        ylim = [[-150, 50], [0, 70], [1.5 * 10**5, 2 * 10**5], [-1, 1], [0, 15]]
        yscale = ["linear", "linear", "linear", "linear", "linear"]

        N_days = 2

        t0 = []

        with plt.style.context(style_file):

            # Create an example plot with some noise added for fun
            filename = DSUtil.get_plot_filename(dataset, "summary", "png")
            with self.storage._tmp.get_temp_filepath(filename) as tmp_path:

                D = pd.DataFrame()
                C = dataset
                for c in extract.keys():
                    D[c] = C[extract[c]].to_dataframe() + offset[c]

                # # indexing, already doing this
                # time = D.index + C["base_time"].values + dtm.datetime(1970, 1, 1)
                # D.index = time

                if t0 == []:
                    t0 = C["time"].data[0]
                    Data = pd.DataFrame()

                Data = pd.concat([Data, D])
                time = C["time"]

                # plots
                if (
                    True
                ):  # time.data[-1] - t0 >= np.timedelta64(int(N_days * 24 * 3600 * 0.9), "s"):
                    t0 = []

                    fig = plt.figure(figsize=(18, 10))
                    ax = subplots([0.1, 3, 1.5, 1, 1, 1], [1])

                    # hatch
                    hopen = Data["Hatch status"].values == 1
                    hclosed = Data["Hatch status"].values == 0
                    hmove = Data["Hatch status"].values == -1

                    plt.sca(ax[0])
                    for i in range(10):
                        plt.plot(
                            Data.index[hopen],
                            np.zeros(np.sum(hopen)) + i,
                            ".g",
                            markersize=0.5,
                        )
                        plt.plot(
                            Data.index[hclosed],
                            np.zeros(np.sum(hclosed)) + i,
                            ".r",
                            markersize=0.5,
                        )
                        plt.plot(
                            Data.index[hmove],
                            np.zeros(np.sum(hmove)) + i,
                            ".b",
                            markersize=0.5,
                        )
                    plt.xticks([])
                    plt.yticks([])
                    plt.title(f"ASSIST summary at {loc} on {date}")

                    i = 2
                    for a, ps, yl, ys in zip(ax[1:], plot_split, ylim, yscale):
                        plt.sca(a)
                        ii = 0
                        for c in ps:
                            y = Data[c]
                            y_in = y.copy()
                            y_out = y.copy()
                            y_out[(y_in < yl[1]) & (y_in > yl[0])] = np.nan
                            y_in[(y > yl[1]) + (y < yl[0])] = np.nan
                            y_out[y_out > yl[1]] = yl[1] - (yl[1] - yl[0]) * 0.01
                            y_out[y_out < yl[0]] = yl[0] + (yl[1] - yl[0]) * 0.01
                            plt.plot(
                                y_in, "-", linewidth=0.5, color=colors[ii], label=c
                            )
                            plt.plot(
                                y_out, "-", linewidth=1, color=colors[ii], alpha=0.5
                            )
                            ii += 1
                        if len(ps) > 1:
                            leg = plt.legend()
                            leg.set_draggable(state=True)
                        else:
                            a.set_ylabel(ps[0], rotation=80)
                        plt.yscale(ys)

                        plt.ylim(yl)
                        if i < len(ax):
                            a.xaxis.set_ticklabels([])
                        i += 1
                        a.grid(b=True)
                    plt.xlabel("UTC time")
                    # plt.show()

                # fig, ax = plt.subplots()

                # noise = np.random.random(dataset["example_var"].data.shape) - 0.5
                # noisy_example = dataset["example_var"] + noise

                # dataset["example_var"].plot(
                #     ax=ax,
                #     x="time",
                #     c=cmocean.cm.deep_r(0.75),
                #     label="example_var",
                # )
                # noisy_example.plot(
                #     ax=ax,
                #     x="time",
                #     c=cmocean.cm.deep_r(0.25),
                #     label="noisy_example_var",
                # )

                # fig.suptitle(f"Example variable at {loc} on {date}")
                # ax.set_title("")  # Remove bogus title created by xarray
                # ax.legend(ncol=2, bbox_to_anchor=(1, -0.05))
                # ax.set_ylabel("Example (m)")
                # ax.set_xlabel("Time (UTC)")
                # format_time_xticks(ax)

                fig.savefig(tmp_path)
                self.storage.save(tmp_path)
                plt.close(fig)
