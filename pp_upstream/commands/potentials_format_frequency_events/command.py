import pandas as pd
import numpy as np
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from typing import List

FORMAT_FREQUENCY = [
    "__pad_num",
    "__well_num",
    "Q",
    "oilWellopCountedOilDebit",
    "water",
    "pump_model",
    "control_station_model",
    "start_type",
    "k_flow",
    "P_bottom_hole_current",
    "H_pump",
    "P_plast",
    "K_prod",
    "mean_freq",
    "Loading",
    "ure",
    "FCF текущий",
    "freq_optimal",
    "Q_freq_optimal",
    "Q_n_freq_optimal",
    "P_bottom_hole_freq_optimal",
    "k_prod_freq",
    "Loading_optimal",
    "FCF оптимальный",
    "diff_Q_freq",
    "diff_Q_oil_freq",
    "diff_FCF_freq",
]


def beautify_df(_df: pd.DataFrame, recommended_col: str, cols_to_fillnull: List[str], formatter: List) -> pd.DataFrame:
    _df.loc[
        _df[recommended_col] == 0,
        cols_to_fillnull
    ] = "-"

    return (
        _df
        .sort_values(by=recommended_col, ascending=False)
        .astype(str)
        .fillna("-")[formatter]
    )


class PotentialsFormatFrequencyEventsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start potentials_format_frequency_events command')
        df["is_recommended_event_freq"] = np.where(
            (
                    (df["mean_freq"] != df["freq_optimal"])
                    & (df["diff_Q_freq"].astype(float) > 0)
                    & (df["diff_Q_oil_freq"].astype(float) > 0)
            ), True, False
        )

        cols_to_fillnull = [
            "Q_freq_optimal",
            "Q_n_freq_optimal",
            "freq_optimal",
            "P_bottom_hole_freq_optimal",
            "k_prod_freq",
            "Loading_optimal",
            "FCF оптимальный",
            "diff_Q_freq",
            "diff_Q_oil_freq",
            "diff_FCF_freq",
        ]

        df_freq = beautify_df(
            df,
            recommended_col="is_recommended_event_freq",
            cols_to_fillnull=cols_to_fillnull,
            formatter=FORMAT_FREQUENCY
        )
        self.log_progress('Start potentials_format_frequency_events command')
        return df_freq
