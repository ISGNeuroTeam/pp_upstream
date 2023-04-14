import pandas as pd
import numpy as np
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from typing import List


FORMAT_PUMP = [
    "__pad_num",
    "__well_num",
    "pump_model",
    "H_pump",
    "Q",
    "oilWellopCountedOilDebit",
    "water",
    "P_bottom_hole_current",
    "ure",
    "FCF текущий",
    "Рекомендуемый УЭЦН",
    "potential_oil_rate_technical_limit",
    "Q_n_new_esp",
    "P_bottom_hole_technical_limit",
    "k_prod_esp",
    "FCF насос",
    "ure_new_esp",
    "diff_Q_esp",
    "diff_Q_oil_esp",
    "diff_FCF_esp",
    "diff_ure_esp",
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


class PotentialsFormatPumpEventsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start potentials_format_pump_events command')
        df["is_recommended_event_pump"] = np.where(
            (df["Рекомендуемый УЭЦН"].astype(str) != "Не удалось подобрать УЭЦН") &
            (df["Рекомендуемый УЭЦН"].astype(str) != "-") &
            (df["diff_Q_esp"].astype(float) > 0),
            True,
            False,
        )

        cols_to_fillnull = [
            "Рекомендуемый УЭЦН",
            "potential_oil_rate_technical_limit",
            "Q_n_new_esp",
            "P_bottom_hole_technical_limit",
            "k_prod_esp",
            "FCF насос",
            "ure_new_esp",
            "diff_Q_esp",
            "diff_Q_oil_esp",
            "diff_FCF_esp",
            "diff_ure_esp",
        ]

        df_pump = beautify_df(
            df.copy(),
            recommended_col="is_recommended_event_pump",
            cols_to_fillnull=cols_to_fillnull,
            formatter=FORMAT_PUMP
        )
        self.log_progress('End potentials_format_pump_events command')

        return df_pump
