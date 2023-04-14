import pandas as pd
import numpy as np
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
import json
from upstream_viz_lib.pages.well.well_what_if import calculate_single_well_schedule


def convert_nulls_to_NaNs(dataset: dict):
    """
    json values cannot hold np.nan, so null should be converted
    so we have to convert [null] -> [np.nan]
    """
    for k in dataset:
        if isinstance(dataset[k], list):
            dataset[k] = [i if i is not None else np.nan for i in dataset[k]]


class WhatIfCalcOnclickCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("dataset", required=True, otl_type=OTLType.TEXT),
            Keyword("mode", required=True, otl_type=OTLType.TEXT),
            Keyword("oil_params", required=True, otl_type=OTLType.TEXT),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start what_if_calc_onclick command')

        dataset = json.loads(self.get_arg("dataset").value)
        mode = json.loads(self.get_arg("mode").value)
        oil_params = json.loads(self.get_arg("oil_params").value)

        convert_nulls_to_NaNs(dataset)

        self.log_progress('Dataset, mode, oil_params got from json', stage=1, total_stages=2)

        solver_df = pd.DataFrame(dataset)
        result_df = calculate_single_well_schedule(solver_df, mode, oil_params)

        self.log_progress('Calculations finished', stage=2, total_stages=2)

        return result_df
