import pandas as pd
import numpy as np
from pp_exec_env.base_command import BaseCommand, Syntax
from otlang.sdk.syntax import Keyword, Positional, OTLType
import json
from upstream_viz_lib.pages.well.well_what_if import get_init_and_calculated_params


def convert_datasets_value_dicts(dataset: dict):
    """
    json keys are limited to string, but upstream-viz expects dicts like "startT": {0: 84}
    json values cannot hold np.nan, so null should be converted
    so we have to convert {"0": null} -> {0: np.nan}
    """
    for k in dataset:
        if isinstance(dataset[k], dict):
            dataset[k] = {int(k): (v if v is not None else np.nan) for k, v in dataset[k].items()}


class CalcWhatIfParamsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("dataset", required=True, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start calc_what_if_params command')

        params_dict = (
            df
            .reset_index()
            .T.to_dict()
            .get(0)
        )

        self.log_progress('Got params dict from input dataframe', stage=1, total_stages=3)

        dataset = json.loads(self.get_arg("dataset").value)
        convert_datasets_value_dicts(dataset)

        dataset_df = pd.DataFrame(dataset, index=[0])

        self.log_progress('Got dataset from arguments', stage=2, total_stages=3)

        result_dict = get_init_and_calculated_params(params_dict, dataset_df)
        result_df = pd.DataFrame(result_dict).set_index("Параметр")

        self.log_progress('Calculations of parameters finished', stage=3, total_stages=3)

        return result_df
