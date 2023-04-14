import pandas as pd
import datetime
import numpy as np

from otlang.sdk.syntax import Keyword, Positional, OTLType, Subsearch
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.aspid import get_watercut_param


class AspidGetWatercutParamCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Subsearch('niz_param_df', required=True),
            Keyword("cuted", required=False, otl_type=OTLType.INTEGER),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['_time'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m"))
        niz_param_df = self.get_arg('niz_param_df').value
        cuted_data = self.get_arg('cuted').value or 0
        with np.errstate(invalid='ignore'):
            df = get_watercut_param(df, niz_param_df['НИЗ'][0], cuted_data)
        return df
