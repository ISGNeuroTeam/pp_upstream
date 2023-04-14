import pandas as pd
import numpy as np
import datetime

from otlang.sdk.syntax import Keyword, Positional, OTLType, Subsearch
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.aspid import fit_watercut

class AspidFitWatercutCommand(BaseCommand):
    # define syntax of your command here
    # define syntax of your command here
    syntax = Syntax(
        [
            Subsearch('niz_param_df', required=True),
            Keyword("extrapolation", required=False, otl_type=OTLType.INTEGER),
            Keyword("only_future", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("cuted", required=False, otl_type=OTLType.INTEGER),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        niz_param_df = self.get_arg('niz_param_df').value
        only_future = self.get_arg('only_future').value or False
        cuted_data = self.get_arg('cuted').value or 0
        extrapolation = self.get_arg('extrapolation').value or 4
        df['_time'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m"))
        with np.errstate(invalid='ignore'):
            result_df = fit_watercut(df, niz_param_df['НИЗ'][0], extrapolation, only_future, cuted_data)
        df = df[['_time', 'Обводненность']]
        return result_df.merge(df, on='_time')
