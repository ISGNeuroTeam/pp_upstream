import pandas as pd
import datetime
import numpy as np

from otlang.sdk.syntax import Keyword, Positional, OTLType, Subsearch
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.well.aspid import fit_liquid


class AspidFitLiquidCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("extrapolation", required=False, otl_type=OTLType.INTEGER),
            Keyword("through_point", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("only_future", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("cuted", required=False, otl_type=OTLType.INTEGER),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        through_point = self.get_arg('through_point').value or False
        only_future = self.get_arg('only_future').value or False
        cuted_data = self.get_arg('cuted').value or 0
        extrapolation = self.get_arg('extrapolation').value or 4
        df['_time'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m"))
        df_init = df[['_time', 'Объем жидкости']]

        with np.errstate(invalid='ignore'):
            df = fit_liquid(df_init, extrapolation, through_point, only_future, cuted_data)
        df_init.rename(columns={'Объем жидкости': 'Объем жидкости факт.'}, inplace=True)
        return df.merge(df_init, on='_time')
