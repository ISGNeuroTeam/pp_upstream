import pandas as pd
import datetime
import numpy as np

from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.well.aspid import get_future_liquid


class AspidGetFutureLiquidCommand(BaseCommand):
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
        df['_time'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m"))
        df = df[['_time', 'Объем жидкости']]
        through_point = self.get_arg('through_point').value or False
        only_future = self.get_arg('only_future').value or False
        cuted_data = self.get_arg('cuted').value or 0
        extrapolation = self.get_arg('extrapolation').value or 4
        with np.errstate(invalid='ignore'):
            df = get_future_liquid(df, extrapolation, through_point, only_future, cuted_data)
        return df
