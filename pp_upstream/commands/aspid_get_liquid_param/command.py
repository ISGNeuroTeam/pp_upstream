import pandas as pd
import datetime
import numpy as np

from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.well.aspid import get_liquid_param_df


class AspidGetLiquidParamCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("through_point", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("cuted", required=False, otl_type=OTLType.INTEGER),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df['_time'] = df['month'].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m"))
        df = df[['_time', 'Объем жидкости']]
        through_point = self.get_arg('through_point').value or False
        cuted_data = self.get_arg('cuted').value or 0
        with np.errstate(invalid='ignore'):
            df = get_liquid_param_df(df, through_point, cuted_data)
        return df

