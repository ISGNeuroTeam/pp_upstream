import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.aspid import calc_niz_params, get_data_for_niz


class AspidGetNizParamCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("calc_method", required=False, otl_type=OTLType.TEXT),
            Keyword("fit_method", required=False, otl_type=OTLType.TEXT),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        calc_method = self.get_arg('calc_method').value or 'NS'
        if calc_method not in ('NS', 'SP'):
            raise ValueError('Calc method: NS or SP')
        fit_method = self.get_arg('fit_method').value or 'least_squares'
        if fit_method not in ('least_squares', 'mnk'):
            raise ValueError('fit method: least_squares or mnk')
        df = get_data_for_niz(df)
        df = calc_niz_params(df, calc_method, fit_method)
        return df
