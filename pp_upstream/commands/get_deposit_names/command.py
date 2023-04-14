import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.config import get_data_conf

data_conf = get_data_conf()
deposits = data_conf['deposits']


class GetDepositNamesCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame(
            {'deposits': deposits},

        )

        return df
