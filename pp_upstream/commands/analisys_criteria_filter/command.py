import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.well.analysis.criteria import get_criteria_by_id


class AnalisysCriteriaFilterCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("id", required=True, otl_type=OTLType.TEXT),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start analisys_criteria_filter command')
        criteria_id = self.get_arg("id").value

        criteria = get_criteria_by_id(criteria_id)

        df_crit = df.query(criteria.formula, engine='python') if criteria.formula is not None else df

        return df_crit
