import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.well.analysis.render_analysis import get_recommendations, get_criteria_by_id


class AnalisysCriteriaRecommendCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("id", required=True, otl_type=OTLType.TEXT),
            Keyword("well_period", required=True, otl_type=OTLType.INTEGER),
            Keyword("calculate", required=False, otl_type=OTLType.TEXT),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start analisys_criteria_recommend command')
        criteria_id = self.get_arg("id").value
        well_period = self.get_arg("well_period").value
        calculate = self.get_arg("calculate").value
        if calculate is None:
            calculate = True
        else:
            calculate = calculate.lower() == 'true'

        if calculate and len(df):
            criteria = get_criteria_by_id(criteria_id)
            if criteria.function is not None:
                df["recommended_event"] = get_recommendations(df, criteria_id, well_period)

        return df
