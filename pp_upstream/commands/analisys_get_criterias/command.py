import pandas as pd
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.analysis.criteria import criteria_list


class AnalisysGetCriteriasCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame(
            [(criteria.id, criteria.name) for criteria in criteria_list],
            columns=('criteria_id', 'criteria_name')
        )

        return df
