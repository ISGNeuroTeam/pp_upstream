import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.pump_selection import get_well_list, get_inclination

class PumpSelectionWellListCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [

        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start pump_selection_well_list command')
        result = pd.DataFrame(get_well_list(get_inclination()), columns=['well_num'])
        self.log_progress('End pump_selection_well_list command')
        return result
