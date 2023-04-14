import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class PotentialsDropNullRecommendedEventsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        REQUIRED_COLUMNS = ["__well_num", "__pad_num", "q_event", "event", "FCF_event"]
        self.log_progress('Start potentials_drop_null_recommended_events command')
        df_with_recommended_events_only = df[df["event"] != "Мероприятие отсутствует"]
        df_with_recommended_events_only = df_with_recommended_events_only[REQUIRED_COLUMNS]
        self.log_progress('End potentials_drop_null_recommended_events command')
        return df_with_recommended_events_only
