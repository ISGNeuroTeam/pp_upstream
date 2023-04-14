import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class PotentialsFormatMetricsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start potentials_format_metrics command')
        STREAMLIT_COLUMNS_NAMES = ('Дебит жидкости до (м3/сут)',
                                   'Дебит жидкости после (м3/сут)',
                                   'Дебит жидкости разница (%)',
                                   'Дебит нефти до (т/сут)',
                                   'Дебит нефти после (т/сут)',
                                   'Дебит нефти разница (%)',
                                   'FCF до (тыс. руб)',
                                   'FCF после (тыс. руб)',
                                   'FCF разница (%)')
        i = 0
        result_df = pd.DataFrame(columns=['name', 'metric', 'value', '_order'])
        for name, values in df.iteritems():
            if 'delta' in name:
                result_df = result_df.append(pd.DataFrame(
                    {'name': [STREAMLIT_COLUMNS_NAMES[i]], 'metric': [name], 'value': str(values[0]) + '%', '_order': i + 1,
                     'metadata': f"{{'icon': 'eva-arrow_long_up', 'range': ['{values[0]}:'], 'colors': ['green']}}"}),
                                             ignore_index=True)
            else:
                result_df = result_df.append(pd.DataFrame(
                    {'name': [STREAMLIT_COLUMNS_NAMES[i]], 'metric': [name], 'value': str(int(values[0])), '_order': i + 1}),
                    ignore_index=True)
            i += 1
        result_df['value'] = result_df['value'].astype('string')
        result_df['_order'] = result_df['_order'].astype('int64')
        self.log_progress('End potentials_format_metrics command')
        return result_df
