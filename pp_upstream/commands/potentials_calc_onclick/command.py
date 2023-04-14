import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
import json
from upstream_viz_lib.pages.well.potentials.potentials_new import calculations_on_button_press


def convert_percent_to_fraction(params: dict):
    # TODO: commented line should probably be divided as well, but are not divided in Streamlit app

    # params['process_loss'] /= 100
    params['burning_perc'] /= 100
    # params['perc_g'] /= 100
    params['perc'] /= 100


def convert_comma_floats_to_floats(params: dict):
    INTEGER_VALUES = frozenset(('Period', 'average_TBR', 'unit_costs_n'))

    for k in params:
        val = params[k]
        if k not in INTEGER_VALUES and isinstance(val, str):
            params[k] = float(val.replace(',', '.'))


class PotentialsCalcOnclickCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("criteria", required=True, otl_type=OTLType.TEXT),
            Keyword("params", required=True, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start potentials_calc_onclick command')
        if df.empty:
            return df

        criteria = json.loads(self.get_arg("criteria").value)
        params = json.loads(self.get_arg("params").value)

        # TODO: THIS IS A WORKAROUND frontend should be responsible for such conversions
        convert_comma_floats_to_floats(params)
        convert_percent_to_fraction(params)

        self.log_progress('Criteria and params got from json', stage=1, total_stages=2)
        try:
            df = calculations_on_button_press(df, criteria, params)
        except KeyError as e:
            raise Exception(f'Missing key: {e.args[0]}')
        # postprocessing doesn't support multiple types in a single column
        df['freq_optimal'] = df['freq_optimal'].astype('string')
        df['technical_limit'] = df['technical_limit'].astype('string')
        self.log_progress('Calculations finished', stage=2, total_stages=2)

        return df
