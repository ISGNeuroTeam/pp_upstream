import pandas as pd
import json
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.potentials.potentials_new import get_debit_and_fcf


def convert_percent_to_fraction(params: dict):

    params['process_loss'] /= 100
    params['burning_perc'] /= 100
    params['perc_g'] /= 100
    params['perc'] /= 100


def convert_comma_floats_to_floats(params: dict):
    INTEGER_VALUES = frozenset(('Period', 'average_TBR', 'unit_costs_n'))

    for k in params:
        val = params[k]
        if k not in INTEGER_VALUES and isinstance(val, str):
            params[k] = float(val.replace(',', '.'))


class PotentialsGetDebitAndFcfCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("params", required=True, otl_type=OTLType.TEXT),
            Keyword("sum_debit", required=True, otl_type=OTLType.DOUBLE),
            Keyword("sum_oildebit", required=True, otl_type=OTLType.DOUBLE),
            Keyword("sum_power", required=True, otl_type=OTLType.DOUBLE)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start potentials_get_debit_and_fcf command')

        params = json.loads(self.get_arg("params").value)
        sum_debit = self.get_arg("sum_debit").value
        sum_oildebit = self.get_arg("sum_oildebit").value
        sum_power = self.get_arg("sum_power").value

        # TODO: THIS IS A WORKAROUND frontend should be responsible for such conversions
        convert_comma_floats_to_floats(params)
        convert_percent_to_fraction(params)

        self.log_progress('Got all arguments', stage=1, total_stages=2)

        result = get_debit_and_fcf(df, params, sum_debit, sum_oildebit, sum_power)

        self.log_progress('Debit and FCF stats counted', stage=2, total_stages=2)

        return result
