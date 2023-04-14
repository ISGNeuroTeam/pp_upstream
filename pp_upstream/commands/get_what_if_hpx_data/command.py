import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.well_what_if import get_pump_curves_and_pump_model_list


class GetWhatIfHpxDataCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("pump_model", required=True, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start get_what_if_hpx_data command')

        pump_model = self.get_arg("pump_model").value

        pump_curves, pump_model_list = get_pump_curves_and_pump_model_list()
        if pump_model in pump_model_list:
            pump_curve = pump_curves.loc[pump_model].sort_values(by="debit")
        else:
            self.log_progress('Pump model not in list', stage=1, total_stages=1)
            return pd.DataFrame({'warning': f"Для насоса модели {pump_model} не найдена НРХ. В расчетах будет использоваться НРХ "
                       f"близкой модели"}, index=[0])

        self.log_progress('Data reading finished', stage=1, total_stages=1)

        return pump_curve[['debit', 'pressure', 'eff']]
