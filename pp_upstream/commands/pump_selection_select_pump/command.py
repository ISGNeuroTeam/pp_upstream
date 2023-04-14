import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.pump_selection import get_well_list, get_inclination, get_pump_chart, get_nkt,\
    get_calc_params, get_tubing, select_pump
import json


def convert_eval_values_from_str_to_float(df: pd.DataFrame) -> pd.DataFrame:
    df['VolumeOilCoeff'] = df['VolumeOilCoeff'].astype('float')
    df['GasFactor'] = df['GasFactor'].astype('float')
    df['OilSaturationP'] = df['OilSaturationP'].astype('float')
    df['SepOilDynamicViscosity'] = df['SepOilDynamicViscosity'].astype('float')
    df['GasDensity'] = df['GasDensity'].astype('float')
    return df


class PumpSelectionSelectPumpCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("well_num", required=True, otl_type=OTLType.TEXT),
            Keyword("liquid_property_dict", required=True, otl_type=OTLType.TEXT),
            Keyword("count_KES", required=True, otl_type=OTLType.BOOLEAN),
            Keyword("base_worktime", required=True, otl_type=OTLType.DOUBLE),
            Keyword("p_zaboy", required=True, otl_type=OTLType.DOUBLE),
            Keyword("p_head", required=True, otl_type=OTLType.DOUBLE),
            Keyword("debit", required=True, otl_type=OTLType.DOUBLE),
            Keyword("current_pump_depth", required=True, otl_type=OTLType.DOUBLE),
            Keyword("perforation", required=True, otl_type=OTLType.DOUBLE),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start pump_selection_select_pump command')
        df = convert_eval_values_from_str_to_float(df)  # TODO remove when otl_v1 eval is fixed

        self.log_progress('pump_selection_select_pump eval converted')

        well_num = self.get_arg("well_num").value
        count_KES = self.get_arg("count_KES").value
        base_worktime = self.get_arg("base_worktime").value
        p_zaboy = self.get_arg("p_zaboy").value
        p_head = self.get_arg("p_head").value
        debit = self.get_arg("debit").value
        current_pump_depth = self.get_arg("current_pump_depth").value
        perforation = self.get_arg("perforation").value
        liquid_property_dict = json.loads(self.get_arg("liquid_property_dict").value)

        df["ZaboyP"].iloc[0] = float(p_zaboy)
        df["WellheadP"].iloc[0] = float(p_head)
        df["dailyDebit"].iloc[0] = float(debit)
        df["pumpDepth"].iloc[0] = float(current_pump_depth)
        df["perforation"].iloc[0] = float(perforation)

        for key in list(liquid_property_dict.keys()):
            df[key] = float(liquid_property_dict[key])

        self.log_progress('pump_selection_select_pump parameters retrieved')

        first_row = df.iloc[0]

        self.log_progress('pump_selection_select_pump start selection')

        result = select_pump(
            calc_params=get_calc_params(first_row),
            tubing=get_tubing(get_inclination(), get_nkt(), well_num),
            pump_chart=get_pump_chart(),
            row=first_row,
            well_num=well_num,
            pump_depth=first_row["pumpDepth"],
            base_worktime=base_worktime,
            count_KES=count_KES
        )
        for column_name in ['wellNum', 'debit', 'pressure', 'InputP', 'eff', 'power', 'Hdyn', 'Depth', 'KES_worktime', 'KES_work_koef']:
            if column_name in result.columns:
                result[column_name] = pd.to_numeric(result[column_name], errors='coerce')

        self.log_progress('End pump_selection_select_pump command')
        return result
