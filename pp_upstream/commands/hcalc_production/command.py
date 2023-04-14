import os
import time

import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

import upstream_viz_lib
from upstream_viz_lib import config
from upstream_viz_lib.pages.pipe.pipeline_production \
    import get_outlet_records, get_default_pressure_values, squeeze_default_pressure_values, \
           collect_state, get_hcalc_results, \
           output_results

## CONSTANTS
hcalc_save_location_default = "/opt/otp/external_data/FS/pipe/hcalc/hcalc_results_pp"

liquidProperties = {
    "sat_P_bar": (66.7, "Давление насыщения"),
    "plastT_C": (84.0, "Пластовая температура"),
    "gasFactor": (39.0, "Газовый фактор"),
    "oildensity_kg_m3": (826.0, "Плотность сепарированной нефти"),
    "waterdensity_kg_m3": (1015.0, "Плотность пластовой воды"),
    "gasdensity_kg_m3": (1.0, "Плотность газа"),
    "oilviscosity_Pa_s": (35e-3, "Вязкость нефти"),
    "volumeoilcoeff": (1.015, "Объемный коэффициент")}


## FUNCTIONS
def getDataPath():
    """
    gets path to data files from upstream_viz_lib package needed for hydraulic calculation (like PumpChart.csv etc.)
    """
    start = os.path.join(*(upstream_viz_lib.__file__.split(os.sep)[:-1]))
    end = os.path.join(*(config.get_data_folder().split(os.sep)[1:]))
    path = os.path.join(" ", start, end)
    return(path.strip())


##
class HcalcProductionCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            ## TEMPLATE: SYNTAX
            ## Positional("first_positional_string_argument", required=True, otl_type=OTLType.TEXT),
            ## Keyword("kwarg_int_argument", required=False, otl_type=OTLType.INTEGER),
            ## Keyword("kwarg_int_double_argument", required=False, otl_type=OTLType.NUMBERIC),
            ## Keyword("some_kwargs", otl_type=OTLType.ALL, inf=True),

            Keyword("selected_scheme", required=True, otl_type=OTLType.TEXT),
            Keyword("only_working", required=True, otl_type=OTLType.ALL),
            Keyword("eff_diam", required=True, otl_type=OTLType.NUMBERIC),
            Keyword("hcalc_save_location", required=True, otl_type=OTLType.TEXT)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress("Start hcalc_production command", stage=1, total_stages=7)
        
        ## TEMPALTE: ARGUMENTS  that is how you get arguments
        ## first_positional_string_argument = self.get_arg("first_positional_string_argument").value
        ## kwarg_int_argument = self.get_arg("kwarg_int_argument").value or 5
        ## some_kwargs = {k.key: k.value for k in self.get_iter("some_kwargs")}

        selected_dns = self.get_arg("selected_scheme").value
        only_working = self.get_arg("only_working").value
        eff_diam = self.get_arg("eff_diam").value
        hcalc_save_location = self.get_arg("hcalc_save_location").value

        ## LOGIC
        dfCalc = df.loc[df.purpose=="HCALC", :]
        dfDnsload = df.loc[df.purpose=="DNSLOAD"]
        self.log_progress("Data separated / Arguments read / Parameters defined", stage=2, total_stages=7)
        
        outlets_dict = squeeze_default_pressure_values(
                                    pressureValues=get_default_pressure_values(
                                        outlet_records=get_outlet_records(dfCalc)))
        self.log_progress("Outlets dict defined", stage=3, total_stages=7)
        
        sidebarValues = {"graph_params": {},            ## not used
                         "eff_diam": eff_diam,          ## !!! from dashboard
                         "outlets_dict": outlets_dict,  ## caclulated above
                         "btn_submit": False,           ## not used
                         "only_working": only_working}  ## !!! from dashboard

        state = collect_state(df=dfCalc, sidebarValues=sidebarValues,
                              liquidProperties=liquidProperties, selected_dns=selected_dns)  
        self.log_progress("Sidebar values and STATE collected. Starting calculation...", stage=4, total_stages=7)

        timeStart = time.time()
        dfResult = get_hcalc_results(df=dfCalc,
                                     state=state,
                                     # data_folder=getDataPath())  ## local sdk
                                     data_folder=config.get_data_folder())     ## 199 machine        
        
        timeTaken = time.time() - timeStart
        self.log_progress("Calculation - Ok! Time taken: {} sec.".format("%.2f"%timeTaken), stage=5, total_stages=7)

        dfResult.to_parquet(hcalc_save_location)
        self.log_progress("Calculation results saved to {}".format(hcalc_save_location), stage=6, total_stages=7)

        dfResult["purpose"] = "HCALC_RESULT"
        df = pd.concat([dfResult, dfDnsload])
        self.log_progress("Data tables concatenated again: dfResult, dfDnsLoad", stage=7, total_stages=7)

        #  Add description of what going on for log progress
        ## self.log_progress('First part is complete.', stage=1, total_stages=2)
        ## self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        #  Use ordinary logger if you need
        ## self.logger.debug(f'Command hcalc_production get first positional argument = {first_positional_string_argument}')
        ## self.logger.debug(f'Command hcalc_production get keyword argument = {kwarg_int_argument}')

        return df
