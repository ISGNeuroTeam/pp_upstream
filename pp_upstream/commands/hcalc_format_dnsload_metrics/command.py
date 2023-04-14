import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


class HcalcFormatDnsloadMetricsCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            ## TEMPLATE
            ## Positional("first_positional_string_argument", required=True, otl_type=OTLType.TEXT),
            ## Keyword("kwarg_int_argument", required=False, otl_type=OTLType.INTEGER),
            ## Keyword("kwarg_int_double_argument", required=False, otl_type=OTLType.NUMBERIC),
            ## Keyword("some_kwargs", otl_type=OTLType.ALL, inf=True),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress("Start hcalc_format_dnsload_metrics command")
        ## TEMPLATE # that is how you get arguments
        ## first_positional_string_argument = self.get_arg("first_positional_string_argument").value
        ## kwarg_int_argument = self.get_arg("kwarg_int_argument").value or 5
        ## some_kwargs = {k.key: k.value for k in self.get_iter("some_kwargs")}

        # LOGIC
        if df.shape[0] > 0:
            dfSingleValue = pd.DataFrame({"metric": ["Дебит жидкости, м3/сут", 
                                                     "Дебит нефти, т/сут", 
                                                     "Загруженность, %"],
                                        "value": [df.metricQ_event.tolist()[0],
                                                  df.metricQn_event.tolist()[0],
                                                  df.metricLoad_event.tolist()[0]],
                                        "_order": [1,2,3]})
            dfSingleValue["value"] = dfSingleValue["value"].round(2)
        else:
            dfSingleValue = pd.DataFrame({"metric": ["Дебит жидкости, м3/сут", 
                                                     "Дебит нефти, т/сут", 
                                                     "Загруженность, %"],
                                        "value": ["--", "--", "--"],
                                        "_order": [1,2,3]})


        ## TEMPLATE # Add description of what going on for log progress
        ## self.log_progress('First part is complete.', stage=1, total_stages=2)
        ## self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        ## TEMPLATE # Use ordinary logger if you need
        ## self.logger.debug(f'Command hcalc_format_dnsload_metrics get first positional argument = {first_positional_string_argument}')
        ## self.logger.debug(f'Command hcalc_format_dnsload_metrics get keyword argument = {kwarg_int_argument}')

        return dfSingleValue
