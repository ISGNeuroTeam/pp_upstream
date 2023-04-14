import os
import time

import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.pages.pipe import pipeline_production as ppr
from upstream_viz_lib.pages.pipe import hcalc_dashboard as dsh


##
def getDnsLoadFn(dfDnsload):
    def underhood(q, selected_dns, only_working=False):
        df_load = dfDnsload[dfDnsload["address"] == selected_dns.lstrip("НС ")]
        df_load["current_load_rate"] = 100 * df_load["current_debit"] / df_load["prod"].astype(float)
        df_load["predict_load_rate"] = 100 * q / df_load["prod"].astype(float)
        df_load["device_type"] = df_load["device_type"].replace(
            {
                "О": "Отстойники нефти",
                "НГС": "Нефтегазовые сепараторы",
                "РВС": "Резервуары РВС",
            }
        )
        return(df_load)
    return(underhood)


def beautify_result_df(df):
    """
    formats results table to be plotted
    
    Arguments:
    df: pd.DataFrame - results table
    
    Returns:
    pd.DataFrame - formatted results table
    """
    def swap_start_end(row):
        if not row["startIsSource"] and row["X_kg_sec"] < 0:
            return row["node_name_end"], row["node_name_start"]
        else:
            return row["node_name_start"], row["node_name_end"]

    def make_positive(row, colname):
        return row[colname] if row["startIsSource"] else abs(row[colname])

    def make_flow_positive(row):
        return make_positive(row, "X_kg_sec")

    def make_velocity_positive(row):
        return make_positive(row, "velocity_m_sec")

    def convert_flow_to_m3_day(row):
        density = float(
            row["density_calc"] * 1000
            if "density_calc" in row
            else row["res_liquid_density_kg_m3"]
        )
        return row["X_kg_sec"] * 86400 / density

    df[["node_name_start", "node_name_end"]] = df.apply(
        swap_start_end, axis=1, result_type="expand"
    )
    df["X_kg_sec"] = df.apply(make_flow_positive, axis=1)
    df["X_m3_day"] = df.apply(convert_flow_to_m3_day, axis=1)
    df["velocity_m_sec"] = df.apply(make_velocity_positive, axis=1)
    return(df)


def output_dns_load(df, selected_dns, getDnsLoadFn=None, only_working=True):
    """
    based on `results.show_dns_load`

    Arguments:
    df: pd.DataFrame - results of hydraulic calculation
    selected_dns: str - selected pipeline subsystem
    getDnsLoadFn: types.FunctionType - function to compute DNS load, accepts arguments
                                                q: float - DNS flow rate, m3/day
                                                selected_dns: str
                                                only_working: bool, default False
    only_working: bool

    Returns:
        1: dataframe with object loads
        2: flow rate metric (scalar)
        3: debit metric (scalar)
        4: load metric (scalar)
    """
    df = df.copy()
    result_df = beautify_result_df(df)
    result_df["qn_m3_day"] = result_df.apply(
        lambda row: (1 - float(row["res_watercut_percent"]) / 100.0) * row["X_m3_day"], axis=1
    )
    result_dns_q = result_df[result_df["endIsOutlet"] == 1]["X_m3_day"].sum()
    result_dns_qn = result_df[result_df["endIsOutlet"] == 1]["qn_m3_day"].sum()
    df_load = getDnsLoadFn(q=result_dns_q,
                           selected_dns=selected_dns,
                           only_working=only_working)
    dns_predict = df_load[df_load["device_type"] == "ДНС"].to_dict(orient="records")[0]

    oil_density = dns_predict["oil_density_counted"]
    predict_qn = result_dns_qn * oil_density

    # 1:
    df_dns_load = df_load[df_load["device_type"] != "ДНС"].sort_values("device_type")
    # 2:
    # metric_q = value_with_locale(result_dns_q, precision=1)    
    metric_q = result_dns_q
    # 3:
    #TBD metric_qn = value_with_locale(predict_qn, precision=1)
    metric_qn = predict_qn
    # 4:
    #TBD metric_load = value_with_locale(dns_predict["predict_load_rate"], precision=2)
    metric_load = dns_predict["predict_load_rate"]

    ##
    return(df_dns_load, metric_q, metric_qn, metric_load)


##
class HcalcDnsLoadCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            ## TEMPLATE SYNTAX
            ## Positional("first_positional_string_argument", required=True, otl_type=OTLType.TEXT),
            ## Keyword("kwarg_int_argument", required=False, otl_type=OTLType.INTEGER),
            ## Keyword("kwarg_int_double_argument", required=False, otl_type=OTLType.NUMBERIC),
            ## Keyword("some_kwargs", otl_type=OTLType.ALL, inf=True),

            Keyword("selected_scheme", required=True, otl_type=OTLType.TEXT),
            Keyword("only_working", required=True, otl_type=OTLType.ALL)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress("Start hcalc_dns_load command", stage=1, total_stages=3)
        
        ## TEMPLATE # that is how you get arguments
        ## first_positional_string_argument = self.get_arg("first_positional_string_argument").value
        ## kwarg_int_argument = self.get_arg("kwarg_int_argument").value or 5
        ## some_kwargs = {k.key: k.value for k in self.get_iter("some_kwargs")}

        selected_scheme = self.get_arg("selected_scheme").value
        only_working = self.get_arg("only_working").value

        ## LOGIC
        dfResult = df.loc[df.purpose=="HCALC_RESULT", :]
        dfDnsload = df.loc[df.purpose=="DNSLOAD"]
        self.log_progress("Data separated / Arguments collected. Starting calculation...", stage=2, total_stages=3)

        dfDnsloadResult, metricQ, metricQn, metricLoad = \
            output_dns_load(df=dfResult, 
                            selected_dns=selected_scheme,
                            getDnsLoadFn=getDnsLoadFn(dfDnsload),
                            only_working=only_working)
        self.log_progress("Calculating DNS load values (4 items) complete", stage=3, total_stages=3)

        dfDnsloadResult["metricQ_event"] = metricQ
        dfDnsloadResult["metricQn_event"] = metricQn
        dfDnsloadResult["metricLoad_event"] = metricLoad

        ## TEMPLATE # Add description of what going on for log progress
        ## self.log_progress('First part is complete.', stage=1, total_stages=2)
        ## self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        ## TEMPLATE # Use ordinary logger if you need
        ## self.logger.debug(f'Command hcalc_dns_load get first positional argument = {first_positional_string_argument}')
        ## self.logger.debug(f'Command hcalc_dns_load get keyword argument = {kwarg_int_argument}')

        return dfDnsloadResult
