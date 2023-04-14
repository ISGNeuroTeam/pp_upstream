import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

'''
from upstream_viz_lib.pages.pipe.pipeline_production \
    import output_results
'''


##
fieldsFormat = {"registration_number": "Рег. номер",
                "node_name_start": "Узел, начало",
                "node_name_end": "Узел, конец",
                "simple_part_id": "ID прост. уч.",
                "startP": "Р нач, атм.",
                "endP": "Р кон, атм.",
                "res_watercut_percent": "Обводненность, %",
                "res_liquid_density_kg_m3": "Плотность, кг/м3",
                "X_m3_day": "Расход, м3/сут",
                "velocity_m_sec": "Скорость, м/с",
                "L": "Длина уч., м",
                "D": "Диам. трубы, мм",
                "S": "Толщина стенки, мм"}


##
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


def output_results(df):
    """
    returns calculation results:
        1: dataframe to show as AgGrid (streamlit)
        2 .. 6: deleted
        
    Arguments
    df: pd.DataFrame - hydraulic calculation results
    
    Returns:
    pd.DataFrame
    """
    wells_df = df[df["juncType"] == "oilwell"]
    draw_df = df[~df["node_id_start"].str.contains("PAD")].copy(deep=True)
    draw_df.loc[
        draw_df["node_id_start"].isin(wells_df["node_id_end"]), "startIsSource"
        ] = True

    # 1:
    df_toShow = beautify_result_df(draw_df)
    
    # 2:
    pass

    # 3,4,5,6:
    pass

    return(df_toShow)


##
class HcalcFormatHcalcOutputCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            ## TEMPLATE: SYNTAX
            ## Positional("first_positional_string_argument", required=True, otl_type=OTLType.TEXT),
            ## Keyword("kwarg_int_argument", required=False, otl_type=OTLType.INTEGER),
            ## Keyword("kwarg_int_double_argument", required=False, otl_type=OTLType.NUMBERIC),
            ## Keyword("some_kwargs", otl_type=OTLType.ALL, inf=True),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start hcalc_format_hcalc_output command')
        
        ## TEMPLATE: ARGUMENTS # that is how you get arguments
        ## first_positional_string_argument = self.get_arg("first_positional_string_argument").value
        ## kwarg_int_argument = self.get_arg("kwarg_int_argument").value or 5
        ## some_kwargs = {k.key: k.value for k in self.get_iter("some_kwargs")}

        ## LOGIC
        #### filter out dnsload part
        dfResult = df.loc[df.purpose=="HCALC_RESULT"]

        #### column types

        #### format results (of hcalc output)
        dfResult = output_results(dfResult)
        dfResult = dfResult.loc[:, list(fieldsFormat.keys())]

        #### precision
        dfResult["simple_part_id"] = dfResult["simple_part_id"].astype(int)
        dfResult["startP"] = dfResult["startP"].round(2)
        dfResult["endP"]   = dfResult["endP"].round(2)
        dfResult["res_watercut_percent"]     = dfResult["res_watercut_percent"].astype("float").round(2)
        dfResult["res_liquid_density_kg_m3"] = dfResult["res_liquid_density_kg_m3"].astype("float").round(2)
        dfResult["X_m3_day"]     = dfResult["X_m3_day"].round(2)
        dfResult["velocity_m_sec"] = dfResult["velocity_m_sec"].round(3)
        dfResult["L"] = dfResult["L"].round(0)
        dfResult["D"] = dfResult["D"].round(1)
        dfResult["S"] = dfResult["S"].round(2)
        
        #### rename columns
        df = dfResult.rename(columns=fieldsFormat) 

        ## TEMPLATE: LOG PROGRESS # Add description of what going on for log progress
        ## self.log_progress('First part is complete.', stage=1, total_stages=2)
        ## self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        ## TEMPLATE: LOGGING # Use ordinary logger if you need
        ## self.logger.debug(f'Command hcalc_format_hcalc_output get first positional argument = {first_positional_string_argument}')
        ## self.logger.debug(f'Command hcalc_format_hcalc_output get keyword argument = {kwarg_int_argument}')

        return df
