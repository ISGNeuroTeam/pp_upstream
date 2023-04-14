import pandas as pd
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.tubing_selection import get_nkt

class GetNktCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("type", required=True, otl_type=OTLType.STRING),
            Keyword("pump_weight", required=True, otl_type=OTLType.INTEGER),
            Keyword("pump_nominal", required=True, otl_type=OTLType.INTEGER),
            Keyword("ped_weight", required=True, otl_type=OTLType.INTEGER),
            Keyword("cable_weight", required=True, otl_type=OTLType.DOUBLE),
            Keyword("p_head", required=True, otl_type=OTLType.INTEGER),
            Keyword("pump_depth", required=True, otl_type=OTLType.INTEGER),
            Keyword("packers", required=True, otl_type=OTLType.STRING),
            Keyword("safety_limit", required=True, otl_type=OTLType.DOUBLE),
            Keyword("is_repaired", required=True, otl_type=OTLType.BOOLEAN),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        nkt_type = self.get_arg("type").value
        pump_weight = self.get_arg("pump_weight").value
        pump_nominal = self.get_arg("pump_nominal").value
        ped_weight = self.get_arg("ped_weight").value
        cable_weight = self.get_arg("cable_weight").value
        p_head = self.get_arg("p_head").value
        pump_depth = self.get_arg("pump_depth").value
        packers = self.get_arg("packers").value
        safety_limit = self.get_arg("safety_limit").value
        is_repaired = self.get_arg("is_repaired").value

        nkt_df = get_nkt(nkt_type, pump_weight, pump_nominal,ped_weight,cable_weight, p_head,pump_depth,packers, safety_limit, is_repaired)

        return nkt_df
