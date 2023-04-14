import pandas as pd
import json
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax
from upstream_viz_lib.pages.well.tubing_selection import calculate_tubing_reliability


class CalculateTubingReliabilityCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("stages_length", required=True, otl_type=OTLType.STRING),
            Keyword("stages_types", required=True, otl_type=OTLType.STRING),
            Keyword("pump_weight", required=True, otl_type=OTLType.INTEGER),
            Keyword("ped_weight", required=True, otl_type=OTLType.INTEGER),
            Keyword("cable_weight", required=True, otl_type=OTLType.DOUBLE),
            Keyword("p_head", required=True, otl_type=OTLType.INTEGER),
            Keyword("pump_depth", required=True, otl_type=OTLType.INTEGER)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        stages_length = self.get_arg("stages_length").value
        stages_types = self.get_arg("stages_types").value
        pump_weight = self.get_arg("pump_weight").value
        ped_weight = self.get_arg("ped_weight").value
        cable_weight = self.get_arg("cable_weight").value
        p_head = self.get_arg("p_head").value
        pump_depth = self.get_arg("pump_depth").value

        stages_length = list(map(int, stages_length.split(',')))
        stages_types = json.loads(stages_types)['stages_types']

        tubing_reliability = calculate_tubing_reliability( stages_types, stages_length, pump_weight, ped_weight, cable_weight, p_head, pump_depth)

        return tubing_reliability