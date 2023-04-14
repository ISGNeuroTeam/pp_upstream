import logging
import os

import pandas as pd
import numpy as np

from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax

from upstream_viz_lib.config import get_data_folder
from ksolver.io.calculate_DF import calculate_DF
from ksolver.tools.MRM import MRM


class KsCalcDfCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            Keyword("check_results", required=False, otl_type=OTLType.BOOLEAN),
            Keyword("network_kind", required=False, otl_type=OTLType.TEXT),
            Keyword("sp.threshold", required=False, otl_type=OTLType.DOUBLE),
            Keyword("sp.it_limit", required=False, otl_type=OTLType.INTEGER),
            Keyword("sp.steps", required=False, otl_type=OTLType.STRING),
            Keyword("sp.cut_factor", required=False, otl_type=OTLType.INTEGER)
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    required_columns = {
        'wellNum', 'padNum', 'node_name_start', 'node_name_end',
        'node_id_start', 'node_id_end', 'rs_schema_name', 'juncType',
        'startKind', 'startValue', 'startT', 'startIsSource', 'endIsOutlet',
        'VolumeWater', 'perforation', 'pumpDepth', 'model', 'frequency',
        'productivity', 'effectiveD', 'roughness', 'predict_mode',
        'predict_mode_str', 'shtr_debit', 'shtr_oil_debit', 'K_pump', 'p_zaboy',
        'p_input', 'active_power', 'work_rate_counted', 'density_calc', 'URE',
        'is_correct_model', 'L', 'd', 's', 'uphillM', 'intD', 'simple_part_id',
        'X_start', 'Y_start', 'X_end', 'Y_end', 'endKind', 'endValue'
    }

    def _check_required_columns(self, df):

        df_columns = set(df.columns)
        if df_columns != self.required_columns:
            abscent_fields = self.required_columns - df_columns
            if abscent_fields:
                raise ValueError(f'Input dataframe han no fields: {abscent_fields}')

    def _calculate_df_for_oil(self, df,  check_results, solver_params):
        solver_params.setdefault('it_limit', 500)
        data_folder = get_data_folder()
        inclination = pd.read_parquet(os.path.join(data_folder, "inclination"), engine="pyarrow")
        HKT = pd.read_parquet(os.path.join(data_folder, "HKT"), engine="pyarrow")
        pump_curves = pd.read_csv(os.path.join(data_folder, "PumpChart.csv"))
        my_mrm = MRM(inclination, HKT, pump_curves, row_type_col='row_type', p_step=1, q_step=0.04)
        aggregate_df = my_mrm.build_df_with_curves(data_folder, df)
        mdf = calculate_DF(aggregate_df, data_folder=data_folder, check_results=check_results,
                           network_kind='oil', solver_params=solver_params, row_type_col='row_type',
                           check_input_df=False)
        df = my_mrm.convert_results(df, mdf)
        df['X_kg_sec'] = df['X_kg_sec'] + 0
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start ks_calc_DF command')

        # configure logger for ksolver
        ks_logger = logging.getLogger("ksolver")
        for handler in self.logger.handlers:
            ks_logger.addHandler(handler)
        ks_logger.setLevel(self.logger.level)

        # self._check_required_columns(df)

        check_results = self.get_arg('check_results').value or False

        network_kind = self.get_arg('network_kind').value or 'oil'
        if network_kind not in ('oil', 'water'):
            raise ValueError('network_kind must one of: oil, watter')

        # add solver params arguments in solver_params dictionary
        solver_params = {}
        for solver_param_name in ('threshold', 'it_limit', 'steps', 'cut_factor'):
            solver_param_value = self.get_arg('sp.' + solver_param_name).value
            if solver_param_value:
                solver_params[solver_param_name] = solver_param_value

        try:
            if 'steps' in solver_params:
                solver_params['steps'] = np.array(
                    [np.float(val.strip()) for val in solver_params['steps'].split(',')]
                )
        except ValueError as e:
            raise ValueError('steps param must be string with comma separated float values')
        if network_kind == 'oil':
            df = self._calculate_df_for_oil(df, check_results, solver_params)
        else:
            df = calculate_DF(
                df, data_folder=get_data_folder(), check_results=check_results, network_kind=network_kind, solver_params=solver_params, row_type_col='row_type'
            )
        self.log_progress('ks_calc_DF complete')

        return df

