import pandas as pd
import numpy as np
from otlang.sdk.syntax import Keyword, Positional, OTLType
from pp_exec_env.base_command import BaseCommand, Syntax


def get_q_power(df_debit, df_power):
    q = []
    power = []
    work_pumps = []
    pumps = max(df_power['pump'].size, df_debit['pump'].size)
    for i in range(1, pumps+1):
        object = f"НА-{i}"
        if object in list(df_debit['pump']) and object in list(df_power['pump']):
            if float(df_debit[df_debit['pump'] == object]['debit'].iloc[0]) > 0:
                q.append(float(df_debit[df_debit['pump'] == object]['debit'].iloc[0]))
                power.append(float(df_power[df_power['pump'] == object]['power_now'].iloc[0]))
                work_pumps.append(i)
            else:
                continue
    if len(q) != len(power):
        print("Ошибка в расчетах: 1!")
    return np.array(q), np.array(power), work_pumps


def linear_power(q, power, x, deriv=False):
    n = q.size
    delta = n * np.sum(np.power(q, 2)) - np.power(np.sum(q), 2)
    delta_a = n * np.sum(q * power) - np.sum(q) * np.sum(power)
    delta_b = np.sum(np.power(q, 2)) * np.sum(power) - np.sum(q) * np.sum(q * power)
    avg_q = np.sum(q) / np.size(q)
    avg_power = np.sum(power) / np.size(power)
    r = np.sum((q - avg_q) * (power - avg_power)) / np.sqrt(
        np.sum(np.power(q - avg_q, 2)) * np.sum(np.power(power - avg_power, 2)))
    # print(f"{delta_a}::{type(delta_a)}")
    # print(f"{delta_b}::{type(delta_b)}")
    # print(f"{delta}::{type(delta)}")
    # print(f"{x}::{type(x)}", '\n')
    if deriv:
        return [delta_a / delta, r]
    else:
        return [delta_a / delta * x + delta_b / delta, r]


def solve_gradient(q_0, power_0, df_nrh, Q_plan, Q_now, delta_t, pumps):
    q = q_0
    lam = 1
    x = q

    def func(z, pump, deriv=False, df=df_nrh, ind=0):
        q = np.array(df[df['pump_no'] == pump]['Q'])
        power = np.array(df[df['pump_no'] == pump]['power'])
        return linear_power(q, power, z, deriv)[ind]
    # corr = []
    # for j in range(0, 4):
    #     corr.append(func(0, j, ind=1))
    for i in range(0, 50):
        f = np.zeros(len(pumps))
        for j, k in enumerate(pumps):
            f[j] = power_0[j] / func(q_0[j], k) * (
                    func(q[j], k, deriv=True) / q[j] - func(q[j], k) / q[j]**2) - lam * delta_t * (
                    Q_plan - Q_now - delta_t * np.sum(q))
        x_old = x
        x = x_old - 0.001 * f
        q = x
    ure = []
    for j, k in enumerate(pumps):
        ure.append(power_0[j] / func(q_0[j], k) * func(q[j], k) / q[j])
    res = pd.DataFrame({'Насос': pumps, 'Новый дебит': q, 'Новый УРЭ': ure})
    res['Закачка на конец суток'] = round(delta_t * np.sum(q) + Q_now, 2)
    res['Суммарный УРЭ'] = round(np.sum(ure), 2)
    return res


class PpdOptPumpCommand(BaseCommand):
    # define syntax of your command here
    syntax = Syntax(
        [
            # Positional("first_positional_string_argument", required=True, otl_type=OTLType.TEXT),
            # Keyword("Q_plan", required=False, otl_type=OTLType.NUMERIC),
            # Keyword("Q_now", required=False, otl_type=OTLType.NUMERIC),
            Keyword("tws", required=False, otl_type=OTLType.NUMERIC),
            Keyword("twf", required=False, otl_type=OTLType.NUMERIC),
            # Keyword("kwarg_int_double_argument", required=False, otl_type=OTLType.NUMBERIC),
            # Keyword("some_kwargs", otl_type=OTLType.ALL, inf=True),
        ],
    )
    use_timewindow = False  # Does not require time window arguments
    idempotent = True  # Does not invalidate cache

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        self.log_progress('Start ppd_opt_pump command')
        # that is how you get arguments
        # first_positional_string_argument = self.get_arg("first_positional_string_argument").value
        # Q_plan = self.get_arg("Q_plan").value or 11500
        # Q_now = self.get_arg("Q_now").value or 4000
        tws = self.get_arg("tws").value or 12
        twf = self.get_arg("twf").value or 12
        # some_kwargs = {k.key: k.value for k in self.get_iter("some_kwargs")}

        # Make your logic here
        # df = pd.DataFrame([[1, 2, 3], [2, 3, 4]], columns=["a", "b", "c"])
        delta_t = 24 - (twf - tws) / 60 / 60
        Q_plan = float(df[['q_plan']].dropna().iloc[0])
        Q_now = float(df[['q_now']].dropna().iloc[0])
        df_nrh = df[['model', 'serial_number', 'Q', 'power', 'pump_no']].dropna()
        df_debit = df[['pump', 'debit']].dropna()
        df_power = df[['pump', 'power_now']].dropna()
        q_0, power_0, pumps = get_q_power(df_debit, df_power)
        # print(Q_plan)
        # print(Q_now)
        # print(df_debit)
        # print(df_power)
        # print(pumps)
        # print(q_0)
        # print(power_0, '\n')
        res = solve_gradient(q_0, power_0, df_nrh, Q_plan, Q_now, delta_t, pumps)

        # Add description of what going on for log progress
        self.log_progress('First part is complete.', stage=1, total_stages=2)
        #
        self.log_progress('Last transformation is complete', stage=2, total_stages=2)

        # Use ordinary logger if you need

        # self.logger.debug(f'Command ppd_opt_pump get first positional argument = {first_positional_string_argument}')
        self.logger.debug(f'Command ppd_opt_pump get Q_plan argument = {Q_plan}')
        self.logger.debug(f'Command ppd_opt_pump get Q_now argument = {Q_now}')
        self.logger.debug(f'Command ppd_opt_pump get delta_t argument = {delta_t}')

        return res
