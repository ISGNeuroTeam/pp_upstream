# pp_cmd_ks_calc_df
Postprocessing command "ks_calc_df"
Command uses ksolver/io/calculate_DF.calculate_DF function

### Usage example:
`... | ks_calc_df`

### Arguments
- check_results - boolean, keyword argument, default: false
- network_kind - text, keyword argument, one of two possible values: `oil` or `water`. default: oil

Solver parameters:  
- sp.it_limit - integer, keyword argument, default: 100
- sp.threshold - float, keyword argument, default: 0.05
- sp.steps - string, keyword argument, list of floats as a string, default: "0.2828, 0.5657, 1.1314, 2.2627"
- sp.cut_factor - integer, keyword argument, default: 5

### Usage example with arguments 
`... | ks_calc_df network_kind=water, sp.it_limit=105, sp.steps="0.2828, 0.5657, 1.1314, 2.2627"`


