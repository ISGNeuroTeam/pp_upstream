All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2023-04-03
### Added
- `ks_prepare` command

## [0.5.0] - 2023-04-03
### Changed
- ks_calc_df command use new ksolver calculation when network_kind='oil'
### Fixed
- Fixed numpy error in `aspid_fit_liquid command`

## [0.4.2] - 2023-03-10
### Changed
- Update Readme
- Use errstate context manager in aspid* commands

## [0.4.1] - 2023-03-07
### Fixed 
- Fix error with empty datafram in potentials_calc_on_click command
### Changed
- Readme updated
- Set strict version on upstream-viz-lib

## [0.4.0] - 2023-02-27
### Added
- aspid* commands
- ppd_opt_pump command

### Fixed
- data folder path in hcalc_production

## [0.3.1] - 2023-02-16
### Added
- row_type attribute in ks_calc_df
### Fixed
- data folder path in hcalc_production
- fix NA ambiguous error in potentials_format_pump_events

## [0.3.0] - 2023-02-07
### Added
- calculate_tubing_reliability command
- get_nkt command

## [0.2.0] - 2023-02-06
### Added
- hcalc* commands
### Fixed
- Fixed error with types  in pump_selection_select_pump command

## [0.1.2] - 2023-01-26
### Added
- get_data.yaml to archive
- md files to archive

## [0.1.1] - 2022-12-26
### Changed
- Change upstream-viz-lib version to 0.1.0

## [0.1.0] - 2022-12-23
### Added
- Changelog.md

### Changed
- Start using "changelog"