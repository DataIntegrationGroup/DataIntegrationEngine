# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## 0.9.2

### Added
- `--sites-only` flag to only retrieve site data
- `--output-format` flag to write out sites/summary tables as csv or geojson.
  - options are `csv` or `geojson`
  - timeseries data is always written to a csv
- NM OSE POD data for sites.
  - can be removed from output with `--no-nmose-pod`
- `--output-dir` to change the output directory to a location other than `.` (the current working directory)

### Changed
- `output` to `output-type` for CLI

### Fixed
- a bug with `--site-limit`. it now exports the number of sets requested by the 

## 0.8.0

### Added
- water level for WQP
- `earliest_date`, `earliest_time`, `earliest_value`, and `earliest_units` to the summary table
- `die wells` to get all wells for which the DIE reports observations
- `die source {parameter}` to list sources that report a particular parameter
- NM OSE PODs, though its information is only currently available for the invocation of `die wells`

### Changed
- NM OSE Roswell data is now pulled from ST2 and not CKAN
- renamed the column `location` to `name` in the summary table to match the format of the `sites` table when timeseries data are exported
- renamed the columns `most_recent_date`, `most_recent_time`, `most_recent_value`, and `most_recent_units` to `latest_date`, `latest_time`, `latest_value`, and `latest_units` respectively for succinctness and juxtaposition with the newly added `earliest` columns.
  - This naming schema also enables the development of datetime filters as the descriptor will apply to the latest datetime within the provided time frame filter, whereas most recent indicates np filters.
- removed sites that are not in New Mexico  

### Fixed
- removed records from USGS where the value is "-999999"


## 0.7.0

### Added

- CHANGELOG.md to document changes to the DIE
- ppb to mg/L unit conversion

### Changed

- Report analyte sources as `False` if source does not have measurements for those analytes
  - e.g. BoR has analyte measurements, but does not have `carbonate` measurements, so if the user invokes `die weave carbonate ...` Config will print to the command line `user_source_bor: False` even if the user does not specify `--no-bor`

### Fixed

- Decreased NMBGMR chunk size from 100 to 10 to prevent ReadTimeoutErrors from occurring while gathering water level data

## 0.6.0 - 2025-02-14

### Added 

- CABQ water level data
- EBID water level data
- `source_parameter_name`, `source_parameter_units`, and `conversion_factor` to all time series tables
- `well_depth`, `well_depth_units`, and `formation` back to NMBGMR summary and site tables
- UNIT_CONVERSIONS.md to document unit conversions done by the DIE