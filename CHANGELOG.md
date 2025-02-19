# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- CHANGELOG.md to document changes to the DIE

### Changed

- Report analyte sources as `False` if source does not have measurements for those analytes
  - e.g. BoR has analyte measurements, but does not have `carbonate` measurements, so if the user invokes `die weave carbonate ...` Config will print to the command line `user_source_bor: False` even if the user does not specify `--no-bor`

## 0.6.0 - 2025-02-14

### Added 

- CABQ water level data
- EBID water level data
- `source_parameter_name`, `source_parameter_units`, and `conversion_factor` to all time series tables
- `well_depth`, `well_depth_units`, and `formation` back to NMBGMR summary and site tables
- UNIT_CONVERSIONS.md to document unit conversions done by the DIE