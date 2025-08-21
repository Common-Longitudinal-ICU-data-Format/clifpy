# Changelog

All notable changes to CLIFpy will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation with MkDocs
- Enhanced docstrings for all modules and classes
- API reference documentation
- User guide and examples

## [0.0.1] - 2024-01-XX

### Added
- Initial release of CLIFpy
- Core implementation of CLIF 2.0.0 specification
- All 9 CLIF table implementations:
  - Patient demographics
  - ADT (Admission, Discharge, Transfer)
  - Hospitalization
  - Laboratory results
  - Vital signs
  - Respiratory support
  - Continuous medication administration
  - Patient assessments
  - Patient positioning
- Data validation against mCIDE schemas
- Timezone handling and conversion
- ClifOrchestrator for multi-table management
- Comprehensive test suite
- Demo dataset based on MIMIC-IV
- Example notebooks

### Features
- Load data from CSV or Parquet files
- Schema-based validation
- Advanced filtering and querying
- Clinical calculations
- Summary statistics and reporting
- Memory-efficient data loading options

[Unreleased]: https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy/compare/v0.0.1...HEAD
[0.0.1]: https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy/releases/tag/v0.0.1