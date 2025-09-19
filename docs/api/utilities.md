# Utilities API Reference

CLIFpy provides several utility modules to support data processing and analysis tasks.

## Med Unit Converter

The unit converter module provides comprehensive medication dose unit conversion functionality.

::: clifpy.utils.unit_converter.convert_dose_units_by_med_category

::: clifpy.utils.unit_converter.standardize_dose_to_base_units

### Constants and Data Structures

#### Acceptable Units

::: clifpy.utils.unit_converter.ACCEPTABLE_AMOUNT_UNITS

::: clifpy.utils.unit_converter.ACCEPTABLE_RATE_UNITS

::: clifpy.utils.unit_converter.ALL_ACCEPTABLE_UNITS

#### Unit Patterns

The following constants define regex patterns for unit classification:

::: clifpy.utils.unit_converter.MASS_REGEX

::: clifpy.utils.unit_converter.VOLUME_REGEX

::: clifpy.utils.unit_converter.UNIT_REGEX

::: clifpy.utils.unit_converter.HR_REGEX

::: clifpy.utils.unit_converter.WEIGHT_REGEX

#### Conversion Mappings

::: clifpy.utils.unit_converter.UNIT_NAMING_VARIANTS

::: clifpy.utils.unit_converter.REGEX_TO_FACTOR_MAPPER

### Internal Functions

The following functions are used internally by the main conversion functions. They are documented here for completeness and advanced usage.

::: clifpy.utils.unit_converter._clean_dose_unit_formats

::: clifpy.utils.unit_converter._clean_dose_unit_names

::: clifpy.utils.unit_converter._convert_clean_units_to_base_units

::: clifpy.utils.unit_converter._convert_base_units_to_preferred_units

::: clifpy.utils.unit_converter._create_unit_conversion_counts_table

::: clifpy.utils.unit_converter._convert_set_to_str_for_sql

::: clifpy.utils.unit_converter._concat_builders_by_patterns

::: clifpy.utils.unit_converter._pattern_to_factor_builder_for_base

::: clifpy.utils.unit_converter._pattern_to_factor_builder_for_preferred


This section documents the utility functions available in CLIFpy for data processing, validation, and specialized operations.

## Encounter Stitching

::: clifpy.utils.stitching_encounters.stitch_encounters

## Respiratory Support Waterfall

::: clifpy.utils.waterfall.process_resp_support_waterfall

## Data I/O Utilities

::: clifpy.utils.io.load_data
::: clifpy.utils.io.create_clif_sample

## Configuration Management

::: clifpy.utils.config.load_clif_config
::: clifpy.utils.config.get_config_or_params

## Data Validation

::: clifpy.utils.validator.validate_patient_id_mapping
::: clifpy.utils.validator.validate_patient_mapping
::: clifpy.utils.validator.validate_schema
::: clifpy.utils.validator.validate_required_columns
::: clifpy.utils.validator.validate_data_values
::: clifpy.utils.validator.validate_datetime_columns

## Outlier Handling

::: clifpy.utils.outlier_handler.OutlierHandler

## Wide Dataset Creation

::: clifpy.utils.wide_dataset.create_wide_dataset_dask
::: clifpy.utils.wide_dataset.process_unit_mapping
::: clifpy.utils.wide_dataset.create_unit_mapping_patient_dask
::: clifpy.utils.wide_dataset.get_tables_with_time_dask
