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

## Core Data Processing

### Encounter Stitching

Stitch together hospital encounters that occur within a specified time window, useful for treating rapid readmissions as a single continuous encounter.

::: clifpy.utils.stitching_encounters.stitch_encounters

### Wide Dataset Creation

Transform CLIF tables into wide format for analysis, with automatic pivoting and high-performance processing.

::: clifpy.utils.wide_dataset.create_wide_dataset

::: clifpy.utils.wide_dataset.convert_wide_to_hourly

## Respiratory Support Processing

### Waterfall Processing

Apply sophisticated data cleaning and imputation to respiratory support data for complete ventilator timelines.

::: clifpy.utils.waterfall.process_resp_support_waterfall

## Clinical Calculations

### Comorbidity Indices

Calculate Charlson and Elixhauser comorbidity indices from diagnosis data.

::: clifpy.utils.comorbidity.calculate_cci

## Data Quality Management

### Outlier Handling

Detect and handle physiologically implausible values using configurable ranges.

::: clifpy.utils.outlier_handler.apply_outlier_handling

::: clifpy.utils.outlier_handler.get_outlier_summary

### Data Validation

Comprehensive validation functions for ensuring data quality and CLIF compliance.

::: clifpy.utils.validator.validate_dataframe

::: clifpy.utils.validator.validate_table

::: clifpy.utils.validator.check_required_columns

::: clifpy.utils.validator.verify_column_dtypes

::: clifpy.utils.validator.validate_categorical_values

::: clifpy.utils.validator.validate_numeric_ranges

## Configuration and I/O

### Configuration Management

Load and manage CLIF configuration files for consistent settings.

::: clifpy.utils.config.load_config

::: clifpy.utils.config.get_config_or_params

### Data Loading

Core data loading functionality with timezone and filtering support.

::: clifpy.utils.io.load_data

## Simplified Import Paths

As of version 0.0.1, commonly used utilities are available directly from the clifpy package:

```python
# Direct imports from clifpy
import clifpy

# Encounter stitching
hospitalization_stitched, adt_stitched, mapping = clifpy.stitch_encounters(
    hospitalization_df, 
    adt_df,
    time_interval=6
)

# Wide dataset creation
wide_df = clifpy.create_wide_dataset(
    clif_instance=orchestrator,
    optional_tables=['vitals', 'labs'],
    category_filters={'vitals': ['heart_rate', 'sbp']}
)

# Calculate comorbidity index
cci_scores = clifpy.calculate_cci(
    hospital_diagnosis_df,
    hospitalization_df
)

# Apply outlier handling
clifpy.apply_outlier_handling(table_object)
```

For backward compatibility, the original import paths (`clifpy.utils.module.function`) remain available.