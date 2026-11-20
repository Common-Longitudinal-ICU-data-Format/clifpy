# Utilities API Reference

CLIFpy provides several utility modules to support data processing and analysis tasks.

## Med Unit Converter

The unit converter module standardizes medication dose units. The
[user guide](../user-guide/med-unit-conversion.md) walks through the workflow:
[reading the status breakdown](../user-guide/med-unit-conversion.md#the-status-breakdown-table),
[patient weight lookup](../user-guide/med-unit-conversion.md#patient-weight-lookup),
[the weight variant](../user-guide/med-unit-conversion.md#accepting-a-targets-weight-variant)
and [what clifpy guards against](../user-guide/med-unit-conversion.md#what-clifpy-guards-against).
This section lists the API.

All the arithmetic runs in DuckDB. `convert_dose_units_by_med_category`,
`standardize_med_dose_units`, `standardize_dose_to_base_units` and
`attach_weight` accept polars DataFrames or LazyFrames and DuckDB relations,
and pandas DataFrames at the input boundary. They return the format named by
their keyword-only `return_format`, with the values of
`clifpy.utils.io.load_data`: `'polars'` (the default), `'polars_lazy'` or
`'duckdb'`. `return_format='pandas'` still works on all four but is
deprecated. `return_rel=True` is accepted only by
`convert_dose_units_by_med_category` and `standardize_med_dose_units`, where it
is also deprecated; `standardize_dose_to_base_units` and `attach_weight` take
`return_format` only. See
[Inputs and return formats](../user-guide/med-unit-conversion.md#inputs-and-return-formats).

::: clifpy.utils.unit_converter.convert_dose_units_by_med_category

::: clifpy.utils.unit_converter.standardize_med_dose_units

::: clifpy.utils.unit_converter.standardize_dose_to_base_units

### Patient Weight

A weight is needed only when a conversion crosses the weight axis.
`attach_weight` looks `weight_kg` up through an ordered chain of strategies and
records which one supplied each row's weight. Attach it once to apply one
weight rule to several tables; the converter calls it itself when
`weight_fallbacks` is given.

::: clifpy.utils.unit_converter.attach_weight

::: clifpy.utils.unit_converter.find_most_recent_weight

::: clifpy.utils.unit_converter.WEIGHT_FALLBACKS

### Targets Files

Per-`med_category` target units are read from an external CLIF mCIDE targets
file, the `targets` that `standardize_med_dose_units` takes. Readers are
registered per file extension, so a new format needs no change to the
converter.

::: clifpy.utils.unit_converter.load_dose_unit_targets

::: clifpy.utils.unit_converter.register_target_reader

::: clifpy.utils.unit_converter.available_target_formats

::: clifpy.utils.unit_converter.TargetReader

### Inputs and Column Names

Input and output column names are parameters, and caller-supplied identifiers
are validated and quoted before reaching SQL. The output names follow the
converted column pair: `<dose_col>_converted` and `<unit_col>_converted` by
default, and the QA columns keep their bare names for the default `med_dose`
pair and end in `__<dose_col>` for any other (`pair_column_name`). An input
column named like a column the converter writes, such as an earlier pass's
output, is set aside for the run and returned unchanged, so two column pairs
can be converted one after the other on the same table; see
[Converting a second column pair](../user-guide/med-unit-conversion.md#converting-a-second-column-pair).
An output name that another input column already holds raises, except for an
explicit in-place overwrite of a source column. The converter's SQL reads your
data under fixed variable names, so a table or view with one of those names on
DuckDB's default connection makes it raise rather than read that table.

::: clifpy.utils.unit_converter.validate_column_name

::: clifpy.utils.unit_converter._columns.pair_column_name

::: clifpy.utils.unit_converter._columns.PAIR_QA_COLUMNS

::: clifpy.utils.unit_converter._columns.PIPELINE_OWNED_COLUMNS

::: clifpy.utils.unit_converter._inputs.REPLACEMENT_SCAN_NAMES

### Constants and Data Structures

#### Unit Grammar

`SUBCLASS_SPEC` is the single source of truth for the amount axis; the regexes
and acceptable-unit sets below are all derived from it, so adding a unit family
is a one-line change.

::: clifpy.utils.unit_converter.SUBCLASS_SPEC

::: clifpy.utils.unit_converter.SUBCLASS_REGEX

::: clifpy.utils.unit_converter.STANDALONE_SUBCLASSES

::: clifpy.utils.unit_converter.TOKEN_TO_BASE_FACTOR

::: clifpy.utils.unit_converter.TIME_TO_BASE_FACTOR

::: clifpy.utils.unit_converter.KG_PER_LB

#### Acceptable Units

::: clifpy.utils.unit_converter.ACCEPTABLE_BASE_AMOUNT_UNITS

::: clifpy.utils.unit_converter.STANDALONE_AMOUNT_UNITS

::: clifpy.utils.unit_converter.ACCEPTABLE_WEIGHT_UNITS

::: clifpy.utils.unit_converter.ACCEPTABLE_TIME_UNITS

::: clifpy.utils.unit_converter.ACCEPTABLE_AMOUNT_UNITS

::: clifpy.utils.unit_converter.ACCEPTABLE_RATE_UNITS

::: clifpy.utils.unit_converter.ALL_ACCEPTABLE_UNITS

#### Classification and Output

::: clifpy.utils.unit_converter.DEFAULT_COUNTABLE_UNITS

::: clifpy.utils.unit_converter.MISSING_UNIT_PLACEHOLDERS

::: clifpy.utils.unit_converter.CANONICAL_UNIT_SPELLING

::: clifpy.utils.unit_converter._preferred.AMBIGUOUS_MU_STATUS

`_convert_status` keeps its released values; `_convert_status_details` splits
a `'success'` by how the row reached its unit, one of the seven values below
(`CONVERT_STATUS_DETAILS` holds them in order), and repeats the failure
reason otherwise.

::: clifpy.utils.unit_converter._preferred.CONVERT_STATUS_DETAILS

::: clifpy.utils.unit_converter._preferred.DETAILS_CONVERTED_TO_TARGET

::: clifpy.utils.unit_converter._preferred.DETAILS_ALREADY_TARGET

::: clifpy.utils.unit_converter._preferred.DETAILS_CONVERTED_TO_VARIANT

::: clifpy.utils.unit_converter._preferred.DETAILS_ALREADY_VARIANT

::: clifpy.utils.unit_converter._preferred.DETAILS_UNLISTED_CLEANED

::: clifpy.utils.unit_converter._preferred.DETAILS_UNLISTED_TO_BASE

::: clifpy.utils.unit_converter._preferred.DETAILS_UNLISTED_ALREADY_BASE

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

::: clifpy.utils.unit_converter._clean_dose_unit_formats_duckdb

::: clifpy.utils.unit_converter._clean_dose_unit_names_duckdb

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

### CDC Adult Sepsis Event (ASE)

Compute CDC Adult Sepsis Events using the March 2018 CDC Sepsis Surveillance Toolkit definition.

::: clifpy.utils.ase.compute_ase

### Comorbidity Indices

Calculate Charlson and Elixhauser comorbidity indices from diagnosis data.

::: clifpy.utils.comorbidity.calculate_cci

## Data Quality Management

### Outlier Handling

Detect and handle physiologically implausible values using configurable ranges.

::: clifpy.utils.outlier_handler.apply_outlier_handling

::: clifpy.utils.outlier_handler.get_outlier_summary

### Data Validation

For the full DQA framework (30+ checks across conformance, completeness, and plausibility), see the dedicated [DQA API Reference](dqa.md).

::: clifpy.utils.validator.run_full_dqa

::: clifpy.utils.validator.validate_dataframe

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

```text
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