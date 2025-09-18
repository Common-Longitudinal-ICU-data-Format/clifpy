# Utilities

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