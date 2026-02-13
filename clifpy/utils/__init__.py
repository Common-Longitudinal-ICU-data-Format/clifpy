from .config import load_config, get_config_or_params, create_example_config
from .io import load_data, convert_datetime_columns_to_site_tz, LazyRelation, fetch_lazy_result, close_lazy_relation
from .wide_dataset import create_wide_dataset, convert_wide_to_hourly
from .outlier_handler import apply_outlier_handling, get_outlier_summary
from .comorbidity import calculate_cci

# DQA functions from validator module
from .validator import (
    # Result containers
    DQAConformanceResult,
    DQACompletenessResult,
    # Schema loader
    _load_schema,
    # Backend info
    _ACTIVE_BACKEND,
    # Conformance checks
    check_table_exists,
    check_table_presence,
    check_table_presence_polars,
    check_table_presence_duckdb,
    check_required_columns,
    check_column_dtypes,
    check_datetime_format,
    check_lab_reference_units,
    check_categorical_values,
    # Completeness checks
    check_missingness,
    check_conditional_requirements,
    check_mcide_value_coverage,
    check_relational_integrity,
    # Orchestration functions
    run_conformance_checks,
    run_completeness_checks,
    run_relational_integrity_checks,
    run_full_dqa,
)

from .waterfall import process_resp_support_waterfall
from .stitching_encounters import stitch_encounters
from .sofa import compute_sofa, _compute_sofa_from_extremal_values, _agg_extremal_values_by_id
from .ase import compute_ase

__all__ = [
      # io
      'load_data',
      'convert_datetime_columns_to_site_tz',
      'LazyRelation',
      'fetch_lazy_result',
      'close_lazy_relation',
      # wide_dataset
      'create_wide_dataset',
      'convert_wide_to_hourly',
      # outlier_handler
      'apply_outlier_handling',
      'get_outlier_summary',
      # comorbidity
      'calculate_cci',
      # config
      'load_config',
      'get_config_or_params',
      'create_example_config',
      # waterfall
      'process_resp_support_waterfall',
      # stitching_encounters
      'stitch_encounters',
      # sofa
      'compute_sofa',
      '_compute_sofa_from_extremal_values',
      '_agg_extremal_values_by_id',
      # ase
      'compute_ase',
      # DQA functions
      '_ACTIVE_BACKEND',
      'DQAConformanceResult',
      'DQACompletenessResult',
      '_load_schema',
      'check_table_exists',
      'check_table_presence',
      'check_table_presence_polars',
      'check_table_presence_duckdb',
      'check_required_columns',
      'check_column_dtypes',
      'check_datetime_format',
      'check_lab_reference_units',
      'check_categorical_values',
      'check_missingness',
      'check_conditional_requirements',
      'check_mcide_value_coverage',
      'check_relational_integrity',
      'run_conformance_checks',
      'run_completeness_checks',
      'run_relational_integrity_checks',
      'run_full_dqa',
  ]
