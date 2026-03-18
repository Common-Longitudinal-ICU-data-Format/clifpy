# DQA (Validation) API Reference

CLIFpy's Data Quality Assessment (DQA) module provides comprehensive validation organized around three pillars: **Conformance**, **Completeness**, and **Plausibility**. All checks support dual backends (Polars and DuckDB) and return structured result objects.

For a user-guide introduction, see [Data Quality Assessment (DQA)](../user-guide/validation.md).

---

## Result Classes

::: clifpy.utils.validator.DQAConformanceResult

::: clifpy.utils.validator.DQACompletenessResult

::: clifpy.utils.validator.DQAPlausibilityResult

---

## Conformance Checks

Conformance checks verify that data matches expected structure, schema, types, and allowed values.

### A.1 — Table Presence

::: clifpy.utils.validator.check_table_exists

::: clifpy.utils.validator.check_table_presence

### A.2 — Required Columns

::: clifpy.utils.validator.check_required_columns

### B.1 — Data Types

::: clifpy.utils.validator.check_column_dtypes

### B.2 — Datetime Format

::: clifpy.utils.validator.check_datetime_format

### B.3 — Lab Reference Units

::: clifpy.utils.validator.check_lab_reference_units

### B.4 — Categorical Values

::: clifpy.utils.validator.check_categorical_values

### B.5 — Category-to-Group Mapping

::: clifpy.utils.validator.check_category_group_mapping

---

## Completeness Checks

Completeness checks evaluate missing data, conditional requirements, and referential coverage.

### A.1 — Missingness

::: clifpy.utils.validator.check_missingness

### A.2 — Conditional Requirements

::: clifpy.utils.validator.check_conditional_requirements

### B — mCIDE Value Coverage

::: clifpy.utils.validator.check_mcide_value_coverage

### C.1 — Relational Integrity

::: clifpy.utils.validator.check_relational_integrity

---

## Plausibility Checks

Plausibility checks validate logical consistency, temporal ordering, and clinical reasonableness.

### A.1 — Temporal Ordering

::: clifpy.utils.validator.check_temporal_ordering

### A.2 — Numeric Range Plausibility

::: clifpy.utils.validator.check_numeric_range_plausibility

### A.3 — Field-Level Plausibility

::: clifpy.utils.validator.check_field_plausibility

### A.4 — Medication Dose Unit Consistency

::: clifpy.utils.validator.check_medication_dose_unit_consistency

### B.1 — Cross-Table Temporal Plausibility

::: clifpy.utils.validator.check_cross_table_temporal_plausibility

### C.1 — Overlapping Periods

::: clifpy.utils.validator.check_overlapping_periods

### C.2 — Category Temporal Consistency

::: clifpy.utils.validator.check_category_temporal_consistency

### D.1 — Duplicate Composite Keys

::: clifpy.utils.validator.check_duplicate_composite_keys

---

## Cross-Table Checks

These checks operate across multiple loaded tables to validate relational and temporal consistency.

::: clifpy.utils.validator.run_relational_integrity_checks

::: clifpy.utils.validator.run_cross_table_completeness_checks

::: clifpy.utils.validator.run_cross_table_plausibility_checks

---

## Orchestration

High-level functions that run groups of checks or the full DQA suite.

### Single-Table Orchestration

::: clifpy.utils.validator.run_conformance_checks

::: clifpy.utils.validator.run_completeness_checks

::: clifpy.utils.validator.run_plausibility_checks

::: clifpy.utils.validator.run_full_dqa

### Cache-Based Cross-Table Pipeline

For memory-optimized cross-table validation, extract lightweight caches and run checks without keeping full DataFrames in memory.

::: clifpy.utils.validator.extract_cross_table_cache

::: clifpy.utils.validator.run_relational_integrity_checks_from_cache

::: clifpy.utils.validator.run_cross_table_completeness_checks_from_cache

::: clifpy.utils.validator.run_cross_table_plausibility_checks_from_cache

---

## Report Generation

::: clifpy.utils.report_generator.collect_dqa_issues

::: clifpy.utils.report_generator.generate_validation_pdf

::: clifpy.utils.report_generator.generate_text_report

---

## Backward Compatibility

::: clifpy.utils.validator.validate_dataframe

::: clifpy.utils.validator.format_clifpy_error

::: clifpy.utils.validator.determine_validation_status

::: clifpy.utils.validator.classify_errors_by_status_impact

::: clifpy.utils.validator.get_validation_summary
