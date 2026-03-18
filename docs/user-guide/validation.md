# Data Quality Assessment (DQA)

CLIFpy provides a comprehensive Data Quality Assessment framework for validating CLIF tables. DQA is organized around three pillars:

- **Conformance** — Does the data match expected structure, types, and allowed values?
- **Completeness** — Is the data sufficiently present and relationally connected?
- **Plausibility** — Are values clinically reasonable and logically consistent?

For full API signatures, see the [DQA API Reference](../api/dqa.md).

## Quick Start

### Full DQA on a Single Table

```python
from clifpy.utils.validator import run_full_dqa

results = run_full_dqa(df, table_name="labs")

# Results organized by pillar
for check_name, result in results["conformance"].items():
    print(f"{check_name}: {'PASS' if result.passed else 'FAIL'}")

for check_name, result in results["completeness"].items():
    print(f"{check_name}: {'PASS' if result.passed else 'FAIL'}")

for check_name, result in results["plausibility"].items():
    print(f"{check_name}: {'PASS' if result.passed else 'FAIL'}")
```

### Via the Orchestrator

```python
from clifpy.clif_orchestrator import ClifOrchestrator

orchestrator = ClifOrchestrator("/data", "parquet", timezone="US/Central")
orchestrator.initialize(tables=["patient", "labs", "vitals"])

# DQA is run during initialization; results are stored per table
for name, table_obj in orchestrator.tables.items():
    print(f"{name}: {table_obj.validation_status}")
```

## Running Individual Check Categories

You can run just one pillar at a time:

```python
from clifpy.utils.validator import (
    run_conformance_checks,
    run_completeness_checks,
    run_plausibility_checks,
    _load_schema,
)

schema = _load_schema("vitals")

# Conformance only
conformance = run_conformance_checks(df, schema, table_name="vitals")

# Completeness only (with custom missingness thresholds)
completeness = run_completeness_checks(
    df, schema, table_name="vitals",
    error_threshold=50.0, warning_threshold=10.0
)

# Plausibility only
plausibility = run_plausibility_checks(df, schema, table_name="vitals")
```

## Understanding Results

Every check returns a result object (`DQAConformanceResult`, `DQACompletenessResult`, or `DQAPlausibilityResult`) with a consistent interface:

```python
result = conformance["required_columns"]

result.passed       # bool — overall pass/fail
result.errors       # list of dicts — critical issues
result.warnings     # list of dicts — non-blocking concerns
result.info         # list of dicts — informational messages
result.metrics      # dict — quantitative details (counts, percentages)

# Serialize for export
result.to_dict()
```

Each error/warning/info entry is a dict with `message` and optional `details` keys.

## Conformance Checks

| Code | Check | Function | Purpose |
|------|-------|----------|---------|
| A.1 | Table Presence | `check_table_exists`, `check_table_presence` | Verify file exists and DataFrame has data |
| A.2 | Required Columns | `check_required_columns` | All schema-required columns are present |
| B.1 | Data Types | `check_column_dtypes` | Column dtypes match schema (VARCHAR, DATETIME, etc.) |
| B.2 | Datetime Format | `check_datetime_format` | Datetime columns are timezone-aware |
| B.3 | Lab Reference Units | `check_lab_reference_units` | Lab units match schema definitions (labs only) |
| B.4 | Categorical Values | `check_categorical_values` | Values match mCIDE permissible values |
| B.5 | Category-Group Mapping | `check_category_group_mapping` | Category-to-group mappings are consistent |

## Completeness Checks

| Code | Check | Function | Purpose |
|------|-------|----------|---------|
| A.1 | Missingness | `check_missingness` | Required columns have data below null thresholds |
| A.2 | Conditional Requirements | `check_conditional_requirements` | If column X = value, then column Y is present |
| B | mCIDE Value Coverage | `check_mcide_value_coverage` | All standardized category values are represented |
| C.1 | Relational Integrity | `check_relational_integrity` | Foreign key coverage between tables |

## Plausibility Checks

| Code | Check | Function | Purpose |
|------|-------|----------|---------|
| A.1 | Temporal Ordering | `check_temporal_ordering` | Start times precede end times |
| A.2 | Numeric Range | `check_numeric_range_plausibility` | Values within clinically plausible ranges |
| A.3 | Field Plausibility | `check_field_plausibility` | Complex conditional field constraints |
| A.4 | Med Dose Units | `check_medication_dose_unit_consistency` | Dose units match admin type (rate vs. discrete) |
| B.1 | Cross-Table Temporal | `check_cross_table_temporal_plausibility` | Events fall within hospitalization bounds |
| C.1 | Overlapping Periods | `check_overlapping_periods` | No overlapping time intervals for same entity |
| C.2 | Category Temporal Consistency | `check_category_temporal_consistency` | Category distributions stable over time |
| D.1 | Duplicate Composite Keys | `check_duplicate_composite_keys` | No duplicate records by composite key |

## Cross-Table Checks

Some checks require data from multiple tables. These are run with lists of table objects or pre-extracted caches:

```python
from clifpy.utils.validator import (
    run_relational_integrity_checks,
    run_cross_table_completeness_checks,
    run_cross_table_plausibility_checks,
)

# tables is a list of loaded table objects
ri_results = run_relational_integrity_checks(tables)
ct_completeness = run_cross_table_completeness_checks(tables)
ct_plausibility = run_cross_table_plausibility_checks(tables)
```

### Memory-Optimized Cache Pipeline

For large datasets, extract lightweight caches to avoid keeping full DataFrames in memory:

```python
from clifpy.utils.validator import (
    extract_cross_table_cache,
    run_relational_integrity_checks_from_cache,
    run_cross_table_completeness_checks_from_cache,
    run_cross_table_plausibility_checks_from_cache,
)

caches = {}
for table_obj in tables:
    caches[table_obj.table_name] = extract_cross_table_cache(table_obj)

ri_results = run_relational_integrity_checks_from_cache(caches)
ct_completeness = run_cross_table_completeness_checks_from_cache(caches)
ct_plausibility = run_cross_table_plausibility_checks_from_cache(caches)
```

## Report Generation

Generate PDF or text reports from DQA results:

```python
from clifpy.utils.report_generator import (
    collect_dqa_issues,
    generate_validation_pdf,
    generate_text_report,
)

# Collect all issues from run_full_dqa output
category_scores, all_issues = collect_dqa_issues(results)

# PDF report
generate_validation_pdf(results, output_path="dqa_report.pdf")

# Text report
generate_text_report(results, output_path="dqa_report.txt")
```

## Configuring Thresholds

### Missingness Thresholds

Control when null percentages trigger errors vs. warnings:

```python
completeness = run_completeness_checks(
    df, schema, table_name="labs",
    error_threshold=50.0,    # >50% null → error
    warning_threshold=10.0,  # >10% null → warning
)
```

### Plausibility Thresholds

Override the default thresholds for plausibility checks:

```python
custom_thresholds = {
    "temporal_ordering": {"error_threshold": 5.0, "warning_threshold": 1.0},
    "numeric_range_plausibility": {"error_threshold": 10.0, "warning_threshold": 2.0},
    "duplicate_composite_keys": {"error_threshold": 5.0, "warning_threshold": 0.0},
}

results = run_full_dqa(
    df, schema, table_name="labs",
    plausibility_thresholds=custom_thresholds,
)
```

Default plausibility thresholds use 10% error / 0% warning for all check types.

## Dual Backend Architecture

The DQA module automatically selects between two backends:

- **Polars** (preferred) — Uses lazy evaluation and streaming for memory efficiency
- **DuckDB** (fallback) — Used when Polars is unavailable

The active backend is detected at import time:

```python
from clifpy.utils.validator import _ACTIVE_BACKEND
print(f"Using backend: {_ACTIVE_BACKEND}")  # 'polars' or 'duckdb'
```

Both backends produce identical results. All DataFrames (Pandas, Polars, or Polars LazyFrames) are accepted as input.

## Best Practices

1. **Run `run_full_dqa` for comprehensive coverage** — It orchestrates all single-table checks in one call.
2. **Use the cache pipeline for multi-table checks on large datasets** — `extract_cross_table_cache` keeps memory usage low.
3. **Check `result.metrics` for quantitative detail** — Pass/fail alone doesn't tell you how close a check was to the threshold.
4. **Customize thresholds per institution** — Default thresholds are starting points; adjust based on your data characteristics.
5. **Generate PDF reports for stakeholder review** — `generate_validation_pdf` produces formatted reports with issue summaries.
6. **Fix data at the source** — DQA identifies issues; the fix belongs in your ETL pipeline.
