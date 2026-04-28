# SOFA Score Computation

Compute Sequential Organ Failure Assessment (SOFA) scores from CLIF data.

## Quick Start

```python
from clifpy.clif_orchestrator import ClifOrchestrator

co = ClifOrchestrator(config_path='config/config.yaml')
sofa_scores = co.compute_sofa_scores()
```

## Parameters

- `wide_df`: Optional pre-computed wide dataset
- `cohort_df`: Optional time windows for filtering
- `id_name`: Grouping column (default: 'encounter_block')
- `extremal_type`: 'worst' (default) or 'latest' (future)
- `fill_na_scores_with_zero`: Handle missing data (default: True)

## Encounter Block vs Hospitalization ID

By default, SOFA scores are computed per `encounter_block`, which groups related hospitalizations:

```python
# Initialize with encounter stitching
co = ClifOrchestrator(
    config_path='config/config.yaml',
    stitch_encounter=True,
    stitch_time_interval=6  # hours between admissions
)

# Default: scores per encounter block (may span multiple hospitalizations)
sofa_by_encounter = co.compute_sofa_scores()  # uses encounter_block

# Alternative: scores per individual hospitalization
sofa_by_hosp = co.compute_sofa_scores(id_name='hospitalization_id')
```

**What happens when using encounter_block:**

- If encounter mapping doesn't exist, it's created automatically via `run_stitch_encounters()`
- Multiple hospitalizations within the time interval are grouped as one encounter
- SOFA score represents the worst values across the entire encounter
- Result has one row per encounter_block instead of per hospitalization

**Example encounter mapping:**
```
hospitalization_id | encounter_block
-------------------|----------------
12345             | E001
12346             | E001  # Same encounter (readmit < 6 hours)
12347             | E002  # Different encounter
```

## Required Data

SOFA requires these variables:

- **Labs**: creatinine, platelet_count, po2_arterial, bilirubin_total
- **Vitals**: map, spo2
- **Assessments**: gcs_total
- **Medications**: norepinephrine, epinephrine, dopamine, dobutamine (pre-converted to mcg/kg/min)
- **Respiratory**: device_category, fio2_set

## Missing Data

- Missing values default to score of 0
- P/F ratio uses PaO2 or imputed from SpO2
- Medications must be pre-converted to standard units

## Example with Time Filtering

```python
import pandas as pd

# Define cohort with time windows
cohort_df = pd.DataFrame({
    'encounter_block': ['E001', 'E002'],  # or 'hospitalization_id'
    'start_time': pd.to_datetime(['2024-01-01', '2024-01-02']),
    'end_time': pd.to_datetime(['2024-01-03', '2024-01-04'])
})

sofa_scores = co.compute_sofa_scores(
    cohort_df=cohort_df,
    id_name='encounter_block'  # must match cohort_df column
)
```

## Output

Returns DataFrame with:

- One row per `id_name` (encounter_block or hospitalization_id)
- Individual component scores (sofa_cv_97, sofa_coag, sofa_liver, sofa_resp, sofa_cns, sofa_renal)
- Total SOFA score (sofa_total)
- Intermediate calculations (p_f, p_f_imputed)

## SOFA Components

| Component | Based on | Score Range |
|-----------|----------|-------------|
| Cardiovascular | Vasopressor doses, MAP | 0-4 |
| Coagulation | Platelet count | 0-4 |
| Liver | Bilirubin levels | 0-4 |
| Respiratory | P/F ratio, respiratory support | 0-4 |
| CNS | GCS score | 0-4 |
| Renal | Creatinine levels | 0-4 |

Higher scores indicate worse organ dysfunction. Total score ranges from 0-24.

## Notes

- **Medication units**: Ensure medications are pre-converted to mcg/kg/min using the unit converter
- **PaO2 imputation**: When PaO2 is missing but SpO2 < 97%, PaO2 is estimated using the Severinghaus equation
- **Missing data philosophy**: Absence of monitoring data suggests the organ wasn't failing enough to warrant close observation (score = 0)

---

## High-Performance SOFA with Polars

For large datasets or performance-critical applications, CLIFpy provides `compute_sofa_polars()`, an optimized implementation using Polars that loads data directly from files.

### Quick Start (Polars)

```python
import polars as pl
from datetime import datetime
from clifpy import compute_sofa_polars

# Define cohort with time windows
cohort_df = pl.DataFrame({
    'hospitalization_id': ['H001', 'H002', 'H003'],
    'start_dttm': [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 3)],
    'end_dttm': [datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2024, 1, 4)]
})

# Compute SOFA scores
sofa_scores = compute_sofa_polars(
    data_directory='/path/to/clif/data',
    cohort_df=cohort_df,
    filetype='parquet',
    timezone='US/Central'
)
```

### Parameters (Polars)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data_directory` | str | required | Path to directory containing CLIF data files |
| `cohort_df` | pl.DataFrame | required | Cohort with hospitalization_id, start_dttm, end_dttm |
| `filetype` | str | 'parquet' | File format ('parquet' or 'csv') |
| `id_name` | str | 'hospitalization_id' | Column name for grouping scores |
| `extremal_type` | str | 'worst' | Aggregation type ('worst' for min/max) |
| `fill_na_scores_with_zero` | bool | True | Fill missing component scores with 0 |
| `remove_outliers` | bool | True | Remove physiologically implausible values |
| `timezone` | str | None | Target timezone (e.g., 'US/Central') |

### With Encounter Blocks

```python
import polars as pl
from datetime import datetime
from clifpy import compute_sofa_polars

# Cohort with encounter blocks
cohort_df = pl.DataFrame({
    'hospitalization_id': ['H001', 'H002', 'H003'],
    'encounter_block': [1, 1, 2],  # H001 and H002 are same encounter
    'start_dttm': [datetime(2024, 1, 1), datetime(2024, 1, 2), datetime(2024, 1, 5)],
    'end_dttm': [datetime(2024, 1, 2), datetime(2024, 1, 3), datetime(2024, 1, 6)]
})

# Group by encounter_block instead of hospitalization_id
sofa_scores = compute_sofa_polars(
    data_directory='/path/to/clif/data',
    cohort_df=cohort_df,
    filetype='parquet',
    id_name='encounter_block',
    timezone='US/Central'
)
```

### Integration with Pandas Workflow

If you have a pandas cohort DataFrame, convert it to Polars:

```python
import pandas as pd
import polars as pl
from clifpy import compute_sofa_polars

# Pandas cohort
cohort_pd = pd.DataFrame({
    'hospitalization_id': ['H001', 'H002'],
    'start_dttm': pd.to_datetime(['2024-01-01', '2024-01-02']),
    'end_dttm': pd.to_datetime(['2024-01-02', '2024-01-03'])
})

# Convert to Polars
cohort_pl = pl.from_pandas(cohort_pd)

# Compute SOFA
sofa_scores_pl = compute_sofa_polars(
    data_directory='/path/to/clif/data',
    cohort_df=cohort_pl,
    timezone='US/Central'
)

# Convert result back to pandas if needed
sofa_scores_pd = sofa_scores_pl.to_pandas()
```

### Performance Benefits

The Polars implementation offers significant performance improvements:

- **Lazy evaluation**: Uses `scan_parquet()` for memory-efficient loading
- **Predicate pushdown**: Filters are applied at the file level
- **Parallel execution**: Polars automatically parallelizes operations
- **Memory efficiency**: Processes data in chunks, avoiding memory exhaustion

Recommended for:
- Large cohorts (>10,000 hospitalizations)
- Memory-constrained environments
- Production pipelines requiring fast execution

### Polars vs Orchestrator Comparison

| Feature | `ClifOrchestrator.compute_sofa_scores()` | `compute_sofa_polars()` |
|---------|------------------------------------------|-------------------------|
| Backend | Pandas + DuckDB | Polars |
| Data loading | Requires pre-loaded tables | Loads directly from files |
| Memory usage | Higher (full tables in memory) | Lower (lazy evaluation) |
| Speed | Good | Faster for large datasets |
| Integration | Works with orchestrator workflow | Standalone function |
| Output | pandas DataFrame | polars DataFrame |

### Additional Polars Utilities

CLIFpy also exports Polars-based utilities for loading and datetime handling:

```python
from clifpy import (
    load_data_polars,
    load_clif_table_polars,
    standardize_datetime_columns_polars,
)

# Load any CLIF table as Polars LazyFrame
labs = load_data_polars(
    table_name='labs',
    table_path='/path/to/clif/data',
    table_format_type='parquet',
    site_tz='US/Central',
    lazy=True  # Returns LazyFrame for deferred execution
)

# Convenience function with filtering
vitals = load_clif_table_polars(
    data_directory='/path/to/clif/data',
    table_name='vitals',
    hospitalization_ids=['H001', 'H002'],
    site_tz='US/Central'
)
```