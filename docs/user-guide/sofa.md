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