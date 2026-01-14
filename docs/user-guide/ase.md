# CDC Adult Sepsis Event (ASE) Computation

Compute CDC Adult Sepsis Events from CLIF data using the [CDC Sepsis Surveillance Toolkit (2018)](https://www.cdc.gov/sepsis/media/pdfs/Sepsis-Surveillance-Toolkit-Aug-2018-508.pdf) criteria.

<div style="background: rgba(255, 145, 0, 0.1); border-left: 4px solid #ff9100; border-radius: 8px; padding: 20px; margin: 20px 0; font-size: 0.9em;">

<p style="font-weight: 600; color: #ff9100; margin-bottom: 12px; font-size: 1.1em;">
Sepsis = Component A (Presumed Serious Infection) + Component B (Acute Organ Dysfunction)
</p>

<div style="display: flex; gap: 20px; flex-wrap: wrap;">

<div style="flex: 1; min-width: 280px;">
<p style="font-weight: 600; margin-bottom: 8px;">Component A: Presumed Serious Infection</p>
<ul style="margin: 0; padding-left: 20px; line-height: 1.6;">
<li>Blood culture obtained (result not required)</li>
<li>≥4 Qualifying Antimicrobial Days (QAD) starting within ±2 calendar days of blood culture
<ul style="padding-left: 16px; margin-top: 4px;">
<li>Must include new antimicrobial (not given in prior 2 days)</li>
<li>At least one IV/IM antimicrobial required on day 1</li>
<li>1-day gaps allowed between antimicrobial days</li>
</ul>
</li>
</ul>
</div>

<div style="flex: 1; min-width: 280px;">
<p style="font-weight: 600; margin-bottom: 8px;">Component B: Acute Organ Dysfunction</p>
<p style="margin: 0 0 8px 0;">At least ONE within ±2 calendar days of blood culture:</p>
<ul style="margin: 0; padding-left: 20px; line-height: 1.6;">
<li><strong>Cardiovascular:</strong> New vasopressor initiation</li>
<li><strong>Respiratory:</strong> New IMV initiation</li>
<li><strong>Renal:</strong> Creatinine ≥2x baseline (ESRD excluded)</li>
<li><strong>Hepatic:</strong> Bilirubin ≥2.0 mg/dL AND ≥2x baseline</li>
<li><strong>Coagulation:</strong> Platelets <100 AND ≥50% decline</li>
<li><strong>Metabolic:</strong> Lactate ≥2.0 mmol/L (optional)</li>
</ul>
</div>

</div>
</div>

!!! warning "Limitations"

    - **No eGFR criterion**: CLIF does not include eGFR data, so the CDC's alternative renal dysfunction criterion (eGFR decrease) is not implemented—only creatinine doubling is used
    - **Baseline selection**: The CDC definition contains an inherent circular dependency: determining onset type requires the onset datetime, which requires organ dysfunction timing, which requires knowing the baseline, which depends on onset type. This implementation resolves the circularity by using **blood culture timing** (Day 1-2 vs Day 3+) to select which baseline to apply, then calculates the **final onset type** from the resulting ASE onset datetime. See <a href="../ase-flow-diagrams.html#circular-dependency" target="_blank">Resolving the Circular Dependency</a> for details.

For a comprehensive visual representation of the ASE algorithm:

<a href="../ase-flow-diagrams.html" target="_blank" style="display: inline-block; padding: 12px 24px; background: linear-gradient(135deg,rgb(169, 39, 19) 0%,rgb(104, 20, 20) 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; box-shadow: 0 4px 15px rgba(25, 35, 171, 0.3); transition: transform 0.2s;">
    🔍 View ASE Flow Diagrams
</a>

## Installation and Import

The ASE module should be imported directly from its submodule to avoid circular dependencies:

```python
from clifpy.utils.ase import compute_ase, ase

# Compute ASE for specific hospitalizations
ase_results = compute_ase(
    hospitalization_ids=hosp_ids,
    data_directory='/path/to/clif/data',
    filetype='parquet',
    timezone='UTC'
)

# Or using a config file
ase_results = compute_ase(
    hospitalization_ids=hosp_ids,
    config_path='config/config.json'
)
```

## Parameters

```python
from clifpy.utils.ase import compute_ase

compute_ase(
    hospitalization_ids=None,      # List of hospitalization IDs (optional)
    config_path=None,              # Path to CLIF config file
    data_directory=None,           # Override data directory path
    filetype='parquet',           # Data format ('parquet' or 'csv')
    timezone='UTC',               # Timezone for datetime handling
    apply_rit=True,               # Apply 14-day Repeat Infection Timeframe
    rit_only_hospital_onset=True, # Apply RIT only to hospital-onset events
    include_lactate=False,        # Include lactate as organ dysfunction (default: False)
    verbose=True                  # Enable detailed logging (default: True)
)
```

### Key Parameters

- **hospitalization_ids**: Process specific hospitalizations or all if None
- **config_path**: CLIF configuration file containing data paths and settings
- **apply_rit**: Filters repeat infections within 14 days (per CDC guidelines)
- **rit_only_hospital_onset**: When True, RIT only applies to hospital-onset events
- **include_lactate**: Include lactate ≥2.0 as a qualifying organ dysfunction

## Output

Returns a DataFrame with one row per blood culture episode containing:

### Key Columns

| Column | Description |
|--------|-------------|
| `hospitalization_id` | Unique hospitalization identifier |
| `bc_id` | Blood culture index within hospitalization (1, 2, 3...) |
| `episode_id` | ASE episode number (after RIT filtering) |
| `type` | Onset type: 'community' (≤2 days) or 'hospital' (>2 days) |
| `presumed_infection` | Component A flag (1=meets criteria) |
| `sepsis` | Final ASE flag with lactate |
| `sepsis_wo_lactate` | ASE flag excluding lactate criterion |
| `no_sepsis_reason` | Why sepsis criteria not met |

### Timestamps

- `blood_culture_dttm`: Blood culture collection/order time
- `ase_onset_w_lactate_dttm`: ASE onset date (earliest qualifying event)
- `ase_first_criteria_w_lactate`: First criterion triggering ASE
- Various organ dysfunction timestamps

### QAD Details

- `total_qad`: Number of Qualifying Antimicrobial Days
- `qad_start_date`: First QAD date
- `qad_end_date`: Last consecutive QAD date
- `anchor_meds_in_window`: New antimicrobials within ±2 days

## Implementation Details

### Baseline Calculation

- **Community baseline**: Uses minimum (creatinine, bilirubin) or maximum (platelets) values across entire hospitalization
- **Hospital baseline**: Uses values within ±2 days of blood culture
- Selection based on blood culture timing (≤2 days = community, >2 days = hospital)

### QAD Censoring Rules

Patients may meet QAD with <4 days if they: 

- Die (expired)
- Transfer to acute care hospital
- Enter hospice care

These events must occur within 3 days of QAD start, with antibiotics continuing until discharge.

### Repeat Infection Timeframe (RIT)

For hospital-onset events, the 14-day RIT prevents duplicate counting: 

- Only the first event in each 14-day window is counted
- Community-onset events are typically excluded from RIT
- Each qualifying event gets a unique `episode_id`

### ESRD Exclusion

Patients with End-Stage Renal Disease are excluded from AKI criterion. ESRD identified by ICD codes:

- N18.6: End stage renal disease
- Z49.31: Encounter for CRRT
- Z49.01: Encounter for dialysis
- I12.0, I13.11, I13.2: Hypertensive CKD with ESRD

## Example: Detailed Usage

```python
from clifpy.utils.ase import compute_ase, ase
import pandas as pd

# Load your cohort
cohort_df = pd.read_parquet('cohort.parquet')
hosp_ids = cohort_df['hospitalization_id'].unique().tolist()

# Compute ASE with all options
ase_results = compute_ase(
    hospitalization_ids=hosp_ids,
    config_path='config/config.json',
    apply_rit=True,                # Apply 14-day filtering
    rit_only_hospital_onset=True,  # RIT for hospital events only
    include_lactate=False,          # Exclude lactate criterion (default)
    verbose=True                    # Show detailed progress (default)
)

# Or use the alias function
ase_results = ase(
    hospitalization_ids=hosp_ids,
    data_directory='/path/to/clif/data',
    filetype='parquet',
    timezone='UTC'
)

# Analyze results
print(f"Total blood cultures evaluated: {len(ase_results)}")
print(f"Presumed infections: {ase_results['presumed_infection'].sum()}")
print(f"ASE sepsis events: {ase_results['sepsis'].sum()}")

# Get first ASE event per patient
first_ase = ase_results[
    (ase_results['sepsis'] == 1) &
    (ase_results['episode_id'] == 1)
]

# Examine onset types
print(ase_results[ase_results['sepsis'] == 1]['type'].value_counts())
```

## Edge Cases and Validation

### Handled Edge Cases

1. **Blood culture times**: Uses collect_dttm (collection time) as primary timestamp
2. **Vancomycin routes**: Treats IV and oral vancomycin as different antibiotics
3. **Procedural medications**: Excludes vasopressors given in procedural locations 
4. **Lab outliers**: Caps extreme values (Cr ≤20, Bili ≤80, Plt ≤2000, Lactate ≤30)
5. **Platelet baselines**: Only uses baseline if any value ≥100 exists
6. **ESRD exclusion**: Patients with ESRD ICD codes excluded from AKI criterion


## References

- [CDC Sepsis Surveillance Toolkit (2018)](https://www.cdc.gov/sepsis/media/pdfs/Sepsis-Surveillance-Toolkit-Aug-2018-508.pdf)
- [Incidence and Trends of Sepsis in US Hospitals Using Clinical vs Claims Data, 2009-2014](https://jamanetwork.com/journals/jama/fullarticle/2654187)

## See Also

- [SOFA Score Computation](sofa.md) - Alternative sepsis severity scoring
- [Comorbidity Index](comorbidity-index.md) - Calculate Charlson Comorbidity Index