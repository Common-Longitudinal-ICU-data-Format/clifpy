# Sepsis Module Documentation

## Overview

The sepsis module implements the CDC Adult Sepsis Event (ASE) criteria for identifying sepsis cases in hospitalized patients. This implementation is based on the CDC surveillance toolkit and follows the methodology used in the linked reference repositories.

## ASE Criteria

Adult Sepsis Event requires **BOTH** of the following:

### A. Presumed Infection
Both conditions must be met:
1. **Blood culture obtained** (irrespective of result)
2. **At least 4 Qualifying Antibiotic Days (QAD)** within -2 to +6 days of blood culture
   - OR 1+ QAD if patient died/transferred to hospice/acute care within 6 days

### B. Organ Dysfunction
At least **ONE** of the following within ±2 days of blood culture:
- **Vasopressor initiation**: Norepinephrine, dopamine, epinephrine, phenylephrine, vasopressin, or angiotensin
- **Invasive mechanical ventilation** (IMV)
- **Lab criteria**:
  - **AKI**: Creatinine doubling from baseline
  - **Hyperbilirubinemia**: Total bilirubin ≥2.0 mg/dL AND doubled from baseline
  - **Thrombocytopenia**: Platelet <100 AND ≥50% decline from baseline (baseline must be ≥100)
  - **Elevated lactate**: Lactate ≥2.0 mmol/L (optional criterion)

## Usage

### Basic Example

```python
from clifpy.utils.sepsis import compute_sepsis

# Compute sepsis flags
sepsis_results = compute_sepsis(
    blood_cultures=blood_culture_df,      # Blood culture data
    antibiotics=antibiotic_df,            # Antibiotic administration data
    hospitalization=hospitalization_df,   # Hospitalization data
    labs=labs_df,                         # Lab results
    continuous_meds=continuous_meds_df,   # Vasopressor data (optional)
    respiratory_support=resp_support_df,  # Respiratory support data (optional)
    window_days=2,                        # Window size (default: 2)
    include_lactate=True                  # Include lactate criterion (default: True)
)
```

### Input Data Requirements

#### Required Inputs

**blood_cultures** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `collect_dttm`: Blood culture collection timestamp
- `fluid_category`: Should be 'blood/buffy coat'

**antibiotics** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `admin_dttm`: Antibiotic administration timestamp
- `med_group`: Should be 'CMS_sepsis_qualifying_antibiotics'

**hospitalization** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `patient_id`: Patient identifier
- `discharge_dttm`: Discharge timestamp
- `discharge_category`: Discharge disposition

**labs** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `lab_category`: Lab type ('creatinine', 'bilirubin_total', 'platelet_count', 'lactate')
- `lab_value_numeric`: Numeric lab value
- `lab_result_dttm`: Lab result timestamp

#### Optional Inputs

**continuous_meds** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `admin_dttm`: Medication administration timestamp
- `med_category`: Medication type (vasoactive medications)
- `med_dose`: Medication dose

**respiratory_support** (pandas DataFrame):
- `hospitalization_id`: Unique hospitalization identifier
- `recorded_dttm`: Recording timestamp
- `device_category`: Should be 'IMV'

**patient** (pandas DataFrame):
- `patient_id`: Patient identifier
- `death_dttm`: Death timestamp (for censoring logic)

### Output

Returns a pandas DataFrame with the following columns:
- `hospitalization_id`: Unique hospitalization identifier
- `ase_flag`: 1 if sepsis criteria met, 0 otherwise
- `presumed_infection_time`: Timestamp of blood culture
- `first_organ_dysfunction_time`: Timestamp of earliest organ dysfunction
- `organ_dysfunction_type`: Type of first organ dysfunction detected

## Functions

### compute_sepsis()

Main function to compute Adult Sepsis Event flags.

**Parameters:**
- `blood_cultures` (pd.DataFrame): Blood culture data
- `antibiotics` (pd.DataFrame): Antibiotic administration data
- `hospitalization` (pd.DataFrame): Hospitalization data
- `labs` (pd.DataFrame): Lab results
- `continuous_meds` (Optional[pd.DataFrame]): Vasopressor data
- `respiratory_support` (Optional[pd.DataFrame]): Respiratory support data
- `patient` (Optional[pd.DataFrame]): Patient data for censoring
- `window_days` (int): Days before/after blood culture (default: 2)
- `include_lactate` (bool): Include lactate criterion (default: True)

**Returns:**
- pd.DataFrame: Sepsis results with ASE flags and metadata

### Helper Functions

#### _identify_presumed_infection()
Identifies presumed infections based on blood culture and qualifying antibiotic days.

#### _identify_organ_dysfunction_vasopressors()
Identifies vasopressor-based organ dysfunction.

#### _identify_organ_dysfunction_ventilation()
Identifies invasive mechanical ventilation-based organ dysfunction.

#### _identify_organ_dysfunction_labs()
Identifies lab-based organ dysfunction (AKI, hyperbilirubinemia, thrombocytopenia, lactate).

## Clinical Interpretation

### Qualifying Antibiotic Days (QAD)
- Days are calculated relative to blood culture time (day 0)
- Window: -2 to +6 days from blood culture
- Consecutive days are counted to find the longest run
- Must have ≥4 consecutive days, OR
- ≥1 day if patient died/transferred within 6 days

### Baselines for Lab Criteria
- **Creatinine**: Minimum value across hospitalization
- **Bilirubin**: Minimum value across hospitalization
- **Platelet**: First recorded value (must be ≥100 for criterion)

### Time Windows
- Blood culture to organ dysfunction: ±2 days (default, configurable)
- Blood culture to antibiotics: -2 to +6 days

## References

1. [CDC Sepsis Surveillance Toolkit](https://www.cdc.gov/sepsis/pdfs/sepsis-surveillance-toolkit-mar-2018_508.pdf)
2. [ASE Revised Thresholds Repository](https://github.com/dmh0817/ASE_revised_thresholds_wbc_temp)
3. [CLIF Sepsis Repository](https://github.com/Common-Longitudinal-ICU-data-Format/CLIF_sepsis)

## Example

See `examples/sepsis_demo.py` for a complete working example with sample data.

## Notes

- The lactate criterion is optional because lactate ordering practices may vary across institutions
- The function uses DuckDB for efficient SQL-based computation
- Missing data is handled gracefully - missing lab baselines will exclude those criteria
- All timestamps should be timezone-aware or consistently in the same timezone
