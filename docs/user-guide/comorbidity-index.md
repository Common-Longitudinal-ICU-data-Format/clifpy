# Comorbidity Index Computation

CLIFpy provides comprehensive functionality for calculating comorbidity indices from hospital diagnosis data. These indices are essential tools in clinical research for quantifying patient complexity and adjusting for disease burden in outcomes studies.

## Overview

Comorbidity indices are standardized scoring systems that summarize the burden of concurrent diseases in hospitalized patients. CLIFpy implements two widely-used indices:

-   **Charlson Comorbidity Index (CCI)** - 17 conditions with differential weighting
-   **Elixhauser Comorbidity Index** - 31 conditions with van Walraven weights

Both indices use ICD-10-CM diagnosis codes and implement hierarchy logic to prevent double-counting of related conditions.

!!! warning "Important Usage Note" Hospital diagnosis codes are finalized billing diagnosis codes for reimbursement. They are appropriate for calculating comorbidity scores but should **not** be used as input features for prediction models of inpatient events.

## Charlson Comorbidity Index (CCI)

The Charlson Comorbidity Index predicts 10-year mortality risk based on 17 comorbid conditions. CLIFpy implements the Quan et al. (2011) adaptation for ICD-10-CM codes.

### CCI Conditions and Weights

**Note:** These weights are based on the Quan et al. (2011) updated version for ICD-10-CM codes, which differ from the original Charlson (1987) weights.

| Condition                     | Weight | Example ICD-10-CM Codes    |
|-------------------------------|--------|----------------------------|
| Myocardial Infarction         | 0      | I21, I22, I252             |
| Congestive Heart Failure      | 2      | I50, I099, I110            |
| Peripheral Vascular Disease   | 0      | I70, I71, I731             |
| Cerebrovascular Disease       | 0      | I60-I69, G45, G46          |
| Dementia                      | 2      | F00-F03, G30, G311         |
| Chronic Pulmonary Disease     | 1      | J40-J47, J60-J67           |
| Connective Tissue Disease     | 1      | M05, M06, M32-M34          |
| Peptic Ulcer Disease          | 0      | K25-K28                    |
| Mild Liver Disease            | 2      | K70-K77                    |
| Diabetes (uncomplicated)      | 0      | E10-E14                    |
| Hemiplegia                    | 2      | G81, G82                   |
| Renal Disease                 | 1      | N18-N19, N052-N057         |
| Diabetes with Complications   | 1      | E10-E14 with complications |
| Cancer                        | 2      | C00-C26, C30-C34, C37-C41  |
| Moderate/Severe Liver Disease | 4      | I85, I864, I982, K704      |
| Metastatic Solid Tumor        | 6      | C77-C80                    |
| AIDS                          | 4      | B20-B22, B24               |

### Basic CCI Usage

``` python
from clifpy.tables.hospital_diagnosis import HospitalDiagnosis
from clifpy.utils.comorbidity import calculate_cci

# Load hospital diagnosis data
hosp_dx = HospitalDiagnosis(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Eastern'
)

# Calculate CCI scores
cci_results = calculate_cci(hosp_dx, hierarchy=True)

# View results
print(cci_results.head())
print(f"CCI score range: {cci_results['cci_score'].min()} - {cci_results['cci_score'].max()}")
```

### CCI Output Format

The function returns a pandas DataFrame with:

-   `hospitalization_id` (index) - Unique hospitalization identifier
-   17 binary condition columns (0/1) - One for each CCI condition
-   `cci_score` - Weighted sum of present conditions

``` python
# Example output structure
hospitalization_id  myocardial_infarction  congestive_heart_failure  ...  cci_score
HOSP_001           1                      0                         ...  3
HOSP_002           0                      1                         ...  1
HOSP_003           0                      0                         ...  0
```

## Elixhauser Comorbidity Index

The Elixhauser Comorbidity Index, originally developed by Elixhauser et al. (1998), captures a broader range of comorbidities compared to CCI. The original index identified 30 conditions as binary indicators (present/absent) without weighting. CLIFpy implements an enhanced version that combines:

-   **ICD-10-CM code mappings** from Quan et al. (2005) adaptation, which expanded to 31 conditions
-   **van Walraven weighted scoring system** (2009) for mortality prediction

### Elixhauser Conditions and van Walraven Weights

**Note:** These weights are based on van Walraven et al. (2009), which converted the original binary Elixhauser (1998) measures into a weighted point system for mortality prediction. ICD-10-CM code mappings are from Quan et al. (2005). Negative weights indicate protective effects.

All 31 conditions with their van Walraven weights for mortality prediction:

| Condition | Weight | Description |
|-------------------------|-------------------|-----------------------------|
| Congestive Heart Failure | 7 | Heart failure, cardiomyopathy |
| Cardiac Arrhythmias | 5 | Atrial fibrillation, other arrhythmias |
| Valvular Disease | -1 | Heart valve disorders |
| Pulmonary Circulation Disorders | 4 | Pulmonary embolism, pulmonary hypertension |
| Peripheral Vascular Disorders | 2 | Peripheral artery disease |
| Hypertension (uncomplicated) | 0 | Essential hypertension |
| Hypertension (complicated) | 0 | Hypertensive complications |
| Paralysis | 7 | Paraplegia, hemiplegia |
| Other Neurological Disorders | 6 | Parkinson's, epilepsy, other |
| Chronic Pulmonary Disease | 3 | COPD, asthma |
| Diabetes (uncomplicated) | 0 | Diabetes without complications |
| Diabetes (complicated) | 0 | Diabetes with complications |
| Hypothyroidism | 0 | Thyroid disorders |
| Renal Failure | 5 | Chronic kidney disease |
| Liver Disease | 11 | Chronic liver disease |
| Peptic Ulcer Disease (excluding bleeding) | 0 | Peptic ulcers without bleeding |
| AIDS/HIV | 0 | HIV/AIDS |
| Lymphoma | 9 | Lymphomas |
| Metastatic Cancer | 12 | Metastatic solid tumors |
| Solid Tumor (without metastasis) | 4 | Non-metastatic cancer |
| Rheumatoid Arthritis/Collagen Vascular Disease | 0 | RA, lupus, connective tissue diseases |
| Coagulopathy | 3 | Bleeding disorders |
| Obesity | -4 | Obesity (protective weight) |
| Weight Loss | 6 | Cachexia, malnutrition |
| Fluid and Electrolyte Disorders | 5 | Electrolyte imbalances |
| Blood Loss Anemia | -2 | Anemia from blood loss |
| Deficiency Anemia | -2 | Iron, B12, folate deficiency |
| Alcohol Abuse | 0 | Alcohol use disorder |
| Drug Abuse | -7 | Substance use disorder (protective weight) |
| Psychoses | 0 | Schizophrenia, psychotic disorders |
| Depression | -3 | Major depressive disorder |

### Basic Elixhauser Usage

``` python
from clifpy.utils.comorbidity import calculate_elix

# Calculate Elixhauser scores
elix_results = calculate_elix(hosp_dx, hierarchy=True)

# View results
print(elix_results.head())
print(f"Elixhauser score range: {elix_results['elix_score'].min()} - {elix_results['elix_score'].max()}")

# Check condition prevalence
condition_prevalence = elix_results.iloc[:, :-1].mean().sort_values(ascending=False)
print("Most common conditions:")
print(condition_prevalence.head(10))
```

## Advanced Usage

### Working with Different Input Types

CLIFpy's comorbidity functions accept multiple input formats:

``` python
import pandas as pd

# Option 1: HospitalDiagnosis object (recommended)
cci_scores = calculate_cci(hosp_dx_table)

# Option 2: pandas DataFrame
df = pd.DataFrame({
    'hospitalization_id': ['H001', 'H001', 'H002'],
    'diagnosis_code': ['I21.45', 'E10.1', 'K25.5'],
    'diagnosis_code_format': ['ICD10CM', 'ICD10CM', 'ICD10CM']
})
cci_scores = calculate_cci(df)

# Option 3: polars DataFrame
import polars as pl
pl_df = pl.from_pandas(df)
cci_scores = calculate_cci(pl_df)
```

### Hierarchy Logic

Both indices implement hierarchy logic (assign0) to prevent double-counting of related conditions:

``` python
# With hierarchy (default, recommended)
cci_with_hierarchy = calculate_cci(hosp_dx, hierarchy=True)

# Without hierarchy (for research comparison)
cci_without_hierarchy = calculate_cci(hosp_dx, hierarchy=False)

# Compare the difference
hierarchy_impact = cci_with_hierarchy['cci_score'] - cci_without_hierarchy['cci_score']
print(f"Hierarchy reduces scores by: {hierarchy_impact.mean():.2f} points on average")
```

### Hierarchy Rules

**CCI Hierarchies:** - Severe liver disease supersedes mild liver disease - Diabetes with complications supersedes uncomplicated diabetes - Metastatic cancer supersedes local cancer

**Elixhauser Hierarchies:** - Complicated hypertension supersedes uncomplicated hypertension - Complicated diabetes supersedes uncomplicated diabetes - Metastatic cancer supersedes solid tumor without metastasis

## Data Requirements

### Input Data Format

Your hospital diagnosis data must include these columns:

``` python
required_columns = [
    'hospitalization_id',      # Unique hospitalization identifier
    'diagnosis_code',          # ICD diagnosis code (e.g., "I21.45")
    'diagnosis_code_format'    # Code format (must be "ICD10CM")
]
```

### ICD Code Processing

-   **Decimal handling**: Codes like "I21.45" are automatically truncated to "I21" for mapping
-   **Format filtering**: Only ICD10CM codes are processed; other formats are ignored
-   **Prefix matching**: Uses prefix matching (e.g., "I21" matches all I21.x codes)

## Clinical Interpretation

### CCI Score Interpretation

| Score Range | Mortality Risk | Typical Patient Population            |
|-------------|----------------|---------------------------------------|
| 0           | Low            | Healthy patients, minor procedures    |
| 1-2         | Moderate       | Single comorbidity, routine surgery   |
| 3-4         | High           | Multiple comorbidities, complex cases |
| ≥5          | Very High      | Severely ill, high-risk procedures    |

### Elixhauser Score Interpretation

Elixhauser scores can be negative due to protective conditions (negative weights). Typical ranges:

-   **≤0**: Low complexity, some protective factors
-   **1-4**: Moderate complexity
-   **5-15**: High complexity
-   **\>15**: Very high complexity, multiple severe conditions

### Research Applications

``` python
# Mortality risk stratification
def risk_category(score, index_type='cci'):
    if index_type == 'cci':
        if score == 0:
            return 'Low'
        elif score <= 2:
            return 'Moderate'
        elif score <= 4:
            return 'High'
        else:
            return 'Very High'
    # Add Elixhauser categorization as needed

# Apply risk stratification
results['risk_category'] = results['cci_score'].apply(
    lambda x: risk_category(x, 'cci')
)

# Analyze by risk category
risk_summary = results.groupby('risk_category').agg({
    'cci_score': ['count', 'mean', 'std']
})
```

## Configuration and Customization

### YAML Configuration Files

Comorbidity mappings are stored in YAML files:

-   `clifpy/data/comorbidity/cci.yaml` - CCI mappings and weights
-   `clifpy/data/comorbidity/elixhauser.yaml` - Elixhauser mappings and weights

### Configuration Structure

``` yaml
# Example CCI configuration structure
name: "Charlson Comorbidity Index"
version: "quan"
supported_formats:
  - ICD10CM

diagnosis_code_mappings:
  ICD10CM:
    myocardial_infarction:
      codes: ["I21", "I22", "I252"]
      description: "History of definite or probable MI"
    # ... other conditions

weights:
  myocardial_infarction: 1
  # ... other weights

hierarchies:
  - higher: "diabetes_with_complications"
    lower: "diabetes_uncomplicated"
  # ... other hierarchies
```

### Custom Mappings

For research requiring custom mappings, you can modify the YAML files or create custom versions. Ensure proper validation and testing when using custom configurations.

## Troubleshooting

### Common Issues

**No ICD10CM codes found:**

``` python
# Check your data format
print(hosp_dx.df['diagnosis_code_format'].value_counts())
# Ensure codes are marked as 'ICD10CM'
```

**Unexpected zero scores:**

``` python
# Check for missing hospitalization_id
missing_ids = hosp_dx.df['hospitalization_id'].isna().sum()
print(f"Missing hospitalization IDs: {missing_ids}")

# Verify diagnosis codes are properly formatted
print(hosp_dx.df['diagnosis_code'].head())
```

**Memory errors with large datasets:**

``` python
# Monitor memory usage
import psutil
print(f"Memory usage: {psutil.virtual_memory().percent}%")

# Consider processing in smaller chunks
```

### Validation

Always validate your results:

``` python
# Basic validation checks
assert not cci_results['cci_score'].isna().any(), "CCI scores contain NaN"
assert (cci_results['cci_score'] >= 0).all(), "CCI scores should be non-negative"
assert cci_results['hospitalization_id'].is_unique, "Hospitalization IDs should be unique"

# Clinical validation
max_score = cci_results['cci_score'].max()
print(f"Maximum CCI score: {max_score}")
if max_score > 20:
    print("Warning: Unusually high CCI scores detected")
```

## References

-   Charlson ME, Pompei P, Ales KL, MacKenzie CR. A new method of classifying prognostic comorbidity in longitudinal studies: development and validation. J Chronic Dis. 1987;40(5):373-383.
-   Elixhauser A, Steiner C, Harris DR, Coffey RM. Comorbidity measures for use with administrative data. Med Care. 1998;36(1):8-27.
-   Quan H, Sundararajan V, Halfon P, et al. Coding algorithms for defining comorbidities in ICD-9-CM and ICD-10 administrative data. Med Care. 2005;43(11):1130-1139.
-   van Walraven C, Austin PC, Jennings A, Quan H, Forster AJ. A modification of the Elixhauser comorbidity measures into a point system for hospital death using administrative data. Med Care. 2009;47(6):626-633.
-   Quan H, Li B, Couris CM, Fushimi K, Graham P, Hider P, Januel JM, Sundararajan V. Updating and Validating the Charlson Comorbidity Index and Score for Risk Adjustment in Hospital Discharge Abstracts Using Data From 6 Countries. Am J Epidemiol. 2011;173(6):676-682. doi:10.1093/aje/kwq433.