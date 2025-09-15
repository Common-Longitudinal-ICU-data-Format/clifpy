# Outlier Handling

The outlier handling functionality in CLIFpy automatically identifies and removes physiologically implausible values from clinical data. This data cleaning process converts outlier values to NaN while preserving the data structure, ensuring that downstream analysis operates on clinically reasonable values.

## Overview

Outlier handling provides:

- **Automated detection** of values outside clinically reasonable ranges
- **Category-specific ranges** for different vital signs, lab tests, medications, and assessments
- **Unit-aware validation** for medication dosing based on category and unit combinations
- **Configurable ranges** using either CLIF standard ranges or custom configurations
- **Detailed statistics** showing the impact of outlier removal
- **Non-destructive preview** capability to assess outliers before removal

## Core Functions

### `apply_outlier_handling()`

Applies outlier handling by converting out-of-range values to NaN:

```python
from clifpy.utils import apply_outlier_handling

# Modify data in-place using CLIF standard ranges
apply_outlier_handling(vitals_table)

# Or use custom configuration
apply_outlier_handling(vitals_table, outlier_config_path="/path/to/custom_config.yaml")
```

**Parameters:**
- `table_obj`: A CLIFpy table object with `.df` and `.table_name` attributes
- `outlier_config_path` (optional): Path to custom YAML configuration file

**Returns:** None (modifies table data in-place)

### `get_outlier_summary()`

Provides a preview of outliers without modifying data:

```python
from clifpy.utils import get_outlier_summary

# Get summary without modifying data
summary = get_outlier_summary(vitals_table)
print(f"Total rows: {summary['total_rows']}")
print(f"Config source: {summary['config_source']}")
```

**Parameters:**
- `table_obj`: A CLIFpy table object with `.df` and `.table_name` attributes  
- `outlier_config_path` (optional): Path to custom YAML configuration file

**Returns:** Dictionary with outlier analysis summary

## Configuration Types

### Internal CLIF Standard Configuration

By default, CLIFpy uses internal clinically-validated ranges:

```python
from clifpy.utils import apply_outlier_handling

# Uses internal CLIF standard ranges automatically
apply_outlier_handling(vitals_table)
# Output: "Using CLIF standard outlier ranges"
```

The internal configuration includes ranges for:
- **Vitals**: Heart rate (0-300), blood pressure (0-300/0-200), temperature (32-44°C), etc.
- **Labs**: Hemoglobin (2-25), sodium (90-210), glucose (0-2000), lactate (0-30), etc.
- **Medications**: Unit-specific dosing ranges (e.g., norepinephrine 0-3 mcg/kg/min)
- **Assessments**: Scale-specific ranges (e.g., GCS 3-15, RASS -5 to +4)

### Custom YAML Configuration

Create custom configurations for specific research needs:

```python
# Apply custom ranges
apply_outlier_handling(vitals_table, outlier_config_path="/path/to/custom_ranges.yaml")
# Output: "Using custom outlier ranges from: /path/to/custom_ranges.yaml"
```

## Usage Examples

### Example 1: Basic Usage with Standard Ranges

```python
from clifpy import Vitals
from clifpy.utils import apply_outlier_handling

# Load vitals data
vitals = Vitals.from_file('/data', 'parquet', 'UTC')

print(f"Before: {vitals.df['vital_value'].notna().sum()} non-null values")

# Apply outlier handling with CLIF standard ranges
apply_outlier_handling(vitals)

print(f"After: {vitals.df['vital_value'].notna().sum()} non-null values")
```

### Example 2: Preview Outliers Before Removal

```python
from clifpy.utils import get_outlier_summary, apply_outlier_handling

# Get summary without modifying data
summary = get_outlier_summary(vitals)
print("Outlier Analysis Summary:")
print(f"- Table: {summary['table_name']}")
print(f"- Total rows: {summary['total_rows']}")
print(f"- Configuration: {summary['config_source']}")

# Apply outlier handling after review
apply_outlier_handling(vitals)
```

### Example 3: Custom Configuration for Research

```python
# Create custom configuration file
custom_config = """
tables:
  vitals:
    vital_value:
      heart_rate:
        min: 40    # More restrictive than standard (0)
        max: 180   # More restrictive than standard (300)
      temp_c:
        min: 35.0  # More restrictive than standard (32)
        max: 42.0  # More restrictive than standard (44)
"""

with open('research_config.yaml', 'w') as f:
    f.write(custom_config)

# Apply custom ranges
apply_outlier_handling(vitals, outlier_config_path='research_config.yaml')
```

### Example 4: Multiple Tables with Different Configurations

```python
from clifpy import Labs, Vitals, MedicationAdminContinuous
from clifpy.utils import apply_outlier_handling

# Load tables
vitals = Vitals.from_file('/data', 'parquet', 'UTC')
labs = Labs.from_file('/data', 'parquet', 'UTC')
meds = MedicationAdminContinuous.from_file('/data', 'parquet', 'UTC')

# Apply outlier handling to each table
for table in [vitals, labs, meds]:
    print(f"\n=== Processing {table.table_name} ===")
    apply_outlier_handling(table)
```

## Table-Specific Handling

### Simple Range Columns

For columns with straightforward min/max ranges:

```python
# Example: Age at admission (0-120 years)
# Configuration:
hospitalization:
  age_at_admission:
    min: 0
    max: 120

# Output statistics:
# age_at_admission              :   5432 values →     23 nullified ( 0.4%)
```

### Category-Dependent Columns  

For vitals, labs, and assessments where ranges depend on the category:

```python
# Example: Vital signs with different ranges per category
# Configuration:
vitals:
  vital_value:
    heart_rate:
      min: 0
      max: 300
    temp_c:
      min: 32
      max: 44

# Output statistics:
# Vitals Table - Category Statistics:
#   heart_rate        :  15234 values →    156 nullified ( 1.0%)
#   temp_c           :   8765 values →     23 nullified ( 0.3%)
```

### Unit-Dependent Medication Dosing

For medications where ranges depend on both category and unit:

```python
# Example: Norepinephrine dosing with different units
# Configuration:
medication_admin_continuous:
  med_dose:
    norepinephrine:
      "mcg/kg/min":
        min: 0.0
        max: 3.0
      "mcg/min":
        min: 0.0
        max: 200.0

# Output statistics:
# Medication Table - Category/Unit Statistics:
#   norepinephrine (mcg/kg/min)  :   2341 values →     12 nullified ( 0.5%)
#   norepinephrine (mcg/min)     :    876 values →      4 nullified ( 0.5%)
```

## Custom YAML Configuration Examples

### Example 1: Research-Specific Vitals Ranges

```yaml
# custom_vitals_config.yaml
tables:
  vitals:
    vital_value:
      heart_rate:
        min: 50     # More restrictive for adults
        max: 150    # Exclude extreme tachycardia
      sbp:
        min: 70     # Focus on hypotension
        max: 200    # Exclude severe hypertension
      temp_c:
        min: 36.0   # Normothermic range
        max: 39.0   # Exclude extreme hyperthermia
      spo2:
        min: 88     # Allow mild hypoxemia
        max: 100    # Standard upper bound
```

### Example 2: Pediatric-Specific Ranges

```yaml
# pediatric_config.yaml
tables:
  vitals:
    vital_value:
      heart_rate:
        min: 60     # Pediatric range
        max: 200    # Higher for children
      sbp:
        min: 60     # Lower for pediatrics
        max: 140
      
  hospitalization:
    age_at_admission:
      min: 0
      max: 18     # Pediatric patients only
```

### Example 3: ICU-Specific Lab Ranges

```yaml
# icu_lab_config.yaml
tables:
  labs:
    lab_value_numeric:
      lactate:
        min: 0.5    # Minimum detectable
        max: 20.0   # ICU-relevant range
      hemoglobin:
        min: 4.0    # Severe anemia threshold
        max: 20.0   # Exclude transfusion artifacts
      creatinine:
        min: 0.3    # Physiologic minimum
        max: 15.0   # Include severe AKI
```

### Example 4: Complete Custom Configuration Template

```yaml
# complete_custom_config.yaml
tables:
  # Simple range columns
  hospitalization:
    age_at_admission:
      min: 18      # Adult patients only
      max: 100     # Exclude very elderly
  
  respiratory_support:
    fio2_set:
      min: 0.21    # Room air minimum
      max: 1.0     # 100% oxygen maximum
    peep_set:
      min: 0       # No PEEP
      max: 25      # High PEEP limit
  
  # Category-dependent columns
  vitals:
    vital_value:
      heart_rate:
        min: 40
        max: 200
      sbp:
        min: 60
        max: 250
      temp_c:
        min: 35.0
        max: 42.0
  
  labs:
    lab_value_numeric:
      hemoglobin:
        min: 5.0
        max: 18.0
      sodium:
        min: 120
        max: 160
  
  # Unit-dependent medication dosing
  medication_admin_continuous:
    med_dose:
      norepinephrine:
        "mcg/kg/min":
          min: 0.01
          max: 2.0
        "mcg/min":
          min: 1.0
          max: 150.0
      propofol:
        "mg/hr":
          min: 1.0
          max: 300.0
  
  # Assessment-specific ranges
  patient_assessments:
    numerical_value:
      gcs_total:
        min: 3
        max: 15
      RASS:
        min: -5
        max: 4
```

## Understanding Output Statistics

The outlier handling provides detailed statistics showing the impact of data cleaning:

### Category-Dependent Statistics
```
Vitals Table - Category Statistics:
  heart_rate        :  15234 values →    156 nullified ( 1.0%)
  sbp              :  12876 values →     45 nullified ( 0.3%)
  temp_c           :   8765 values →     23 nullified ( 0.3%)
```

### Medication Unit-Dependent Statistics
```
Medication Table - Category/Unit Statistics:
  norepinephrine (mcg/kg/min)  :   2341 values →     12 nullified ( 0.5%)
  propofol (mg/hr)            :   1876 values →      8 nullified ( 0.4%)
```

### Simple Range Statistics
```
age_at_admission              :   5432 values →     23 nullified ( 0.4%)
fio2_set                     :   3456 values →     12 nullified ( 0.3%)
```

## Integration with ClifOrchestrator

The outlier handling integrates seamlessly with the ClifOrchestrator workflow:

```python
from clifpy.clif_orchestrator import ClifOrchestrator
from clifpy.utils import apply_outlier_handling

# Initialize orchestrator and load tables
co = ClifOrchestrator('/data', 'parquet', 'UTC')
co.initialize(['vitals', 'labs', 'medication_admin_continuous'])

# Apply outlier handling to all loaded tables
for table_name in co.get_loaded_tables():
    table_obj = getattr(co, table_name)
    print(f"\n=== Cleaning {table_name} ===")
    apply_outlier_handling(table_obj)

# Validate after outlier handling
co.validate_all()

# Create wide dataset with clean data
wide_df = co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp'],
        'labs': ['hemoglobin', 'sodium']
    }
)
```

## Best Practices

### 1. Preview Before Application
```python
# Always preview outliers first
summary = get_outlier_summary(table)
print(f"Will affect {summary['total_rows']} rows")

# Apply after review
apply_outlier_handling(table)
```

### 2. Keep Original Data
```python
# Make backup before outlier handling
original_df = vitals.df.copy()

# Apply outlier handling
apply_outlier_handling(vitals)

# Compare results
print(f"Original: {original_df['vital_value'].notna().sum()} values")
print(f"Cleaned:  {vitals.df['vital_value'].notna().sum()} values")
print(f"Removed:  {original_df['vital_value'].notna().sum() - vitals.df['vital_value'].notna().sum()} values")
```

## Troubleshooting

### Common Issues and Solutions

**No Configuration Found**
```
# Error: "No outlier configuration found for table: custom_table"
# Solution: Add table configuration to your custom YAML

tables:
  custom_table:
    numeric_column:
      min: 0
      max: 100
```

**Missing Columns**
```
# Warning: Configuration references columns not in data
# Solution: Check column names in your data vs. configuration
print(vitals.df.columns.tolist())  # Check actual column names
```

**No Outliers Detected**
```
# All values are within range - this is normal for clean data
# The statistics will show "0 nullified" for all categories
```

## Internal CLIF Standard Ranges

The internal configuration includes clinically-validated ranges for:

### Vitals
- Heart rate: 0-300 bpm
- Blood pressure: SBP 0-300, DBP 0-200, MAP 0-250 mmHg  
- Temperature: 32-44°C
- SpO2: 50-100%
- Respiratory rate: 0-60/min
- Height: 76-255 cm, Weight: 30-1100 kg

### Common Labs
- Hemoglobin: 2.0-25.0 g/dL
- Sodium: 90-210 mEq/L
- Potassium: 0-15 mEq/L
- Creatinine: 0-20 mg/dL
- Glucose: 0-2000 mg/dL
- Lactate: 0-30 mmol/L

### Medication Dosing (examples)
- Norepinephrine: 0-3 mcg/kg/min, 0-200 mcg/min
- Propofol: 0-400 mg/hr, 0-200 mcg/kg/min
- Fentanyl: 0-500 mcg/hr, 0-10 mcg/kg/hr

### Clinical Assessments
- GCS Total: 3-15
- RASS: -5 to +4
- Richmond Agitation Sedation Scale: 1-7
- Braden Total: 6-23

## Next Steps

- Learn about [data validation](validation.md) to ensure data quality after outlier removal
- Explore [wide dataset creation](wide-dataset.md) with cleaned data
- Review [individual table guides](tables/index.md) for table-specific considerations
- See the [orchestrator guide](orchestrator.md) for workflow integration