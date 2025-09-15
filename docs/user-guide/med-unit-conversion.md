# Medication Unit Conversion

CLIFpy provides robust medication dose unit conversion functionality to standardize medication dosing across different unit systems. This is essential for clinical data analysis where medications may be recorded in various units across different systems.

## Overview

The medication unit converter handles the complexity of converting between different dose unit representations while maintaining clinical accuracy. It supports:

- **Rate units**: Doses per time (e.g., mcg/min, ml/hr, u/kg/hr)
- **Amount units**: Total doses (e.g., mcg, ml, u)
- **Weight-based dosing**: Per-kilogram or per-pound calculations
- **Multiple unit variants**: Handles various input formats and abbreviations

## Main Functions

### Primary Function: `convert_dose_units_by_med_category()`

This is the main function most users should use. It converts medication doses to user-specified preferred units for each medication category.

```python
from clifpy.utils.unit_converter import convert_dose_units_by_med_category
import pandas as pd

# Load your medication data
med_df = pd.read_parquet('clifpy/data/clif_demo/clif_medication_admin_continuous.parquet')

# Define preferred units for each medication
preferred_units = {
    'propofol': 'mcg/kg/min',
    'fentanyl': 'mcg/hr',
    'insulin': 'u/hr',
    'midazolam': 'mg/hr'
}

# Convert units
converted_df, summary_df = convert_dose_units_by_med_category(
    med_df=med_df,
    preferred_units=preferred_units,
    override=False
)
```

### Secondary Function: `standardize_dose_to_base_units()`

This function is for advanced users who need to standardize all units to a base set without medication-specific preferences.

```python
from clifpy.utils.unit_converter import standardize_dose_to_base_units

# Standardize to base units only
base_df, counts_df = standardize_dose_to_base_units(med_df)
```

## Function Outputs

Both public functions return **two DataFrames**:

1. **Main DataFrame**: Original data with additional conversion columns
2. **Summary DataFrame**: Conversion statistics grouped by:
   - `med_category` (if applicable)
   - `med_dose_unit` (original unit)
   - `_clean_unit` (cleaned unit)
   - `_base_unit` (standardized base unit)
   - `_unit_class` (rate/amount/unrecognized)
   - `_preferred_unit` (target unit)
   - `med_dose_unit_converted` (final unit)
   - `_convert_status` (success/failure reason)
   - `count` (number of records)

## Unit Classification System

### Unit Classes

- **`rate`**: Dose per time units (e.g., mcg/min, ml/hr, u/kg/hr)
- **`amount`**: Total dose units (e.g., mcg, ml, u)
- **`unrecognized`**: Units that cannot be parsed or converted

### Unit Subclasses

- **`mass`**: Weight-based units (mcg, mg, ng, g)
- **`volume`**: Volume-based units (ml, l)
- **`unit`**: Unit-based dosing (u, mu)
- **`unrecognized`**: Units that don't fit standard categories

Unit class and subclass compatibility determines whether conversions are allowed. For example:
- ✅ `rate` → `rate` (same class)
- ✅ `mass` → `mass` (same subclass)
- ❌ `rate` → `amount` (different class)
- ❌ `mass` → `volume` (different subclass)

## Acceptable Units Reference

The following table shows all supported units and their variations:

| Unit Class | Unit Subclass | _clean_unit | Acceptable Variations | _base_unit |
|------------|---------------|-------------|----------------------|------------|
| **Amount Units** |
| amount | mass | mcg | MCG, µg, μg, ug | mcg |
| amount | mass | mg | MG, milligram | mcg |
| amount | mass | ng | NG, nanogram | mcg |
| amount | mass | g | G, gram, grams | mcg |
| amount | volume | ml | mL, milliliter, milliliters | ml |
| amount | volume | l | L, liter, liters, litre, litres | ml |
| amount | unit | u | U, unit, units | u |
| amount | unit | mu | MU, milliunit, milliunits, milli-unit, milli-units | u |
| **Rate Units** |
| rate | mass | mcg/min | MCG/MIN, µg/min, μg/min, mcg/minute, micrograms/minute | mcg/min |
| rate | mass | mcg/hr | MCG/HR, µg/hr, μg/hr, mcg/hour, micrograms/hour | mcg/min |
| rate | mass | mcg/kg/min | MCG/KG/MIN, µg/kg/min, mcg/kg/minute | mcg/min |
| rate | mass | mcg/kg/hr | MCG/KG/HR, µg/kg/hr, mcg/kg/hour | mcg/min |
| rate | mass | mcg/lb/min | MCG/LB/MIN, µg/lb/min, mcg/lb/minute | mcg/min |
| rate | mass | mcg/lb/hr | MCG/LB/HR, µg/lb/hr, mcg/lb/hour | mcg/min |
| rate | volume | ml/min | mL/min, ml/m, milliliter/minute | ml/min |
| rate | volume | ml/hr | mL/hr, ml/h, milliliter/hour, milliliters/hour, millilitres/hour | ml/min |
| rate | volume | ml/kg/min | mL/kg/min, milliliter/kg/minute | ml/min |
| rate | volume | ml/kg/hr | mL/kg/hr, milliliter/kg/hour | ml/min |
| rate | volume | ml/lb/min | mL/lb/min, milliliter/lb/minute | ml/min |
| rate | volume | ml/lb/hr | mL/lb/hr, milliliter/lb/hour | ml/min |
| rate | unit | u/min | U/min, units/minute, unit/minute | u/min |
| rate | unit | u/hr | U/hr, units/hour, unit/hour | u/min |
| rate | unit | u/kg/min | U/kg/min, units/kg/minute | u/min |
| rate | unit | u/kg/hr | U/kg/hr, u/kg/h, units/kg/hour | u/min |
| rate | unit | u/lb/min | U/lb/min, units/lb/minute | u/min |
| rate | unit | u/lb/hr | U/lb/hr, units/lb/hour, unit/lb/hr | u/min |

### Important Notes

- **_clean_unit**: The exact format you must use when specifying preferred units
- **Acceptable Variations**: Raw `med_dose_unit` strings in your original DataFrame that the converter can detect and clean (these are NOT acceptable formats for preferred units)
- **_base_unit**: The standardized unit all conversions target (mcg/min, ml/min, u/min for rates; mcg, ml, u for amounts)

## Conversion Status and Error Handling

### The `_convert_status` Column

After conversion, each record includes a `_convert_status` field indicating the outcome:

- **`success`**: Conversion completed successfully
- **`original unit is missing`**: No unit provided in source data
- **`original unit [unit] is not recognized`**: Input unit cannot be parsed
- **`user-preferred unit [unit] is not recognized`**: Target unit is invalid
- **`cannot convert [class1] to [class2]`**: Incompatible unit classes (e.g., rate → amount)
- **`cannot convert [subclass1] to [subclass2]`**: Incompatible unit subclasses (e.g., mass → volume)

### Failure Handling

When conversion fails:
- `med_dose_converted` = original `_base_dose` (or original dose if base conversion failed)
- `med_dose_unit_converted` = `_clean_unit` (or original unit if cleaning failed)

### Override Option

Use `override=True` to bypass detection of unacceptable conversions and continue processing with warnings instead of errors:

```python
converted_df, summary_df = convert_dose_units_by_med_category(
    med_df=med_df,
    preferred_units=preferred_units,
    override=True  # Print warnings but continue processing
)
```

### Understanding Underscore Columns

Columns with underscore prefixes (`_`) are intermediate results that users typically don't need to worry about:

- `_clean_unit`: Cleaned and standardized input unit
- `_base_dose`: Dose converted to base units
- `_base_unit`: Standardized base unit
- `_unit_class`: Unit classification (rate/amount/unrecognized)
- `_unit_subclass`: Unit subclassification (mass/volume/unit)
- `_convert_status`: Conversion outcome
- Various `_multiplier` columns: Conversion factors applied

## Practical Examples

### Example 1: Basic Conversion with Real Data

```python
import pandas as pd
from clifpy.utils.unit_converter import convert_dose_units_by_med_category

# Load real medication data
med_df = pd.read_parquet('clifpy/data/clif_demo/clif_medication_admin_continuous.parquet')

# Add patient weights (required for weight-based conversions)
med_df['weight_kg'] = 75.0  # Or join with actual weight data

# Define preferred units
preferred_units = {
    'propofol': 'mcg/kg/min',
    'fentanyl': 'mcg/hr',
    'insulin': 'u/hr',
    'midazolam': 'mg/hr'
}

# Convert
converted_df, summary_df = convert_dose_units_by_med_category(
    med_df=med_df,
    preferred_units=preferred_units
)

# Check results
print(f"Total records: {len(converted_df)}")
print(f"Successful conversions: {(converted_df['_convert_status'] == 'success').sum()}")

# View conversion summary
summary_df.head()
```

### Example 2: Handling Weight-Based Dosing

```python
# Weight-based medications require patient weights
weight_based_units = {
    'propofol': 'mcg/kg/min',     # Requires weight
    'norepinephrine': 'ng/kg/min'  # Requires weight
}

# Ensure weight_kg column exists
if 'weight_kg' not in med_df.columns:
    # Join with vitals data or add default weights
    med_df['weight_kg'] = 70.0  # kg

converted_df, summary_df = convert_dose_units_by_med_category(
    med_df=med_df,
    preferred_units=weight_based_units
)
```

### Example 3: Corner Cases and Error Handling

```python
# Test data with various edge cases
test_data = pd.DataFrame({
    'med_category': ['propofol', 'propofol', 'fentanyl', 'insulin'],
    'med_dose': [6, 7, 2, 5],
    'med_dose_unit': ['MCG/KG/HR', 'MCG', 'mcg/kg/hr', 'units/hr'],
    'weight_kg': [70, 70, 80, 75]
})

preferred_units = {
    'propofol': 'mcg/kg/min',  # Valid conversion
    'fentanyl': 'mcg/hr',      # Valid conversion
    'insulin': 'u/hr'          # Valid conversion
}

converted_df, summary_df = convert_dose_units_by_med_category(
    test_data,
    preferred_units=preferred_units
)

# Check conversion status
status_counts = converted_df['_convert_status'].value_counts()
print("Conversion outcomes:")
print(status_counts)

# Failed conversions keep original values
failed_records = converted_df[converted_df['_convert_status'] != 'success']
print("\nFailed conversions:")
print(failed_records[['med_dose', 'med_dose_unit', 'med_dose_converted',
                     'med_dose_unit_converted', '_convert_status']])
```

### Example 4: Summary Analysis

```python
# Analyze conversion patterns
summary_analysis = summary_df.groupby(['med_category', '_convert_status'])['count'].sum()
print("Conversion summary by medication:")
print(summary_analysis)

# Check for problematic units
problematic_units = summary_df[summary_df['_convert_status'] != 'success']
print("\nUnits requiring attention:")
print(problematic_units[['med_dose_unit', '_convert_status', 'count']])
```

## Best Practices

1. **Always specify patient weights** when using weight-based units (e.g., mcg/kg/min)
2. **Check conversion status** after processing to identify failed conversions
3. **Use exact _clean_unit formats** when specifying preferred units
4. **Review the summary DataFrame** to understand conversion patterns and identify data quality issues
5. **Test with override=True** first to see all potential issues before requiring strict validation
6. **Validate your preferred_units dictionary** against the acceptable units table above

## Troubleshooting

### Common Issues

**Issue**: "Cannot convert rate to amount"
**Solution**: Ensure unit classes match (rate→rate, amount→amount)

**Issue**: "Cannot convert mass to volume"
**Solution**: Ensure unit subclasses match (mass→mass, volume→volume)

**Issue**: "User-preferred unit [unit] is not recognized"
**Solution**: Use exact `_clean_unit` format from the reference table above

**Issue**: Weight-based conversions failing
**Solution**: Ensure `weight_kg` column exists in your DataFrame

### Getting Help

If you encounter units not in the reference table or unexpected conversion failures:

1. Check the `_convert_status` column for specific error messages
2. Review the summary DataFrame for patterns in failed conversions
3. Use `override=True` to see warnings instead of stopping on errors
4. Consult the API reference for detailed function documentation