# Medication Unit Conversion

CLIFpy provides robust medication dose unit conversion functionality to standardize medication dosing across different unit systems. This is essential for clinical data analysis where medications may be recorded in various units across different systems.

## Standardize dose units by medication

In the most common use cases, we want to **standardize dose units by medication and pattern of administration** -- all propofol doses to be presented in mcg/kg/min in the continuous table and in mcg in the intermittent table, for example.

To achieve this, simply call one of the two `convert_dose_units_*` functions (one for continuous and one for intermittent) from the CLIF orchestrator and provide **a dictionary mapping of medication categories to their preferred units**:

```python
from clifpy.clif_orchestrator import ClifOrchestrator

co = ClifOrchestrator(config_path="config/config.yaml")

preferred_units_cont = {
    "propofol": "mcg/min",
    "fentanyl": "mcg/hr",
    "insulin": "u/hr",
    "midazolam": "mg/hr",
    "heparin": "u/min"
}

co.convert_dose_units_for_continuous_meds(preferred_units=preferred_units_cont)
```

### Returns

Under the hood, this function automatically loads and uses the medication and vitals tables to generate two dataframes that are saved to the corresponding medication table instance by default:

1. **`co.medication_admin_continuous.df_converted`** gives the updated medication table with the new columns appended:
   - `weight_kg`: the most recent weight relative to the `admin_dttm` pulled from the `vitals` table.
   - `_clean_unit`: cleaned source unit string where both 'U/h' and 'units / hour' would be standardized to 'u/hr', for example.
   - `_unit_class`: distinguishes where the source unit is an amount (e.g. 'mcg'), a 'rate' (e.g. 'mcg/hr'), or 'unrecognized'.
   - `_convert_status`: documents whether the conversion is a "success" or, in the case of failure, the reason for failure, e.g. 'cannot convert amount to rate' for rows of propofol in 'mcg' that the users want to convert to 'mcg/kg/min'.
   - `med_dose_converted`, `med_dose_unit_converted`: the converted results if the `_convert_status` is 'success', or fall back to the original `med_dose` and `_clean_unit` if failure.

*Note: the following demo output omits some rows and columns for display purposes*

2. **`co.medication_admin_continuous.conversion_counts`** shows an aggregated summary of which source units of which `med_category` are converted to which preferred units -- and their frequency counts. A useful quality check would be to filter for all the `_convert_status` that are not 'success.'

To access the results directly instead of from the table instance, turn off the `save_to_table` argument:

```python
cont_converted, cont_counts = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False
)
```

### Override option

The function automatically parses whether the provided `med_categories` and preferred units in the dictionary are acceptable and return errors or warnings when they are not. To override any code-breaking error such as an unidentified `med_category` or preferred unit string, turn on the arg `override=True`:

```python
co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    override=True
)
```

### Acceptable unit formatting

The unit strings in `preferred_units` dictionary need to be formatted a certain way for them to be accepted. (The original source unit strings in `med_dose_unit` do _not_ face such restrictions. Both 'mL' and 'milliliter' in `med_dose_unit` can be correctly parsed as 'ml', for example.)

For a list of acceptable preferred units:

- **amount:**
  - mass: `mcg`, `mg`, `ng`, `g`
  - volume: `ml`, `l`
  - unit: `mu`, `u`

- **weight:** `/kg`, `/lb`

- **time:** `/hr`, `/min`

- **rate:** a combination of amount, weight, and time, e.g. 'mcg/kg/min', 'u/hr'.
  - the unit can be either weight-adjusted or not -- that is, both 'mcg/kg/min' and 'mcg/min' are acceptable. When no weight is available from the `vitals` table to enable conversion between weight-adjusted and weight-less units, an error will be returned.

All strings should be in lower case with no whitespaces in between.


## Standardize to base units across medications

In rarer cases, one might prefer all applicable units of the same class be collapsed onto the same scale across medications, e.g. both 'mcg/kg/min' and 'mg/hour' would be converted to the same 'mcg/min' -- referred to here as the "base unit" -- across all medications applicable.

To enable this, turn on the `show_intermediate=True` argument:

```python
cont_converted_detailed, _ = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False,
    show_intermediate=True
)
```

This would append a series of additional columns that were the intermediate results generated during the conversion, including the `_base_dose` and `_base_unit`.

The set of base units are:

- **amount**: `mcg`, `ml`, `u`
- **time**: `/min`
- **rate**: a combination of amount and time, e.g. `mcg/min`, `u/min`.
  - Note that all base units would be weight-less.

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

### Reference Table

| unit class | unit subclass | _clean_unit | acceptable source `med_dose_unit` examples | _base_unit |
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



## Error Handling

### The `_convert_status` Column

After conversion, each record includes a `_convert_status` field indicating the outcome:

- **`success`**: Conversion completed successfully
- **`original unit is missing`**: No unit provided in source data
- **`original unit [unit] is not recognized`**: Input unit cannot be parsed
- **`user-preferred unit [unit] is not recognized`**: Target unit is invalid
- **`cannot convert [class1] to [class2]`**: Incompatible unit classes (e.g., rate → amount)
- **`cannot convert [subclass1] to [subclass2]`**: Incompatible unit subclasses (e.g., mass → volume)
- **`cannot convert to a weighted unit if weight_kg is missing`**: Weight-based conversion attempted without patient weight

### Failure Handling

When conversion fails:
- `med_dose_converted` = original `_base_dose` (or original dose if base conversion failed)
- `med_dose_unit_converted` = `_clean_unit` (or original unit if cleaning failed)

## Alternative: Direct Unit Converter Usage

For advanced users who need more control or want to use the unit converter directly without the ClifOrchestrator:

### Primary Function: `convert_dose_units_by_med_category()`

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

## Best Practices

1. **Check conversion status** after processing to identify failed conversions
2. **Use exact _clean_unit formats** when specifying preferred units
3. **Review the conversion counts summary DataFrame** to understand conversion patterns and identify data quality issues
4. **Test with override=True** first to see all potential issues before requiring strict validation
5. **Validate your preferred_units dictionary** against the acceptable units table above

## Troubleshooting

### Common Issues

**Issue**: "Cannot convert rate to amount"
    - **Solution**: Ensure unit classes match (rate→rate, amount→amount)

**Issue**: "Cannot convert mass to volume"
    - **Solution**: Ensure unit subclasses match (mass→mass, volume→volume)

**Issue**: "User-preferred unit [unit] is not recognized"
    - **Solution**: Use exact `_clean_unit` format from the reference table above

**Issue**: Weight-based conversions failing
    - **Solution**: Ensure `weight_kg` column exists in your DataFrame or is available in vitals data

**Issue**: "Cannot convert to a weighted unit if weight_kg is missing"
    - **Solution**: Provide patient weights in the vitals table or med_df

### Getting Help

If you encounter units not in the reference table or unexpected conversion failures:

1. Check the `_convert_status` column for specific error messages
2. Review the summary DataFrame for patterns in failed conversions
3. Use `override=True` to see warnings instead of stopping on errors
4. Consult the API reference for detailed function documentation

## Example Analysis Workflow

```python
# 1. Basic conversion
converted_df, summary_df = co.convert_dose_units_for_continuous_meds(
    preferred_units=preferred_units_cont,
    save_to_table=False
)

# 2. Check conversion success
print(f"Total records: {len(converted_df)}")
print(f"Successful conversions: {(converted_df['_convert_status'] == 'success').sum()}")

# 3. Analyze conversion patterns
summary_analysis = summary_df.groupby(['med_category', '_convert_status'])['count'].sum()
print("Conversion summary by medication:")
print(summary_analysis)

# 4. Check for problematic units
problematic_units = summary_df[summary_df['_convert_status'] != 'success']
print("\\nUnits requiring attention:")
print(problematic_units[['med_dose_unit', '_convert_status', 'count']])
```