# Encounter Stitching

The encounter stitching functionality identifies and groups hospitalizations that occur within a specified time window of each other, treating them as a single continuous encounter. This is particularly useful for handling cases where patients are discharged and quickly readmitted, such as transfers between the emergency department and inpatient units.

## Overview

In clinical data, what appears as separate hospitalizations may actually represent a single continuous episode of care. Common scenarios include:

- **ED to inpatient transfers** - Patient admitted through ED, then formally admitted to hospital
- **Inter-facility transfers** - Patient moved between hospitals within a health system
- **Brief discharges** - Patient discharged and readmitted within hours (e.g., for procedures)
- **Administrative separations** - Billing or administrative reasons create multiple records

The encounter stitching algorithm links these related hospitalizations using a configurable time window (default: 6 hours) between discharge and subsequent admission.

## How It Works

The stitching algorithm:

1. **Sorts hospitalizations** by patient and admission time
2. **Calculates gaps** between discharge and next admission for each patient
3. **Links encounters** when the gap is less than the specified time window
4. **Assigns encounter blocks** - a unique identifier grouping linked hospitalizations
5. **Updates tables in-place** - adds `encounter_block` column to both hospitalization and ADT tables

## Basic Usage

### Quick Start with Automatic Stitching

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Initialize orchestrator with stitching enabled
clif = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='UTC',
    stitch_encounter=True,          # Enable automatic stitching
    stitch_time_interval=6          # 6-hour window (default)
)

# Load tables - stitching happens automatically
clif.initialize(['hospitalization', 'adt'])

# Access the encounter mapping
mapping = clif.get_encounter_mapping()
print(f"Created {mapping['encounter_block'].nunique()} encounter blocks")
```

### Custom Time Windows

```python
# Use a 12-hour window for linking encounters
clif = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='UTC',
    stitch_encounter=True,
    stitch_time_interval=12  # 12-hour window
)

# Use a 2-hour window for stricter linking
clif_strict = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='UTC',
    stitch_encounter=True,
    stitch_time_interval=2   # 2-hour window
)
```

### Direct Function Usage

You can also use the stitching function directly without the orchestrator:

```python
from clifpy.utils.stitching_encounters import stitch_encounters

# Load your dataframes
hospitalization_df = pd.read_parquet('hospitalization.parquet')
adt_df = pd.read_parquet('adt.parquet')

# Perform stitching
hosp_stitched, adt_stitched, encounter_mapping = stitch_encounters(
    hospitalization=hospitalization_df,
    adt=adt_df,
    time_interval=12  # 12-hour window
)
```

## Parameters

### ClifOrchestrator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stitch_encounter` | bool | False | Enable automatic encounter stitching during initialization |
| `stitch_time_interval` | int | 6 | Hours between discharge and next admission to consider encounters linked |

### Direct Function Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `hospitalization` | pd.DataFrame | Required | Hospitalization table with required columns |
| `adt` | pd.DataFrame | Required | ADT table with required columns |
| `time_interval` | int | 6 | Hours between discharge and next admission to consider encounters linked |

## Required Data Columns

### Hospitalization Table
- `patient_id`
- `hospitalization_id`
- `admission_dttm`
- `discharge_dttm`
- `age_at_admission`
- `admission_type_category`
- `discharge_category`

### ADT Table
- `hospitalization_id`
- `in_dttm`
- `out_dttm`
- `location_category`
- `hospital_id`

## Output

When stitching is enabled, the process:

1. **Updates hospitalization table** - Adds `encounter_block` column
2. **Updates ADT table** - Adds `encounter_block` column
3. **Creates encounter mapping** - Available via `clif.get_encounter_mapping()`:
   - `hospitalization_id`: Original hospitalization identifier
   - `encounter_block`: Assigned encounter block number

## Understanding Encounter Blocks

Each encounter block represents a continuous episode of care:

```python
# Access the mapping after initialization
mapping = clif.get_encounter_mapping()

# Find multi-hospitalization encounters
multi_hosp = mapping.groupby('encounter_block').size()
multi_hosp_encounters = multi_hosp[multi_hosp > 1]

print(f"Encounters with multiple hospitalizations: {len(multi_hosp_encounters)}")

# Get details for a specific encounter block
block_1_hosps = mapping[mapping['encounter_block'] == 1]
print(f"Hospitalizations in encounter block 1: {block_1_hosps['hospitalization_id'].tolist()}")
```

## Practical Examples

### Calculate True Length of Stay

When encounters are stitched, you can calculate the true length of stay across linked hospitalizations:

```python
# Access stitched hospitalization data
stitched_df = clif.hospitalization.df

# Calculate encounter-level statistics
encounter_stats = stitched_df.groupby('encounter_block').agg({
    'admission_dttm': 'min',  # First admission
    'discharge_dttm': 'max',  # Last discharge
    'hospitalization_id': 'count',  # Number of linked hospitalizations
    'patient_id': 'first'
})

# Calculate total length of stay
encounter_stats['total_los_days'] = (
    (encounter_stats['discharge_dttm'] - encounter_stats['admission_dttm'])
    .dt.total_seconds() / 86400
)

print(encounter_stats[['patient_id', 'hospitalization_id', 'total_los_days']].head())
```

### Analyze ICU Stays Across Encounters

```python
# Access stitched ADT data
adt_stitched_df = clif.adt.df

# Find ICU stays by encounter
icu_by_encounter = adt_stitched_df[
    adt_stitched_df['location_category'] == 'icu'
].groupby('encounter_block').agg({
    'in_dttm': 'min',
    'out_dttm': 'max',
    'hospitalization_id': 'nunique'
})

print("ICU stays by encounter block:")
print(icu_by_encounter.head())
```

### Filter Data by Encounter Properties

```python
# Find encounters with ED to inpatient transfers
ed_admits = clif.adt.df[
    clif.adt.df['location_category'] == 'ed'
]['encounter_block'].unique()

inpatient_admits = clif.adt.df[
    clif.adt.df['location_category'].isin(['icu', 'ward'])
]['encounter_block'].unique()

ed_to_inpatient = set(ed_admits) & set(inpatient_admits)
print(f"Encounters with ED to inpatient transfer: {len(ed_to_inpatient)}")
```

### Compare Different Time Windows

```python
# Test effect of different time windows
windows = [3, 6, 12, 24]
results = []

for window in windows:
    clif_test = ClifOrchestrator(
        data_directory='/path/to/data',
        filetype='parquet',
        timezone='UTC',
        stitch_encounter=True,
        stitch_time_interval=window
    )
    clif_test.initialize(['hospitalization', 'adt'])
    
    mapping = clif_test.get_encounter_mapping()
    results.append({
        'window_hours': window,
        'total_encounters': mapping['encounter_block'].nunique(),
        'multi_hosp_encounters': (mapping.groupby('encounter_block').size() > 1).sum()
    })

results_df = pd.DataFrame(results)
print(results_df)
```

## Integration with Other Features

### Wide Dataset Creation

Stitched encounters are automatically used when creating wide datasets:

```python
# Initialize with stitching
clif = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='UTC',
    stitch_encounter=True
)

# Load tables (stitching happens automatically)
clif.initialize(['hospitalization', 'adt', 'labs', 'vitals'])

# Create wide dataset using stitched encounters
wide_df = clif.create_wide_dataset(
    start_time='admission_dttm',
    end_time='discharge_dttm',
    time_col='charttime'
)
```

### Validation

The stitched tables maintain compatibility with validation methods:

```python
# Validate all loaded tables (including stitched ones)
validation_results = clif.validate_all()

# Check specific tables
clif.hospitalization.validate()
clif.adt.validate()
```

## Best Practices

1. **Choose appropriate time windows**:
   - **2-4 hours**: Strict linking for direct transfers only
   - **6 hours** (default): Balances capturing related encounters while avoiding over-grouping
   - **12-24 hours**: Liberal definition, captures day surgery readmissions
   
2. **Validate stitching results**:
   ```python
   # Check for suspiciously large encounter blocks
   mapping = clif.get_encounter_mapping()
   hosp_counts = mapping.groupby('encounter_block').size()
   suspicious = hosp_counts[hosp_counts > 5]
   if len(suspicious) > 0:
       print(f"Review encounters with >5 hospitalizations: {suspicious.index.tolist()}")
   ```

3. **Consider your analysis goals**:
   - **Outcome studies**: Use stitched encounters to avoid counting transfers as readmissions
   - **Resource utilization**: May want to keep encounters separate for accurate billing
   - **Quality metrics**: Check if measure specifications require episode-based analysis

4. **Document your choices**:
   ```python
   # Save stitching parameters for reproducibility
   if clif.encounter_mapping is not None:
       stitching_info = {
           'time_interval_hours': clif.stitch_time_interval,
           'timestamp': pd.Timestamp.now(),
           'num_encounters_created': clif.encounter_mapping['encounter_block'].nunique(),
           'num_multi_hosp_encounters': (
               clif.encounter_mapping.groupby('encounter_block').size().gt(1).sum()
           )
       }
       # Save to file or include in analysis metadata
   ```

## Technical Details

### Algorithm Implementation

The stitching algorithm:

1. Filters required columns from hospitalization and ADT tables
2. Joins hospitalization and ADT data
3. Sorts by patient_id and admission_dttm
4. Calculates hours between discharge and next admission
5. Creates linked flag for gaps < time_interval
6. Iteratively propagates encounter_block IDs through linked chains
7. Updates original dataframes with encounter_block column

### Performance Considerations

- Stitching is performed in-memory using pandas operations
- Performance scales linearly with number of hospitalizations
- For datasets with >1M hospitalizations, ensure adequate RAM (8GB+ recommended)
- Processing time is typically seconds to minutes depending on data size

### Error Handling

The orchestrator handles common issues:

- **Missing tables**: Warns if hospitalization or ADT tables are not loaded
- **Missing columns**: Raises ValueError with specific missing columns listed
- **Processing errors**: Catches exceptions and reports them without failing initialization

### Limitations

- Currently only links hospitalizations for the same patient
- Does not consider clinical criteria (purely time-based)
- Requires both hospitalization and ADT tables to be present
- Does not link across different hospital systems (requires same patient_id)

## Troubleshooting

### Common Issues

**Issue**: "Encounter stitching requires both hospitalization and ADT tables to be loaded"
- **Solution**: Include both 'hospitalization' and 'adt' in your `initialize()` call

**Issue**: "Missing required columns in hospitalization DataFrame"
- **Solution**: Ensure your data contains all required columns listed above
- **Check**: Use `clif.hospitalization.df.columns` to see available columns

**Issue**: No encounters are being stitched despite close admissions
- **Check**: Verify datetime columns are properly parsed and in the same timezone
- **Check**: Ensure discharge_dttm is not null for hospitalizations you expect to link
- **Try**: Increase the time window to see if encounters get linked

### Debugging

```python
# Enable detailed output during initialization
clif = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='UTC',
    stitch_encounter=True,
    stitch_time_interval=6
)

# Check if stitching was attempted
clif.initialize(['hospitalization', 'adt'])

# Verify encounter_block was added
print("Hospitalization columns:", clif.hospitalization.df.columns.tolist())
print("Has encounter_block:", 'encounter_block' in clif.hospitalization.df.columns)

# Check mapping
if clif.encounter_mapping is not None:
    print(f"Mapping shape: {clif.encounter_mapping.shape}")
else:
    print("No encounter mapping created")
```

## See Also

- [ClifOrchestrator](../api/orchestrator.md) - Main interface for CLIF data operations
- [Hospitalization Table](../api/tables.md#hospitalization) - Structure of hospitalization data
- [ADT Table](../api/tables.md#adt-admission-discharge-transfer) - Structure of ADT data
- [Wide Dataset Creation](wide-dataset.md) - Creating analysis-ready datasets
- [Examples Notebook](https://github.com/clif-consortium/CLIFpy/blob/main/examples/stitching_encounters_demo.ipynb) - Interactive examples