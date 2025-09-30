# Basic Usage

This guide covers everything you need to work with CLIFpy effectively.

## 1. Getting Started

The fastest way to explore CLIFpy is with the built-in demo data:

```python
from clifpy import ClifOrchestrator

# Demo data - fastest path
co = ClifOrchestrator(config_path="config/demo_data_config.yaml")
co.initialize(tables=['patient', 'labs', 'vitals'])
co.validate_all()

# Access data
patient_df = co.patient.df
```

## 2. Configuration

CLIFpy uses YAML configuration files to manage data locations and settings.

### 2.1 YAML File Structure

**Required fields:**
- `tables_path` (or `data_directory`): Path to data directory
- `filetype`: `'csv'` or `'parquet'`
- `timezone`: Timezone string (e.g., `'US/Eastern'`, `'UTC'`)

**Optional:**
- `output_directory`: Where to save validation reports

**Example config.yaml:**

```yaml
site: MY_HOSPITAL
tables_path: /path/to/clif/data
filetype: parquet
timezone: US/Eastern
output_directory: /path/to/output
```

### 2.2 Configuration Methods

CLIFpy provides three ways to configure your orchestrator, listed in priority order:

**Method 1: Using YAML file (Recommended)**

<!-- skip: next -->
```python
# Explicit path
co = ClifOrchestrator(config_path='path/to/config.yaml')

# Auto-detection (looks for config.yaml or config.yml in current directory)
co = ClifOrchestrator()
```

**Method 2: Direct parameters**

<!-- skip: next -->
```python
co = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Eastern',
    output_directory='/path/to/output'
)
```

**Method 3: YAML + parameter overrides**

<!-- skip: next -->
```python
# Use config but override specific settings
co = ClifOrchestrator(
    config_path='config.yaml',
    timezone='UTC',  # Override config's timezone
    output_directory='/custom/output'  # Override output directory
)
```

**Note:** Parameters always override config file values when both are provided.

## 3. Core Workflow

### 3.1 Loading Tables with the Orchestrator

**Minimal:**

```python
co.initialize(tables=['patient', 'labs'])
```

**Practical (memory-efficient, what you'll actually use):**

```python
co.initialize(
    tables=['patient', 'labs', 'vitals'],
    sample_size=1000,  # Load subset for testing
    columns={
        'labs': ['hospitalization_id', 'lab_result_dttm', 'lab_value', 'lab_category'],
        'vitals': ['hospitalization_id', 'recorded_dttm', 'vital_value', 'vital_category']
    },
    filters={
        'labs': {'lab_category': ['hemoglobin', 'sodium', 'creatinine']},
        'vitals': {'vital_category': ['heart_rate', 'sbp', 'spo2']}
    }
)
```

### 3.2 Loading Individual Tables

**When to use individual tables instead of orchestrator:**
- Single table workflows
- Custom loading logic per table
- Pipeline integration where you control each step

<!-- skip: next -->
```python
from clifpy.tables import Labs

# Minimal
labs = Labs.from_file(config_path='config.yaml')

# Practical (with filters)
labs = Labs.from_file(
    config_path='config.yaml',
    columns=['hospitalization_id', 'lab_result_dttm', 'lab_value', 'lab_category'],
    filters={'lab_category': ['lactate', 'creatinine']},
    sample_size=1000
)

labs.validate()
```

*For most workflows, the orchestrator (Section 3.1) is recommended. See [Tables Guide](tables.md) for table-specific methods.*

### 3.3 Validation

```python
co.validate_all()

# Check results
for table_name in co.get_loaded_tables():
    table = getattr(co, table_name)
    if table.isvalid():
        print(f"✓ {table_name}")
    else:
        print(f"✗ {table_name}: {len(table.errors)} errors")
```

### 3.4 Accessing Data

```python
# Via orchestrator attributes
patient_df = co.patient.df
labs_df = co.labs.df
vitals_df = co.vitals.df

# List loaded tables
loaded = co.get_loaded_tables()

# Get table objects
tables = co.get_tables_obj_list()
```

## 4. Feature Tour

Each feature below shows minimal and practical examples using demo data.

### 4.1 Wide Datasets

Transform time-series data into wide format for machine learning. See [Wide Dataset Guide](wide-dataset.md) for details.

**Minimal:**

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs']
)
wide_df = co.wide_df
```

**Practical:**

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium', 'creatinine']
    },
    sample=True,  # 20 random patients - fast for testing
    batch_size=500,
    memory_limit='8GB',
    show_progress=True
)
```

### 4.2 SOFA Scores

Compute Sequential Organ Failure Assessment scores. See [SOFA Guide](sofa.md) for details.

**Minimal:**

<!-- skip: next -->
```python
sofa_df = co.compute_sofa_scores(id_name='hospitalization_id')
```

**Practical:**

<!-- skip: next -->
```python
# First: convert medication units (required for cardiovascular SOFA)
co.convert_dose_units_for_continuous_meds(
    preferred_units={
        'norepinephrine': 'mcg/kg/min',
        'epinephrine': 'mcg/kg/min',
        'dopamine': 'mcg/kg/min'
    }
)

# Then: compute SOFA
sofa_df = co.compute_sofa_scores(
    id_name='hospitalization_id',  # or 'encounter_block' after stitching
    extremal_type='worst',
    fill_na_scores_with_zero=True
)
```

## 5. Universal Patterns

### Memory Management

**Key parameters that reduce memory:**
- `sample_size`: Load subset of rows
- `columns`: Load only needed columns
- `filters`: Filter data during load
- `batch_size`: Process in chunks
- `sample=True`: Random 20 patients (for wide datasets)

**Example: Memory-efficient loading**

```python
co.initialize(
    tables=['labs'],
    sample_size=5000,
    columns={'labs': ['hospitalization_id', 'lab_result_dttm', 'lab_value']},
    filters={'labs': {'lab_category': ['lactate']}}
)
```

### Error Handling

```python
try:
    co.initialize(tables=['patient', 'labs'])
    co.validate_all()

    if not co.labs.isvalid():
        import pandas as pd
        error_df = pd.DataFrame(co.labs.errors)
        error_df.to_csv('lab_errors.csv')

except FileNotFoundError:
    print("Data files not found - check config.yaml")
except ValueError as e:
    print(f"Configuration error: {e}")
```

### Best Practices

1. **Use YAML configuration** for consistent settings across projects
2. **Load with filters** to reduce memory (especially for labs/vitals)
3. **Use sample_size** when prototyping (e.g., 1000 rows)
4. **Validate early** after loading to catch data issues
5. **Check resources** before wide datasets: `co.get_sys_resource_info()`
6. **Use sample=True** for testing wide dataset pipelines

## Next Steps

- [Explore specific table features](tables.md)
- [Create wide datasets for ML](wide-dataset.md)
- [Compute SOFA scores](sofa.md)
- [Handle outliers](outlier-handling.md)
- [Calculate comorbidity indices](comorbidity-index.md)
- [Review API reference](../api/index.md)
