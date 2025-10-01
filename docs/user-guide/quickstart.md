# Quickstart

This tutorial will get you up to speed with the core functionalities of `CLIFpy`.

<details>
<summary> The built-in demo data </summary>
Here and throughout the user guide, we use the built-in CLIF-MIMIC demo data, but any interfaces presented would be exactly the same as when you use your own site's data.
</details>

## Core workflow

The easiest way to use `CLIFpy` is via the CLIF orchestrator, which allows you to seamlessly load and process multiple CLIF tables at the same time through an unified interface. 

### Initialize a CLIF orchestrator
To initialize a clif orchestrator, share with it your usual CLIF configurations, either via a `.yaml` config file (details below) or by directly populating the parameters: 

```python
from clifpy import ClifOrchestrator

# approach 1 (recommended) - provide the path to your CLIF config file
co = ClifOrchestrator(config_path="config/demo_data_config.yaml")

# approach 2 - populate all the parameters
co = ClifOrchestrator(
    data_directory='clifpy/data/clif_demo',
    filetype='parquet',
    timezone='US/Eastern',
    output_directory='output/demo'
)
```

The config file, conventionally stored at `<PROJECT_ROOT>/config/config.yaml`, should look like:
```yaml
data_directory: /path/to/where/you/store/your/clif/tables
filetype: parquet
timezone: US/Central
output_directory: /path/to/where/you/want/to/store/any/clifpy/outputs
```

The parameters always override config file settings if both are provided:

<!-- skip: next -->

```python
# approach 3 - use the config file but override specific settings
co = ClifOrchestrator(
    config_path="config/demo_data_config.yaml",
    timezone='UTC',  # Override the timezone setting of 'US/Central' in the config file
)
```


### Load data

To load data from multiple CLIF tables at once, call the `.initialize` method from the orchestrator instance. To be memory-efficient, specify and only load the columns and rows you need:

```python
required_labs_columns = ['hospitalization_id', 'lab_result_dttm', 'lab_value', 'lab_category']
required_vitals_columns = ['hospitalization_id', 'recorded_dttm', 'vital_value', 'vital_category']
required_labs_filters = {'lab_category': ['hemoglobin', 'sodium', 'creatinine']}
required_vitals_filters = {'vital_category': ['heart_rate', 'sbp', 'spo2']}

co.initialize(
    tables=['patient', 'labs', 'vitals'],
    # sample_size=1000, 
    columns={
        'labs': required_labs_columns,
        'vitals': required_vitals_columns
    },
    filters={
        'labs': required_labs_filters,
        'vitals': required_vitals_filters
    }
)
```

```python
>>> co.get_loaded_tables() # list the names of loaded tables
['patient', 'labs', 'vitals']
```

If no `columns` or `filters` are specified, the entire tables would be loaded.


<details>
<summary> Loading without the orchestrator </summary>
Alternatively (and more verbosely), you can load data individually from their table-specific classes:

```python
from clifpy import Patient, Labs, Vitals

patient_table = Patient.from_file(config_path='config/demo_data_config.yaml')

labs_table = Labs.from_file(
    config_path='config/demo_data_config.yaml',
    columns=required_labs_columns,
    filters=required_labs_filters
)

vitals_table = Vitals.from_file(
    config_path='config/demo_data_config.yaml',
    columns=required_vitals_columns,
    filters=required_vitals_filters
)
```

In most cases, using the orchestrator is recommended since it provides an unified interface where the individually loaded `patient_table` object, e.g., can be accessed from the orchestrator at `co.patients`:

```python
>>> isinstance(co.patient, Patient)
True

>>> isinstance(co.labs, Labs)
True
```
</details>

Once loaded, the data would be available as `pd.DataFrame` at the `.df` attribute of their corresponding table object:

```python
>>> import pandas as pd
>>> isinstance(co.patient.df, pd.DataFrame)
True
>>> isinstance(co.labs.df, pd.DataFrame)
True
```

### Validate data quality

Through the orchestrator, you can batch validate if your CLIF tables are in line with the schema:

<!-- skip: next -->
```python
>>> co.validate_all()
Validating 3 table(s)...

Validating patient...
Validation completed with 5 error(s). See `errors` attribute.

Validating labs...
Validation completed with 3 error(s). See `errors` attribute.

Validating vitals...
Validation completed with 4 error(s). See `errors` attribute.
```

<details>
<summary> Validating tables individually </summary>
This is equivalent to running each of the following individually: 

```python
>>> co.patient.validate()
Validation completed with 5 error(s). See `errors` attribute.

>>> co.labs.validate()
Validation completed with 3 error(s). See `errors` attribute.

>>> co.vitals.validate()
Validation completed with 4 error(s). See `errors` attribute.
```

</details>

The validation failures for the `labs` table, for example, can be viewed at:

<!-- skip: next -->
```python
>>> co.labs.errors
[
  {
    "type": "datetime_timezone",
    "column": "lab_result_dttm",
    "timezone": "US/Eastern",
    "expected": "UTC",
    "status": "warning",
    "message": "Column 'lab_result_dttm' has timezone 'US/Eastern' but expected 'UTC'"
  },
  {
    "type": "missing_categorical_values",
    "column": "lab_category",
    "missing_values": [
      "basophils_percent",
      "chloride",
      "so2_central_venous",
    ],
    "total_missing": 3,
    "message": "Column 'lab_category' is missing 3 expected category values"
  }
]
```

## Features Tour

### Wide Datasets

Transform time-series data into wide format. See [Wide Dataset Guide](wide-dataset.md) for details.

```python
co.create_wide_dataset(
    tables_to_load=['vitals', 'labs'],
    category_filters={
        'vitals': ['heart_rate', 'sbp', 'spo2'],
        'labs': ['hemoglobin', 'sodium', 'creatinine']
    },
    sample=True,  # 20 random patients
    # batch_size=500,
    # memory_limit='8GB',
    show_progress=True
)
```

## Recommended Practices

- **Use YAML configuration** for consistent settings across projects
- **Load with filters** to reduce memory load
- **Use sample_size** when prototyping (e.g., 1000 rows)
- **Validate early** after loading to catch data issues
