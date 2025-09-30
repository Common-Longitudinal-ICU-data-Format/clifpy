# Quick Start

This guide will get you up and running with CLIFpy in just a few minutes.

## Loading Demo Data

CLIFpy includes demo data to help you get started:

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Load all demo tables from the packaged configuration
co = ClifOrchestrator(config_path="config/demo_data_config.yaml")
co.initialize(tables=["patient", "labs", "vitals"])

# Access individual tables via the orchestrator
patient_df = co.patient.df
labs_df = co.labs.df
vitals_df = co.vitals.df
```

Need the convenience wrapper? ``from clifpy.data import load_demo_clif`` still
returns an orchestrator, but the configuration-based pattern above mirrors the
workflow you'll use with your own data.

## Using Individual Tables

### Loading a Single Table

```python
from clifpy.tables import Patient

# Load patient data using configuration (recommended)
patient = Patient.from_file(config_path='config/demo_data_config.yaml')

# Validate the data
patient.validate()

# Check if data is valid
if patient.isvalid():
    print("Data validation passed!")
else:
    print(f"Found {len(patient.errors)} validation errors")
```

## Using the Orchestrator

For working with multiple tables at once:

```python
from clifpy.clif_orchestrator import ClifOrchestrator

# Initialize orchestrator using configuration
orchestrator = ClifOrchestrator(config_path='config/demo_data_config.yaml')

# Load multiple tables
orchestrator.initialize(
    tables=['patient', 'labs', 'vitals', 'adt'],
    sample_size=1000  # Optional: load sample for testing
)

# Validate all tables
orchestrator.validate_all()

# Get summary of loaded tables
loaded = orchestrator.get_loaded_tables()
print(f"Loaded tables: {loaded}")
```



## Next Steps

- [Learn about basic usage patterns](basic-usage.md)
- [Explore the full User Guide](../user-guide/index.md)
- [Read the API documentation](../api/index.md)