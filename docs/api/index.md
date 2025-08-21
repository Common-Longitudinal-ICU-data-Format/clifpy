# API Reference

This section contains the complete API documentation for CLIFpy, automatically generated from the source code docstrings.

## Core Components

### [ClifOrchestrator](orchestrator.md)
The main orchestration class for managing multiple CLIF tables with consistent configuration.

### [BaseTable](base-table.md)
The base class that all CLIF table implementations inherit from, providing common functionality for data loading, validation, and reporting.

## Table Classes

### [Tables Overview](tables.md)
Complete API documentation for all CLIF table implementations:

- **Patient** - Patient demographics and identification
- **Adt** - Admission, discharge, and transfer events  
- **Hospitalization** - Hospital stay information
- **Labs** - Laboratory test results
- **Vitals** - Vital signs measurements
- **RespiratorySupport** - Ventilation and oxygen therapy
- **MedicationAdminContinuous** - Continuous medication infusions
- **PatientAssessments** - Clinical assessment scores
- **Position** - Patient positioning data

## Utilities

### [Utility Functions](utilities.md)
Helper functions for data loading, validation, and I/O operations:

- **io** - Data loading utilities
- **validator** - Data validation functions

## Quick Links

- [ClifOrchestrator API](orchestrator.md) - Multi-table management
- [BaseTable API](base-table.md) - Common table functionality
- [Table Classes API](tables.md) - Individual table implementations
- [Utilities API](utilities.md) - Helper functions

## Usage Example

```python
from clifpy.clif_orchestrator import ClifOrchestrator
from clifpy.tables import Patient, Labs, Vitals

# Using the orchestrator
orchestrator = ClifOrchestrator(
    data_directory='/path/to/data',
    filetype='parquet',
    timezone='US/Central'
)
orchestrator.initialize(tables=['patient', 'labs', 'vitals'])

# Using individual tables
patient = Patient.from_file('/path/to/data', 'parquet')
patient.validate()
```