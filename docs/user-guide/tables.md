# CLIF Tables Overview

This page provides a concise overview of all CLIF tables. For detailed field definitions, see the [CLIF Data Dictionary](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0).

## Core Tables

### Patient
Core demographic information including birth date, sex, race, ethnicity, and language. This is the primary table that links `patient_id` field to the `hospitalization_id` field in the Hospitalization table.

- **Data Dictionary**: [Patient Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient)
- **API Reference**: [Patient API](../api/tables.md#patient)

### Hospitalization
Contains information about each hospital admission including admission/discharge times, disposition, and care unit details.

- **Data Dictionary**: [Hospitalization Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#hospitalization)
- **API Reference**: [Hospitalization API](../api/tables.md#hospitalization)

### ADT (Admission, Discharge, Transfer)
Tracks patient movement throughout their hospitalization with location categories and timestamps.

- **Data Dictionary**: [ADT Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#adt)
- **API Reference**: [ADT API](../api/tables.md#adt)

## Clinical Data Tables

### Vitals
Physiological measurements including heart rate, blood pressure, temperature, respiratory rate, and oxygen saturation.

- **Data Dictionary**: [Vitals Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#vitals)
- **API Reference**: [Vitals API](../api/tables.md#vitals)

### Labs
Laboratory test results with standardized categories and units for common ICU tests.

- **Data Dictionary**: [Labs Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#labs)
- **API Reference**: [Labs API](../api/tables.md#labs)

### Respiratory Support
Detailed ventilation parameters and respiratory support device information.

- **Data Dictionary**: [Respiratory Support Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#respiratory-support)
- **API Reference**: [Respiratory Support API](../api/tables.md#respiratory-support)

## Medication Tables

### Medication Administration - Continuous
Continuous infusions with rates and units, typically for vasoactive drugs and sedatives.

- **Data Dictionary**: [Medication Admin Continuous Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-continuous)
- **API Reference**: [Medication Admin Continuous API](../api/tables.md#medication-admin-continuous)

### Medication Administration - Intermittent
Discrete medication doses including antibiotics, analgesics, and other scheduled medications.

- **Data Dictionary**: [Medication Admin Intermittent Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-intermittent)
- **API Reference**: [Medication Admin Intermittent API](../api/tables.md#medication-admin-intermittent)

## Assessment Tables

### Patient Assessments
Clinical assessments and scores including pain scales, sedation scores, and delirium assessments.

- **Data Dictionary**: [Patient Assessments Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient-assessments)
- **API Reference**: [Patient Assessments API](../api/tables.md#patient-assessments)

### Position
Patient positioning data crucial for prone positioning protocols and pressure ulcer prevention.

- **Data Dictionary**: [Position Table Specification](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#position)
- **API Reference**: [Position API](../api/tables.md#position)

## Usage Example

```python
from clifpy import ClifOrchestrator

# Load all tables
orchestrator = ClifOrchestrator(
    data_directory='/path/to/clif/data',
    timezone='US/Eastern'
)

# Access individual tables
patient_df = orchestrator.patient.df
vitals_df = orchestrator.vitals.df
labs_df = orchestrator.labs.df

# Join tables for analysis
patient_vitals = vitals_df.merge(
    orchestrator.hospitalization.df[['hospitalization_id', 'patient_id']],
    on='hospitalization_id'
).merge(
    patient_df[['patient_id', 'birth_date', 'sex_category']],
    on='patient_id'
)
```

## Next Steps

- Review the [Data Validation Guide](validation.md) to ensure data quality
- Explore [Advanced Features](../user-guide/index.md#advanced-features) for complex analyses
- Check the [API Reference](../api/index.md) for detailed method documentation