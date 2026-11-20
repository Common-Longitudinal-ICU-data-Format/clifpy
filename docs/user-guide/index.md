# User Guide

Welcome to the CLIFpy User Guide. CLIFpy makes working with CLIF (Common Longitudinal ICU data Format) data straightforward and efficient for researchers, data scientists, and clinicians analyzing ICU outcomes and building predictive models.

## Getting Started

| Guide | Description |
|-------|-------------|
| [Installation](installation.md) | Install CLIFpy and set up your environment |
| [Quickstart](quickstart.md) | Learn core workflows: loading data with ClifOrchestrator, configuration, validation, and key features |

## Core Features

| Guide | Description |
|-------|-------------|
| [Data Validation](validation.md) | Understand how CLIFpy validates data against CLIF schemas |
| [Outlier Handling](outlier-handling.md) | Detect and remove physiologically implausible values with configurable ranges |
| [Encounter Stitching](encounter-stitching.md) | Link related hospital encounters to create continuous patient care timelines |
| [Wide Dataset Creation](wide-dataset.md) | Create comprehensive time-series datasets by joining multiple CLIF tables with automatic pivoting |
| [Working with Timezones](timezones.md) | Best practices for handling timezone-aware datetime data |

## Advanced Features

| Guide | Description |
|-------|-------------|
| [Medication Unit Conversion](med-unit-conversion.md) | Standardize continuous and intermittent doses to mCIDE or your own target units, read the status breakdown, choose a patient-weight fallback chain, and see what clifpy guards against |
| [SOFA Score Computation](sofa.md) | Compute Sequential Organ Failure Assessment scores for sepsis identification |
| [CDC Adult Sepsis Event (ASE)](ase.md) | Identify Adult Sepsis Events using the CDC surveillance definition |
| [Respiratory Support Waterfall](waterfall.md) | Visualize patient trajectories and treatment timelines with customizable plots |
| [Comorbidity Index Computation](comorbidity-index.md) | Calculate Charlson and Elixhauser comorbidity indices from diagnosis data |
| [MDRO Flag Calculation](mdro-flags.md) | Calculate MDR, XDR, PDR, and DTR flags for multi-drug resistant organisms from susceptibility data |

## API Reference

Complete API reference: **[API Documentation](../api/index.md)**

## CLIF Tables Reference

This section provides a reference for all CLIF tables available in CLIFpy. For detailed field definitions, see the [CLIF Data Dictionary](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0).

| Table | Data Dictionary | API Reference | Orchestrator Methods | Table Methods |
|-------|----|----|---------------------|---------------|
| patient | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient) | [⚙️](../api/tables.md#patient) | - | - |
| hospitalization | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#hospitalization) | [⚙️](../api/tables.md#hospitalization) | - | - |
| adt | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#adt) | [⚙️](../api/tables.md#adt) | - | - |
| labs | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#labs) | [⚙️](../api/tables.md#labs) | - | - |
| vitals | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#vitals) | [⚙️](../api/tables.md#vitals) | - | - |
| medication_admin_continuous | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-continuous) | [⚙️](../api/tables.md#medication-admin-continuous) | `standardize_dose_units_for_continuous_meds()`, `convert_dose_units_for_continuous_meds()` | - |
| medication_admin_intermittent | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#medication-admin-intermittent) | [⚙️](../api/tables.md#medication-admin-intermittent) | `standardize_dose_units_for_intermittent_meds()`, `convert_dose_units_for_intermittent_meds()` | - |
| patient_assessments | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient-assessments) | [⚙️](../api/tables.md#patient-assessments) | - | - |
| respiratory_support | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#respiratory-support) | [⚙️](../api/tables.md#respiratory-support) | - | - |
| position | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#position) | [⚙️](../api/tables.md#position) | - | - |
| hospital_diagnosis | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#hospital-diagnosis) | [⚙️](../api/tables.md#hospital-diagnosis) | - | - |
| microbiology_culture | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-culture) | [⚙️](../api/tables.md#microbiology-culture) | - | - |
| crrt_therapy | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#crrt-therapy) | [⚙️](../api/tables.md#crrt-therapy) | - | - |
| patient_procedures | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#patient-procedures) | [⚙️](../api/tables.md#patient-procedures) | - | - |
| microbiology_susceptibility | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-susceptibility) | [⚙️](../api/tables.md#microbiology-susceptibility) | - | - |
| ecmo_mcs | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#ecmo-mcs) | [⚙️](../api/tables.md#ecmo-mcs) | - | - |
| microbiology_nonculture | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#microbiology-nonculture) | [⚙️](../api/tables.md#microbiology-nonculture) | - | - |
| code_status | [📖](https://clif-icu.com/data-dictionary/data-dictionary-2.1.0#code-status) | [⚙️](../api/tables.md#code-status) | - | - |

**Legend:**

- 📖 Links to CLIF Data Dictionary for field specifications

- ⚙️ Links to CLIFpy API Reference for class documentation

- **Orchestrator Methods**: Table-specific methods callable from `ClifOrchestrator` instance

- **Table Methods**: Table-specific class methods
