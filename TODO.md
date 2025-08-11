# BaseTable Restructuring and Validation Enhancement Plan

## Overview
Restructure the pyCLIF codebase to use a BaseTable class with inheritance pattern. Each table will inherit from BaseTable, and validation functions will be enhanced with comprehensive data quality checks.

## Implementation Phases

### Phase 1: Convert JSON schemas to YAML format ✅
- [ ] Create new `src/pyclif/schemas/` directory
- [ ] Convert all 9 JSON schema files to YAML format:
  - [ ] ADTModel.json → adt_schema.yaml
  - [ ] HospitalizationModel.json → hospitalization_schema.yaml
  - [ ] LabsModel.json → labs_schema.yaml
  - [ ] Medication_admin_continuousModel.json → medication_admin_continuous_schema.yaml
  - [ ] PatientModel.json → patient_schema.yaml
  - [ ] Patient_assessmentsModel.json → patient_assessments_schema.yaml
  - [ ] PositionModel.json → position_schema.yaml
  - [ ] Respiratory_supportModel.json → respiratory_support_schema.yaml
  - [ ] VitalsModel.json → vitals_schema.yaml
- [ ] Preserve all schema information:
  - Table metadata (name, columns, data types)
  - Required columns
  - Category columns with permissible values
  - Special mappings (vital_ranges, lab_reference_units, etc.)
- [ ] Add composite keys in the yaml file for each table, and use "hospitalization_id", "recorded_dttm" and table specific category columns. 

### Phase 2: User inputs
- [ ] We expect the BaseTable class to accept four arguments- data_directory (required), filetype(required), timezone(required), and output_directory (optional- default set to a new output folder created in the root directory of the file)

### Phase 3: Create BaseTable class
- [ ] Create `src/pyclif/tables/base_table.py` with:
  - [ ] Constructor accepting the four arguments
  - [ ] YAML schema loading functionality
  - [ ] Common methods:
    - [ ] `from_file()` - Load data from data_directory and filetype argument
    - [ ] `validate()` - Run all validations (explained in pahse 4)
    - [ ] `isvalid()` - Check validation status
  - [ ] Integration with enhanced validator functions
  - [ ] Structured logging for warnings and progress (save in output_directory)
  - [ ] Error handling with try/except blocks

### Phase 4: Enhance validator.py with comprehensive checks

#### Column Validation Functions
- [ ] `check_required_columns(df, column_names, table_name)`
  - Validates required columns are present
  - Returns missing columns list
- [ ] `verify_column_dtypes(df, schema)`
  - Ensures columns have correct data types per schema
  - Special handling for datetime columns
- [ ] `validate_datetime_timezone(df, datetime_columns)`
  - Validates all *_dttm fields are in the 'UTC' format 

#### Missing Data Analysis
- [ ] `calculate_missing_stats(df, format='long')`
  - Reports count and percentage of missing values
  - Supports both long and wide format data
- [ ] `report_missing_data_summary(df)`
  - Generates comprehensive missing data report

#### Category and Value Validation
- [ ] `validate_categorical_values(df, schema)`
  - Checks values against permitted categories
  - Reports invalid values with counts
- [ ] `check_for_duplicates(df, composite_keys)`
  - Validates uniqueness constraints on composite keys. For now, add composite keys in the yaml file, and use "hospitalization_id", "recorded_dttm" and table specific category columns. 

#### Statistical Analysis
- [ ] `generate_summary_statistics(df, numeric_columns)`
  - Calculates Q1, Q3, median for numeric columns. Save as .csv
- [ ] `analyze_skewed_distributions(df)`
  - Identifies and reports skewed variables. Save as .csv

#### Unit Validation
- [ ] `validate_units(df, unit_mappings, table_name)`
  - Verifies units match schema wherever available in the schema
  - Critical for labs and vitals
  - Table-specific rules

#### Cohort Analysis
- [ ] `calculate_cohort_sizes(df, id_columns)`
  - Distinct counts of ID columns
  - Supports patient_id, hospitalization_id, etc.
- [ ] `get_distinct_counts(df, columns)`
  - General distinct count function

#### Error Handling
- [ ] All functions include try/except blocks
- [ ] Continue execution on failures
- [ ] Return structured error dictionaries
- [ ] Generate warning logs for failures and progress and save in the output_directory path provided by the user. All output and log files are saved *_<table_name>.csv for easy post-processing. 

### Phase 5: Refactor existing table classes
- [ ] Modify all 9 table classes to inherit from BaseTable:
  - [ ] `adt.py` → `class adt(BaseTable)`
  - [ ] `hospitalization.py` → `class hospitalization(BaseTable)`
  - [ ] `labs.py` → `class labs(BaseTable)`
  - [ ] `medication_admin_continuous.py` → `class medication_admin_continuous(BaseTable)`
  - [ ] `patient.py` → `class patient(BaseTable)`
  - [ ] `patient_assessments.py` → `class patient_assessments(BaseTable)`
  - [ ] `position.py` → `class position(BaseTable)`
  - [ ] `respiratory_support.py` → `class respiratory_support(BaseTable)`
  - [ ] `vitals.py` → `class vitals(BaseTable)`
- [ ] Remove duplicate code from each class
- [ ] Keep only table-specific methods
- [ ] Override validation methods where needed for special rules

### Phase 6: Update CLIF main class
- [ ] Update `src/pyclif/clif.py` to:
  - [ ] Load config and pass to table constructors
  - [ ] Support new validation workflow
  - [ ] Maintain backward compatibility


### Phase 7: Documentation and Examples
- [ ] All functions, methods, and classes should have detailed doc strings
- [ ] Create ONE new example notebooks with the example directory to test the updated code. 
