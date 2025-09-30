# BaseTable class for pyCLIF tables.

This module provides the base class that all pyCLIF table classes inherit from.
It handles common functionality including data loading, validation, and reporting.

## Overview

The `BaseTable` class is the foundational component of all table classes in clifpy. It implements the common functionality that every CLIF table needs, following an inheritance pattern where specific tables (patient, hospitalization, adt, etc.) inherit from it.

## Design Pattern

BaseTable implements the **Template Method Pattern** where:
- **Common behavior** is defined in the base class
- **Specific behavior** is implemented in child classes  
- **Extensibility** is provided through method overriding

## Core Responsibilities

### 1. Data Management
```python
# Stores the actual data
self.df: pd.DataFrame = None

# Configuration
self.data_directory: str    # Path to data files
self.filetype: str         # File format (parquet, csv, etc.)
self.timezone: str         # Timezone for datetime conversions
self.output_directory: str # Where to save validation outputs
```

### 2. Schema Management
```python
# Automatically loads YAML schema based on table name
self.schema: Dict = None  # Loaded from clifpy/schemas/{table_name}_schema.yaml
```

### 3. Validation System
```python
self.errors: List[Dict] = []  # Stores validation errors and warnings
```

### 4. Logging System  
```python
self.logger: logging.Logger  # Per-table logger that writes to output directory
```

## Key Methods

### Constructor (`__init__`)

```python
def __init__(
    self,
    data_directory: str,
    filetype: str, 
    timezone: str,
    output_directory: Optional[str] = None,
    data: Optional[pd.DataFrame] = None
)
```

**Parameters:**
- `data_directory`: Path to directory containing data files
- `filetype`: File format ("parquet", "csv", etc.)
- `timezone`: Timezone for datetime columns (default: "UTC") 
- `output_directory`: Where to save validation outputs (optional)
- `data`: Pre-loaded DataFrame (optional)

**What it does:**
1. Stores configuration parameters
2. Sets up output directory (creates if doesn't exist)
3. Determines table name from class name
4. Sets up logging system
5. Loads YAML schema for the table
6. If data provided, automatically runs validation

### Class Method: `from_file()`

```python
@classmethod
def from_file(
    cls,
    data_directory: str,
    filetype: str,
    timezone: str = "UTC", 
    output_directory: Optional[str] = None,
    sample_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None
)
```

Alternative constructor that loads data from files with additional options:
- `sample_size`: Limit number of rows to load
- `columns`: Only load specific columns
- `filters`: Apply filters during loading

**Example:**
```python
patient_table = patient.from_file(
    data_directory="/path/to/data",
    filetype="parquet",
    timezone="US/Eastern",
    sample_size=1000  # Load only first 1000 rows
)
```

### Validation Methods

#### `validate()`
Runs comprehensive validation on the loaded data:
- Schema validation (required columns, data types, categories)
- Enhanced validation (missing data, duplicates, statistics)  
- Table-specific validation (can be overridden by child classes)

#### `isvalid() -> bool`
Returns `True` if no errors were found in the last validation run.

```python
if table.isvalid():
    print("✅ Data passed all validations!")
else:
    print(f"❌ Found {len(table.errors)} validation issues")
```

## How Tables Inherit from BaseTable

### Basic Inheritance Pattern
```python
class patient(BaseTable):
    """Patient table with demographic information."""
    
    def __init__(self, data_directory: str = None, filetype: str = None, 
                 timezone: str = "UTC", output_directory: Optional[str] = None,
                 data: Optional[pd.DataFrame] = None):
        # Handle backward compatibility
        if data_directory is None and filetype is None and data is not None:
            data_directory = "."
            filetype = "parquet" 
            
        # Call parent constructor
        super().__init__(
            data_directory=data_directory,
            filetype=filetype, 
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
    
    # Add patient-specific methods
    def get_demographics_summary(self):
        """Return demographic breakdown of patients."""
        # Implementation here
        pass
```

### Table-Specific Methods

Child classes can add methods specific to their domain:

```python
# hospitalization.py
class hospitalization(BaseTable):
    def get_mortality_rate(self) -> float:
        """Calculate in-hospital mortality rate."""
        if 'discharge_category' not in self.df.columns:
            return 0.0
        total = len(self.df)
        expired = len(self.df[self.df['discharge_category'] == 'Expired'])
        return (expired / total) * 100 if total > 0 else 0.0
        
    def calculate_length_of_stay(self) -> pd.DataFrame:
        """Calculate length of stay for each hospitalization."""
        # Implementation here
        pass

```

## Validation Flow

When you create a table instance, BaseTable automatically:

1. **Schema Loading**: Reads `{table_name}_schema.yaml` from the schemas directory
2. **Logging Setup**: Creates log files in the output directory
3. **Data Validation** (if data provided):
   - ✅ **Required columns** - Ensures all mandatory columns are present
   - ✅ **Data types** - Validates column data types match schema
   - ✅ **Categorical values** - Checks values against permitted categories
   - ✅ **Datetime timezones** - Validates timezone-aware datetime columns
   - ✅ **Missing data analysis** - Calculates missing data statistics
   - ✅ **Duplicate detection** - Checks composite keys for uniqueness
   - ✅ **Statistical analysis** - Generates summaries and skewness analysis
   - ✅ **Unit validation** - For tables like vitals/labs, validates measurement units
   - ✅ **Numeric ranges** - Checks values fall within expected clinical ranges

## Output Files Generated

BaseTable creates several files during validation in the output directory:

| File Type | Example | Purpose |
|-----------|---------|---------|
| **Log files** | `validation_log_patient.log` | Detailed validation logs with timestamps |
| **Missing data** | `missing_data_stats_patient.csv` | Missing value counts and percentages |
| **Statistics** | `summary_statistics_patient.csv` | Q1, Q3, median for numeric columns |
| **Skewness** | `skewness_analysis_patient.csv` | Distribution analysis for numeric columns |
| **Validation errors** | `validation_errors_patient.csv` | Summary of all validation issues |

## Usage Examples

### Method 1: Direct Instantiation with Data
```python
# When you already have a DataFrame
patient_table = patient(
    data_directory="./data",      # Required for schema/logging
    filetype="parquet",           # Required for metadata
    timezone="UTC",               # Timezone for datetime columns  
    output_directory="./output",  # Where to save validation files
    data=my_dataframe            # Your pre-loaded DataFrame
)
```

### Method 2: Load from File
```python
# Load data from files
patient_table = patient.from_file(
    data_directory="./data",
    filetype="parquet", 
    timezone="US/Eastern",
    columns=['patient_id', 'age_at_admission', 'sex_category'],  # Only load specific columns
    sample_size=5000  # Only load first 5000 rows
)
```

### Method 3: Demo Data (Recommended for Learning)
```python
# Use built-in demo datasets via configuration
from clifpy.tables import Patient

patient_table = Patient.from_file(config_path='config/demo_data_config.yaml')
```

### All Methods Result in Same Capabilities:
```python
# Check validation status
print(f"Valid: {patient_table.isvalid()}")
print(f"Errors: {len(patient_table.errors)}")  
print(f"Records: {len(patient_table.df)}")

# Access the data
df = patient_table.df
print(df.head())

# Use table-specific methods (if implemented)
if hasattr(patient_table, 'get_demographics_summary'):
    demographics = patient_table.get_demographics_summary()
```

## Benefits of This Design

### 1. Code Reuse
- All tables get validation, logging, and schema loading automatically
- No duplicate code across table implementations
- Consistent behavior across all table types

### 2. Consistency  
- Same API across all table types: `validate()`, `isvalid()`, `from_file()`
- Standardized output file formats and naming conventions
- Uniform error handling and logging

### 3. Extensibility
- Easy to add new tables by inheriting from BaseTable
- Can override specific methods for table-specific behavior
- Template method pattern allows customization while preserving structure

### 4. Separation of Concerns
- **BaseTable**: Infrastructure (validation, logging, I/O, schema management)
- **Child classes**: Domain-specific methods and business logic
- **Validator module**: Reusable validation functions
- **Schema files**: Data structure definitions

### 5. Maintainability
- Changes to validation logic automatically apply to all tables
- Schema changes are managed in separate YAML files  
- Logging and error handling centralized in one place

## Advanced Features

### Custom Validation
Tables can override validation methods for specific requirements:

```python
class vitals(BaseTable):
    def _run_table_specific_validations(self):
        """Add vitals-specific validation rules."""
        super()._run_table_specific_validations()
        
        # Custom validation for vital signs ranges
        if 'vital_category' in self.df.columns and 'vital_value' in self.df.columns:
            # Check for physiologically impossible values
            extreme_values = self.df[
                (self.df['vital_category'] == 'heart_rate') & 
                ((self.df['vital_value'] < 0) | (self.df['vital_value'] > 300))
            ]
            if not extreme_values.empty:
                self.errors.append({
                    "type": "extreme_vital_values",
                    "message": f"Found {len(extreme_values)} extreme heart rate values",
                    "count": len(extreme_values)
                })
```

### Custom Output Methods
```python
class hospitalization(BaseTable):
    def save_mortality_report(self, filename: str = None):
        """Save detailed mortality analysis to file."""
        if filename is None:
            filename = os.path.join(self.output_directory, f'mortality_report_{self.table_name}.csv')
            
        mortality_data = self.analyze_mortality_by_demographics()
        mortality_data.to_csv(filename, index=False)
        self.logger.info(f"Saved mortality report to {filename}")
```

## Best Practices

### 1. When to Inherit from BaseTable
- ✅ **Always** for CLIF table implementations
- ✅ When you need validation and schema management
- ✅ For tables that will be used in production workflows

### 2. Method Naming Conventions
- Use descriptive names: `get_mortality_rate()` not `mortality()`
- Follow existing patterns: `filter_by_*()`, `get_*()`, `calculate_*()`
- Return appropriate types: DataFrames for subsets, numbers for metrics

### 3. Error Handling
```python
def custom_method(self):
    """Custom analysis method with proper error handling."""
    try:
        if self.df is None or self.df.empty:
            self.logger.warning("No data available for analysis")
            return None
            
        # Your analysis here
        result = self.df.groupby('category').mean()
        
        self.logger.info(f"Analysis completed successfully with {len(result)} groups")
        return result
        
    except Exception as e:
        self.logger.error(f"Analysis failed: {str(e)}")
        return None
```

## Future Enhancements

The BaseTable design supports future enhancements such as:

- **Caching**: Store validation results to avoid re-computation
- **Streaming**: Handle large datasets that don't fit in memory  
- **Parallel processing**: Run validation checks in parallel
- **Custom validators**: Plugin system for domain-specific validation rules
- **Data lineage**: Track data transformations and sources
- **Version control**: Schema versioning and migration support
