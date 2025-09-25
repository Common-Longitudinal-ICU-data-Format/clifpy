# Adding New Tables to CLIFpy

This guide provides step-by-step instructions for developers who want to add new CLIF tables to the CLIFpy package.

## Overview

CLIFpy uses an object-oriented architecture where all table classes inherit from `BaseTable`. This provides consistent functionality for data loading, validation, and reporting across all tables.

## Architecture

```
clifpy/
├── tables/
│   ├── __init__.py          # Table exports
│   ├── base_table.py        # Base class all tables inherit from
│   └── your_table.py        # Your new table class
├── schemas/
│   └── your_table_schema.yaml  # YAML schema definition
├── clif_orchestrator.py     # Central table registry
└── __init__.py              # Package-level exports
```

## Step-by-Step Guide

### 1. Create the Schema File

First, create a YAML schema file in `clifpy/schemas/your_table_schema.yaml`:

```yaml
table_name: your_table
composite_keys:
  - hospitalization_id
  - your_primary_key_field

columns:
  - name: hospitalization_id
    data_type: VARCHAR
    required: true
    is_category_column: false
    is_group_column: false
  - name: your_datetime_field
    data_type: DATETIME
    required: true
    is_category_column: false
    is_group_column: false
  - name: your_category_field
    data_type: VARCHAR
    required: true
    is_category_column: true
    is_group_column: false
    permissible_values:
      - value1
      - value2
      - value3
  - name: your_numeric_field
    data_type: DOUBLE
    required: false
    is_category_column: false
    is_group_column: false

required_columns:
  - hospitalization_id
  - your_datetime_field
  - your_category_field

category_columns:
  - your_category_field

group_columns: []
```

#### Schema Field Definitions:
- **table_name**: Snake_case name matching your table (e.g., `medication_admin_intermittent`)
- **composite_keys**: Fields that together form unique records
- **columns**: List of all columns with their properties:
  - `name`: Column name in the data
  - `data_type`: SQL-like type (VARCHAR, DATETIME, DOUBLE, INT, etc.)
  - `required`: Whether the column must be present
  - `is_category_column`: True for categorical fields with limited values
  - `is_group_column`: True for grouping fields
  - `permissible_values`: List of allowed values for categorical columns
- **required_columns**: Columns that must exist in the data
- **category_columns**: List of categorical columns
- **group_columns**: List of grouping columns

### 2. Create the Table Class

Create your table class in `clifpy/tables/your_table.py`:

#### Minimal Implementation

For tables without special validation requirements:

```python
from .base_table import BaseTable


class YourTable(BaseTable):
    """
    Your table description here.
    
    This class handles your_table-specific data and validations
    while leveraging the common functionality provided by BaseTable.
    """
    pass
```

#### Advanced Implementation

For tables with custom validation or methods:

```python
from typing import Optional, List, Dict
import pandas as pd
from .base_table import BaseTable


class YourTable(BaseTable):
    """
    Your table description here.
    
    This class handles your_table-specific data and validations including
    any special validation logic required.
    """
    
    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the your_table table.
        
        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        # Initialize any table-specific attributes
        self.custom_validation_errors: List[dict] = []
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
    
    def _run_table_specific_validations(self):
        """
        Run your_table-specific validations.
        
        This overrides the base class method to add table-specific validation.
        """
        # Run custom validation
        self.validate_custom_rules()
    
    def validate_custom_rules(self):
        """Implement your custom validation logic."""
        self.custom_validation_errors = []
        
        if self.df is None:
            return
        
        # Example: Check for specific conditions
        # Add validation logic here
        
        # Add errors to main errors list
        if self.custom_validation_errors:
            self.errors.extend(self.custom_validation_errors)
            self.logger.warning(f"Found {len(self.custom_validation_errors)} custom validation errors")
    
    def isvalid(self) -> bool:
        """Return True if the last validation finished without errors."""
        return not self.errors and not self.custom_validation_errors
    
    # Add table-specific methods
    def get_summary_by_category(self) -> pd.DataFrame:
        """Return summary statistics grouped by your_category_field."""
        if self.df is None or 'your_category_field' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df.groupby('your_category_field').agg({
            'your_numeric_field': ['count', 'mean', 'std', 'min', 'max']
        }).round(2)
```

### 3. Register the Table

#### Update `clifpy/tables/__init__.py`

Add your table import and export:

```python
from .patient import Patient
from .adt import Adt
# ... other imports ...
from .your_table import YourTable  # Add this line

__all__ = [
    'Patient',
    'Adt',
    # ... other tables ...
    'YourTable',  # Add this line
]
```

#### Update `clifpy/__init__.py`

Add your table to the package-level exports:

```python
from .tables import (
    Patient,
    Adt,
    # ... other tables ...
    YourTable,  # Add this line
)

__all__ = [
    # ... existing exports ...
    "YourTable",  # Add this line
]
```

#### Update `clifpy/clif_orchestrator.py`

1. Import your table class:
```python
from .tables.your_table import YourTable
```

2. Add to TABLE_CLASSES dictionary:
```python
TABLE_CLASSES = {
    'patient': Patient,
    # ... other tables ...
    'your_table': YourTable,  # Add this line
}
```

3. Add as an attribute in the class docstring:
```python
"""
Attributes
----------
# ... existing attributes ...
your_table : YourTable
    Your table description
"""
```

4. Add to `__init__` method:
```python
def __init__(self, ...):
    # ... existing code ...
    self.your_table: YourTable = None
```

5. Update the `load_table` return type hint:
```python
def load_table(
    self,
    table_name: str,
    # ... parameters ...
) -> Union[Patient, Hospitalization, ..., YourTable]:  # Add YourTable
```

### 4. Create Tests

Create a test file `tests/tables/test_your_table.py`:

```python
"""
Tests for the your_table module.
"""
import pytest
import pandas as pd
from clifpy.tables.your_table import YourTable


class TestYourTable:
    """Test suite for YourTable class."""
    
    @pytest.fixture
    def sample_valid_data(self):
        """Create valid test data."""
        return pd.DataFrame({
            'hospitalization_id': ['H001', 'H001', 'H002'],
            'your_datetime_field': pd.to_datetime([
                '2023-01-01 10:00',
                '2023-01-01 11:00',
                '2023-01-02 09:00'
            ]),
            'your_category_field': ['value1', 'value2', 'value1'],
            'your_numeric_field': [10.5, 20.3, 15.7]
        })
    
    def test_initialization(self, tmp_path):
        """Test YourTable initialization."""
        table = YourTable(
            data_directory=str(tmp_path),
            filetype='csv',
            timezone='UTC'
        )
        assert table is not None
        assert table.table_name == 'your_table'
    
    def test_validation_with_valid_data(self, sample_valid_data, tmp_path):
        """Test validation with valid data."""
        table = YourTable(
            data_directory=str(tmp_path),
            filetype='csv',
            timezone='UTC',
            data=sample_valid_data
        )
        table.validate()
        assert table.isvalid()
        assert len(table.errors) == 0
    
    def test_custom_method(self, sample_valid_data, tmp_path):
        """Test custom table methods."""
        table = YourTable(
            data_directory=str(tmp_path),
            filetype='csv',
            timezone='UTC',
            data=sample_valid_data
        )
        summary = table.get_summary_by_category()
        assert not summary.empty
        assert len(summary) == 2  # Two unique categories
```

### 5. Update Documentation

#### Add to User Guide

Update `docs/user-guide/tables.md` to include your new table:

```markdown
### YourTable

Brief description of what this table contains and its purpose in CLIF.

**Key Features:**
- Feature 1
- Feature 2

**Common Use Cases:**
- Use case 1
- Use case 2

**Links:**
- **Data Dictionary**: [YourTable Specification](https://clif-icu.com/data-dictionary#your-table)
- **API Reference**: [YourTable API](../api/tables.md#yourtable)
```

#### Update API Documentation

The API documentation should be automatically generated from your docstrings if you're using mkdocstrings.

### 6. Best Practices

1. **Follow Naming Conventions**:
   - Table class: PascalCase (e.g., `MedicationAdminIntermittent`)
   - Table name: snake_case (e.g., `medication_admin_intermittent`)
   - Schema file: snake_case with `_schema.yaml` suffix

2. **Inherit from BaseTable**:
   - Always inherit from `BaseTable` to get standard functionality
   - Override `_run_table_specific_validations()` for custom validation
   - Override `isvalid()` if you add custom error tracking

3. **Schema Design**:
   - Include all CLIF-required fields
   - Use appropriate data types
   - Define permissible values for categorical columns
   - Specify composite keys correctly

4. **Documentation**:
   - Use NumPy-style docstrings
   - Document all public methods
   - Include usage examples where helpful

5. **Testing**:
   - Test initialization
   - Test validation with valid and invalid data
   - Test custom methods
   - Use fixtures for reusable test data

## Example: Adding MedicationAdminIntermittent

Here's how `MedicationAdminIntermittent` was added as a minimal implementation:

1. **Schema**: Would need `medication_admin_intermittent_schema.yaml` (currently missing)
2. **Class**: Simple inheritance from BaseTable in `medication_admin_intermittent.py`
3. **Registration**: Added to TABLE_CLASSES and imports
4. **Tests**: Would need `test_medication_admin_intermittent.py`

## Troubleshooting

### Common Issues

1. **Schema not found warning**: Ensure schema file name matches table_name
2. **Validation errors**: Check data types match schema definitions
3. **Import errors**: Verify all import statements are updated
4. **Table not accessible**: Check ClifOrchestrator registration

### Debugging Tips

1. Check logs in `output/validation_log_your_table.log`
2. Use `table.get_summary()` to inspect loaded data
3. Run `table.validate()` and check `table.errors`
4. Verify schema is loaded with `table.schema`

## Next Steps

After adding your table:
1. Run the test suite: `uv run pytest tests/tables/test_your_table.py`
2. Test with ClifOrchestrator: Load and validate your table
3. Update any relevant examples or tutorials
4. Submit a pull request with all changes

For questions or issues, please open an issue on the [CLIFpy GitHub repository](https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy/issues).