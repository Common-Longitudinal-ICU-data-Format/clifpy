# CLIFpy Examples

This directory contains Python scripts and Jupyter notebooks demonstrating the capabilities of the CLIFpy library for working with CLIF data.

## Prerequisites

1. **Virtual Environment**: The project includes a `.venv` virtual environment with all dependencies installed.
2. **Data Location**: Update the `DATA_DIR` variable in each script/notebook to point to your CLIF data directory:
   ```python
   DATA_DIR = "/path/to/your/CLIF_data"
   ```
3. **Data Format**: These examples assume your data is in Parquet format.
4. **Site Timezone**: Examples use `US/Eastern` timezone. Adjust as needed for your location.

## Getting Started

### 1. Activate the Virtual Environment
```bash
# From the project root directory
source .venv/bin/activate
```

### 2. Start Jupyter (for notebook files)

```bash
jupyter notebook examples/
```

### 3. Run the Examples

Start with `00_basic_usage.py` and proceed through the examples.

## Examples Overview

### 📚 [00_basic_usage.py](00_basic_usage.py)
**Getting Started with CLIFpy**
- Initialize CLIF objects with timezone settings
- Load single and multiple tables
- Basic data validation and exploration
- Understanding the main CLIF class approach

**Key Topics:**
- CLIF class initialization
- Table loading with timezone conversion
- Data validation basics
- Simple data exploration

### 🔧 [01_demo_from_files.py](01_demo_from_files.py)
**Demo Loading from Files**
- Demonstrates loading CLIF data from files
- File-based data processing workflows
- Basic table operations and validation

**Key Topics:**
- File-based data loading
- Table initialization from files
- Basic data processing patterns

### ⚙️ [clif_orchestrator_simple.py](clif_orchestrator_simple.py)
**Simple CLIF Orchestrator**
- Orchestration patterns for CLIF data processing
- Simplified workflow management

**Key Topics:**
- Data orchestration patterns
- Workflow automation

### 🧪 [labs_demo.ipynb](labs_demo.ipynb)
**Laboratory Data Analysis**
- Sample laboratory data exploration
- Lab value distribution by *_cateorgy
- Quality assurance for lab data

**Key Topics:**
- Lab data loading and validation

### 📍 [position_demo.ipynb](position_demo.ipynb)
**Patient Position Data Analysis**
- Patient positioning data exploration

**Key Topics:**
- Position data validation

### 🫁 [respiratory_support_demo.ipynb](respiratory_support_demo.ipynb)
**Respiratory Support Analysis**
- Respiratory support device data analysis
- Device utilization patterns

**Key Topics:**
- Respiratory device data validation
- Ventilation parameter analysis

## Configuration for Your Environment

### Data Directory
Update this in each script/notebook:
```python
DATA_DIR = "/path/to/your/CLIF_data"
```

### Timezone Setting
Your site timezone (default US/Eastern):
```python
# In CLIF class
clif = CLIF(data_dir=DATA_DIR, filetype='parquet', timezone='US/Eastern')

# In load_data function
data = load_data(..., site_tz='US/Eastern')
```

### File Format
Your data is in Parquet format:
```python
table_format_type = "parquet"
```

## Common Usage Patterns

### Quick Start - Load and Validate Data
```python
from clifpy import CLIF

# Initialize with your settings
clif = CLIF(
    data_dir=DATA_DIR,
    filetype='parquet',
    timezone='US/Eastern'
)

# Load tables
clif.initialize(tables=['patient', 'vitals'])

# Check validation
print(f"Patient data valid: {clif.patient.isvalid()}")
print(f"Vitals data valid: {clif.vitals.isvalid()}")
```

### Individual Table Loading
```python
from clifpy import vitals

# Load vitals table directly
vitals_table = vitals.from_file(
    table_path=DATA_DIR,
    table_format_type="parquet"
)

# Use table-specific methods
hr_data = vitals_table.filter_by_vital_category('heart_rate')
summary = vitals_table.get_summary_stats()
```

## Troubleshooting

### Common Issues

1. **Data Directory Not Found**
   ```
   FileNotFoundError: The file ... does not exist
   ```
   - Verify your `DATA_DIR` path is correct
   - Ensure CLIF data files exist in the directory

2. **Timezone Conversion Errors**
   ```
   TypeError: Cannot convert tz-naive timestamps
   ```
   - Check if your data has timezone information
   - Ensure you're using supported timezone formats

3. **Memory Issues**
   ```
   MemoryError: Unable to allocate memory
   ```
   - Use `sample_size` parameter to limit data
   - Process data in smaller chunks

4. **Validation Failures**
   - Check data format and column names
   - Review validation error messages for specific issues
   - Ensure data matches CLIF schema specifications

### Getting Help

1. Check the validation error messages for specific guidance
2. Review the examples for best practices
3. Start with smaller data samples to test your workflows
4. Check the main CLIFpy documentation

## Next Steps

After working through these examples:

1. **Adapt to Your Research**: Modify the examples for your specific research questions
2. **Create Custom Functions**: Build reusable functions based on the patterns shown
3. **Combine Techniques**: Integrate different analysis approaches from the examples
4. **Scale Up**: Apply the techniques to your full datasets
5. **Automate Workflows**: Create scripts based on the example patterns

## Additional Resources

- **CLIFpy Documentation**: Check the main project documentation
- **CLIFpy Specifications**: Review the mCIDE schema files in `clifpy/mCIDE/`
- **Healthcare Data Standards**: Consult CLIF specification documents for clinical context

---

*These examples demonstrate CLIFpy's capabilities for healthcare data analysis. Start with the basic usage examples and progress through the specialized demos as needed for your use cases.*

