# pyCLIF Examples

This directory contains comprehensive Jupyter notebooks demonstrating the capabilities of the pyCLIF library for working with CLIF (Critical Care Data Exchange Format) data.

## Prerequisites

1. **Virtual Environment**: The project includes a `.venv` virtual environment with all dependencies installed.
2. **Data Location**: Update the `DATA_DIR` variable in each notebook to point to your CLIF data directory:
   ```python
   DATA_DIR = "/Users/vaishvik/downloads/CLIF_MIMIC"
   ```
3. **Data Format**: These examples assume your data is in Parquet format.
4. **Site Timezone**: Examples use `US/Eastern` timezone. Adjust as needed for your location.

## Getting Started

### 1. Activate the Virtual Environment

```bash
# From the project root directory
source .venv/bin/activate
```

### 2. Start Jupyter

```bash
jupyter notebook examples/
```

### 3. Run the Notebooks

Start with `01_basic_usage.ipynb` and proceed through the examples in order.

## Notebook Overview

### 📚 [01_basic_usage.ipynb](01_basic_usage.ipynb)
**Getting Started with pyCLIF**
- Initialize CLIF objects with timezone settings
- Load single and multiple tables
- Basic data validation and exploration
- Understanding the main CLIF class approach

**Key Topics:**
- CLIF class initialization
- Table loading with timezone conversion
- Data validation basics
- Simple data exploration

### 🔧 [02_individual_tables.ipynb](02_individual_tables.ipynb)
**Working with Individual Table Classes**
- Alternative approach using individual table classes
- Memory-efficient loading strategies
- Custom data processing workflows
- Comparing CLIF class vs individual tables

**Key Topics:**
- `from_file()` class methods
- Custom DataFrame initialization
- Table-specific features
- Memory and performance considerations

### ✅ [03_data_validation.ipynb](03_data_validation.ipynb)
**Data Validation and Quality Assurance**
- Comprehensive validation workflows
- Schema compliance checking
- Range validation for clinical data
- Error handling and reporting

**Key Topics:**
- Schema validation against CLIF specifications
- Range validation for vital signs
- Data quality metrics
- Validation best practices

### 📊 [04_vitals_analysis.ipynb](04_vitals_analysis.ipynb)
**Advanced Vitals Analysis**
- In-depth vital signs analysis
- Time-series patterns and trends
- Clinical insights and correlations
- Patient cohort analysis

**Key Topics:**
- Vital signs exploration
- Temporal pattern analysis
- Range validation and outlier detection
- Correlation analysis between vitals
- Clinical cohort creation

### 🌍 [05_timezone_handling.ipynb](05_timezone_handling.ipynb)
**Timezone Handling and Conversion**
- Comprehensive timezone management
- Automatic conversion capabilities
- Best practices for healthcare data
- Validation and troubleshooting

**Key Topics:**
- Timezone format support
- Automatic datetime conversion
- Daylight saving time handling
- Validation and troubleshooting
- Performance considerations

### 🔍 [06_data_filtering.ipynb](06_data_filtering.ipynb)
**Advanced Data Filtering and Querying**
- Efficient data filtering strategies
- Memory-optimized processing
- Complex multi-condition filtering
- Performance optimization

**Key Topics:**
- Load-time filtering for efficiency
- Complex pandas operations
- Statistical outlier removal
- Memory-efficient chunking
- Performance benchmarking

## Configuration for Your Environment

### Data Directory
Update this in each notebook:
```python
DATA_DIR = "/path/to/your/CLIF_MIMIC/data"
```

### Timezone Setting
Your site timezone (US/Eastern):
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
from pyclif import CLIF

# Initialize with your settings
clif = CLIF(
    data_dir="/Users/vaishvik/downloads/CLIF_MIMIC",
    filetype='parquet',
    timezone='US/Eastern'
)

# Load tables
clif.initialize(tables=['patient', 'vitals'])

# Check validation
print(f"Patient data valid: {clif.patient.isvalid()}")
print(f"Vitals data valid: {clif.vitals.isvalid()}")
```

### Memory-Efficient Loading
```python
from pyclif.utils.io import load_data

# Load with filters and timezone conversion
vitals_data = load_data(
    table_name="vitals",
    table_path="/Users/vaishvik/downloads/CLIF_MIMIC",
    table_format_type="parquet",
    columns=['patient_id', 'vital_category', 'vital_value', 'recorded_dttm'],
    filters={'vital_category': ['heart_rate', 'sbp', 'dbp']},
    sample_size=1000,
    site_tz="US/Eastern"
)
```

### Individual Table Loading
```python
from pyclif.tables.vitals import vitals

# Load vitals table directly
vitals_table = vitals.from_file(
    table_path="/Users/vaishvik/downloads/CLIF_MIMIC",
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
   - Try chunked processing approaches shown in notebook 6

4. **Validation Failures**
   - Check data format and column names
   - Review validation error messages for specific issues
   - Ensure data matches CLIF schema specifications

### Getting Help

1. Check the validation error messages for specific guidance
2. Review the best practices sections in each notebook
3. Start with smaller data samples to test your workflows
4. Use the troubleshooting sections in the timezone and validation notebooks

## Next Steps

After working through these examples:

1. **Adapt to Your Research**: Modify the examples for your specific research questions
2. **Create Custom Functions**: Build reusable functions based on the patterns shown
3. **Combine Techniques**: Integrate filtering, validation, and analysis approaches
4. **Scale Up**: Apply memory-efficient techniques to your full datasets
5. **Automate Workflows**: Create scripts based on the notebook examples

## Additional Resources

- **pyCLIF Documentation**: Check the main project documentation
- **CLIF Specifications**: Review the mCIDE schema files in `src/pyclif/mCIDE/`
- **Healthcare Data Standards**: Consult CLIF specification documents for clinical context

---

*These examples are designed to showcase the full capabilities of pyCLIF for healthcare data analysis. Start with the basic usage notebook and progress through the advanced topics as needed for your specific use cases.*