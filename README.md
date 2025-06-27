# pyCLIF - Python Client for CLIF (In Progress)

**⚠️ Status: This project is currently in active development**

pyCLIF is a Python package for working with CLIF (Common Longitudinal ICU Data Format) data. It provides a standardized interface for loading, validating, and analyzing critical care data across different healthcare systems.

## 🚧 Project Status

### ✅ Completed Features
- Core [CLIF-2.0.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html) class implementation
- All 9 [CLIF-2.0.0](https://clif-consortium.github.io/website/data-dictionary/data-dictionary-2.0.0.html) beta table implementations (patient, vitals, labs, etc.)
- Data validation against mCIDE schemas
- Timezone handling and conversion
- Advanced filtering and querying capabilities
- Comprehensive test suite
- CLIF Demo Dataset created using [MIMIC-IV Clinical Database Demo](https://physionet.org/content/mimic-iv-demo/2.2/)
- Example notebooks demonstrating usage

### 🔄 In Progress
- Package distribution setup (PyPI)
- Additional clinical calculation functions
- Performance optimizations for large datasets
- Enhanced documentation
- Integration with additional data sources

### 📋 Planned Features
- SOFA score calculations
- Additional clinical severity scores
- Data visualization utilities
- Export functionality to other formats

## 📦 Installation

### Development Installation
```bash
# Clone the repository
git clone https://github.com/Common-Longitudinal-ICU-data-Format/pyCLIF.git
cd pyCLIF

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .
```

### Package Installation (Coming Soon)
```bash
# Will be available after PyPI release
pip install pyclif
```

## 🚀 Quick Start

### Basic Usage - Main CLIF Class
```python
from pyclif import CLIF

# Initialize CLIF object with your data directory
clif = CLIF(
    data_dir="/path/to/your/clif/data",
    filetype='parquet',  # or 'csv'
    timezone='US/Eastern'  # Your site timezone
)

# Load specific tables
clif.initialize(tables=['patient', 'vitals', 'labs'])

# Access loaded data
print(f"Patients: {len(clif.patient.df)}")
print(f"Vitals records: {len(clif.vitals.df)}")

# Check validation status
print(f"Patient data valid: {clif.patient.isvalid()}")
print(f"Vitals data valid: {clif.vitals.isvalid()}")
```

### Alternative Usage - Individual Tables
```python
from pyclif.tables.vitals import vitals
from pyclif.tables.patient import patient

# Load individual tables directly
vitals_table = vitals.from_file(
    table_path="/path/to/your/clif/data",
    table_format_type="parquet"
)

# Use table-specific methods
heart_rate_data = vitals_table.filter_by_vital_category('heart_rate')
summary_stats = vitals_table.get_summary_stats()
```

### Advanced Filtering
```python
from pyclif.utils.io import load_data

# Load with filters and timezone conversion
filtered_vitals = load_data(
    table_name="vitals",
    table_path="/path/to/data",
    table_format_type="parquet",
    columns=['patient_id', 'vital_category', 'vital_value', 'recorded_dttm'],
    filters={'vital_category': ['heart_rate', 'sbp', 'dbp']},
    site_tz="US/Eastern"
)
```

## 📊 Available Tables

1. **patient** - Patient demographics and identifiers
2. **hospitalization** - Hospital admission information
3. **vitals** - Vital signs measurements
4. **labs** - Laboratory test results
5. **adt** - Admission, discharge, and transfer events
6. **respiratory_support** - Ventilation and oxygen therapy data
7. **medication_admin_continuous** - Continuous medication administration
8. **patient_assessments** - Clinical assessments and scores
9. **position** - Patient positioning data

## 🔧 Key Features

### Data Validation
- Automatic validation against mCIDE schema specifications
- Range checking for clinical values
- Data type verification
- Missing data detection

### Timezone Handling
- Automatic timezone conversion during data loading
- Support for all major timezone formats
- Preserves data integrity across different time zones

### Memory Efficiency
- Column selection during loading
- Filtering at load time
- Chunked processing for large datasets
- Optimized data types

### Clinical Focus
- Vital signs range validation
- Medication grouping and categorization
- Respiratory support device tracking
- Laboratory unit standardization

## 📚 Example Notebooks

Comprehensive example notebooks are available in the `examples/` directory:

1. **01_basic_usage.ipynb** - Getting started with pyCLIF
2. **02_individual_tables.ipynb** - Working with individual table classes
3. **03_data_validation.ipynb** - Data validation and quality checks
4. **04_vitals_analysis.ipynb** - Advanced vital signs analysis
5. **05_timezone_handling.ipynb** - Timezone conversion and management
6. **06_data_filtering.ipynb** - Efficient data filtering techniques

## 🧪 Testing

The project includes a comprehensive test suite:

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=pyclif

# Run specific test module
pytest tests/test_patient.py

# Run with verbose output
pytest -v
```

## 📋 Requirements

- Python 3.8+
- pandas >= 2.0.0
- duckdb >= 0.9.0
- pyarrow >= 10.0.0
- pytz
- pydantic >= 2.0

See `pyproject.toml` for complete dependencies.

## 🤝 Contributing

We welcome contributions! Please see our contributing guidelines (coming soon).

### Development Setup
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests (`pytest`)
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 📄 License

This project is licensed under the [LICENSE] file in the repository.

## 🔗 Links

- [CLIF Specification](https://github.com/Common-Longitudinal-ICU-data-Format)
- [Documentation](https://github.com/Common-Longitudinal-ICU-data-Format/pyCLIF/wiki) (Coming Soon)
- [Issue Tracker](https://github.com/Common-Longitudinal-ICU-data-Format/pyCLIF/issues)

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**Note**: This project is under active development. APIs may change between versions until the 1.0 release.
