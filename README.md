# clifpy - Python Client for CLIF 

**⚠️ Status: This project is currently in active development**

clifpy is a Python package for working with CLIF (Common Longitudinal ICU data Format) data. It provides a standardized interface for loading, validating, and analyzing critical care data across different healthcare systems.

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
git clone https://github.com/<your github username>/clifpy.git
cd clifpy

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in development mode
pip install -e .
```

### Package Installation (Coming Soon)
```bash
# Will be available after PyPI release
pip install clifpy
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

- [CLIF Specification](clif-icu.com)
- [Issue Tracker](https://github.com/Common-Longitudinal-ICU-data-Format/pyCLIF/issues)

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**Note**: This project is under active development. APIs may change between versions until the 1.0 release.
