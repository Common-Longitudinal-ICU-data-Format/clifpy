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

### User Installation

For most users, simply install from PyPI using pip:

```bash
pip install clifpy
```

This is all you need to start using clifpy in your projects.



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

### Prerequisites

First, install [uv](https://docs.astral.sh/uv/) if you haven't already:
```bash
# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
# Or on Windows: powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### Development Setup

1. Fork the repository
2. Clone your fork and set up the development environment:
   ```bash
   # Clone the repository
   git clone https://github.com/<your github username>/clifpy.git
   cd clifpy
   
   # Install dependencies and create virtual environment automatically
   uv sync
   ```
3. Create a feature branch (`git checkout -b feature/amazing-feature`)
4. Make your changes
5. Add new dependencies with `uv add <package>` (for permanent dependencies)
6. Run tests (`uv run pytest`)
7. Commit your changes (`git commit -m 'Add amazing feature'`)
8. Push to the branch (`git push origin feature/amazing-feature`)
9. Open a Pull Request

### Development Commands
```bash
# Run tests
uv run pytest

# Add a new dependency (updates pyproject.toml)
uv add <package>

# Add a development dependency
uv add --dev <package>

# Install temporary/experimental package (not committed to pyproject.toml)
uv pip install <package>

# Run any Python script
uv run python your_script.py

# Sync dependencies after pulling changes
uv sync
```

## 📄 License

This project is licensed under the [LICENSE] file in the repository.

## 🔗 Links

- [CLIF Specification](clif-icu.com)
- [Issue Tracker](https://github.com/Common-Longitudinal-ICU-data-Format/pyCLIF/issues)

## 📧 Contact

For questions or support, please open an issue on GitHub.

---

**Note**: This project is under active development. APIs may change between versions until the 1.0 release.