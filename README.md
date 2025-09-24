# clifpy - Python Client for CLIF

<p align="center">
  <img src="https://raw.githubusercontent.com/Common-Longitudinal-ICU-data-Format/CLIFpy/main/docs/images/clif_logo_red_2.png" alt="CLIF Logo" width="400">
</p>

<p align="center">
  <i>Transform critical care data into actionable insights </i>
</p>

<p align="center">
  <a href="https://pypi.org/project/clifpy/"><img src="https://img.shields.io/pypi/v/clifpy?color=blue" alt="PyPI version"></a>
  <a href="https://pypi.org/project/clifpy/"><img src="https://img.shields.io/pypi/pyversions/clifpy" alt="Python Versions"></a>
  <a href="https://opensource.org/licenses/Apache-2.0"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://common-longitudinal-icu-data-format.github.io/clifpy/"><img src="https://img.shields.io/badge/docs-latest-brightgreen" alt="Documentation"></a>
  <a href="https://github.com/astral-sh/uv"><img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json" alt="uv"></a>
</p>

<p align="center">
  <a href="https://common-longitudinal-icu-data-format.github.io/clifpy/">Documentation</a> | 
  <a href="https://common-longitudinal-icu-data-format.github.io/clifpy/getting-started/quickstart/">Quick Start</a> | 
  <a href="https://clif-icu.com">CLIF Website</a>
</p>

## Standardized framework for critical care data analysis and research

CLIFpy is the official Python implementation for working with CLIF (Common Longitudinal ICU data Format) data. Transform heterogeneous ICU data into standardized, analysis-ready datasets with built-in validation, clinical calculations, and powerful data manipulation tools.

## Key Features

- 📊 **Comprehensive CLIF Support**: Full implementation of all CLIF 2.0 tables with automatic schema validation
- 🏥 **Clinical Calculations**: Built-in SOFA scores, comorbidity indices, and other ICU-specific metrics  
- 💊 **Smart Unit Conversion**: Automatically standardize medication dosages across different unit systems
- 🔗 **Encounter Stitching**: Link related ICU stays within configurable time windows
- ⚡ **High Performance**: Leverages DuckDB and Polars for efficient processing of large datasets
- 🌍 **Timezone Aware**: Proper timestamp handling across different healthcare systems
- 📈 **Wide Format Support**: Transform longitudinal data into hourly resolution for analysis

## Installation

```bash
pip install clifpy
```

## Quick Example

```python
from clifpy import ClifOrchestrator

# Load and validate CLIF data
orchestrator = ClifOrchestrator(
    data_directory='/path/to/clif/data',
    timezone='US/Eastern'
)

# Validate all tables against CLIF schemas
orchestrator.validate_all()

# Access individual tables
vitals = orchestrator.vitals.df
labs = orchestrator.labs.df

# Advanced features
wide_df = orchestrator.create_wide_dataset()  # Hourly resolution data
sofa_scores = orchestrator.compute_sofa_scores()  # Calculate SOFA scores
```

## Development

CLIFpy uses [uv](https://docs.astral.sh/uv/) for fast, reliable dependency management.

### Quick Setup

1. Install uv:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

2. Clone and install:
   ```bash
   git clone https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy.git
   cd CLIFpy
   uv sync
   ```

3. Run tests:
   ```bash
   uv run pytest
   ```

## Links & Resources

- 📚 [Full Documentation](https://common-longitudinal-icu-data-format.github.io/clifpy/)
- 🏥 [CLIF Specification](https://clif-icu.com/data-dictionary)
- 🐛 [Issue Tracker](https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy/issues)
- 📦 [PyPI Package](https://pypi.org/project/clifpy/)