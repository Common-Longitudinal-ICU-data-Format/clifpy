# Installation

This guide will help you install CLIFpy and its dependencies.

## Requirements

- Python 3.9 or higher
- pip (Python package installer)

## Basic Installation

### From PyPI (Recommended)

<!-- skip: next -->
```bash
pip install clifpy
```

### From Source

Clone the repository and install in development mode:

<!-- skip: next -->
```bash
# Clone the repository
git clone https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy.git
cd CLIFpy

# Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e .
```

## Optional Dependencies

### Documentation

To build the documentation locally:

<!-- skip: next -->
```bash
pip install clifpy[docs]
```

## Verifying Installation

After installation, verify that CLIFpy is properly installed:

```python
import clifpy
print(clifpy.__version__)
```

You should see the version number (e.g., `0.0.8`).


## Platform Support

CLIFpy is tested on:

- Linux (Ubuntu 20.04+)
- macOS (10.15+)
- Windows (10+)

## Troubleshooting

### Import Errors

If you encounter import errors, ensure you're using the correct Python environment:

```bash
which python
python --version
```

### Permission Errors

On some systems, you may need to use `pip install --user`:

<!-- skip: next -->
```bash
pip install --user clifpy
```

### Dependency Conflicts

If you encounter dependency conflicts, consider using a virtual environment:

<!-- skip: next -->
```bash
python -m venv clifpy-env
source clifpy-env/bin/activate  # On Windows: clifpy-env\Scripts\activate
pip install clifpy
```

## Next Steps

- [Follow the Quick Start guide](quickstart.md)
- [Learn about basic usage](basic-usage.md)
- [Explore the User Guide](../user-guide/index.md)