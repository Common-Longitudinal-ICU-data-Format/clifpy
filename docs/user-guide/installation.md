# Installation

## Requirements

- Python 3.9 or higher

- pip or uv (package installer)

## Installation for Users

Using pip:

<!-- skip: next -->
```bash
pip install clifpy
```

Using uv:

<!-- skip: next -->
```bash
uv pip install clifpy
```

Optionally, if you want to build the documentation locally:

<!-- skip: next -->
```bash
pip install clifpy[docs]
# or with uv
uv pip install clifpy[docs]
```

### Verifying Installation

After installation, verify that CLIFpy is properly installed:

```python
import clifpy
print(clifpy.__version__)
```

You should see the version number (e.g., `0.0.8`).

## Installation for Contributors

If you want to contribute to CLIFpy or test the latest development version:

<!-- skip: next -->
```bash
# Clone the repository
git clone https://github.com/Common-Longitudinal-ICU-data-Format/CLIFpy.git
cd CLIFpy

# Create virtual environment (if using uv, it handles this automatically)
uv venv  # or: python -m venv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate (or venv\Scripts\activate)

# Install in editable mode with dev dependencies
uv pip install -e .
# or with pip
pip install -e .
```
For testing and documentation development, install the dev dependency group:

<!-- skip: next -->
```bash
# Using uv (reads from pyproject.toml [dependency-groups])
uv pip install -e . --group dev

# Or manually install dev dependencies
pip install marimo mkdocs mkdocs-material mkdocstrings[python] nbformat pytest-doctestplus sybil
```

Dev dependencies include:

- **sybil**: For markdown documentation testing

- **mkdocs** + **mkdocs-material**: For building documentation

- **mkdocstrings**: For API reference generation

- **pytest-doctestplus**: For enhanced docstring testing

- **marimo**: For interactive notebook development

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

If you encounter dependency conflicts, use a fresh virtual environment:

<!-- skip: next -->
```bash
# With uv
uv venv --python 3.9
source .venv/bin/activate

# With venv
python -m venv fresh-env
source fresh-env/bin/activate

# Then install
uv pip install clifpy  # or: pip install clifpy
```
