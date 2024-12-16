# pyCLIF
Python package for CLIF


## Installation instruction- TBD
```
pip install pyclif
```


## Basic usage example
```
from pyclif.clif import CLIF

# Initialize
c = CLIF(data_dir="path/to/data", filetype="csv")

# Load tables
c.load(["patient", "hospitalization"])

# Validate tables
c.patient.validate()
c.hospitalization.validate()

# Perform analysis...
```

## Documentation

### For contributors
