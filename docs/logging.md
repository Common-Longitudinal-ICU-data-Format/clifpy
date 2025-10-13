# Logging System Documentation

## Overview

The `clifpy` package uses a centralized logging system that provides:

- **Dual log files**: Separate files for all events and errors-only
- **Console output**: Maintains familiar print()-like user experience
- **Emoji formatting**: Visual indicators for quick log level identification
- **Automatic setup**: Logging initializes automatically when using ClifOrchestrator
- **Hierarchical loggers**: Organized namespace (`clifpy.*`) for all modules

## How It Works: Two Key Functions

The logging system uses two functions with distinct purposes:

### `setup_logging()` - Configure the System (Call Once)

This function **configures** the entire logging infrastructure:
- Creates log files (`clifpy_all.log`, `clifpy_errors.log`)
- Sets up console output
- Configures emoji formatting
- Determines where and how logs are saved

**Call this once** at your application's entry point.

### `get_logger()` - Get a Logger (Call Anywhere)

This function simply **retrieves** a logger instance. It does not configure anything.

**Where Each Is Used:**

| Called In | Function Used | Purpose |
|-----------|---------------|---------|
| `ClifOrchestrator.__init__()` | `setup_logging()` | Entry point - configures logging |
| `BaseTable.__init__()` | `setup_logging()` | Entry point - configures logging |
| `utils/wide_dataset.py` | `get_logger()` | Utility - just needs a logger |
| `utils/sofa.py` | `get_logger()` | Utility - just needs a logger |
| `utils/io.py` | `get_logger()` | Utility - just needs a logger |
| `utils/config.py` | `get_logger()` | Utility - just needs a logger |

**Why utility modules don't call `setup_logging()`:**

When you create a `ClifOrchestrator`, it calls `setup_logging()` internally. By the time utility modules like `wide_dataset.py` run, logging is already configured. They just need to retrieve their logger with `get_logger()`.

```python
# User's script
clif = ClifOrchestrator(...)  # ← setup_logging() called here

# Later, when you call this:
wide_df = clif.create_wide_dataset(...)  # ← wide_dataset.py just uses get_logger()
```

## Logger Namespaces and Why They Matter

### What Are Logger Names?

Each logger has a hierarchical name (like `clifpy.utils.sofa` or `clifpy.tables.labs`). Think of it like a file path - the dots create a parent-child relationship.

### The Logger Hierarchy

```
clifpy                              ← Root logger (configured by setup_logging)
├── clifpy.ClifOrchestrator         ← Inherits config from parent
├── clifpy.tables                   ← Inherits config from parent
│   ├── clifpy.tables.labs          ← Inherits config from grandparent
│   ├── clifpy.tables.vitals        ← Inherits config from grandparent
│   └── clifpy.tables.meds          ← Inherits config from grandparent
└── clifpy.utils                    ← Inherits config from parent
    ├── clifpy.utils.wide_dataset   ← Inherits config from grandparent
    ├── clifpy.utils.sofa           ← Inherits config from grandparent
    └── clifpy.utils.io             ← Inherits config from grandparent
```

**Key concept:** When `setup_logging()` configures the `'clifpy'` root logger, all child loggers (`clifpy.*`) automatically inherit that configuration. This is why utility modules don't need to call `setup_logging()` - they inherit everything from the parent.

### Why Logger Names Matter

Logger names provide several practical benefits:

#### 1. Identify Where Logs Come From

Each log message shows exactly which module generated it:

```
2025-10-13 10:30:15 | 📢 INFO | clifpy.ClifOrchestrator | Starting analysis
2025-10-13 10:30:16 | 📢 INFO | clifpy.utils.wide_dataset | Loading tables
2025-10-13 10:30:17 | ❌ ERROR | clifpy.tables.labs | Missing column: creatinine
2025-10-13 10:30:18 | 📢 INFO | clifpy.utils.sofa | Computing SOFA scores
```

Without names, you'd just see messages with no indication of which file or module has a problem.

#### 2. Control Verbosity for Specific Modules

You can make specific parts of your code more or less verbose:

```python
import logging

# Make ONLY sofa.py show detailed debug messages
logging.getLogger('clifpy.utils.sofa').setLevel(logging.DEBUG)

# Quiet down the noisy wide_dataset.py (warnings only)
logging.getLogger('clifpy.utils.wide_dataset').setLevel(logging.WARNING)

# Everything else stays at INFO level (default)
```

**Real scenario:** You're debugging SOFA calculations but don't care about all the verbose wide dataset processing logs. Just make SOFA verbose and keep everything else quiet.

#### 3. Control Entire Subsystems

Hierarchical names let you control entire groups at once:

```python
import logging

# Silence ALL table-related logging
logging.getLogger('clifpy.tables').setLevel(logging.ERROR)

# This automatically affects all child loggers:
#   - clifpy.tables.labs
#   - clifpy.tables.vitals
#   - clifpy.tables.medication_admin_continuous
#   - All other tables
```

#### 4. Search and Filter Logs

Use grep to find logs from specific modules:

```bash
# Find all SOFA calculation logs
grep "clifpy.utils.sofa" output/logs/clifpy_all.log

# Find all table loading issues
grep "clifpy.tables" output/logs/clifpy_errors.log

# Find all wide dataset processing
grep "clifpy.utils.wide_dataset" output/logs/clifpy_all.log
```

#### 5. Trace Execution Flow

Follow your code's execution path across multiple modules:

```
10:30:15 | clifpy.ClifOrchestrator      | Starting wide dataset creation
10:30:16 | clifpy.utils.wide_dataset    | Loading labs table
10:30:17 | clifpy.tables.labs           | Loaded 50,000 records
10:30:18 | clifpy.utils.wide_dataset    | Pivoting labs data
10:30:19 | clifpy.utils.wide_dataset    | ERROR: Pivot failed
```

You can see the execution flow: orchestrator → wide_dataset → labs → back to wide_dataset → error.

## Log File Structure

All logs are stored in the `output/logs/` subdirectory:

```
output/
├── logs/
│   ├── clifpy_all.log          # All INFO+ messages
│   ├── clifpy_errors.log        # Only WARNING+ messages
│   ├── validation_log_*.log     # Per-table validation logs (supplementary)
```

### Log File Contents

#### `clifpy_all.log`
Contains all informational messages, warnings, and errors. Use this for:
- Debugging processing steps
- Understanding data flow
- Tracking what operations were performed
- Performance analysis

**Example:**
```
2025-10-13 10:30:15 | ✅ INFO     | clifpy.ClifOrchestrator | [create_wide_dataset:520] | 🚀 WIDE DATASET CREATION STARTED
2025-10-13 10:30:16 | ✅ INFO     | clifpy.tables.labs | [validate:145] | ✓ All required columns present
2025-10-13 10:30:17 | ⚠️ WARNING  | clifpy.utils.io | [convert_datetime_columns_to_site_tz:191] | event_dttm: Naive datetime localized to US/Central
```

#### `clifpy_errors.log`
Contains only warnings and errors. Use this to:
- Quickly identify problems
- Review issues without reading through info logs
- Troubleshoot failures

**Example:**
```
2025-10-13 10:30:17 | ⚠️ WARNING  | clifpy.utils.io | [convert_datetime_columns_to_site_tz:191] | event_dttm: Naive datetime localized to US/Central
2025-10-13 10:31:45 | ❌ ERROR    | clifpy.tables.vitals | [validate:152] | Missing required columns: ['heart_rate']
```

## Emoji Legend

Logs use emojis for quick visual parsing:

| Level    | Emoji | When Used |
|----------|-------|-----------|
| DEBUG    | 🐛    | Detailed internal operations, variable values |
| INFO     | 📢    | Normal operations, progress updates |
| WARNING  | ⚠️    | Potential issues, missing optional data |
| ERROR    | ❌    | Failures, validation errors |
| CRITICAL | 🆘    | Severe failures requiring immediate attention |

## Usage

### Automatic Setup (Recommended)

When using `ClifOrchestrator`, logging is configured automatically:

```python
from clifpy import ClifOrchestrator

# Logging automatically initializes when creating orchestrator
clif = ClifOrchestrator(
    data_directory="./data",
    filetype="parquet",
    timezone="US/Central",
    output_directory="./output"  # Logs go to ./output/logs/
)

# All operations now log automatically
clif.load_table("labs")
clif.create_wide_dataset(tables_to_include=["labs", "vitals"])
```

### Manual Setup

For standalone scripts or custom workflows:

```python
from clifpy.utils.logging_config import setup_logging, get_logger

# Initialize centralized logging
setup_logging(output_directory="./output")

# Get a logger for your module
logger = get_logger('my_analysis')

# Use the logger
logger.info("Starting custom analysis")
logger.warning("Missing optional parameter, using default")
logger.error("Failed to process data")
```

#### Understanding `get_logger()` vs `logging.getLogger()`

The `get_logger()` function is a convenience wrapper that ensures your logger inherits the centralized configuration. Here's what it does:

```python
# Using our wrapper (recommended) ✅
from clifpy.utils.logging_config import get_logger
logger = get_logger('my_analysis')
# → Creates logger named 'clifpy.my_analysis' (automatically prefixed!)
# → Inherits all configuration (log files, console, emojis)

# Direct call with full prefix ✅
import logging
logger = logging.getLogger('clifpy.my_analysis')
# → Creates logger named 'clifpy.my_analysis' (manual prefix)
# → Inherits all configuration

# Direct call WITHOUT prefix ❌
import logging
logger = logging.getLogger('my_analysis')
# → Creates logger named 'my_analysis' (root level, no prefix)
# → Does NOT inherit clifpy configuration
# → Uses Python's default logging (no files, no emojis)
```

**Why the prefix matters:**

Only loggers whose names start with `'clifpy.'` inherit the centralized configuration. The `get_logger()` wrapper automatically adds this prefix, so you don't have to remember it.

**Best practice:** Use `get_logger()` to ensure your logger is properly configured.

### Configuration Options

```python
setup_logging(
    output_directory="./output",      # Base directory (logs go in output/logs/)
    level=logging.INFO,                # Minimum level to capture
    console_output=True,               # Show messages in console
    separate_error_log=True            # Create separate error log file
)
```

## Log Levels Guide

### When to Use Each Level

**DEBUG** - Detailed diagnostics for development:
```python
logger.debug(f"Processing batch {i} of {total_batches}")
logger.debug(f"Query: {sql_query}")
logger.debug(f"Intermediate result shape: {df.shape}")
```

**INFO** - Normal operation progress:
```python
logger.info("Loading patient data")
logger.info(f"Loaded {len(df)} records")
logger.info("✅ Validation complete")
```

**WARNING** - Potential issues that don't stop execution:
```python
logger.warning("Missing optional column 'weight_kg', using defaults")
logger.warning("No data found for hospitalization_id=12345")
logger.warning(f"Outlier values detected: {outlier_count} records")
```

**ERROR** - Failures that prevent operation completion:
```python
logger.error(f"Missing required columns: {missing_cols}")
logger.error("File not found: {file_path}")
logger.error("Data validation failed")
```

**CRITICAL** - Severe failures requiring immediate action:
```python
logger.critical("Database connection lost")
logger.critical("Insufficient memory to process dataset")
```

## Common Workflows

### Reviewing Processing Results

After running data processing:

1. **Check console output** for high-level progress and warnings
2. **Review `clifpy_errors.log`** for any issues
3. **Check `clifpy_all.log`** if you need detailed processing steps

### Debugging Issues

When something goes wrong:

1. **Start with `clifpy_errors.log`**:
   ```bash
   cat output/logs/clifpy_errors.log
   ```

2. **Search for specific patterns**:
   ```bash
   grep "ERROR" output/logs/clifpy_all.log
   grep "hospitalization_id=12345" output/logs/clifpy_all.log
   ```

3. **Check table-specific validation**:
   ```bash
   cat output/logs/validation_log_labs.log
   ```

### Adjusting Log Verbosity

For more detailed logs during development:

```python
import logging
from clifpy import ClifOrchestrator

clif = ClifOrchestrator(
    data_directory="./data",
    filetype="parquet",
    timezone="US/Central",
    output_directory="./output"
)

# Enable DEBUG level for more details
setup_logging(output_directory="./output", level=logging.DEBUG)
```

For quieter logs (warnings/errors only):

```python
setup_logging(
    output_directory="./output",
    level=logging.WARNING,
    console_output=True  # Still show warnings in console
)
```

## Per-Table Validation Logs

In addition to the centralized logs, each table creates a supplementary validation log:

```
output/logs/validation_log_labs.log
output/logs/validation_log_vitals.log
output/logs/validation_log_medication_admin_continuous.log
```

These logs contain:
- Column validation results
- Data type checks
- Required field presence
- Table-specific validation rules

**Note**: These are supplementary - validation messages also appear in the main `clifpy_all.log` and `clifpy_errors.log` files.

## Best Practices

### 1. **Use Appropriate Log Levels**
- Don't overuse ERROR for warnings
- Use DEBUG for verbose internal details
- INFO should tell the "story" of what's happening

### 2. **Include Context in Messages**
```python
# Good - includes context
logger.info(f"Processing {len(df)} records for {table_name}")

# Less helpful
logger.info("Processing records")
```

### 3. **Log Important Parameters**
```python
logger.info(f"Starting SOFA calculation with extremal_type='{extremal_type}'")
logger.info(f"Cohort filtering: {len(cohort_df)} hospitalizations")
```

### 4. **Use Structured Sections**
```python
logger.info("=" * 50)
logger.info("🚀 ANALYSIS STARTED")
logger.info("=" * 50)
# ... processing ...
logger.info("✅ ANALYSIS COMPLETED")
```

### 5. **Clean Up Logs Between Runs**
Log files are overwritten on each run (mode='w'), so previous runs are automatically cleaned up.

## Integration with Existing Code

The logging system integrates with all existing `clifpy` modules:

| Module | Logger Name | Purpose |
|--------|-------------|---------|
| `ClifOrchestrator` | `clifpy.ClifOrchestrator` | High-level workflow orchestration |
| `tables.*` | `clifpy.tables.{table_name}` | Table loading and validation |
| `utils.wide_dataset` | `clifpy.utils.wide_dataset` | Wide dataset creation |
| `utils.sofa` | `clifpy.utils.sofa` | SOFA score calculation |
| `utils.io` | `clifpy.utils.io` | File I/O operations |
| `utils.config` | `clifpy.utils.config` | Configuration loading |

All modules use the same centralized configuration and write to the same log files.

## Troubleshooting

### Logs Not Appearing

**Issue**: No log files created

**Solution**: Ensure `output_directory` is writable:
```python
import os
output_dir = "./output/logs"
os.makedirs(output_dir, exist_ok=True)
```

### Console Output Missing

**Issue**: Not seeing messages in terminal

**Solution**: Ensure `console_output=True`:
```python
setup_logging(output_directory="./output", console_output=True)
```

### Too Verbose / Too Quiet

**Issue**: Too many/few messages

**Solution**: Adjust the log level:
```python
import logging

# More verbose
setup_logging(level=logging.DEBUG)

# Less verbose
setup_logging(level=logging.WARNING)
```

### Duplicate Log Messages

**Issue**: Same message appears multiple times

**Solution**: Avoid calling `setup_logging()` multiple times in custom code. The system is designed to be idempotent, but it's best to call it once at the start of your script.

## Examples

### Example 1: Basic Analysis Script

```python
from clifpy import ClifOrchestrator, setup_logging, get_logger

# Initialize logging
setup_logging(output_directory="./my_analysis/output")

# Get a custom logger for your script
logger = get_logger('my_analysis')

logger.info("Starting sepsis analysis")

# Create orchestrator (inherits logging configuration)
clif = ClifOrchestrator(
    data_directory="./data",
    filetype="parquet",
    timezone="US/Central",
    output_directory="./my_analysis/output"
)

# All operations are logged automatically
logger.info("Loading clinical tables")
clif.load_table("labs")
clif.load_table("vitals")

logger.info("Creating wide dataset")
wide_df = clif.create_wide_dataset(
    tables_to_include=["labs", "vitals"]
)

logger.info(f"✅ Analysis complete - processed {len(wide_df)} records")
```

### Example 2: Custom Processing with Detailed Logging

```python
import logging
from clifpy import setup_logging, get_logger
from clifpy.utils.sofa import compute_sofa

# Enable DEBUG level for detailed tracking
setup_logging(output_directory="./output", level=logging.DEBUG)

logger = get_logger('sofa_analysis')

logger.info("=" * 50)
logger.info("SOFA Score Calculation")
logger.info("=" * 50)

logger.debug(f"Input dataset shape: {wide_df.shape}")
logger.debug(f"Columns: {wide_df.columns.tolist()}")

# Compute SOFA scores
sofa_df = compute_sofa(
    wide_df,
    id_name='hospitalization_id',
    extremal_type='worst'
)

logger.info(f"Computed SOFA scores for {len(sofa_df)} hospitalizations")
logger.debug(f"SOFA score distribution:\n{sofa_df['sofa_total'].describe()}")

# Check results
logger.info(f"Logs saved to: output/logs/clifpy_all.log")
logger.info(f"Error log: output/logs/clifpy_errors.log")
```

### Example 3: Quiet Mode (Errors Only)

```python
import logging
from clifpy import ClifOrchestrator, setup_logging

# Only show warnings and errors
setup_logging(
    output_directory="./output",
    level=logging.WARNING,
    console_output=True
)

clif = ClifOrchestrator(
    data_directory="./data",
    filetype="parquet",
    timezone="US/Central",
    output_directory="./output"
)

# Console will only show warnings/errors
# All info messages still go to clifpy_all.log
clif.load_table("labs")
clif.create_wide_dataset(tables_to_include=["labs"])
```

## Summary

The `clifpy` logging system provides:

- ✅ **Automatic logging** for all operations
- ✅ **Dual log files** (all events + errors-only)
- ✅ **Console output** for real-time feedback
- ✅ **Emoji formatting** for readability
- ✅ **Organized structure** in `output/logs/` directory
- ✅ **Flexible configuration** for different use cases

No additional setup required when using `ClifOrchestrator` - just review the logs in `output/logs/` after running your analysis!
