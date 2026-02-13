"""
Data Quality Assessment (DQA) module for CLIFpy tables.

This module provides comprehensive DQA functions implementing:

CONFORMANCE CHECKS:
- A.1. Table presence verification
- A.2. Required columns presence check
- B.1. Data type validation
- B.2. Datetime format validation
- B.4. Categorical values validation against mCIDE
- B.5. Category-to-group mapping validation

COMPLETENESS CHECKS:
- A.1. Missingness analysis for required columns
- A.2. Conditional required fields validation
- B. mCIDE value coverage checks
- C.1. Relational integrity checks

DESIGN PRINCIPLES:
- Default backend is Polars (memory-efficient, lazy evaluation)
- Falls back to DuckDB if Polars is unavailable or fails a runtime smoke test
- Uses garbage collection and cache clearing for memory management
"""
import os
import yaml
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import logging
import duckdb
from pathlib import Path 
import gc

# Logger for this module
_logger = logging.getLogger(__name__)

try:
    import polars as pl
    _logger.debug("Polars imported (version %s), running smoke tests", pl.__version__)
    # Smoke test: exercise the compute paths used by DQA checks so that
    # platforms where Polars imports but fails at runtime (e.g. Windows
    # thread-pool/SIMD issues) fall back to DuckDB immediately.
    _smoke_a = pl.LazyFrame({"k": [1, 1, 2], "v": [10, 20, 30]})
    _smoke_a.collect_schema()                                      # 1. schema inspection
    _smoke_a.group_by("k").agg(pl.col("v").sum()).collect()        # 2. group_by + agg
    _smoke_b = pl.LazyFrame({"k": [1, 2]})
    _smoke_a.join(_smoke_b, on="k").collect()                      # 3. join
    _smoke_a.filter(pl.col("k").is_in([1])).collect()              # 4. filter + is_in
    _smoke_a.select(pl.col("v").sum()).collect(streaming=True)     # 5. streaming collect
    del _smoke_a, _smoke_b
    HAS_POLARS = True
except (ImportError, Exception) as _pl_err:
    HAS_POLARS = False
    if not isinstance(_pl_err, ImportError):
        _logger.warning("Polars imported but failed runtime check, falling back to DuckDB: %s", _pl_err)

_ACTIVE_BACKEND = 'polars' if HAS_POLARS else 'duckdb'
_logger.info("DQA backend: %s", _ACTIVE_BACKEND)


def _load_schema(table_name: str, schema_dir: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load YAML schema for a table."""
    if schema_dir is None:
        schema_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'schemas')

    schema_file = os.path.join(schema_dir, f'{table_name}_schema.yaml')
    _logger.debug("Loading schema for '%s' from %s", table_name, schema_file)
    if not os.path.exists(schema_file):
        _logger.warning("Schema file not found: %s", schema_file)
        return None

    with open(schema_file, 'r') as f:
        return yaml.safe_load(f)


class DQAConformanceResult:
    """Container for DQA conformance check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics
        }


class DQACompletenessResult:
    """Container for DQA completeness check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics
        }


class DQAPlausibilityResult:
    """Container for DQA plausibility check results."""

    def __init__(self, check_type: str, table_name: str):
        self.check_type = check_type
        self.table_name = table_name
        self.passed = True
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.info: List[Dict[str, Any]] = []
        self.metrics: Dict[str, Any] = {}

    def add_error(self, message: str, details: Optional[Dict] = None):
        self.passed = False
        self.errors.append({"message": message, "details": details or {}})

    def add_warning(self, message: str, details: Optional[Dict] = None):
        self.warnings.append({"message": message, "details": details or {}})

    def add_info(self, message: str, details: Optional[Dict] = None):
        self.info.append({"message": message, "details": details or {}})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "check_type": self.check_type,
            "table_name": self.table_name,
            "passed": self.passed,
            "errors": self.errors,
            "warnings": self.warnings,
            "info": self.info,
            "metrics": self.metrics
        }


# ---------------------------------------------------------------------------
# CONFORMANCE CHECKS - A. Structure Checks
# ---------------------------------------------------------------------------

# A.1. Whether table is present
def check_table_exists(
    table_path: Union[str, Path],
    table_name: str,
    filetype: str = 'parquet'
) -> DQAConformanceResult:
    """
    Check if a table file exists at the specified path.

    Parameters
    ----------
    table_path : str or Path
        Directory containing the table files
    table_name : str
        Name of the table to check
    filetype : str
        File extension (parquet, csv, etc.)

    Returns
    -------
    DQAConformanceResult
        Result object with check status
    """
    result = DQAConformanceResult("table_exists", table_name)

    table_path = Path(table_path)
    expected_file = table_path / f"{table_name}.{filetype}"

    if expected_file.exists():
        result.add_info(f"Table file found: {expected_file}")
        result.metrics["file_path"] = str(expected_file)
        result.metrics["file_size_mb"] = expected_file.stat().st_size / (1024 * 1024)
    else:
        result.add_error(
            f"Table file not found: {expected_file}",
            {"expected_path": str(expected_file)}
        )

    return result


# A.1b. Table presence check (DataFrame-level)
def check_table_presence_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str
) -> DQAConformanceResult:
    """
    Check that a loaded DataFrame has rows and columns using Polars.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        The data to validate
    table_name : str
        Name of the table
    """
    result = DQAConformanceResult("table_presence", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        column_names = lf.collect_schema().names()
        column_count = len(column_names)
        row_count = lf.select(pl.len()).collect().item()

        result.metrics["row_count"] = row_count
        result.metrics["column_count"] = column_count

        if column_count == 0:
            result.add_error(
                f"Table '{table_name}' has no columns",
                {"column_count": column_count},
            )
        if row_count == 0:
            result.add_error(
                f"Table '{table_name}' has 0 rows",
                {"row_count": row_count},
            )
        if result.passed:
            result.add_info(
                f"Table '{table_name}' present with {row_count} rows and {column_count} columns"
            )
    except Exception as e:
        _logger.error("Check 'table_presence' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking table presence: {str(e)}")

    return result


def check_table_presence_duckdb(
    df: pd.DataFrame,
    table_name: str
) -> DQAConformanceResult:
    """
    Check that a loaded DataFrame has rows and columns using DuckDB/pandas.

    Parameters
    ----------
    df : pd.DataFrame
        The data to validate
    table_name : str
        Name of the table
    """
    result = DQAConformanceResult("table_presence", table_name)

    try:
        row_count = len(df)
        column_count = len(df.columns)

        result.metrics["row_count"] = row_count
        result.metrics["column_count"] = column_count

        if column_count == 0:
            result.add_error(
                f"Table '{table_name}' has no columns",
                {"column_count": column_count},
            )
        if row_count == 0:
            result.add_error(
                f"Table '{table_name}' has 0 rows",
                {"row_count": row_count},
            )
        if result.passed:
            result.add_info(
                f"Table '{table_name}' present with {row_count} rows and {column_count} columns"
            )
    except Exception as e:
        _logger.error("Check 'table_presence' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking table presence: {str(e)}")

    return result


def check_table_presence(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str
) -> DQAConformanceResult:
    """
    Check that a loaded DataFrame has rows and columns.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    table_name : str
        Name of the table
    """
    _logger.debug("check_table_presence: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_table_presence_polars(df, table_name)
    else:
        result = check_table_presence_duckdb(df, table_name)
    _logger.debug("check_table_presence: table '%s' — rows=%s, cols=%s",
                  table_name, result.metrics.get("row_count"), result.metrics.get("column_count"))
    return result


# A.2. Whether all required columns are present
def check_required_columns_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if all required columns are present using Polars.

    Memory-efficient implementation using lazy evaluation.
    """
    result = DQAConformanceResult("required_columns", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        actual_columns = set(lf.collect_schema().names())
        required_columns = set(schema.get('required_columns', []))

        missing = required_columns - actual_columns
        extra = actual_columns - set(col['name'] for col in schema.get('columns', []))

        result.metrics["total_required"] = len(required_columns)
        result.metrics["total_present"] = len(required_columns - missing)
        result.metrics["total_missing"] = len(missing)

        if missing:
            result.add_error(
                f"Missing {len(missing)} required columns: {sorted(missing)}",
                {"missing_columns": sorted(missing)}
            )
        else:
            result.add_info("All required columns present")

        if extra:
            result.add_info(
                f"Found {len(extra)} extra columns not in schema",
                {"extra_columns": sorted(extra)}
            )

    except Exception as e:
        _logger.error("Check 'required_columns' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking columns: {str(e)}")

    return result


def check_required_columns_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if all required columns are present using DuckDB."""
    result = DQAConformanceResult("required_columns", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        # Get columns from relation
        actual_columns = set(df.columns)
        required_columns = set(schema.get('required_columns', []))

        missing = required_columns - actual_columns

        result.metrics["total_required"] = len(required_columns)
        result.metrics["total_present"] = len(required_columns - missing)
        result.metrics["total_missing"] = len(missing)

        if missing:
            result.add_error(
                f"Missing {len(missing)} required columns: {sorted(missing)}",
                {"missing_columns": sorted(missing)}
            )
        else:
            result.add_info("All required columns present")

        con.close()

    except Exception as e:
        _logger.error("Check 'required_columns' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking columns: {str(e)}")

    return result


def check_required_columns(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if all required columns are present.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema containing required_columns
    table_name : str
        Name of the table
    """
    _logger.debug("check_required_columns: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_required_columns_polars(df, schema, table_name)
    else:
        result = check_required_columns_duckdb(df, schema, table_name)
    if result.metrics.get("total_missing", 0) > 0:
        _logger.info("check_required_columns: table '%s' missing %d of %d required columns",
                     table_name, result.metrics["total_missing"], result.metrics.get("total_required", 0))
    _logger.debug("check_required_columns: table '%s' — required=%s, present=%s, missing=%s",
                  table_name, result.metrics.get("total_required"),
                  result.metrics.get("total_present"), result.metrics.get("total_missing"))
    return result


# ---------------------------------------------------------------------------
# CONFORMANCE CHECKS - B. Value Checks
# ---------------------------------------------------------------------------

# B.1. Data type validation
def check_column_dtypes_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if columns have correct data types using Polars."""
    result = DQAConformanceResult("column_dtypes", table_name)

    # mCIDE type to Polars type mapping
    type_mapping = {
        'VARCHAR': [pl.Utf8, pl.String, pl.Categorical],
        'DATETIME': [pl.Datetime],
        'DATE': [pl.Date],
        'INTEGER': [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64],
        'INT': [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64],
        'FLOAT': [pl.Float32, pl.Float64],
        'DOUBLE': [pl.Float32, pl.Float64],
    }

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        schema_dict = lf.collect_schema()

        dtype_errors = []
        dtype_warnings = []

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            expected_type = col_spec.get('data_type')

            if not expected_type or col_name not in schema_dict.names():
                continue

            actual_dtype = schema_dict[col_name]
            expected_pl_types = type_mapping.get(expected_type, [])

            type_matches = any(
                isinstance(actual_dtype, t) or actual_dtype == t
                for t in expected_pl_types
            )

            if not type_matches:
                dtype_str = str(actual_dtype)
                # Check common patterns
                if expected_type in ('DATETIME',) and 'Datetime' in dtype_str:
                    continue
                if expected_type in ('VARCHAR',) and ('Utf8' in dtype_str or 'String' in dtype_str):
                    continue

                castable = _check_castable_polars(lf, col_name, expected_type)

                if castable:
                    dtype_warnings.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": str(actual_dtype),
                        "castable": True
                    })
                else:
                    dtype_errors.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": str(actual_dtype),
                        "castable": False
                    })

        result.metrics["columns_checked"] = len(schema.get('columns', []))
        result.metrics["dtype_errors"] = len(dtype_errors)
        result.metrics["dtype_warnings"] = len(dtype_warnings)

        for err in dtype_errors:
            result.add_error(
                f"Column '{err['column']}' has type {err['actual']}, cannot cast to {err['expected']}",
                err
            )

        for warn in dtype_warnings:
            result.add_warning(
                f"Column '{warn['column']}' has type {warn['actual']}, can be cast to {warn['expected']}",
                warn
            )

        if not dtype_errors and not dtype_warnings:
            result.add_info("All column data types match schema")

    except Exception as e:
        _logger.error("Check 'column_dtypes' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking dtypes: {str(e)}")

    return result


def _check_castable_polars(lf: 'pl.LazyFrame', col_name: str, target_type: str) -> bool:
    """Check if a column can be cast to the target type using Polars."""
    try:
        if target_type in ('INTEGER', 'INT'):
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).cast(pl.Int64))
            return True
        elif target_type in ('FLOAT', 'DOUBLE'):
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).cast(pl.Float64))
            return True
        elif target_type == 'DATETIME':
            sample = lf.select(pl.col(col_name).drop_nulls().head(100)).collect()
            if len(sample) > 0:
                sample.select(pl.col(col_name).str.to_datetime())
            return True
        elif target_type == 'VARCHAR':
            return True
        return False
    except Exception:
        return False


def check_column_dtypes_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if columns have correct data types using DuckDB."""
    result = DQAConformanceResult("column_dtypes", table_name)

    type_mapping = {
        'VARCHAR': ['VARCHAR', 'TEXT', 'STRING'],
        'DATE': ['TIMESTAMP', 'TIMESTAMP WITH TIME ZONE'],
        'DATE': ['DATE'],
        'INTEGER': ['INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT'],
        'INT': ['INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT', 'INT'],
        'FLOAT': ['FLOAT', 'DOUBLE', 'REAL', 'DECIMAL'],
        'DOUBLE': ['FLOAT', 'DOUBLE', 'REAL', 'DECIMAL'],
    }

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        describe_result = con.execute("DESCRIBE df").fetchall()
        actual_types = {row[0]: row[1].upper() for row in describe_result}

        dtype_errors = []
        dtype_warnings = []

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            expected_type = col_spec.get('data_type')

            if not expected_type or col_name not in actual_types:
                continue

            actual_dtype = actual_types[col_name]
            expected_duckdb_types = type_mapping.get(expected_type, [])

            type_matches = any(
                expected_t in actual_dtype
                for expected_t in expected_duckdb_types
            )

            if not type_matches:
                castable = _check_castable_duckdb(con, col_name, expected_type)

                if castable:
                    dtype_warnings.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_dtype,
                        "castable": True
                    })
                else:
                    dtype_errors.append({
                        "column": col_name,
                        "expected": expected_type,
                        "actual": actual_dtype,
                        "castable": False
                    })

        result.metrics["columns_checked"] = len(schema.get('columns', []))
        result.metrics["dtype_errors"] = len(dtype_errors)
        result.metrics["dtype_warnings"] = len(dtype_warnings)

        for err in dtype_errors:
            result.add_error(
                f"Column '{err['column']}' has type {err['actual']}, cannot cast to {err['expected']}",
                err
            )

        for warn in dtype_warnings:
            result.add_warning(
                f"Column '{warn['column']}' has type {warn['actual']}, can be cast to {warn['expected']}",
                warn
            )

        if not dtype_errors and not dtype_warnings:
            result.add_info("All column data types match schema")

        con.close()

    except Exception as e:
        _logger.error("Check 'column_dtypes' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking dtypes: {str(e)}")

    return result


def _check_castable_duckdb(con, col_name: str, target_type: str) -> bool:
    """Check if a column can be cast to the target type using DuckDB."""
    try:
        if target_type in ('INTEGER', 'INT'):
            con.execute(f'SELECT TRY_CAST("{col_name}" AS BIGINT) FROM df LIMIT 100')
        elif target_type in ('FLOAT', 'DOUBLE'):
            con.execute(f'SELECT TRY_CAST("{col_name}" AS DOUBLE) FROM df LIMIT 100')
        elif target_type == 'DATETIME':
            con.execute(f'SELECT TRY_CAST("{col_name}" AS TIMESTAMP) FROM df LIMIT 100')
        elif target_type == 'VARCHAR':
            con.execute(f'SELECT CAST("{col_name}" AS VARCHAR) FROM df LIMIT 100')
        return True
    except Exception:
        return False


def check_column_dtypes(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """
    Check if columns have correct data types.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema
    table_name : str
        Name of the table
    """
    _logger.debug("check_column_dtypes: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_column_dtypes_polars(df, schema, table_name)
    else:
        result = check_column_dtypes_duckdb(df, schema, table_name)
    _logger.debug("check_column_dtypes: table '%s' — checked=%s, errors=%s, warnings=%s",
                  table_name, result.metrics.get("columns_checked"),
                  result.metrics.get("dtype_errors"), result.metrics.get("dtype_warnings"))
    return result


# B.2. Datetime format validation
def check_datetime_format_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    expected_tz: str = 'UTC'
) -> DQAConformanceResult:
    """Validate datetime columns are in correct format using Polars."""
    result = DQAConformanceResult("datetime_format", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        schema_dict = lf.collect_schema()

        datetime_columns = [
            col['name'] for col in schema.get('columns', [])
            if col.get('data_type') == 'DATETIME' and col['name'] in schema_dict.names()
        ]

        result.metrics["datetime_columns_checked"] = len(datetime_columns)

        for col in datetime_columns:
            col_dtype = schema_dict[col]
            dtype_str = str(col_dtype)

            if 'Datetime' not in dtype_str and 'Date' not in dtype_str:
                result.add_warning(
                    f"Column '{col}' should be DATETIME but is {col_dtype}",
                    {"column": col, "actual_type": str(col_dtype)}
                )
                continue

            if expected_tz and 'Datetime' in dtype_str:
                if expected_tz not in dtype_str and 'time_zone' not in dtype_str:
                    result.add_info(
                        f"Column '{col}' may be timezone-naive, expected {expected_tz}",
                        {"column": col, "expected_tz": expected_tz}
                    )

        if not result.errors and not result.warnings:
            result.add_info("All datetime columns are properly formatted")

    except Exception as e:
        _logger.error("Check 'datetime_format' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking datetime format: {str(e)}")

    return result


def check_datetime_format_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str,
    expected_tz: str = 'UTC'
) -> DQAConformanceResult:
    """Validate datetime columns are in correct format using DuckDB."""
    result = DQAConformanceResult("datetime_format", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        describe_result = con.execute("DESCRIBE df").fetchall()
        actual_types = {row[0]: row[1].upper() for row in describe_result}

        datetime_columns = [
            col['name'] for col in schema.get('columns', [])
            if col.get('data_type') == 'DATETIME' and col['name'] in actual_types
        ]

        result.metrics["datetime_columns_checked"] = len(datetime_columns)

        for col in datetime_columns:
            col_dtype = actual_types[col]

            if 'TIMESTAMP' not in col_dtype and 'DATE' not in col_dtype:
                result.add_warning(
                    f"Column '{col}' should be DATETIME but is {col_dtype}",
                    {"column": col, "actual_type": col_dtype}
                )

        if not result.errors and not result.warnings:
            result.add_info("All datetime columns are properly formatted")

        con.close()

    except Exception as e:
        _logger.error("Check 'datetime_format' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking datetime format: {str(e)}")

    return result


def check_datetime_format(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    expected_tz: str = 'UTC'
) -> DQAConformanceResult:
    """Validate datetime columns are in correct format."""
    _logger.debug("check_datetime_format: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_datetime_format_polars(df, schema, table_name, expected_tz)
    else:
        result = check_datetime_format_duckdb(df, schema, table_name, expected_tz)
    _logger.debug("check_datetime_format: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("datetime_columns_checked"))
    return result


# B.3. Lab reference units validation
def check_lab_reference_units_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str = 'labs'
) -> DQAConformanceResult:
    """Check if lab reference units match schema definitions using Polars."""
    result = DQAConformanceResult("lab_reference_units", table_name)

    lab_units = schema.get('lab_reference_units', {})
    if not lab_units:
        result.add_info("No lab reference units defined in schema")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if 'lab_category' not in col_names or 'reference_unit' not in col_names:
            result.add_error("Missing required columns: lab_category and/or reference_unit")
            return result

        unit_counts = (
            lf
            .group_by(['lab_category', 'reference_unit'])
            .agg(pl.len().alias('count'))
            .collect(streaming=True)
        )

        invalid_units = []
        valid_count = 0
        total_count = 0

        for row in unit_counts.iter_rows(named=True):
            lab_cat = row['lab_category']
            ref_unit = row['reference_unit']
            count = row['count']
            total_count += count

            expected_units = lab_units.get(lab_cat, [])
            if expected_units:
                ref_unit_lower = str(ref_unit).lower().strip() if ref_unit else ''
                expected_lower = [str(u).lower().strip() for u in expected_units]

                if ref_unit_lower not in expected_lower and ref_unit not in expected_units:
                    invalid_units.append({
                        "lab_category": lab_cat,
                        "reference_unit": ref_unit,
                        "expected_units": expected_units,
                        "count": count
                    })
                else:
                    valid_count += count

        result.metrics["total_records"] = total_count
        result.metrics["valid_units"] = valid_count
        result.metrics["invalid_unit_categories"] = len(invalid_units)

        if invalid_units:
            invalid_units.sort(key=lambda x: x['count'], reverse=True)
            top_invalid = invalid_units[:20]

            result.add_warning(
                f"Found {len(invalid_units)} lab categories with non-standard units",
                {"top_invalid_units": top_invalid}
            )
        else:
            result.add_info("All lab reference units match schema definitions")
        gc.collect()

    except Exception as e:
        _logger.error("Check 'lab_reference_units' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking lab reference units: {str(e)}")

    return result


def check_lab_reference_units_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str = 'labs'
) -> DQAConformanceResult:
    """Check if lab reference units match schema definitions using DuckDB."""
    result = DQAConformanceResult("lab_reference_units", table_name)

    lab_units = schema.get('lab_reference_units', {})
    if not lab_units:
        result.add_info("No lab reference units defined in schema")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('labs_df', df)

        if 'lab_category' not in df.columns or 'reference_unit' not in df.columns:
            result.add_error("Missing required columns: lab_category and/or reference_unit")
            con.close()
            return result

        unit_counts = con.execute("""
            SELECT lab_category, reference_unit, COUNT(*) as count
            FROM labs_df
            GROUP BY lab_category, reference_unit
        """).fetchall()

        invalid_units = []
        valid_count = 0
        total_count = 0

        for row in unit_counts:
            lab_cat, ref_unit, count = row
            total_count += count

            expected_units = lab_units.get(lab_cat, [])
            if expected_units:
                ref_unit_lower = str(ref_unit).lower().strip() if ref_unit else ''
                expected_lower = [str(u).lower().strip() for u in expected_units]

                if ref_unit_lower not in expected_lower and ref_unit not in expected_units:
                    invalid_units.append({
                        "lab_category": lab_cat,
                        "reference_unit": ref_unit,
                        "expected_units": expected_units,
                        "count": int(count)
                    })
                else:
                    valid_count += count

        result.metrics["total_records"] = int(total_count)
        result.metrics["valid_units"] = int(valid_count)
        result.metrics["invalid_unit_categories"] = len(invalid_units)

        if invalid_units:
            invalid_units.sort(key=lambda x: x['count'], reverse=True)
            top_invalid = invalid_units[:20]

            result.add_warning(
                f"Found {len(invalid_units)} lab categories with non-standard units",
                {"top_invalid_units": top_invalid}
            )
        else:
            result.add_info("All lab reference units match schema definitions")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'lab_reference_units' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking lab reference units: {str(e)}")

    return result


def check_lab_reference_units(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str = 'labs'
) -> DQAConformanceResult:
    """Check if lab reference units match schema definitions."""
    _logger.debug("check_lab_reference_units: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_lab_reference_units_polars(df, schema, table_name)
    else:
        result = check_lab_reference_units_duckdb(df, schema, table_name)
    _logger.debug("check_lab_reference_units: table '%s' — valid=%s, invalid_categories=%s",
                  table_name, result.metrics.get("valid_units"),
                  result.metrics.get("invalid_unit_categories"))
    return result


# B.4. Categorical values validation
def check_categorical_values_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if categorical values match mCIDE permissible values using Polars."""
    result = DQAConformanceResult("categorical_values", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        category_columns = schema.get('category_columns', [])
        invalid_values_by_col = {}

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible or col_name not in col_names:
                continue

            if col_name not in category_columns:
                continue

            unique_vals = (
                lf
                .select(pl.col(col_name))
                .drop_nulls()
                .group_by(col_name)
                .agg(pl.len().alias('count'))
                .collect(streaming=True)
            )

            permissible_lower = {str(v).lower() for v in permissible}

            invalid_for_col = []
            for row in unique_vals.iter_rows(named=True):
                val = row[col_name]
                count = row['count']

                val_str = str(val).lower() if val is not None else ''
                if val_str not in permissible_lower and val not in permissible:
                    invalid_for_col.append({
                        "value": val,
                        "count": count
                    })

            if invalid_for_col:
                invalid_for_col.sort(key=lambda x: x['count'], reverse=True)
                invalid_values_by_col[col_name] = {
                    "invalid_values": invalid_for_col[:20],
                    "total_invalid_unique": len(invalid_for_col),
                    "permissible_values": permissible
                }

        result.metrics["category_columns_checked"] = len(category_columns)
        result.metrics["columns_with_invalid_values"] = len(invalid_values_by_col)

        for col_name, details in invalid_values_by_col.items():
            result.add_error(
                f"Column '{col_name}' has {details['total_invalid_unique']} invalid categorical values",
                {
                    "column": col_name,
                    "top_invalid": details['invalid_values'][:10],
                    "permissible_values": details['permissible_values']
                }
            )

        if not invalid_values_by_col:
            result.add_info("All categorical values match mCIDE permissible values")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'categorical_values' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking categorical values: {str(e)}")

    return result


def check_categorical_values_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if categorical values match mCIDE permissible values using DuckDB."""
    result = DQAConformanceResult("categorical_values", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        category_columns = schema.get('category_columns', [])
        invalid_values_by_col = {}

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible or col_name not in df.columns:
                continue

            if col_name not in category_columns:
                continue

            unique_vals = con.execute(f"""
                SELECT "{col_name}", COUNT(*) as count
                FROM df
                WHERE "{col_name}" IS NOT NULL
                GROUP BY "{col_name}"
            """).fetchdf()

            permissible_lower = {str(v).lower() for v in permissible}

            invalid_for_col = []
            for _, row in unique_vals.iterrows():
                val = row[col_name]
                count = row['count']

                val_str = str(val).lower() if val is not None else ''
                if val_str not in permissible_lower and val not in permissible:
                    invalid_for_col.append({
                        "value": val,
                        "count": int(count)
                    })

            if invalid_for_col:
                invalid_for_col.sort(key=lambda x: x['count'], reverse=True)
                invalid_values_by_col[col_name] = {
                    "invalid_values": invalid_for_col[:20],
                    "total_invalid_unique": len(invalid_for_col),
                    "permissible_values": permissible
                }

        result.metrics["category_columns_checked"] = len(category_columns)
        result.metrics["columns_with_invalid_values"] = len(invalid_values_by_col)

        for col_name, details in invalid_values_by_col.items():
            result.add_error(
                f"Column '{col_name}' has {details['total_invalid_unique']} invalid categorical values",
                {
                    "column": col_name,
                    "top_invalid": details['invalid_values'][:10],
                    "permissible_values": details['permissible_values']
                }
            )

        if not invalid_values_by_col:
            result.add_info("All categorical values match mCIDE permissible values")

        con.close()

    except Exception as e:
        _logger.error("Check 'categorical_values' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking categorical values: {str(e)}")

    return result


def check_categorical_values(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if categorical values match mCIDE permissible values."""
    _logger.debug("check_categorical_values: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_categorical_values_polars(df, schema, table_name)
    else:
        result = check_categorical_values_duckdb(df, schema, table_name)
    _logger.debug("check_categorical_values: table '%s' — columns_checked=%s, columns_with_invalid=%s",
                  table_name, result.metrics.get("category_columns_checked"),
                  result.metrics.get("columns_with_invalid_values"))
    return result


# B.5. Category-to-group mapping validation

def check_category_group_mapping_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if category-to-group mappings match schema definitions using Polars."""
    result = DQAConformanceResult("category_group_mapping", table_name)

    # Discover all *_category_to_group_mapping keys in the schema
    mapping_keys = [k for k in schema if k.endswith('_category_to_group_mapping')]
    if not mapping_keys:
        result.add_info("No category-to-group mappings defined in schema")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        for mapping_key in mapping_keys:
            mapping = schema[mapping_key]
            if not mapping:
                continue

            # Derive column names: e.g. "med_category_to_group_mapping" -> category_col="med_category", group_col="med_group"
            category_col = mapping_key.replace('_to_group_mapping', '')
            group_col = category_col.replace('_category', '_group')

            if category_col not in col_names or group_col not in col_names:
                result.add_info(
                    f"Skipping mapping '{mapping_key}': columns '{category_col}' and/or '{group_col}' not in DataFrame"
                )
                continue

            # Group by (category_col, group_col) where both non-null
            pair_counts = (
                lf
                .filter(pl.col(category_col).is_not_null() & pl.col(group_col).is_not_null())
                .group_by([category_col, group_col])
                .agg(pl.len().alias('count'))
                .collect(streaming=True)
            )

            mismatched = []
            valid_count = 0
            total_count = 0

            for row in pair_counts.iter_rows(named=True):
                cat_val = row[category_col]
                grp_val = row[group_col]
                count = row['count']
                total_count += count

                expected_group = mapping.get(cat_val)
                if expected_group is None:
                    # Category not in mapping — not a mismatch, just unmapped
                    valid_count += count
                    continue

                cat_lower = str(grp_val).lower().strip() if grp_val else ''
                expected_lower = str(expected_group).lower().strip()

                if cat_lower == expected_lower or grp_val == expected_group:
                    valid_count += count
                else:
                    mismatched.append({
                        "category": cat_val,
                        "actual_group": grp_val,
                        "expected_group": expected_group,
                        "count": count
                    })

            result.metrics[f"{mapping_key}_total_records"] = total_count
            result.metrics[f"{mapping_key}_valid_count"] = valid_count
            result.metrics[f"{mapping_key}_mismatch_count"] = len(mismatched)

            if mismatched:
                mismatched.sort(key=lambda x: x['count'], reverse=True)
                result.add_warning(
                    f"Found {len(mismatched)} mismatched category-group pairs for '{mapping_key}'",
                    {"mismatched_pairs": mismatched[:20]}
                )
            else:
                result.add_info(f"All category-group pairs match for '{mapping_key}'")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_group_mapping' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category-group mapping: {str(e)}")

    return result


def check_category_group_mapping_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if category-to-group mappings match schema definitions using DuckDB."""
    result = DQAConformanceResult("category_group_mapping", table_name)

    # Discover all *_category_to_group_mapping keys in the schema
    mapping_keys = [k for k in schema if k.endswith('_category_to_group_mapping')]
    if not mapping_keys:
        result.add_info("No category-to-group mappings defined in schema")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('mapping_df', df)
        col_names = list(df.columns)

        for mapping_key in mapping_keys:
            mapping = schema[mapping_key]
            if not mapping:
                continue

            # Derive column names
            category_col = mapping_key.replace('_to_group_mapping', '')
            group_col = category_col.replace('_category', '_group')

            if category_col not in col_names or group_col not in col_names:
                result.add_info(
                    f"Skipping mapping '{mapping_key}': columns '{category_col}' and/or '{group_col}' not in DataFrame"
                )
                continue

            # Group by (category_col, group_col) where both non-null
            pair_counts = con.execute(f"""
                SELECT "{category_col}", "{group_col}", COUNT(*) as count
                FROM mapping_df
                WHERE "{category_col}" IS NOT NULL AND "{group_col}" IS NOT NULL
                GROUP BY "{category_col}", "{group_col}"
            """).fetchall()

            mismatched = []
            valid_count = 0
            total_count = 0

            for row in pair_counts:
                cat_val, grp_val, count = row
                total_count += count

                expected_group = mapping.get(cat_val)
                if expected_group is None:
                    # Category not in mapping — not a mismatch, just unmapped
                    valid_count += count
                    continue

                cat_lower = str(grp_val).lower().strip() if grp_val else ''
                expected_lower = str(expected_group).lower().strip()

                if cat_lower == expected_lower or grp_val == expected_group:
                    valid_count += count
                else:
                    mismatched.append({
                        "category": cat_val,
                        "actual_group": grp_val,
                        "expected_group": expected_group,
                        "count": int(count)
                    })

            result.metrics[f"{mapping_key}_total_records"] = int(total_count)
            result.metrics[f"{mapping_key}_valid_count"] = int(valid_count)
            result.metrics[f"{mapping_key}_mismatch_count"] = len(mismatched)

            if mismatched:
                mismatched.sort(key=lambda x: x['count'], reverse=True)
                result.add_warning(
                    f"Found {len(mismatched)} mismatched category-group pairs for '{mapping_key}'",
                    {"mismatched_pairs": mismatched[:20]}
                )
            else:
                result.add_info(f"All category-group pairs match for '{mapping_key}'")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_group_mapping' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category-group mapping: {str(e)}")

    return result


def check_category_group_mapping(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQAConformanceResult:
    """Check if category-to-group mappings match schema definitions."""
    _logger.debug("check_category_group_mapping: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_category_group_mapping_polars(df, schema, table_name)
    else:
        result = check_category_group_mapping_duckdb(df, schema, table_name)
    _logger.debug("check_category_group_mapping: table '%s' — completed", table_name)
    return result


# ---------------------------------------------------------------------------
# COMPLETENESS CHECKS
# ---------------------------------------------------------------------------

# A.1. Missingness in required columns
def check_missingness_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> DQACompletenessResult:
    """Check missingness in required columns using Polars."""
    result = DQACompletenessResult("missingness", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        required_columns = schema.get('required_columns', [])
        required_in_df = [c for c in required_columns if c in col_names]

        total_rows = lf.select(pl.len()).collect()[0, 0]

        if total_rows == 0:
            result.add_error("DataFrame is empty")
            return result

        null_exprs = [
            pl.col(c).is_null().sum().alias(f"{c}_null")
            for c in required_in_df
        ]

        null_counts = lf.select(null_exprs).collect(streaming=True)

        missingness_stats = []
        high_missingness = []

        for col in required_in_df:
            null_count = null_counts[0, f"{col}_null"]
            pct_missing = (null_count / total_rows) * 100

            missingness_stats.append({
                "column": col,
                "null_count": int(null_count),
                "total_rows": int(total_rows),
                "percent_missing": round(pct_missing, 2)
            })

            if pct_missing >= error_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "error"
                })
            elif pct_missing >= warning_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "warning"
                })

        missingness_stats.sort(key=lambda x: x['percent_missing'], reverse=True)

        result.metrics["total_rows"] = int(total_rows)
        result.metrics["required_columns_checked"] = len(required_in_df)
        result.metrics["missingness_stats"] = missingness_stats

        for item in high_missingness:
            if item["severity"] == "error":
                result.add_error(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )
            else:
                result.add_warning(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )

        if not high_missingness:
            result.add_info("All required columns have acceptable missingness levels")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'missingness' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking missingness: {str(e)}")

    return result


def check_missingness_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> DQACompletenessResult:
    """Check missingness in required columns using DuckDB."""
    result = DQACompletenessResult("missingness", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        required_columns = schema.get('required_columns', [])
        required_in_df = [c for c in required_columns if c in df.columns]

        total_rows = con.execute("SELECT COUNT(*) FROM df").fetchone()[0]

        if total_rows == 0:
            result.add_error("DataFrame is empty")
            con.close()
            return result

        # Build a single query for all null counts (efficient single scan)
        null_count_exprs = [f'COUNT(*) - COUNT("{col}") as "{col}_null"' for col in required_in_df]
        if null_count_exprs:
            null_query = f"SELECT {', '.join(null_count_exprs)} FROM df"
            null_counts = con.execute(null_query).fetchone()
        else:
            null_counts = []

        missingness_stats = []
        high_missingness = []

        for i, col in enumerate(required_in_df):
            null_count = null_counts[i] if null_counts else 0
            pct_missing = (null_count / total_rows) * 100

            missingness_stats.append({
                "column": col,
                "null_count": int(null_count),
                "total_rows": int(total_rows),
                "percent_missing": round(pct_missing, 2)
            })

            if pct_missing >= error_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "error"
                })
            elif pct_missing >= warning_threshold:
                high_missingness.append({
                    "column": col,
                    "percent_missing": round(pct_missing, 2),
                    "severity": "warning"
                })

        missingness_stats.sort(key=lambda x: x['percent_missing'], reverse=True)

        result.metrics["total_rows"] = int(total_rows)
        result.metrics["required_columns_checked"] = len(required_in_df)
        result.metrics["missingness_stats"] = missingness_stats

        for item in high_missingness:
            if item["severity"] == "error":
                result.add_error(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )
            else:
                result.add_warning(
                    f"Column '{item['column']}' has {item['percent_missing']}% missing values",
                    item
                )

        if not high_missingness:
            result.add_info("All required columns have acceptable missingness levels")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'missingness' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking missingness: {str(e)}")

    return result


def check_missingness(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> DQACompletenessResult:
    """
    Check missingness in required columns.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate (already loaded)
    schema : dict
        Table schema containing required_columns
    table_name : str
        Name of the table
    error_threshold : float
        Percent missing above which an error is raised
    warning_threshold : float
        Percent missing above which a warning is raised

    Returns
    -------
    DQACompletenessResult
        Result containing missingness statistics
    """
    _logger.debug("check_missingness: starting for table '%s' (error_threshold=%.1f%%, warning_threshold=%.1f%%)",
                  table_name, error_threshold, warning_threshold)
    if _ACTIVE_BACKEND == 'polars':
        result = check_missingness_polars(df, schema, table_name, error_threshold, warning_threshold)
    else:
        result = check_missingness_duckdb(df, schema, table_name, error_threshold, warning_threshold)
    if result.errors:
        for err in result.errors:
            _logger.info("check_missingness: table '%s' — %s", table_name, err["message"])
    _logger.debug("check_missingness: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("required_columns_checked"))
    return result


# A.2. Conditional required fields
def _load_validation_rules() -> Dict[str, Any]:
    """Load the centralised validation rules YAML."""
    rules_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'schemas', 'validation_rules.yaml'
    )
    if not os.path.exists(rules_path):
        _logger.warning("Validation rules file not found: %s", rules_path)
        return {}
    with open(rules_path, 'r') as f:
        return yaml.safe_load(f) or {}


def _get_default_conditions(table_name: str) -> List[Dict[str, Any]]:
    """Load default conditional requirements for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('conditional_requirements', {}).get(table_name, [])


def check_conditional_requirements_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields using Polars."""
    result = DQACompletenessResult("conditional_requirements", table_name)

    if conditions is None:
        conditions = _get_default_conditions(table_name)

    if not conditions:
        result.add_info("No conditional requirements defined for this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        for cond in conditions:
            when_col = cond['when_column']
            when_values = cond['when_value']
            then_required = cond['then_required']
            description = cond.get('description', '')

            if when_col not in col_names:
                continue

            if not isinstance(when_values, list):
                when_values = [when_values]

            filtered = lf.filter(pl.col(when_col).is_in(when_values))

            # Optional compound condition: and_column / and_value
            and_col = cond.get('and_column')
            and_values = cond.get('and_value')
            if and_col and and_values is not None:
                if and_col not in col_names:
                    continue
                if not isinstance(and_values, list):
                    and_values = [and_values]
                filtered = filtered.filter(pl.col(and_col).is_in(and_values))

            condition_label = f"{when_col} IN {when_values}"
            if and_col and and_values is not None:
                condition_label += f" AND {and_col} IN {and_values}"

            for req_col in then_required:
                if req_col not in col_names:
                    continue

                stats = filtered.select([
                    pl.len().alias('total'),
                    pl.col(req_col).is_null().sum().alias('null_count')
                ]).collect(streaming=True)

                total = stats[0, 'total']
                null_count = stats[0, 'null_count']

                if total > 0 and null_count > 0:
                    pct_missing = (null_count / total) * 100
                    result.add_warning(
                        f"Conditional requirement violated: {description}",
                        {
                            "condition": condition_label,
                            "required_column": req_col,
                            "rows_meeting_condition": int(total),
                            "rows_with_missing": int(null_count),
                            "percent_missing": round(pct_missing, 2)
                        }
                    )

        if not result.warnings and not result.errors:
            result.add_info("All conditional requirements satisfied")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'conditional_requirements' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking conditional requirements: {str(e)}")

    return result


def check_conditional_requirements_duckdb(
    df: pd.DataFrame,
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields using DuckDB."""
    result = DQACompletenessResult("conditional_requirements", table_name)

    if conditions is None:
        conditions = _get_default_conditions(table_name)

    if not conditions:
        result.add_info("No conditional requirements defined for this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        for cond in conditions:
            when_col = cond['when_column']
            when_values = cond['when_value']
            then_required = cond['then_required']
            description = cond.get('description', '')

            if when_col not in df.columns:
                continue

            if not isinstance(when_values, list):
                when_values = [when_values]

            # Optional compound condition: and_column / and_value
            and_col = cond.get('and_column')
            and_values = cond.get('and_value')
            and_clause = ""
            if and_col and and_values is not None:
                if and_col not in df.columns:
                    continue
                if not isinstance(and_values, list):
                    and_values = [and_values]
                and_values_str = ', '.join([f"'{v.lower()}'" for v in and_values])
                and_clause = f' AND LOWER("{and_col}") IN ({and_values_str})'

            condition_label = f"{when_col} IN {when_values}"
            if and_col and and_values is not None:
                condition_label += f" AND {and_col} IN {and_values}"

            for req_col in then_required:
                if req_col not in df.columns:
                    continue

                values_str = ', '.join([f"'{v.lower()}'" for v in when_values])
                stats = con.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT(*) - COUNT("{req_col}") as null_count
                    FROM df
                    WHERE LOWER("{when_col}") IN ({values_str}){and_clause}
                """).fetchone()

                total, null_count = stats

                if total > 0 and null_count > 0:
                    pct_missing = (null_count / total) * 100
                    result.add_warning(
                        f"Conditional requirement violated: {description}",
                        {
                            "condition": condition_label,
                            "required_column": req_col,
                            "rows_meeting_condition": int(total),
                            "rows_with_missing": int(null_count),
                            "percent_missing": round(pct_missing, 2)
                        }
                    )

        if not result.warnings and not result.errors:
            result.add_info("All conditional requirements satisfied")

        con.close()

    except Exception as e:
        _logger.error("Check 'conditional_requirements' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking conditional requirements: {str(e)}")

    return result


def check_conditional_requirements(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields."""
    n_conditions = len(conditions) if conditions else 0
    _logger.debug("check_conditional_requirements: starting for table '%s' (%d explicit conditions)",
                  table_name, n_conditions)
    if _ACTIVE_BACKEND == 'polars':
        result = check_conditional_requirements_polars(df, table_name, conditions)
    else:
        result = check_conditional_requirements_duckdb(df, table_name, conditions)
    if result.warnings:
        _logger.info("check_conditional_requirements: table '%s' — %d violations found",
                     table_name, len(result.warnings))
    _logger.debug("check_conditional_requirements: table '%s' complete", table_name)
    return result

# B. mCIDE value coverage
def check_mcide_value_coverage_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQACompletenessResult:
    """Check if all mCIDE standardized values are present in the data using Polars."""
    result = DQACompletenessResult("mcide_value_coverage", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        category_columns = schema.get('category_columns', [])
        coverage_by_col = {}

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible or col_name not in col_names:
                continue

            if col_name not in category_columns:
                continue

            unique_vals = (
                lf
                .select(pl.col(col_name).drop_nulls().unique())
                .collect(streaming=True)
                .to_series()
                .to_list()
            )

            unique_vals_lower = {str(v).lower() for v in unique_vals if v is not None}

            missing_vals = [
                v for v in permissible
                if str(v).lower() not in unique_vals_lower
            ]

            coverage_pct = ((len(permissible) - len(missing_vals)) / len(permissible)) * 100

            coverage_by_col[col_name] = {
                "expected_values": len(permissible),
                "found_values": len(permissible) - len(missing_vals),
                "missing_values": missing_vals,
                "coverage_percent": round(coverage_pct, 2)
            }

        result.metrics["category_columns_checked"] = len(coverage_by_col)
        result.metrics["coverage_by_column"] = coverage_by_col

        for col_name, details in coverage_by_col.items():
            if details['missing_values']:
                result.add_info(
                    f"Column '{col_name}' is missing {len(details['missing_values'])} mCIDE values",
                    {
                        "column": col_name,
                        "missing_values": details['missing_values'],
                        "coverage_percent": details['coverage_percent']
                    }
                )

        gc.collect()

    except Exception as e:
        _logger.error("Check 'mcide_value_coverage' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking mCIDE value coverage: {str(e)}")

    return result


def check_mcide_value_coverage_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str
) -> DQACompletenessResult:
    """Check if all mCIDE standardized values are present in the data using DuckDB."""
    result = DQACompletenessResult("mcide_value_coverage", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        category_columns = schema.get('category_columns', [])
        coverage_by_col = {}

        for col_spec in schema.get('columns', []):
            col_name = col_spec['name']
            permissible = col_spec.get('permissible_values', [])

            if not permissible or col_name not in df.columns:
                continue

            if col_name not in category_columns:
                continue

            unique_vals = con.execute(f"""
                SELECT DISTINCT "{col_name}" FROM df WHERE "{col_name}" IS NOT NULL
            """).fetchdf()[col_name].tolist()

            unique_vals_lower = {str(v).lower() for v in unique_vals if v is not None}

            missing_vals = [
                v for v in permissible
                if str(v).lower() not in unique_vals_lower
            ]

            coverage_pct = ((len(permissible) - len(missing_vals)) / len(permissible)) * 100

            coverage_by_col[col_name] = {
                "expected_values": len(permissible),
                "found_values": len(permissible) - len(missing_vals),
                "missing_values": missing_vals,
                "coverage_percent": round(coverage_pct, 2)
            }

        result.metrics["category_columns_checked"] = len(coverage_by_col)
        result.metrics["coverage_by_column"] = coverage_by_col

        for col_name, details in coverage_by_col.items():
            if details['missing_values']:
                result.add_info(
                    f"Column '{col_name}' is missing {len(details['missing_values'])} mCIDE values",
                    {
                        "column": col_name,
                        "missing_values": details['missing_values'],
                        "coverage_percent": details['coverage_percent']
                    }
                )

        con.close()

    except Exception as e:
        _logger.error("Check 'mcide_value_coverage' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking mCIDE value coverage: {str(e)}")

    return result


def check_mcide_value_coverage(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> DQACompletenessResult:
    """Check if all mCIDE standardized values are present in the data."""
    _logger.debug("check_mcide_value_coverage: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_mcide_value_coverage_polars(df, schema, table_name)
    else:
        result = check_mcide_value_coverage_duckdb(df, schema, table_name)
    _logger.debug("check_mcide_value_coverage: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("category_columns_checked"))
    return result

# C. Relational integrity checks
def check_relational_integrity_polars(
    source_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    reference_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    source_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check relational integrity between tables using Polars."""
    result = DQACompletenessResult("relational_integrity", f"{source_table}->{reference_table}")

    try:
        source_lf = source_df if isinstance(source_df, pl.LazyFrame) else source_df.lazy()
        ref_lf = reference_df if isinstance(reference_df, pl.LazyFrame) else reference_df.lazy()

        source_ids = (
            source_lf
            .select(pl.col(key_column).drop_nulls().unique())
            .collect(streaming=True)
        )

        ref_ids = (
            ref_lf
            .select(pl.col(key_column).drop_nulls().unique())
            .collect(streaming=True)
        )

        source_id_set = set(source_ids[key_column].to_list())
        ref_id_set = set(ref_ids[key_column].to_list())

        orphan_ids = source_id_set - ref_id_set

        total_source = len(source_id_set)
        total_orphan = len(orphan_ids)
        coverage_pct = ((total_source - total_orphan) / total_source * 100) if total_source > 0 else 100

        result.metrics["source_unique_ids"] = total_source
        result.metrics["reference_unique_ids"] = len(ref_id_set)
        result.metrics["orphan_ids"] = total_orphan
        result.metrics["coverage_percent"] = round(coverage_pct, 2)

        if orphan_ids:
            result.add_warning(
                f"{total_orphan} {key_column} values in {source_table} not found in {reference_table}",
                {
                    "orphan_count": total_orphan,
                    "sample_orphan_ids": list(orphan_ids)[:10],
                    "coverage_percent": round(coverage_pct, 2)
                }
            )
        else:
            result.add_info(f"All {key_column} values in {source_table} exist in {reference_table}")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'relational_integrity' failed for '%s' -> '%s': %s", source_table, reference_table, e)
        result.add_error(f"Error checking relational integrity: {str(e)}")

    return result


def check_relational_integrity_duckdb(
    source_df: pd.DataFrame,
    reference_df: pd.DataFrame,
    source_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check relational integrity between tables using DuckDB."""
    result = DQACompletenessResult("relational_integrity", f"{source_table}->{reference_table}")

    try:
        con = duckdb.connect(':memory:')
        con.register('source_tbl', source_df)
        con.register('ref_tbl', reference_df)

        # Efficient anti-join query
        orphan_query = f"""
            WITH source_ids AS (
                SELECT DISTINCT "{key_column}" as id
                FROM source_tbl
                WHERE "{key_column}" IS NOT NULL
            ),
            ref_ids AS (
                SELECT DISTINCT "{key_column}" as id
                FROM ref_tbl
                WHERE "{key_column}" IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM source_ids) as total_source,
                (SELECT COUNT(*) FROM ref_ids) as total_ref,
                (SELECT COUNT(*) FROM source_ids s
                 WHERE NOT EXISTS (SELECT 1 FROM ref_ids r WHERE r.id = s.id)) as orphan_count
        """

        stats = con.execute(orphan_query).fetchone()
        total_source, total_ref, orphan_count = stats

        coverage_pct = ((total_source - orphan_count) / total_source * 100) if total_source > 0 else 100

        result.metrics["source_unique_ids"] = int(total_source)
        result.metrics["reference_unique_ids"] = int(total_ref)
        result.metrics["orphan_ids"] = int(orphan_count)
        result.metrics["coverage_percent"] = round(coverage_pct, 2)

        if orphan_count > 0:
            # Fetch sample orphans lazily with LIMIT
            sample_query = f"""
                SELECT DISTINCT s."{key_column}"
                FROM source_tbl s
                WHERE s."{key_column}" IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM ref_tbl r
                      WHERE r."{key_column}" = s."{key_column}"
                  )
                LIMIT 10
            """
            sample_result = con.execute(sample_query).fetchall()
            sample_orphans = [row[0] for row in sample_result]

            result.add_warning(
                f"{orphan_count} {key_column} values in {source_table} not found in {reference_table}",
                {
                    "orphan_count": int(orphan_count),
                    "sample_orphan_ids": sample_orphans,
                    "coverage_percent": round(coverage_pct, 2)
                }
            )
        else:
            result.add_info(f"All {key_column} values in {source_table} exist in {reference_table}")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'relational_integrity' failed for '%s' -> '%s': %s", source_table, reference_table, e)
        result.add_error(f"Error checking relational integrity: {str(e)}")

    return result


def check_relational_integrity(
    target_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    reference_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check bidirectional relational integrity between tables.

    Runs the backend-specific check in both directions:
    - **Forward** (reference → target): What percentage of reference IDs
      appear in the target table?  (e.g., "what % of hospitalizations
      have labs?")
    - **Reverse** (target → reference): What percentage of target IDs
      exist in the reference table?  (e.g., "what % of lab hosp_ids are
      valid?")

    Parameters
    ----------
    target_df : DataFrame
        The target table (e.g., labs).
    reference_df : DataFrame
        The reference table (e.g., hospitalization).
    target_table : str
        Name of the target table.
    reference_table : str
        Name of the reference table.
    key_column : str
        The shared key column (e.g., ``hospitalization_id``).

    Returns
    -------
    DQACompletenessResult
        Consolidated result with forward/reverse coverage metrics.
    """
    _logger.debug(
        "check_relational_integrity: '%s' <-> '%s' on key '%s'",
        target_table, reference_table, key_column,
    )
    result = DQACompletenessResult(
        "relational_integrity",
        f"{target_table}<->{reference_table}",
    )

    # Pick the right backend dispatcher
    _backend_fn = (check_relational_integrity_polars
                   if _ACTIVE_BACKEND == 'polars'
                   else check_relational_integrity_duckdb)

    try:
        # Forward: reference → target  (source=reference, ref=target)
        fwd = _backend_fn(
            reference_df, target_df, reference_table, target_table, key_column
        )
        # Reverse: target → reference  (source=target, ref=reference)
        rev = _backend_fn(
            target_df, reference_df, target_table, reference_table, key_column
        )

        result.metrics["forward_coverage_percent"] = fwd.metrics.get("coverage_percent", 0)
        result.metrics["forward_orphan_ids"] = fwd.metrics.get("orphan_ids", 0)
        result.metrics["forward_reference_unique_ids"] = fwd.metrics.get("source_unique_ids", 0)
        result.metrics["reverse_coverage_percent"] = rev.metrics.get("coverage_percent", 0)
        result.metrics["reverse_orphan_ids"] = rev.metrics.get("orphan_ids", 0)
        result.metrics["reverse_target_unique_ids"] = rev.metrics.get("source_unique_ids", 0)

        # Propagate warnings/errors from both directions
        for w in fwd.warnings:
            result.add_warning(f"[forward] {w['message']}", w.get("details", {}))
        for w in rev.warnings:
            result.add_warning(f"[reverse] {w['message']}", w.get("details", {}))
        for e in fwd.errors:
            result.add_error(f"[forward] {e['message']}", e.get("details", {}))
        for e in rev.errors:
            result.add_error(f"[reverse] {e['message']}", e.get("details", {}))

        # Info when both directions are clean
        if fwd.passed and rev.passed:
            result.add_info(
                f"Full bidirectional coverage for {key_column} between "
                f"{target_table} and {reference_table}"
            )

        _logger.info(
            "check_relational_integrity: '%s' <-> '%s' — "
            "fwd_coverage=%.1f%%, rev_coverage=%.1f%%",
            target_table, reference_table,
            result.metrics["forward_coverage_percent"],
            result.metrics["reverse_coverage_percent"],
        )

    except Exception as e:
        _logger.error(
            "Check 'relational_integrity' failed for '%s' <-> '%s': %s",
            target_table, reference_table, e,
        )
        result.add_error(f"Error checking relational integrity: {str(e)}")

    return result


# ---------------------------------------------------------------------------
# PLAUSIBILITY CHECKS
# ---------------------------------------------------------------------------

# Helpers for plausibility checks

def _load_outlier_config() -> Dict[str, Any]:
    """Load the outlier_config.yaml for numeric range checks."""
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'schemas', 'outlier_config.yaml'
    )
    if not os.path.exists(config_path):
        _logger.warning("Outlier config file not found: %s", config_path)
        return {}
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}


def _get_temporal_ordering_rules(table_name: str) -> List[Dict[str, str]]:
    """Load temporal ordering rules for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('temporal_ordering', {}).get(table_name, [])


def _get_field_plausibility_rules(table_name: str) -> List[Dict[str, Any]]:
    """Load field plausibility rules for a table from validation_rules.yaml."""
    rules = _load_validation_rules()
    return rules.get('field_plausibility_rules', {}).get(table_name, [])


def _get_composite_keys(table_name: str, schema: Optional[Dict[str, Any]] = None) -> List[str]:
    """Get composite keys for a table from schema or validation_rules.yaml."""
    if schema and 'composite_keys' in schema:
        return schema['composite_keys']
    rules = _load_validation_rules()
    entry = rules.get('composite_keys', {}).get(table_name, {})
    return entry.get('keys', [])


# Category column mapping for numeric range checks
_CATEGORY_COLUMN_MAP = {
    'labs': {'lab_value_numeric': 'lab_category'},
    'vitals': {'vital_value': 'vital_category'},
    'patient_assessments': {'numerical_value': 'assessment_category'},
    'medication_admin_continuous': {'med_dose': ('med_category', 'med_dose_unit')},
    'medication_admin_intermittent': {'med_dose': ('med_category', 'med_dose_unit')},
    'ecmo_mcs': {'sweep': 'device_category', 'flow': 'device_category', 'fdO2': 'device_category'},
}

# Datetime columns per table for cross-table temporal checks
_CROSS_TABLE_TIME_COLUMNS = {
    'adt': ['in_dttm', 'out_dttm'],
    'labs': ['lab_order_dttm', 'lab_collect_dttm', 'lab_result_dttm'],
    'vitals': ['recorded_dttm'],
    'respiratory_support': ['recorded_dttm'],
    'medication_admin_continuous': ['admin_dttm'],
    'medication_admin_intermittent': ['admin_dttm'],
    'patient_assessments': ['recorded_dttm'],
    'position': ['recorded_dttm'],
    'microbiology_culture': ['order_dttm', 'collect_dttm', 'result_dttm'],
    'microbiology_nonculture': ['order_dttm', 'collect_dttm', 'result_dttm'],
    'crrt_therapy': ['recorded_dttm'],
    'ecmo_mcs': ['recorded_dttm'],
}

# Time-denominator patterns for medication dose unit checks
_TIME_DENOMINATOR_PATTERNS = ['/sec', '/min', '/hr', '/hour', '/day']


# ---------------------------------------------------------------------------
# A.1 Temporal ordering
# ---------------------------------------------------------------------------

def check_temporal_ordering_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    temporal_rules: Optional[List[Dict[str, str]]] = None,
) -> DQAPlausibilityResult:
    """Check that datetime pairs follow expected temporal ordering using Polars."""
    result = DQAPlausibilityResult("temporal_ordering", table_name)

    if temporal_rules is None:
        temporal_rules = _get_temporal_ordering_rules(table_name)

    if not temporal_rules:
        result.add_info("No temporal ordering rules defined for this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        violations_by_pair = {}

        for rule in temporal_rules:
            earlier = rule['earlier']
            later = rule['later']
            description = rule.get('description', f"{earlier} <= {later}")

            if earlier not in col_names or later not in col_names:
                continue

            applicable = lf.filter(
                pl.col(earlier).is_not_null() & pl.col(later).is_not_null()
            )

            stats = applicable.select([
                pl.len().alias('total'),
                (pl.col(earlier) > pl.col(later)).sum().alias('violations')
            ]).collect(streaming=True)

            total = stats[0, 'total']
            violation_count = stats[0, 'violations']
            pct = (violation_count / total * 100) if total > 0 else 0

            violations_by_pair[f"{earlier}->{later}"] = {
                "total_applicable": int(total),
                "violations": int(violation_count),
                "violation_percent": round(pct, 2),
                "description": description,
            }

            if pct > 5:
                result.add_error(
                    f"Temporal ordering violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )
            elif violation_count > 0:
                result.add_warning(
                    f"Temporal ordering violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )

        result.metrics["pairs_checked"] = len(violations_by_pair)
        result.metrics["violations_by_pair"] = violations_by_pair

        if not result.errors and not result.warnings:
            result.add_info("All temporal ordering constraints satisfied")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'temporal_ordering' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking temporal ordering: {str(e)}")

    return result


def check_temporal_ordering_duckdb(
    df: pd.DataFrame,
    table_name: str,
    temporal_rules: Optional[List[Dict[str, str]]] = None,
) -> DQAPlausibilityResult:
    """Check that datetime pairs follow expected temporal ordering using DuckDB."""
    result = DQAPlausibilityResult("temporal_ordering", table_name)

    if temporal_rules is None:
        temporal_rules = _get_temporal_ordering_rules(table_name)

    if not temporal_rules:
        result.add_info("No temporal ordering rules defined for this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)
        violations_by_pair = {}

        for rule in temporal_rules:
            earlier = rule['earlier']
            later = rule['later']
            description = rule.get('description', f"{earlier} <= {later}")

            if earlier not in df.columns or later not in df.columns:
                continue

            stats = con.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN "{earlier}" > "{later}" THEN 1 ELSE 0 END) as violations
                FROM df
                WHERE "{earlier}" IS NOT NULL AND "{later}" IS NOT NULL
            """).fetchone()

            total, violation_count = stats
            pct = (violation_count / total * 100) if total > 0 else 0

            violations_by_pair[f"{earlier}->{later}"] = {
                "total_applicable": int(total),
                "violations": int(violation_count),
                "violation_percent": round(pct, 2),
                "description": description,
            }

            if pct > 5:
                result.add_error(
                    f"Temporal ordering violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )
            elif violation_count > 0:
                result.add_warning(
                    f"Temporal ordering violation: {description} — {violation_count}/{total} rows ({pct:.1f}%)",
                    {"pair": f"{earlier}->{later}", "violations": int(violation_count),
                     "total": int(total), "percent": round(pct, 2)}
                )

        result.metrics["pairs_checked"] = len(violations_by_pair)
        result.metrics["violations_by_pair"] = violations_by_pair

        if not result.errors and not result.warnings:
            result.add_info("All temporal ordering constraints satisfied")

        con.close()

    except Exception as e:
        _logger.error("Check 'temporal_ordering' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking temporal ordering: {str(e)}")

    return result


def check_temporal_ordering(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    temporal_rules: Optional[List[Dict[str, str]]] = None,
) -> DQAPlausibilityResult:
    """Check that datetime pairs follow expected temporal ordering."""
    _logger.debug("check_temporal_ordering: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_temporal_ordering_polars(df, table_name, temporal_rules)
    else:
        result = check_temporal_ordering_duckdb(df, table_name, temporal_rules)
    _logger.debug("check_temporal_ordering: table '%s' — pairs_checked=%s",
                  table_name, result.metrics.get("pairs_checked"))
    return result


# ---------------------------------------------------------------------------
# A.2 Numeric range plausibility
# ---------------------------------------------------------------------------

def check_numeric_range_plausibility_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    outlier_config: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check numeric values are within plausible ranges using Polars."""
    result = DQAPlausibilityResult("numeric_range_plausibility", table_name)

    if outlier_config is None:
        outlier_config = _load_outlier_config()

    table_config = outlier_config.get('tables', {}).get(table_name, {})
    if not table_config:
        result.add_info("No numeric range configuration for this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        oor_summary = {}
        cat_map = _CATEGORY_COLUMN_MAP.get(table_name, {})

        for col_name, col_ranges in table_config.items():
            if col_name not in col_names:
                continue

            if isinstance(col_ranges, dict) and 'min' in col_ranges and 'max' in col_ranges:
                # Simple range
                rmin, rmax = col_ranges['min'], col_ranges['max']
                stats = lf.select([
                    pl.col(col_name).drop_nulls().len().alias('total'),
                    ((pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)).sum().alias('oor'),
                    (pl.col(col_name) < rmin).sum().alias('below'),
                    (pl.col(col_name) > rmax).sum().alias('above'),
                ]).collect(streaming=True)

                total = stats[0, 'total']
                oor = stats[0, 'oor']
                below = stats[0, 'below']
                above = stats[0, 'above']
                pct = (oor / total * 100) if total > 0 else 0

                oor_summary[col_name] = {
                    "total_non_null": int(total), "out_of_range": int(oor),
                    "out_of_range_percent": round(pct, 2),
                    "below_min": int(below), "above_max": int(above),
                    "min": rmin, "max": rmax,
                }

                if pct > 10:
                    result.add_warning(
                        f"Column '{col_name}': {oor}/{total} values ({pct:.1f}%) outside range [{rmin}, {rmax}]",
                        {"column": col_name, "percent": round(pct, 2)}
                    )

            elif isinstance(col_ranges, dict):
                # Category-dependent ranges
                cat_col_info = cat_map.get(col_name)
                if cat_col_info is None:
                    continue

                if isinstance(cat_col_info, tuple):
                    # 2-level: (category_col, unit_col)
                    cat_col, unit_col = cat_col_info
                    if cat_col not in col_names or unit_col not in col_names:
                        continue
                    total_oor = 0
                    total_count = 0
                    for cat_val, unit_ranges in col_ranges.items():
                        if not isinstance(unit_ranges, dict):
                            continue
                        for unit_val, ranges in unit_ranges.items():
                            if not isinstance(ranges, dict) or 'min' not in ranges:
                                continue
                            rmin, rmax = ranges['min'], ranges['max']
                            filtered = lf.filter(
                                (pl.col(cat_col).cast(pl.Utf8).str.to_lowercase() == str(cat_val).lower()) &
                                (pl.col(unit_col).cast(pl.Utf8).str.to_lowercase() == str(unit_val).lower()) &
                                pl.col(col_name).is_not_null()
                            )
                            stats = filtered.select([
                                pl.len().alias('total'),
                                ((pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)).sum().alias('oor'),
                            ]).collect(streaming=True)
                            total_count += stats[0, 'total']
                            total_oor += stats[0, 'oor']

                    pct = (total_oor / total_count * 100) if total_count > 0 else 0
                    oor_summary[col_name] = {
                        "total_non_null": int(total_count), "out_of_range": int(total_oor),
                        "out_of_range_percent": round(pct, 2),
                    }
                    if pct > 10:
                        result.add_warning(
                            f"Column '{col_name}': {total_oor}/{total_count} values ({pct:.1f}%) outside plausible ranges",
                            {"column": col_name, "percent": round(pct, 2)}
                        )
                else:
                    # 1-level category-dependent
                    cat_col = cat_col_info
                    if cat_col not in col_names:
                        continue
                    total_oor = 0
                    total_count = 0
                    for cat_val, ranges in col_ranges.items():
                        if not isinstance(ranges, dict) or 'min' not in ranges:
                            continue
                        rmin, rmax = ranges['min'], ranges['max']
                        filtered = lf.filter(
                            (pl.col(cat_col).cast(pl.Utf8).str.to_lowercase() == str(cat_val).lower()) &
                            pl.col(col_name).is_not_null()
                        )
                        stats = filtered.select([
                            pl.len().alias('total'),
                            ((pl.col(col_name) < rmin) | (pl.col(col_name) > rmax)).sum().alias('oor'),
                        ]).collect(streaming=True)
                        total_count += stats[0, 'total']
                        total_oor += stats[0, 'oor']

                    pct = (total_oor / total_count * 100) if total_count > 0 else 0
                    oor_summary[col_name] = {
                        "total_non_null": int(total_count), "out_of_range": int(total_oor),
                        "out_of_range_percent": round(pct, 2),
                    }
                    if pct > 10:
                        result.add_warning(
                            f"Column '{col_name}': {total_oor}/{total_count} values ({pct:.1f}%) outside plausible ranges",
                            {"column": col_name, "percent": round(pct, 2)}
                        )

        result.metrics["columns_checked"] = len(oor_summary)
        result.metrics["out_of_range_summary"] = oor_summary

        if not result.warnings and not result.errors:
            result.add_info("All numeric values within plausible ranges")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'numeric_range_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking numeric range plausibility: {str(e)}")

    return result


def check_numeric_range_plausibility_duckdb(
    df: pd.DataFrame,
    table_name: str,
    outlier_config: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check numeric values are within plausible ranges using DuckDB."""
    result = DQAPlausibilityResult("numeric_range_plausibility", table_name)

    if outlier_config is None:
        outlier_config = _load_outlier_config()

    table_config = outlier_config.get('tables', {}).get(table_name, {})
    if not table_config:
        result.add_info("No numeric range configuration for this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)
        actual_cols = list(df.columns)
        oor_summary = {}
        cat_map = _CATEGORY_COLUMN_MAP.get(table_name, {})

        for col_name, col_ranges in table_config.items():
            if col_name not in actual_cols:
                continue

            if isinstance(col_ranges, dict) and 'min' in col_ranges and 'max' in col_ranges:
                rmin, rmax = col_ranges['min'], col_ranges['max']
                stats = con.execute(f"""
                    SELECT
                        COUNT("{col_name}") as total,
                        SUM(CASE WHEN "{col_name}" < {rmin} OR "{col_name}" > {rmax} THEN 1 ELSE 0 END) as oor,
                        SUM(CASE WHEN "{col_name}" < {rmin} THEN 1 ELSE 0 END) as below,
                        SUM(CASE WHEN "{col_name}" > {rmax} THEN 1 ELSE 0 END) as above
                    FROM df
                    WHERE "{col_name}" IS NOT NULL
                """).fetchone()

                total, oor, below, above = stats
                pct = (oor / total * 100) if total > 0 else 0

                oor_summary[col_name] = {
                    "total_non_null": int(total), "out_of_range": int(oor),
                    "out_of_range_percent": round(pct, 2),
                    "below_min": int(below), "above_max": int(above),
                    "min": rmin, "max": rmax,
                }

                if pct > 10:
                    result.add_warning(
                        f"Column '{col_name}': {oor}/{total} values ({pct:.1f}%) outside range [{rmin}, {rmax}]",
                        {"column": col_name, "percent": round(pct, 2)}
                    )

            elif isinstance(col_ranges, dict):
                cat_col_info = cat_map.get(col_name)
                if cat_col_info is None:
                    continue

                total_oor = 0
                total_count = 0

                if isinstance(cat_col_info, tuple):
                    cat_col, unit_col = cat_col_info
                    if cat_col not in actual_cols or unit_col not in actual_cols:
                        continue
                    for cat_val, unit_ranges in col_ranges.items():
                        if not isinstance(unit_ranges, dict):
                            continue
                        for unit_val, ranges in unit_ranges.items():
                            if not isinstance(ranges, dict) or 'min' not in ranges:
                                continue
                            rmin, rmax = ranges['min'], ranges['max']
                            stats = con.execute(f"""
                                SELECT COUNT(*) as total,
                                       SUM(CASE WHEN "{col_name}" < {rmin} OR "{col_name}" > {rmax} THEN 1 ELSE 0 END) as oor
                                FROM df
                                WHERE LOWER(CAST("{cat_col}" AS VARCHAR)) = '{str(cat_val).lower()}'
                                  AND LOWER(CAST("{unit_col}" AS VARCHAR)) = '{str(unit_val).lower()}'
                                  AND "{col_name}" IS NOT NULL
                            """).fetchone()
                            total_count += stats[0]
                            total_oor += stats[1]
                else:
                    cat_col = cat_col_info
                    if cat_col not in actual_cols:
                        continue
                    for cat_val, ranges in col_ranges.items():
                        if not isinstance(ranges, dict) or 'min' not in ranges:
                            continue
                        rmin, rmax = ranges['min'], ranges['max']
                        stats = con.execute(f"""
                            SELECT COUNT(*) as total,
                                   SUM(CASE WHEN "{col_name}" < {rmin} OR "{col_name}" > {rmax} THEN 1 ELSE 0 END) as oor
                            FROM df
                            WHERE LOWER(CAST("{cat_col}" AS VARCHAR)) = '{str(cat_val).lower()}'
                              AND "{col_name}" IS NOT NULL
                        """).fetchone()
                        total_count += stats[0]
                        total_oor += stats[1]

                pct = (total_oor / total_count * 100) if total_count > 0 else 0
                oor_summary[col_name] = {
                    "total_non_null": int(total_count), "out_of_range": int(total_oor),
                    "out_of_range_percent": round(pct, 2),
                }
                if pct > 10:
                    result.add_warning(
                        f"Column '{col_name}': {total_oor}/{total_count} values ({pct:.1f}%) outside plausible ranges",
                        {"column": col_name, "percent": round(pct, 2)}
                    )

        result.metrics["columns_checked"] = len(oor_summary)
        result.metrics["out_of_range_summary"] = oor_summary

        if not result.warnings and not result.errors:
            result.add_info("All numeric values within plausible ranges")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'numeric_range_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking numeric range plausibility: {str(e)}")

    return result


def check_numeric_range_plausibility(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    outlier_config: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check numeric values are within plausible ranges."""
    _logger.debug("check_numeric_range_plausibility: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_numeric_range_plausibility_polars(df, table_name, outlier_config)
    else:
        result = check_numeric_range_plausibility_duckdb(df, table_name, outlier_config)
    _logger.debug("check_numeric_range_plausibility: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("columns_checked"))
    return result


# ---------------------------------------------------------------------------
# A.3 Field-level plausibility rules
# ---------------------------------------------------------------------------

def check_field_plausibility_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> DQAPlausibilityResult:
    """Check field-level plausibility constraints using Polars."""
    result = DQAPlausibilityResult("field_plausibility", table_name)

    if rules is None:
        rules = _get_field_plausibility_rules(table_name)

    if not rules:
        result.add_info("No field plausibility rules defined for this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()
        violations_by_rule = {}

        for rule in rules:
            when_col = rule['when_column']
            when_not_values = rule['when_not_value']
            then_null_cols = rule['then_null_or_absent']
            description = rule.get('description', '')

            if when_col not in col_names:
                continue

            if not isinstance(when_not_values, list):
                when_not_values = [when_not_values]

            # Filter rows where when_col is NOT in the specified values
            filtered = lf.filter(
                ~pl.col(when_col).cast(pl.Utf8).str.to_lowercase().is_in(
                    [str(v).lower() for v in when_not_values]
                )
            )

            for check_col in then_null_cols:
                if check_col not in col_names:
                    continue

                stats = filtered.select([
                    pl.len().alias('total'),
                    pl.col(check_col).is_not_null().sum().alias('non_null')
                ]).collect(streaming=True)

                total = stats[0, 'total']
                non_null = stats[0, 'non_null']
                pct = (non_null / total * 100) if total > 0 else 0

                if non_null > 0:
                    violations_by_rule[description] = {
                        "total_applicable": int(total),
                        "violations": int(non_null),
                        "violation_percent": round(pct, 2),
                    }
                    result.add_warning(
                        f"Field plausibility violation: {description} — {non_null}/{total} rows ({pct:.1f}%)",
                        {"rule": description, "violations": int(non_null),
                         "total": int(total), "percent": round(pct, 2)}
                    )

        result.metrics["rules_checked"] = len(rules)
        result.metrics["violations_by_rule"] = violations_by_rule

        if not result.warnings and not result.errors:
            result.add_info("All field plausibility rules satisfied")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'field_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking field plausibility: {str(e)}")

    return result


def check_field_plausibility_duckdb(
    df: pd.DataFrame,
    table_name: str,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> DQAPlausibilityResult:
    """Check field-level plausibility constraints using DuckDB."""
    result = DQAPlausibilityResult("field_plausibility", table_name)

    if rules is None:
        rules = _get_field_plausibility_rules(table_name)

    if not rules:
        result.add_info("No field plausibility rules defined for this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)
        violations_by_rule = {}

        for rule in rules:
            when_col = rule['when_column']
            when_not_values = rule['when_not_value']
            then_null_cols = rule['then_null_or_absent']
            description = rule.get('description', '')

            if when_col not in df.columns:
                continue

            if not isinstance(when_not_values, list):
                when_not_values = [when_not_values]

            values_str = ', '.join([f"'{str(v).lower()}'" for v in when_not_values])

            for check_col in then_null_cols:
                if check_col not in df.columns:
                    continue

                stats = con.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        SUM(CASE WHEN "{check_col}" IS NOT NULL THEN 1 ELSE 0 END) as non_null
                    FROM df
                    WHERE LOWER(CAST("{when_col}" AS VARCHAR)) NOT IN ({values_str})
                """).fetchone()

                total, non_null = stats
                pct = (non_null / total * 100) if total > 0 else 0

                if non_null > 0:
                    violations_by_rule[description] = {
                        "total_applicable": int(total),
                        "violations": int(non_null),
                        "violation_percent": round(pct, 2),
                    }
                    result.add_warning(
                        f"Field plausibility violation: {description} — {non_null}/{total} rows ({pct:.1f}%)",
                        {"rule": description, "violations": int(non_null),
                         "total": int(total), "percent": round(pct, 2)}
                    )

        result.metrics["rules_checked"] = len(rules)
        result.metrics["violations_by_rule"] = violations_by_rule

        if not result.warnings and not result.errors:
            result.add_info("All field plausibility rules satisfied")

        con.close()

    except Exception as e:
        _logger.error("Check 'field_plausibility' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking field plausibility: {str(e)}")

    return result


def check_field_plausibility(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    rules: Optional[List[Dict[str, Any]]] = None,
) -> DQAPlausibilityResult:
    """Check field-level plausibility constraints."""
    _logger.debug("check_field_plausibility: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_field_plausibility_polars(df, table_name, rules)
    else:
        result = check_field_plausibility_duckdb(df, table_name, rules)
    _logger.debug("check_field_plausibility: table '%s' — rules_checked=%s",
                  table_name, result.metrics.get("rules_checked"))
    return result


# ---------------------------------------------------------------------------
# A.4 Medication dose unit consistency
# ---------------------------------------------------------------------------

def check_medication_dose_unit_consistency_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
) -> DQAPlausibilityResult:
    """Check medication dose unit consistency using Polars."""
    result = DQAPlausibilityResult("medication_dose_unit_consistency", table_name)

    if table_name not in ('medication_admin_continuous', 'medication_admin_intermittent'):
        result.add_info("Medication dose unit check not applicable to this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if 'med_dose_unit' not in col_names:
            result.add_info("Column 'med_dose_unit' not found in table")
            return result

        rules = _load_validation_rules().get('medication_dose_unit_rules', {}).get(table_name, {})
        expect = rules.get('expect', 'per_time' if 'continuous' in table_name else 'discrete')

        # Build expression to detect time denominators
        has_time_denom = pl.lit(False)
        for pat in _TIME_DENOMINATOR_PATTERNS:
            has_time_denom = has_time_denom | pl.col('med_dose_unit').cast(pl.Utf8).str.contains(pat.replace('/', '\\/'))

        non_null = lf.filter(pl.col('med_dose_unit').is_not_null())
        total = non_null.select(pl.len()).collect(streaming=True).item()

        if total == 0:
            result.add_info("No non-null med_dose_unit values to check")
            return result

        if expect == 'per_time':
            # Continuous: expect time denominators
            violations = non_null.filter(~has_time_denom).select(pl.len()).collect(streaming=True).item()
        else:
            # Intermittent: expect NO time denominators
            violations = non_null.filter(has_time_denom).select(pl.len()).collect(streaming=True).item()

        pct = (violations / total * 100) if total > 0 else 0

        result.metrics["total_rows"] = int(total)
        result.metrics["unit_pattern_violations"] = int(violations)
        result.metrics["violation_percent"] = round(pct, 2)

        if violations > 0:
            # Get sample violations
            if expect == 'per_time':
                sample = non_null.filter(~has_time_denom)
            else:
                sample = non_null.filter(has_time_denom)

            sample_cols = ['med_dose_unit']
            if 'med_category' in col_names:
                sample_cols.insert(0, 'med_category')

            sample_data = (
                sample
                .group_by(sample_cols)
                .agg(pl.len().alias('count'))
                .sort('count', descending=True)
                .head(10)
                .collect(streaming=True)
            )
            sample_list = sample_data.to_dicts()
            result.metrics["sample_violations"] = sample_list

            if pct > 10:
                result.add_warning(
                    f"Medication dose unit inconsistency: {violations}/{total} rows ({pct:.1f}%) have unexpected unit patterns",
                    {"violations": int(violations), "total": int(total), "percent": round(pct, 2)}
                )
            else:
                result.add_warning(
                    f"Medication dose unit inconsistency: {violations}/{total} rows ({pct:.1f}%) have unexpected unit patterns",
                    {"violations": int(violations), "total": int(total), "percent": round(pct, 2)}
                )
        else:
            result.add_info("All medication dose units consistent with administration type")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'medication_dose_unit_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking medication dose unit consistency: {str(e)}")

    return result


def check_medication_dose_unit_consistency_duckdb(
    df: pd.DataFrame,
    table_name: str,
) -> DQAPlausibilityResult:
    """Check medication dose unit consistency using DuckDB."""
    result = DQAPlausibilityResult("medication_dose_unit_consistency", table_name)

    if table_name not in ('medication_admin_continuous', 'medication_admin_intermittent'):
        result.add_info("Medication dose unit check not applicable to this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        if 'med_dose_unit' not in df.columns:
            result.add_info("Column 'med_dose_unit' not found in table")
            con.close()
            return result

        rules = _load_validation_rules().get('medication_dose_unit_rules', {}).get(table_name, {})
        expect = rules.get('expect', 'per_time' if 'continuous' in table_name else 'discrete')

        # Build CASE expression for time denominator detection
        time_conditions = ' OR '.join([
            f"CAST(\"med_dose_unit\" AS VARCHAR) LIKE '%{pat}%'"
            for pat in _TIME_DENOMINATOR_PATTERNS
        ])

        total_row = con.execute("""
            SELECT COUNT(*) FROM df WHERE "med_dose_unit" IS NOT NULL
        """).fetchone()
        total = total_row[0]

        if total == 0:
            result.add_info("No non-null med_dose_unit values to check")
            con.close()
            return result

        if expect == 'per_time':
            violation_query = f"""
                SELECT COUNT(*) FROM df
                WHERE "med_dose_unit" IS NOT NULL AND NOT ({time_conditions})
            """
        else:
            violation_query = f"""
                SELECT COUNT(*) FROM df
                WHERE "med_dose_unit" IS NOT NULL AND ({time_conditions})
            """

        violations = con.execute(violation_query).fetchone()[0]
        pct = (violations / total * 100) if total > 0 else 0

        result.metrics["total_rows"] = int(total)
        result.metrics["unit_pattern_violations"] = int(violations)
        result.metrics["violation_percent"] = round(pct, 2)

        if violations > 0:
            result.add_warning(
                f"Medication dose unit inconsistency: {violations}/{total} rows ({pct:.1f}%) have unexpected unit patterns",
                {"violations": int(violations), "total": int(total), "percent": round(pct, 2)}
            )
        else:
            result.add_info("All medication dose units consistent with administration type")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'medication_dose_unit_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking medication dose unit consistency: {str(e)}")

    return result


def check_medication_dose_unit_consistency(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
) -> DQAPlausibilityResult:
    """Check medication dose unit consistency."""
    _logger.debug("check_medication_dose_unit_consistency: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_medication_dose_unit_consistency_polars(df, table_name)
    else:
        result = check_medication_dose_unit_consistency_duckdb(df, table_name)
    _logger.debug("check_medication_dose_unit_consistency: table '%s' — violations=%s",
                  table_name, result.metrics.get("unit_pattern_violations"))
    return result


# ---------------------------------------------------------------------------
# B.1 Cross-table temporal plausibility
# ---------------------------------------------------------------------------

def check_cross_table_temporal_plausibility_polars(
    target_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    hospitalization_df: Union['pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    time_columns: List[str],
) -> DQAPlausibilityResult:
    """Check that datetime values fall within hospitalization bounds using Polars."""
    result = DQAPlausibilityResult("cross_table_temporal", target_table)

    try:
        target_lf = target_df if isinstance(target_df, pl.LazyFrame) else target_df.lazy()
        hosp_lf = hospitalization_df if isinstance(hospitalization_df, pl.LazyFrame) else hospitalization_df.lazy()

        target_cols = target_lf.collect_schema().names()
        hosp_cols = hosp_lf.collect_schema().names()

        if 'hospitalization_id' not in target_cols or 'hospitalization_id' not in hosp_cols:
            result.add_info("Missing hospitalization_id column; skipping cross-table check")
            return result

        if 'admission_dttm' not in hosp_cols or 'discharge_dttm' not in hosp_cols:
            result.add_info("Missing admission/discharge columns in hospitalization table")
            return result

        hosp_bounds = hosp_lf.select([
            'hospitalization_id', 'admission_dttm', 'discharge_dttm'
        ])

        joined = target_lf.join(hosp_bounds, on='hospitalization_id', how='inner')
        violations_by_col = {}

        for time_col in time_columns:
            if time_col not in target_cols:
                continue

            applicable = joined.filter(
                pl.col(time_col).is_not_null() &
                pl.col('admission_dttm').is_not_null() &
                pl.col('discharge_dttm').is_not_null()
            )

            stats = applicable.select([
                pl.len().alias('total'),
                (pl.col(time_col) < pl.col('admission_dttm')).sum().alias('before_admission'),
                (pl.col(time_col) > pl.col('discharge_dttm')).sum().alias('after_discharge'),
            ]).collect(streaming=True)

            total = stats[0, 'total']
            before = stats[0, 'before_admission']
            after = stats[0, 'after_discharge']
            violation_total = before + after
            pct = (violation_total / total * 100) if total > 0 else 0

            violations_by_col[time_col] = {
                "total_joined": int(total),
                "before_admission": int(before),
                "after_discharge": int(after),
                "violation_count": int(violation_total),
                "violation_percent": round(pct, 2),
            }

            if pct > 5:
                result.add_error(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside hospitalization bounds",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )
            elif violation_total > 0:
                result.add_warning(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside hospitalization bounds",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )

        result.metrics["time_columns_checked"] = list(violations_by_col.keys())
        result.metrics["violations_by_column"] = violations_by_col

        if not result.errors and not result.warnings:
            result.add_info("All records fall within hospitalization bounds")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'cross_table_temporal' failed for table '%s': %s", target_table, e)
        result.add_error(f"Error checking cross-table temporal plausibility: {str(e)}")

    return result


def check_cross_table_temporal_plausibility_duckdb(
    target_df: pd.DataFrame,
    hospitalization_df: pd.DataFrame,
    target_table: str,
    time_columns: List[str],
) -> DQAPlausibilityResult:
    """Check that datetime values fall within hospitalization bounds using DuckDB."""
    result = DQAPlausibilityResult("cross_table_temporal", target_table)

    try:
        con = duckdb.connect(':memory:')
        con.register('target_tbl', target_df)
        con.register('hosp_tbl', hospitalization_df)

        if 'hospitalization_id' not in target_df.columns or 'hospitalization_id' not in hospitalization_df.columns:
            result.add_info("Missing hospitalization_id column; skipping cross-table check")
            con.close()
            return result

        if 'admission_dttm' not in hospitalization_df.columns or 'discharge_dttm' not in hospitalization_df.columns:
            result.add_info("Missing admission/discharge columns in hospitalization table")
            con.close()
            return result

        violations_by_col = {}

        for time_col in time_columns:
            if time_col not in target_df.columns:
                continue

            stats = con.execute(f"""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN t."{time_col}" < h.admission_dttm THEN 1 ELSE 0 END) as before_admission,
                    SUM(CASE WHEN t."{time_col}" > h.discharge_dttm THEN 1 ELSE 0 END) as after_discharge
                FROM target_tbl t
                INNER JOIN hosp_tbl h ON t.hospitalization_id = h.hospitalization_id
                WHERE t."{time_col}" IS NOT NULL
                  AND h.admission_dttm IS NOT NULL
                  AND h.discharge_dttm IS NOT NULL
            """).fetchone()

            total, before, after = stats
            violation_total = before + after
            pct = (violation_total / total * 100) if total > 0 else 0

            violations_by_col[time_col] = {
                "total_joined": int(total),
                "before_admission": int(before),
                "after_discharge": int(after),
                "violation_count": int(violation_total),
                "violation_percent": round(pct, 2),
            }

            if pct > 5:
                result.add_error(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside hospitalization bounds",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )
            elif violation_total > 0:
                result.add_warning(
                    f"Column '{time_col}': {violation_total}/{total} records ({pct:.1f}%) outside hospitalization bounds",
                    {"column": time_col, "before_admission": int(before),
                     "after_discharge": int(after), "percent": round(pct, 2)}
                )

        result.metrics["time_columns_checked"] = list(violations_by_col.keys())
        result.metrics["violations_by_column"] = violations_by_col

        if not result.errors and not result.warnings:
            result.add_info("All records fall within hospitalization bounds")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'cross_table_temporal' failed for table '%s': %s", target_table, e)
        result.add_error(f"Error checking cross-table temporal plausibility: {str(e)}")

    return result


def check_cross_table_temporal_plausibility(
    target_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    hospitalization_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    target_table: str,
    time_columns: List[str],
) -> DQAPlausibilityResult:
    """Check that datetime values fall within hospitalization bounds."""
    _logger.debug("check_cross_table_temporal_plausibility: starting for table '%s'", target_table)
    if _ACTIVE_BACKEND == 'polars':
        result = check_cross_table_temporal_plausibility_polars(
            target_df, hospitalization_df, target_table, time_columns)
    else:
        result = check_cross_table_temporal_plausibility_duckdb(
            target_df, hospitalization_df, target_table, time_columns)
    _logger.debug("check_cross_table_temporal_plausibility: table '%s' complete", target_table)
    return result


# ---------------------------------------------------------------------------
# C.1 Overlapping time periods
# ---------------------------------------------------------------------------

def check_overlapping_periods_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    entity_col: str = 'hospitalization_id',
    start_col: str = 'in_dttm',
    end_col: str = 'out_dttm',
) -> DQAPlausibilityResult:
    """Check for overlapping time periods within entities using Polars."""
    result = DQAPlausibilityResult("overlapping_periods", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if entity_col not in col_names or start_col not in col_names or end_col not in col_names:
            result.add_info(f"Required columns ({entity_col}, {start_col}, {end_col}) not all present")
            return result

        # Filter to rows where both start and end are non-null, sort, then compare with previous
        sorted_lf = lf.filter(
            pl.col(start_col).is_not_null() & pl.col(end_col).is_not_null()
        ).sort([entity_col, start_col])

        with_prev = sorted_lf.with_columns(
            pl.col(end_col).shift(1).over(entity_col).alias('_prev_end')
        )

        overlaps_df = with_prev.filter(
            pl.col('_prev_end').is_not_null() & (pl.col(start_col) < pl.col('_prev_end'))
        )

        total_records = sorted_lf.select(pl.len()).collect(streaming=True).item()
        overlap_count = overlaps_df.select(pl.len()).collect(streaming=True).item()
        entities_checked = sorted_lf.select(
            pl.col(entity_col).n_unique()
        ).collect(streaming=True).item()
        pct = (overlap_count / total_records * 100) if total_records > 0 else 0

        result.metrics["total_records"] = int(total_records)
        result.metrics["entities_checked"] = int(entities_checked)
        result.metrics["overlapping_records"] = int(overlap_count)
        result.metrics["overlap_percent"] = round(pct, 2)

        if overlap_count > 0:
            result.add_warning(
                f"{overlap_count} overlapping time periods detected ({pct:.1f}% of records)",
                {"overlapping_records": int(overlap_count), "percent": round(pct, 2)}
            )
        else:
            result.add_info("No overlapping time periods detected")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'overlapping_periods' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking overlapping periods: {str(e)}")

    return result


def check_overlapping_periods_duckdb(
    df: pd.DataFrame,
    table_name: str,
    entity_col: str = 'hospitalization_id',
    start_col: str = 'in_dttm',
    end_col: str = 'out_dttm',
) -> DQAPlausibilityResult:
    """Check for overlapping time periods within entities using DuckDB."""
    result = DQAPlausibilityResult("overlapping_periods", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        if entity_col not in df.columns or start_col not in df.columns or end_col not in df.columns:
            result.add_info(f"Required columns ({entity_col}, {start_col}, {end_col}) not all present")
            con.close()
            return result

        stats = con.execute(f"""
            WITH filtered AS (
                SELECT * FROM df
                WHERE "{start_col}" IS NOT NULL AND "{end_col}" IS NOT NULL
            ),
            ordered AS (
                SELECT *,
                       LAG("{end_col}") OVER (
                           PARTITION BY "{entity_col}" ORDER BY "{start_col}"
                       ) AS prev_end
                FROM filtered
            )
            SELECT
                (SELECT COUNT(*) FROM filtered) as total_records,
                (SELECT COUNT(DISTINCT "{entity_col}") FROM filtered) as entities_checked,
                COUNT(*) as overlap_count
            FROM ordered
            WHERE prev_end IS NOT NULL AND "{start_col}" < prev_end
        """).fetchone()

        total_records, entities_checked, overlap_count = stats
        pct = (overlap_count / total_records * 100) if total_records > 0 else 0

        result.metrics["total_records"] = int(total_records)
        result.metrics["entities_checked"] = int(entities_checked)
        result.metrics["overlapping_records"] = int(overlap_count)
        result.metrics["overlap_percent"] = round(pct, 2)

        if overlap_count > 0:
            result.add_warning(
                f"{overlap_count} overlapping time periods detected ({pct:.1f}% of records)",
                {"overlapping_records": int(overlap_count), "percent": round(pct, 2)}
            )
        else:
            result.add_info("No overlapping time periods detected")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'overlapping_periods' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking overlapping periods: {str(e)}")

    return result


def check_overlapping_periods(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    entity_col: str = 'hospitalization_id',
    start_col: str = 'in_dttm',
    end_col: str = 'out_dttm',
) -> DQAPlausibilityResult:
    """Check for overlapping time periods within entities."""
    _logger.debug("check_overlapping_periods: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_overlapping_periods_polars(df, table_name, entity_col, start_col, end_col)
    else:
        result = check_overlapping_periods_duckdb(df, table_name, entity_col, start_col, end_col)
    _logger.debug("check_overlapping_periods: table '%s' — overlaps=%s",
                  table_name, result.metrics.get("overlapping_records"))
    return result


# ---------------------------------------------------------------------------
# C.2 Category temporal consistency
# ---------------------------------------------------------------------------

def _detect_time_column(col_names: List[str], table_name: str) -> Optional[str]:
    """Auto-detect the primary datetime column for a table."""
    candidates = ['recorded_dttm', 'admin_dttm', 'admission_dttm',
                   'lab_result_dttm', 'in_dttm', 'procedure_billed_dttm',
                   'result_dttm', 'start_dttm']
    for c in candidates:
        if c in col_names:
            return c
    return None


def check_category_temporal_consistency_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    time_column: Optional[str] = None,
) -> DQAPlausibilityResult:
    """Check category distribution consistency over time using Polars."""
    result = DQAPlausibilityResult("category_temporal_consistency", table_name)

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        if time_column is None:
            time_column = _detect_time_column(col_names, table_name)

        if time_column is None or time_column not in col_names:
            result.add_info("No suitable datetime column found for temporal consistency check")
            return result

        # Find category columns from schema
        category_columns = schema.get('category_columns', [])
        cat_cols_present = [c for c in category_columns if c in col_names]

        if not cat_cols_present:
            result.add_info("No category columns found for temporal consistency check")
            return result

        # Need hospitalization_id for unique ID counting
        id_col = 'hospitalization_id' if 'hospitalization_id' in col_names else (
            'patient_id' if 'patient_id' in col_names else None
        )

        yearly_distributions = {}
        missing_in_years = {}

        for cat_col in cat_cols_present:
            # Get yearly distribution
            if id_col:
                yearly = (
                    lf.filter(pl.col(time_column).is_not_null() & pl.col(cat_col).is_not_null())
                    .with_columns(pl.col(time_column).dt.year().alias('_year'))
                    .group_by(['_year', cat_col])
                    .agg(pl.col(id_col).n_unique().alias('unique_ids'))
                    .sort(['_year', cat_col])
                    .collect(streaming=True)
                )
            else:
                yearly = (
                    lf.filter(pl.col(time_column).is_not_null() & pl.col(cat_col).is_not_null())
                    .with_columns(pl.col(time_column).dt.year().alias('_year'))
                    .group_by(['_year', cat_col])
                    .agg(pl.len().alias('unique_ids'))
                    .sort(['_year', cat_col])
                    .collect(streaming=True)
                )

            if len(yearly) == 0:
                continue

            # Build year -> {value: count} dict
            dist = {}
            all_years = set()
            all_values = set()
            for row in yearly.iter_rows(named=True):
                year = int(row['_year'])
                val = row[cat_col]
                count = row['unique_ids']
                all_years.add(year)
                all_values.add(val)
                dist.setdefault(year, {})[val] = int(count)

            yearly_distributions[cat_col] = dist

            # Check for values absent in some years (requires >= 2 years)
            if len(all_years) >= 2:
                absent = {}
                for val in all_values:
                    absent_years = [y for y in sorted(all_years) if val not in dist.get(y, {})]
                    if absent_years and len(absent_years) < len(all_years):
                        absent[str(val)] = absent_years
                if absent:
                    missing_in_years[cat_col] = absent

        result.metrics["category_columns_checked"] = len(cat_cols_present)
        result.metrics["yearly_distributions"] = yearly_distributions
        result.metrics["missing_in_years"] = missing_in_years

        for cat_col, absent in missing_in_years.items():
            for val, years in absent.items():
                result.add_warning(
                    f"Category '{cat_col}' value '{val}' absent in years: {years}",
                    {"column": cat_col, "value": val, "absent_years": years}
                )

        if not result.warnings and not result.errors:
            result.add_info("Category distributions are consistent over time")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_temporal_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category temporal consistency: {str(e)}")

    return result


def check_category_temporal_consistency_duckdb(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    table_name: str,
    time_column: Optional[str] = None,
) -> DQAPlausibilityResult:
    """Check category distribution consistency over time using DuckDB."""
    result = DQAPlausibilityResult("category_temporal_consistency", table_name)

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)
        actual_cols = list(df.columns)

        if time_column is None:
            time_column = _detect_time_column(actual_cols, table_name)

        if time_column is None or time_column not in actual_cols:
            result.add_info("No suitable datetime column found for temporal consistency check")
            con.close()
            return result

        category_columns = schema.get('category_columns', [])
        cat_cols_present = [c for c in category_columns if c in actual_cols]

        if not cat_cols_present:
            result.add_info("No category columns found for temporal consistency check")
            con.close()
            return result

        id_col = 'hospitalization_id' if 'hospitalization_id' in actual_cols else (
            'patient_id' if 'patient_id' in actual_cols else None
        )

        yearly_distributions = {}
        missing_in_years = {}

        for cat_col in cat_cols_present:
            if id_col:
                agg_expr = f'COUNT(DISTINCT "{id_col}") as unique_ids'
            else:
                agg_expr = 'COUNT(*) as unique_ids'

            rows = con.execute(f"""
                SELECT EXTRACT(YEAR FROM "{time_column}") as yr,
                       "{cat_col}",
                       {agg_expr}
                FROM df
                WHERE "{time_column}" IS NOT NULL AND "{cat_col}" IS NOT NULL
                GROUP BY yr, "{cat_col}"
                ORDER BY yr, "{cat_col}"
            """).fetchall()

            if not rows:
                continue

            dist = {}
            all_years = set()
            all_values = set()
            for row in rows:
                year = int(row[0])
                val = row[1]
                count = int(row[2])
                all_years.add(year)
                all_values.add(val)
                dist.setdefault(year, {})[val] = count

            yearly_distributions[cat_col] = dist

            if len(all_years) >= 2:
                absent = {}
                for val in all_values:
                    absent_years = [y for y in sorted(all_years) if val not in dist.get(y, {})]
                    if absent_years and len(absent_years) < len(all_years):
                        absent[str(val)] = absent_years
                if absent:
                    missing_in_years[cat_col] = absent

        result.metrics["category_columns_checked"] = len(cat_cols_present)
        result.metrics["yearly_distributions"] = yearly_distributions
        result.metrics["missing_in_years"] = missing_in_years

        for cat_col, absent in missing_in_years.items():
            for val, years in absent.items():
                result.add_warning(
                    f"Category '{cat_col}' value '{val}' absent in years: {years}",
                    {"column": cat_col, "value": val, "absent_years": years}
                )

        if not result.warnings and not result.errors:
            result.add_info("Category distributions are consistent over time")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'category_temporal_consistency' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking category temporal consistency: {str(e)}")

    return result


def check_category_temporal_consistency(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    time_column: Optional[str] = None,
) -> DQAPlausibilityResult:
    """Check category distribution consistency over time."""
    _logger.debug("check_category_temporal_consistency: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_category_temporal_consistency_polars(df, schema, table_name, time_column)
    else:
        result = check_category_temporal_consistency_duckdb(df, schema, table_name, time_column)
    _logger.debug("check_category_temporal_consistency: table '%s' — columns_checked=%s",
                  table_name, result.metrics.get("category_columns_checked"))
    return result


# ---------------------------------------------------------------------------
# D.1 Duplicate composite keys
# ---------------------------------------------------------------------------

def check_duplicate_composite_keys_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    composite_keys: Optional[List[str]] = None,
    schema: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check for duplicate composite keys using Polars."""
    result = DQAPlausibilityResult("duplicate_composite_keys", table_name)

    if composite_keys is None:
        composite_keys = _get_composite_keys(table_name, schema)

    if not composite_keys:
        result.add_info("No composite keys defined for this table")
        return result

    try:
        lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
        col_names = lf.collect_schema().names()

        # Verify all key columns exist
        missing_keys = [k for k in composite_keys if k not in col_names]
        if missing_keys:
            result.add_info(f"Composite key columns missing: {missing_keys}")
            return result

        total = lf.select(pl.len()).collect().item()
        unique = lf.select(composite_keys).unique().select(pl.len()).collect().item()
        duplicates = total - unique
        pct = (duplicates / total * 100) if total > 0 else 0

        result.metrics["composite_keys"] = composite_keys
        result.metrics["total_records"] = int(total)
        result.metrics["unique_records"] = int(unique)
        result.metrics["duplicate_records"] = int(duplicates)
        result.metrics["duplicate_percent"] = round(pct, 2)

        if pct > 10:
            result.add_error(
                f"{duplicates} duplicate composite key records ({pct:.1f}%) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        elif duplicates > 0:
            result.add_warning(
                f"{duplicates} duplicate composite key records ({pct:.1f}%) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        else:
            result.add_info("No duplicate composite keys found")

        gc.collect()

    except Exception as e:
        _logger.error("Check 'duplicate_composite_keys' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking duplicate composite keys: {str(e)}")

    return result


def check_duplicate_composite_keys_duckdb(
    df: pd.DataFrame,
    table_name: str,
    composite_keys: Optional[List[str]] = None,
    schema: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check for duplicate composite keys using DuckDB."""
    result = DQAPlausibilityResult("duplicate_composite_keys", table_name)

    if composite_keys is None:
        composite_keys = _get_composite_keys(table_name, schema)

    if not composite_keys:
        result.add_info("No composite keys defined for this table")
        return result

    try:
        con = duckdb.connect(':memory:')
        con.register('df', df)

        missing_keys = [k for k in composite_keys if k not in df.columns]
        if missing_keys:
            result.add_info(f"Composite key columns missing: {missing_keys}")
            con.close()
            return result

        key_cols_str = ', '.join([f'"{k}"' for k in composite_keys])
        stats = con.execute(f"""
            SELECT
                (SELECT COUNT(*) FROM df) as total,
                (SELECT COUNT(*) FROM (SELECT DISTINCT {key_cols_str} FROM df)) as unique_count
        """).fetchone()

        total, unique = stats
        duplicates = total - unique
        pct = (duplicates / total * 100) if total > 0 else 0

        result.metrics["composite_keys"] = composite_keys
        result.metrics["total_records"] = int(total)
        result.metrics["unique_records"] = int(unique)
        result.metrics["duplicate_records"] = int(duplicates)
        result.metrics["duplicate_percent"] = round(pct, 2)

        if pct > 10:
            result.add_error(
                f"{duplicates} duplicate composite key records ({pct:.1f}%) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        elif duplicates > 0:
            result.add_warning(
                f"{duplicates} duplicate composite key records ({pct:.1f}%) in {table_name}",
                {"duplicate_records": int(duplicates), "percent": round(pct, 2),
                 "keys": composite_keys}
            )
        else:
            result.add_info("No duplicate composite keys found")

        con.close()
        gc.collect()

    except Exception as e:
        _logger.error("Check 'duplicate_composite_keys' failed for table '%s': %s", table_name, e)
        result.add_error(f"Error checking duplicate composite keys: {str(e)}")

    return result


def check_duplicate_composite_keys(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    composite_keys: Optional[List[str]] = None,
    schema: Optional[Dict[str, Any]] = None,
) -> DQAPlausibilityResult:
    """Check for duplicate composite keys."""
    _logger.debug("check_duplicate_composite_keys: starting for table '%s'", table_name)
    if _ACTIVE_BACKEND == 'polars':
        result = check_duplicate_composite_keys_polars(df, table_name, composite_keys, schema)
    else:
        result = check_duplicate_composite_keys_duckdb(df, table_name, composite_keys, schema)
    _logger.debug("check_duplicate_composite_keys: table '%s' — duplicates=%s",
                  table_name, result.metrics.get("duplicate_records"))
    return result
# ---------------------------------------------------------------------------
# COMPREHENSIVE DQA RUNNER
# ---------------------------------------------------------------------------

def run_conformance_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str
) -> Dict[str, DQAConformanceResult]:
    """
    Run all conformance checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table

    Returns
    -------
    Dict[str, DQAConformanceResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running conformance checks for table '%s' using %s backend", table_name, _ACTIVE_BACKEND)

    # Convert pandas DataFrame to Polars if the active backend is Polars
    if _ACTIVE_BACKEND == 'polars' and isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    results = {}

    results['table_presence'] = check_table_presence(df, table_name)
    gc.collect()

    results['required_columns'] = check_required_columns(df, schema, table_name)
    gc.collect()

    results['column_dtypes'] = check_column_dtypes(df, schema, table_name)
    gc.collect()

    results['datetime_format'] = check_datetime_format(df, schema, table_name)
    gc.collect()

    if table_name == 'labs':
        results['lab_reference_units'] = check_lab_reference_units(df, schema, table_name)
        gc.collect()

    results['categorical_values'] = check_categorical_values(df, schema, table_name)
    gc.collect()

    results['category_group_mapping'] = check_category_group_mapping(df, schema, table_name)
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Conformance checks complete for '%s': %d passed, %d failed", table_name, passed, failed)

    return results


def run_completeness_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0
) -> Dict[str, DQACompletenessResult]:
    """
    Run all completeness checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table
    error_threshold : float
        Percent missing above which an error is raised
    warning_threshold : float
        Percent missing above which a warning is raised

    Returns
    -------
    Dict[str, DQACompletenessResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running completeness checks for table '%s' using %s backend", table_name, _ACTIVE_BACKEND)

    # Convert pandas DataFrame to Polars if the active backend is Polars
    if _ACTIVE_BACKEND == 'polars' and isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    results = {}

    results['missingness'] = check_missingness(
        df, schema, table_name, error_threshold, warning_threshold
    )
    gc.collect()

    results['conditional_requirements'] = check_conditional_requirements(df, table_name)
    gc.collect()

    results['mcide_value_coverage'] = check_mcide_value_coverage(df, schema, table_name)
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Completeness checks complete for '%s': %d passed, %d failed", table_name, passed, failed)

    return results


def run_relational_integrity_checks(
    tables: list,
) -> Dict[str, Dict[str, DQACompletenessResult]]:
    """Auto-detect and run relational integrity checks for loaded tables.

    Reads FK rules from ``validation_rules.yaml`` and runs
    :func:`check_relational_integrity` for every applicable
    (table, fk_column) pair.

    Parameters
    ----------
    tables : list
        Objects with ``.table_name`` (str) and ``.df`` (DataFrame)
        attributes — typically :class:`BaseTable` instances.

    Returns
    -------
    Dict[str, Dict[str, DQACompletenessResult]]
        ``{table_name: {fk_column: DQACompletenessResult}}``.
    """
    _logger.info("run_relational_integrity_checks: starting with %d tables", len(tables))

    # Build lookup: table_name -> DataFrame
    # Convert pandas DataFrames to Polars when the Polars backend is active,
    # matching the pattern used by run_conformance_checks / run_completeness_checks.
    lookup = {}
    for obj in tables:
        df = obj.df
        if _ACTIVE_BACKEND == 'polars' and isinstance(df, pd.DataFrame):
            _logger.debug("Converting pandas DataFrame to Polars for table '%s'", obj.table_name)
            df = pl.from_pandas(df)
        lookup[obj.table_name] = df

    # Load FK rules
    fk_rules = _load_validation_rules().get('relational_integrity', {})
    _logger.debug("run_relational_integrity_checks: loaded %d FK rules", len(fk_rules))

    results: Dict[str, Dict[str, DQACompletenessResult]] = {}

    for table_name, df in lookup.items():
        # Determine column names from the DataFrame
        if HAS_POLARS and isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
            col_names = lf.collect_schema().names()
        else:
            col_names = df.columns.tolist()

        for fk_column, rule in fk_rules.items():
            if fk_column not in col_names:
                continue

            ref_table_name = rule['references_table']

            # Skip self-references
            if ref_table_name == table_name:
                _logger.debug(
                    "run_relational_integrity_checks: skipping self-ref "
                    "%s.%s -> %s", table_name, fk_column, ref_table_name,
                )
                continue

            # Skip if the reference table isn't loaded
            if ref_table_name not in lookup:
                _logger.debug(
                    "run_relational_integrity_checks: skipping %s.%s — "
                    "reference table '%s' not loaded",
                    table_name, fk_column, ref_table_name,
                )
                continue

            _logger.info(
                "run_relational_integrity_checks: checking %s.%s -> %s",
                table_name, fk_column, ref_table_name,
            )
            result = check_relational_integrity(
                target_df=df,
                reference_df=lookup[ref_table_name],
                target_table=table_name,
                reference_table=ref_table_name,
                key_column=fk_column,
            )
            results.setdefault(table_name, {})[fk_column] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_relational_integrity_checks: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_plausibility_checks(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
) -> Dict[str, DQAPlausibilityResult]:
    """
    Run all single-table plausibility checks on a table.

    Parameters
    ----------
    df : DataFrame
        The data to validate
    schema : dict
        Schema for the table
    table_name : str
        Name of the table

    Returns
    -------
    Dict[str, DQAPlausibilityResult]
        Dictionary of check results keyed by check type
    """
    _logger.info("Running plausibility checks for table '%s' using %s backend", table_name, _ACTIVE_BACKEND)

    if _ACTIVE_BACKEND == 'polars' and isinstance(df, pd.DataFrame):
        _logger.debug("Converting pandas DataFrame to Polars for table '%s'", table_name)
        df = pl.from_pandas(df)

    results = {}

    # A.1 Temporal ordering
    results['temporal_ordering'] = check_temporal_ordering(df, table_name)
    gc.collect()

    # A.2 Numeric range plausibility
    results['numeric_range_plausibility'] = check_numeric_range_plausibility(df, table_name)
    gc.collect()

    # A.3 Field-level plausibility
    results['field_plausibility'] = check_field_plausibility(df, table_name)
    gc.collect()

    # A.4 Medication dose unit consistency (only for med tables)
    if table_name in ('medication_admin_continuous', 'medication_admin_intermittent'):
        results['medication_dose_unit_consistency'] = check_medication_dose_unit_consistency(df, table_name)
        gc.collect()

    # C.1 Overlapping periods
    overlap_rules = _load_validation_rules().get('overlapping_periods', {}).get(table_name)
    if overlap_rules:
        results['overlapping_periods'] = check_overlapping_periods(
            df, table_name,
            entity_col=overlap_rules.get('entity_column', 'hospitalization_id'),
            start_col=overlap_rules.get('start_column', 'in_dttm'),
            end_col=overlap_rules.get('end_column', 'out_dttm'),
        )
        gc.collect()

    # C.2 Category temporal consistency
    results['category_temporal_consistency'] = check_category_temporal_consistency(df, schema, table_name)
    gc.collect()

    # D.1 Duplicate composite keys
    results['duplicate_composite_keys'] = check_duplicate_composite_keys(df, table_name, schema=schema)
    gc.collect()

    passed = sum(1 for r in results.values() if r.passed)
    failed = len(results) - passed
    _logger.info("Plausibility checks complete for '%s': %d passed, %d failed", table_name, passed, failed)
    return results


def run_cross_table_plausibility_checks(
    tables: list,
) -> Dict[str, Dict[str, DQAPlausibilityResult]]:
    """Run cross-table plausibility checks (B.1).

    Parameters
    ----------
    tables : list
        Objects with ``.table_name`` and ``.df`` attributes.

    Returns
    -------
    Dict[str, Dict[str, DQAPlausibilityResult]]
        ``{table_name: {"cross_table_temporal": DQAPlausibilityResult}}``.
    """
    _logger.info("run_cross_table_plausibility_checks: starting with %d tables", len(tables))

    lookup = {}
    for obj in tables:
        tdf = obj.df
        if _ACTIVE_BACKEND == 'polars' and isinstance(tdf, pd.DataFrame):
            tdf = pl.from_pandas(tdf)
        lookup[obj.table_name] = tdf

    if 'hospitalization' not in lookup:
        _logger.info("Hospitalization table not loaded; skipping cross-table plausibility")
        return {}

    hosp_df = lookup['hospitalization']
    results: Dict[str, Dict[str, DQAPlausibilityResult]] = {}

    for tbl_name, tdf in lookup.items():
        if tbl_name == 'hospitalization':
            continue
        time_cols = _CROSS_TABLE_TIME_COLUMNS.get(tbl_name, [])
        if not time_cols:
            continue

        if HAS_POLARS and isinstance(tdf, (pl.DataFrame, pl.LazyFrame)):
            lf = tdf if isinstance(tdf, pl.LazyFrame) else tdf.lazy()
            actual_cols = lf.collect_schema().names()
        else:
            actual_cols = tdf.columns.tolist()

        available_time_cols = [c for c in time_cols if c in actual_cols]
        if not available_time_cols or 'hospitalization_id' not in actual_cols:
            continue

        result = check_cross_table_temporal_plausibility(
            tdf, hosp_df, tbl_name, available_time_cols
        )
        results.setdefault(tbl_name, {})["cross_table_temporal"] = result

    checked = sum(len(v) for v in results.values())
    _logger.info(
        "run_cross_table_plausibility_checks: completed %d checks across %d tables",
        checked, len(results),
    )
    return results


def run_full_dqa(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: str,
    tables: Optional[list] = None,
    error_threshold: float = 50.0,
    warning_threshold: float = 10.0,
) -> Dict[str, Any]:
    """Run the complete DQA suite on a single table.

    Orchestrates conformance checks, completeness checks, plausibility
    checks, and — when *tables* is provided — auto-detected relational
    integrity and cross-table plausibility checks.

    Parameters
    ----------
    df : DataFrame
        The data to validate.
    schema : dict
        Schema for the table.
    table_name : str
        Name of the table.
    tables : list, optional
        Objects with ``.table_name`` and ``.df`` attributes (e.g.
        :class:`BaseTable` instances).  When provided, relational
        integrity and cross-table plausibility checks are run.
    error_threshold : float
        Percent missing above which an error is raised (default 50).
    warning_threshold : float
        Percent missing above which a warning is raised (default 10).

    Returns
    -------
    Dict[str, Any]
        Keys: ``table_name``, ``backend``, ``conformance``,
        ``completeness``, ``relational``, ``plausibility``.
    """
    _logger.info("Starting full DQA for table: %s", table_name)

    results: Dict[str, Any] = {
        'table_name': table_name,
        'backend': _ACTIVE_BACKEND,
        'conformance': {},
        'completeness': {},
        'relational': {},
        'plausibility': {},
    }

    results['conformance'] = {
        k: v.to_dict()
        for k, v in run_conformance_checks(df, schema, table_name).items()
    }

    results['completeness'] = {
        k: v.to_dict()
        for k, v in run_completeness_checks(
            df, schema, table_name, error_threshold, warning_threshold
        ).items()
    }

    if tables is not None:
        rel_results = run_relational_integrity_checks(tables)
        if table_name in rel_results:
            results['relational'] = {
                k: v.to_dict()
                for k, v in rel_results[table_name].items()
            }

    # Plausibility checks (single-table)
    results['plausibility'] = {
        k: v.to_dict()
        for k, v in run_plausibility_checks(df, schema, table_name).items()
    }

    # Cross-table plausibility checks (when tables provided)
    if tables is not None:
        cross_plaus = run_cross_table_plausibility_checks(tables)
        if table_name in cross_plaus:
            for k, v in cross_plaus[table_name].items():
                results['plausibility'][k] = v.to_dict()

    gc.collect()
    _logger.info("Completed full DQA for table: %s", table_name)
    return results


# ---------------------------------------------------------------------------
# CLIF-TABLEONE COMPATIBILITY LAYER
# ---------------------------------------------------------------------------
# These functions provide backward compatibility with CLIF-TableOne's
# validation interface expectations.

def validate_dataframe(
    df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    schema: Dict[str, Any],
    table_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Validate a dataframe against schema and return list of errors.

    This function provides compatibility with CLIF-TableOne's expected
    validation interface.

    Parameters
    ----------
    df : pd.DataFrame, pl.DataFrame, or pl.LazyFrame
        Data to validate
    schema : dict
        Table schema containing columns, required_columns, etc.
    table_name : str, optional
        Name of the table (inferred from schema if not provided)

    Returns
    -------
    List[Dict[str, Any]]
        List of error dictionaries with keys:
        - type: str - Error type/check name
        - description: str - Human-readable error description
        - details: dict - Additional error details
        - category: str - 'schema' or 'data_quality'
    """
    table_name = table_name or schema.get('table_name', 'unknown')
    _logger.info("validate_dataframe: starting validation for table '%s'", table_name)
    errors = []

    # Schema-level checks (affect 'incomplete' status)
    schema_checks = ['required_columns', 'column_dtypes']

    # Run conformance checks
    conformance_results = run_conformance_checks(df, schema, table_name)
    for check_name, result in conformance_results.items():
        category = 'schema' if check_name in schema_checks else 'data_quality'

        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': category
            })

        # Include warnings as data quality issues
        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    # Run completeness checks
    completeness_results = run_completeness_checks(df, schema, table_name)
    for check_name, result in completeness_results.items():
        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': 'data_quality'
            })

        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    # Run plausibility checks
    plausibility_results = run_plausibility_checks(df, schema, table_name)
    for check_name, result in plausibility_results.items():
        for err in result.errors:
            errors.append({
                'type': _format_check_type(check_name),
                'description': err['message'],
                'details': err.get('details', {}),
                'category': 'data_quality'
            })

        for warn in result.warnings:
            errors.append({
                'type': _format_check_type(check_name),
                'description': warn['message'],
                'details': warn.get('details', {}),
                'category': 'data_quality',
                'severity': 'warning'
            })

    gc.collect()
    error_count = sum(1 for e in errors if e.get('severity', 'error') == 'error')
    warning_count = sum(1 for e in errors if e.get('severity') == 'warning')
    _logger.info("validate_dataframe: table '%s' complete — %d errors, %d warnings",
                 table_name, error_count, warning_count)
    return errors


def _format_check_type(check_name: str) -> str:
    """Convert internal check names to human-readable format."""
    type_mapping = {
        'required_columns': 'Missing Required Columns',
        'column_dtypes': 'Data Type Mismatch',
        'datetime_format': 'Datetime Format Issue',
        'lab_reference_units': 'Lab Unit Mismatch',
        'categorical_values': 'Invalid Categorical Values',
        'missingness': 'High Missingness',
        'conditional_requirements': 'Conditional Requirement Violation',
        'mcide_value_coverage': 'mCIDE Coverage Gap',
        'relational_integrity': 'Relational Integrity',
        'temporal_ordering': 'Temporal Ordering Violation',
        'numeric_range_plausibility': 'Numeric Range Implausibility',
        'field_plausibility': 'Field Plausibility Violation',
        'medication_dose_unit_consistency': 'Medication Dose Unit Inconsistency',
        'cross_table_temporal': 'Cross-Table Temporal Implausibility',
        'overlapping_periods': 'Overlapping Time Periods',
        'category_temporal_consistency': 'Category Distribution Shift',
        'duplicate_composite_keys': 'Duplicate Composite Keys',
    }
    return type_mapping.get(check_name, check_name.replace('_', ' ').title())


def format_clifpy_error(
    error: Dict[str, Any],
    row_count: int,
    table_name: str
) -> Dict[str, Any]:
    """
    Format a validation error for display.

    Parameters
    ----------
    error : dict
        Error dictionary from validate_dataframe()
    row_count : int
        Total row count of the table
    table_name : str
        Name of the table

    Returns
    -------
    dict
        Formatted error with type, description, category, and details
    """
    formatted = {
        'type': error.get('type', 'Unknown Error'),
        'description': error.get('description', str(error)),
        'category': error.get('category', 'other'),
        'details': error.get('details', {}),
        'table_name': table_name,
        'row_count': row_count
    }

    # Add severity if present
    if 'severity' in error:
        formatted['severity'] = error['severity']

    return formatted


def determine_validation_status(
    errors: List[Dict[str, Any]],
    required_columns: Optional[List[str]] = None,
    table_name: Optional[str] = None
) -> str:
    """
    Determine validation status based on errors.

    Status Logic:
    - INCOMPLETE (red): Missing required columns OR non-castable datatype errors
      OR 100% null in required columns
    - PARTIAL (yellow): Required columns present but has data quality issues
      (missing categorical values, high missingness, etc.)
    - COMPLETE (green): All required columns present, no critical issues

    Parameters
    ----------
    errors : list
        List of formatted error dictionaries
    required_columns : list, optional
        List of required column names
    table_name : str, optional
        Name of the table (for table-specific logic)

    Returns
    -------
    str
        'complete', 'partial', or 'incomplete'
    """
    if not errors:
        _logger.debug("determine_validation_status: no errors — status='complete'")
        return 'complete'

    # Check for schema-level errors that make data incomplete
    for error in errors:
        error_type = error.get('type', '').lower()
        category = error.get('category', '')
        details = error.get('details', {})
        severity = error.get('severity', 'error')

        # Missing required columns -> incomplete
        if 'missing required columns' in error_type or 'missing_columns' in details:
            _logger.debug("determine_validation_status: missing required columns — status='incomplete'")
            return 'incomplete'

        # Non-castable data type errors -> incomplete
        if 'data type' in error_type and category == 'schema':
            if details.get('castable') is False:
                _logger.debug("determine_validation_status: non-castable dtype — status='incomplete'")
                return 'incomplete'

        # 100% missingness in required columns -> incomplete
        if 'missingness' in error_type and severity != 'warning':
            pct_missing = details.get('percent_missing', 0)
            column = details.get('column', '')
            if pct_missing >= 100 and required_columns and column in required_columns:
                _logger.debug("determine_validation_status: 100%% null in '%s' — status='incomplete'", column)
                return 'incomplete'

    # Has errors but not critical -> partial
    has_errors = any(
        e.get('severity', 'error') == 'error'
        for e in errors
    )

    if has_errors:
        _logger.debug("determine_validation_status: has non-critical errors — status='partial'")
        return 'partial'

    # Only warnings -> complete
    _logger.debug("determine_validation_status: warnings only — status='complete'")
    return 'complete'


def classify_errors_by_status_impact(
    errors: Dict[str, List[Dict[str, Any]]],
    required_columns: List[str],
    table_name: str,
    config_timezone: Optional[str] = None
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Classify errors into status-affecting and informational categories.

    Used by PDF/report generators to separate critical errors from
    informational messages.

    Parameters
    ----------
    errors : dict
        Dictionary with keys 'schema_errors', 'data_quality_issues', 'other_errors'
    required_columns : list
        List of required column names
    table_name : str
        Name of the table
    config_timezone : str, optional
        Configured timezone (to filter timezone-related errors)

    Returns
    -------
    dict
        Dictionary with 'status_affecting' and 'informational', each containing
        'schema_errors', 'data_quality_issues', and 'other_errors' lists
    """
    status_affecting = {
        'schema_errors': [],
        'data_quality_issues': [],
        'other_errors': []
    }
    informational = {
        'schema_errors': [],
        'data_quality_issues': [],
        'other_errors': []
    }

    # Columns that are optional and shouldn't affect status
    optional_columns = {
        'patient': ['race', 'ethnicity', 'language'],
        'hospitalization': ['discharge_category'],
    }
    table_optional = optional_columns.get(table_name, [])

    # Process each error category
    for category_key in ['schema_errors', 'data_quality_issues', 'other_errors']:
        for error in errors.get(category_key, []):
            error_type = error.get('type', '').lower()
            details = error.get('details', {})
            description = error.get('description', '').lower()

            is_informational = False

            # Filter out timezone errors for configured timezone
            if config_timezone and 'timezone' in error_type:
                if config_timezone.lower() in description:
                    is_informational = True

            # Filter out optional column issues
            column = details.get('column', '')
            if column in table_optional:
                is_informational = True

            # mCIDE coverage gaps are informational
            if 'mcide' in error_type or 'coverage' in error_type:
                is_informational = True

            # Plausibility warnings are informational; plausibility errors affect status
            plausibility_keywords = ['temporal ordering', 'numeric range', 'field plausibility',
                                     'dose unit', 'overlapping', 'distribution shift',
                                     'duplicate composite', 'cross-table temporal']
            is_plausibility = any(p in error_type for p in plausibility_keywords)
            if is_plausibility and error.get('severity') == 'warning':
                is_informational = True

            # Warnings are generally informational
            if error.get('severity') == 'warning':
                is_informational = True

            # Classify into appropriate bucket
            if is_informational:
                informational[category_key].append(error)
            else:
                status_affecting[category_key].append(error)

    return {
        'status_affecting': status_affecting,
        'informational': informational
    }


def get_validation_summary(validation_results: Dict[str, Any]) -> str:
    """
    Generate a text summary of validation results.

    Parameters
    ----------
    validation_results : dict
        Validation results from validate() method

    Returns
    -------
    str
        Human-readable summary string
    """
    status = validation_results.get('status', 'unknown')
    errors = validation_results.get('errors', {})

    schema_count = len(errors.get('schema_errors', []))
    dq_count = len(errors.get('data_quality_issues', []))
    other_count = len(errors.get('other_errors', []))
    total = schema_count + dq_count + other_count

    status_emoji = {
        'complete': '✓',
        'partial': '⚠',
        'incomplete': '✗'
    }

    summary_parts = [
        f"Status: {status_emoji.get(status, '?')} {status.upper()}"
    ]

    if total > 0:
        summary_parts.append(f"Issues: {total} total")
        if schema_count:
            summary_parts.append(f"  - Schema: {schema_count}")
        if dq_count:
            summary_parts.append(f"  - Data Quality: {dq_count}")
        if other_count:
            summary_parts.append(f"  - Other: {other_count}")
    else:
        summary_parts.append("No issues found")

    return '\n'.join(summary_parts)
