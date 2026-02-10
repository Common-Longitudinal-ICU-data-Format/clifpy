"""
Data Quality Assessment (DQA) module for CLIFpy tables.

This module provides comprehensive DQA functions implementing:

CONFORMANCE CHECKS:
- A.1. Table presence verification
- A.2. Required columns presence check
- B.1. Data type validation
- B.2. Datetime format validation
- B.3. Lab reference units validation
- B.4. Categorical values validation against mCIDE

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
def check_conditional_requirements_polars(
    df: Union['pl.DataFrame', 'pl.LazyFrame'],
    table_name: str,
    conditions: Optional[List[Dict[str, Any]]] = None
) -> DQACompletenessResult:
    """Check conditional required fields using Polars."""
    result = DQACompletenessResult("conditional_requirements", table_name)

    default_conditions = {
        'adt': [
            {
                'when_column': 'location_category',
                'when_value': ['icu'],
                'then_required': ['location_type'],
                'description': 'ICU locations must have location_type specified'
            }
        ],
        'respiratory_support': [
            {
                'when_column': 'device_category',
                'when_value': ['IMV', 'NIPPV'],
                'then_required': ['mode_category'],
                'description': 'IMV/NIPPV must have mode_category specified'
            },
            {
                'when_column': 'device_category',
                'when_value': ['IMV'],
                'then_required': ['fio2_set', 'peep_set'],
                'description': 'IMV must have fio2_set and peep_set specified'
            }
        ],
        'crrt_therapy': [
            {
                'when_column': 'crrt_status',
                'when_value': ['running', 'active'],
                'then_required': ['blood_flow_rate', 'dialysate_flow_rate'],
                'description': 'Active CRRT must have flow rates specified'
            }
        ]
    }

    if conditions is None:
        conditions = default_conditions.get(table_name, [])

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
                            "condition": f"{when_col} IN {when_values}",
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

    default_conditions = {
        'adt': [
            {
                'when_column': 'location_category',
                'when_value': ['icu'],
                'then_required': ['location_type'],
                'description': 'ICU locations must have location_type specified'
            }
        ],
        'respiratory_support': [
            {
                'when_column': 'device_category',
                'when_value': ['IMV', 'NIPPV'],
                'then_required': ['mode_category'],
                'description': 'IMV/NIPPV must have mode_category specified'
            }
        ]
    }

    if conditions is None:
        conditions = default_conditions.get(table_name, [])

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

            for req_col in then_required:
                if req_col not in df.columns:
                    continue

                values_str = ', '.join([f"'{v.lower()}'" for v in when_values])
                stats = con.execute(f"""
                    SELECT
                        COUNT(*) as total,
                        COUNT(*) - COUNT("{req_col}") as null_count
                    FROM df
                    WHERE LOWER("{when_col}") IN ({values_str})
                """).fetchone()

                total, null_count = stats

                if total > 0 and null_count > 0:
                    pct_missing = (null_count / total) * 100
                    result.add_warning(
                        f"Conditional requirement violated: {description}",
                        {
                            "condition": f"{when_col} IN {when_values}",
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
    source_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    reference_df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
    source_table: str,
    reference_table: str,
    key_column: str
) -> DQACompletenessResult:
    """Check relational integrity between tables."""
    _logger.debug("check_relational_integrity: starting for '%s' -> '%s' on key '%s'",
                  source_table, reference_table, key_column)
    if _ACTIVE_BACKEND == 'polars':
        result = check_relational_integrity_polars(
            source_df, reference_df, source_table, reference_table, key_column
        )
    else:
        result = check_relational_integrity_duckdb(
            source_df, reference_df,
            source_table, reference_table, key_column
        )
    orphan_count = result.metrics.get("orphan_ids", 0)
    if orphan_count > 0:
        _logger.info("check_relational_integrity: '%s' -> '%s' — %d orphan IDs found",
                     source_table, reference_table, orphan_count)
    _logger.debug("check_relational_integrity: '%s' -> '%s' — source_ids=%s, ref_ids=%s, orphans=%s",
                  source_table, reference_table, result.metrics.get("source_unique_ids"),
                  result.metrics.get("reference_unique_ids"), orphan_count)
    return result

# ---------------------------------------------------------------------------
# DQA: PLAUSIBILITY CHECKS
# ---------------------------------------------------------------------------
# Placeholder for future plausibility checks (e.g., value range validation)


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


# def run_full_dqa(
#     df: Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame'],
#     schema: Dict[str, Any],
#     table_name: str,
#     reference_tables: Optional[Dict[str, Union[pd.DataFrame, 'pl.DataFrame', 'pl.LazyFrame']]] = None
# ) -> Dict[str, Any]:
#     """
#     Run complete DQA suite on a table.

#     Parameters
#     ----------
#     df : DataFrame
#         The data to validate
#     schema : dict
#         Schema for the table
#     table_name : str
#         Name of the table
#     reference_tables : dict, optional
#         Dictionary of reference tables for relational checks
#         Keys should be 'patient' and/or 'hospitalization'

#     Returns
#     -------
#     Dict[str, Any]
#         Complete DQA results including conformance and completeness checks
#     """
#     _logger.info(f"Starting full DQA for table: {table_name}")

#     # Determine backend for reporting
#     backend = 'polars' if HAS_POLARS else ('duckdb' if HAS_DUCKDB else 'none')

#     results = {
#         'table_name': table_name,
#         'backend': backend,
#         'conformance': {},
#         'completeness': {},
#         'relational': {}
#     }

#     results['conformance'] = {
#         k: v.to_dict()
#         for k, v in run_conformance_checks(df, schema, table_name).items()
#     }

#     results['completeness'] = {
#         k: v.to_dict()
#         for k, v in run_completeness_checks(df, schema, table_name).items()
#     }

#     if reference_tables:
#         if HAS_POLARS:
#             lf = df if isinstance(df, pl.LazyFrame) else df.lazy()
#             col_names = lf.collect_schema().names()
#         else:
#             col_names = df.columns.tolist()

#         if 'patient' in reference_tables and 'patient_id' in col_names:
#             results['relational']['patient_id'] = check_relational_integrity(
#                 df, reference_tables['patient'], table_name, 'patient', 'patient_id'
#             ).to_dict()

#         if 'hospitalization' in reference_tables and 'hospitalization_id' in col_names:
#             results['relational']['hospitalization_id'] = check_relational_integrity(
#                 df, reference_tables['hospitalization'], table_name, 'hospitalization', 'hospitalization_id'
#             ).to_dict()

#     _clear_memory()
#     _logger.info(f"Completed full DQA for table: {table_name}")

#     return results


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
        'mcide_value_coverage': 'mCIDE Coverage Gap'
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
