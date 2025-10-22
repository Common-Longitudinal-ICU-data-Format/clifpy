"""Comprehensive validator module for CLIFpy tables.

This module provides validation functions for CLIFpy tables including:

- Column presence and data type validation with casting capability checks
- Missing data analysis
- Categorical value validation
- Duplicate checking
- Numeric range validation
- Statistical analysis
- Unit validation
- Cohort analysis

Datatype Validation Behavior:

- The validator first checks if columns match their expected types exactly
- If not, it checks whether the data can be cast to the correct type
- Castable mismatches generate warnings (type: "datatype_castable")
- Non-castable mismatches generate errors (type: "datatype_mismatch")
- This allows for more flexible data handling while maintaining type safety

All validation functions include proper error handling and return
structured results for integration with the BaseTable class.
"""
from __future__ import annotations

import json
import os
import yaml
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from multiprocessing import Pool, cpu_count
from functools import partial

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _is_varchar_dtype(series: pd.Series) -> bool:
    """Check if series is VARCHAR-compatible (string or object dtype with strings)."""
    # Check for pandas string dtype
    if pd.api.types.is_string_dtype(series):
        return True
    
    # Check for object dtype that contains strings
    if pd.api.types.is_object_dtype(series):
        # Sample a few non-null values to check if they're strings
        non_null = series.dropna()
        if len(non_null) == 0:
            return True  # Empty series is considered valid
        
        # Check first few values to see if they're strings
        sample_size = min(100, len(non_null))
        sample = non_null.iloc[:sample_size]
        return all(isinstance(x, str) for x in sample)
    
    return False

def _is_integer_dtype(series: pd.Series) -> bool:
    """Check if series is integer-compatible."""
    return pd.api.types.is_integer_dtype(series)

def _is_float_dtype(series: pd.Series) -> bool:
    """Check if series is float-compatible (includes integers)."""
    return pd.api.types.is_numeric_dtype(series)

def _can_cast_to_varchar(series: pd.Series) -> bool:
    """Check if series can be cast to VARCHAR (string)."""
    try:
        # Almost everything can be converted to string
        non_null = series.dropna()
        if len(non_null) == 0:
            return True

        # Try converting a sample
        sample_size = min(10, len(non_null))
        sample = non_null.iloc[:sample_size]
        sample.astype(str)
        return True
    except Exception:
        return False

def _can_cast_to_datetime(series: pd.Series) -> bool:
    """Check if series can be cast to DATETIME."""
    try:
        non_null = series.dropna()
        if len(non_null) == 0:
            return True

        # Try converting a sample
        sample_size = min(10, len(non_null))
        sample = non_null.iloc[:sample_size]
        pd.to_datetime(sample, errors='raise')
        return True
    except Exception:
        return False

def _can_cast_to_integer(series: pd.Series) -> bool:
    """Check if series can be cast to INTEGER."""
    try:
        non_null = series.dropna()
        if len(non_null) == 0:
            return True

        # Check if already numeric
        if pd.api.types.is_numeric_dtype(series):
            # Check if all values are whole numbers
            return all(float(x).is_integer() for x in non_null)

        # Try converting string/object to numeric then check if integers
        sample_size = min(10, len(non_null))
        sample = non_null.iloc[:sample_size]
        numeric_sample = pd.to_numeric(sample, errors='raise')
        return all(float(x).is_integer() for x in numeric_sample)
    except Exception:
        return False

def _can_cast_to_float(series: pd.Series) -> bool:
    """Check if series can be cast to FLOAT."""
    try:
        non_null = series.dropna()
        if len(non_null) == 0:
            return True

        # Check if already numeric
        if pd.api.types.is_numeric_dtype(series):
            return True

        # Try converting string/object to numeric
        sample_size = min(10, len(non_null))
        sample = non_null.iloc[:sample_size]
        pd.to_numeric(sample, errors='raise')
        return True
    except Exception:
        return False

# Map mCIDE "data_type" values to simple pandas dtype checkers.
# Extend as more types are introduced.
_DATATYPE_CHECKERS: dict[str, callable[[pd.Series], bool]] = {
    "VARCHAR": _is_varchar_dtype,
    "DATETIME": pd.api.types.is_datetime64_any_dtype,
    "INTEGER": _is_integer_dtype,
    "INT": _is_integer_dtype,  # Alternative naming
    "FLOAT": _is_float_dtype,
    "DOUBLE": _is_float_dtype,  # Alternative naming for float
}

# Map mCIDE "data_type" values to casting checkers.
_DATATYPE_CAST_CHECKERS: dict[str, callable[[pd.Series], bool]] = {
    "VARCHAR": _can_cast_to_varchar,
    "DATETIME": _can_cast_to_datetime,
    "INTEGER": _can_cast_to_integer,
    "INT": _can_cast_to_integer,  # Alternative naming
    "FLOAT": _can_cast_to_float,
    "DOUBLE": _can_cast_to_float,  # Alternative naming for float
}


class ValidationError(Exception):
    """Exception raised when validation fails.

    The *errors* attribute contains a list describing validation issues.
    """

    def __init__(self, errors: List[Dict[str, Any]]):
        super().__init__("Validation failed")
        self.errors = errors


# ---------------------------------------------------------------------------
# JSON spec utilities
# ---------------------------------------------------------------------------

_DEF_SPEC_DIR = os.path.join(os.path.dirname(__file__), os.pardir, "mCIDE")


def _load_spec(table_name: str, spec_dir: str | None = None) -> dict[str, Any]:
    """Load and return the mCIDE JSON spec for *table_name*."""

    spec_dir = spec_dir or _DEF_SPEC_DIR
    filename = f"{table_name.capitalize()}Model.json"
    path = os.path.join(spec_dir, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"mCIDE spec not found: {path}")

    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Public validation helpers
# ---------------------------------------------------------------------------

def validate_dataframe(df: pd.DataFrame, spec: dict[str, Any]) -> List[dict[str, Any]]:
    """Validate *df* against *spec*.

    Returns a list of error dictionaries. An empty list means success.

    For datatype validation:
    
    - If a column doesn't match the expected type exactly, the validator checks
      if the data can be cast to the correct type
    - Castable type mismatches return warnings with type "datatype_castable"
    - Non-castable type mismatches return errors with type "datatype_mismatch"
    - Both include descriptive messages about the casting capability
    """

    errors: List[dict[str, Any]] = []

    # 1. Required columns present ------------------------------------------------
    req_cols = set(spec.get("required_columns", []))
    missing = req_cols - set(df.columns)
    if missing:
        missing_list = sorted(missing)
        errors.append({
            "type": "missing_columns",
            "columns": missing_list,
            "message": f"Missing required columns: {', '.join(missing_list)}"
        })

    # 2. Per-column checks -------------------------------------------------------
    for col_spec in spec.get("columns", []):
        name = col_spec["name"]
        if name not in df.columns:
            # If it's required the above block already captured the issue.
            continue

        series = df[name]

        # 2a. NULL checks -----------------------------------------------------
        if col_spec.get("required", False):
            null_cnt = int(series.isna().sum())
            total_cnt = int(len(series))
            null_pct = (null_cnt / total_cnt * 100) if total_cnt > 0 else 0.0
            if null_cnt:
                errors.append({
                    "type": "null_values",
                    "column": name,
                    "count": null_cnt,
                    "percent": round(null_pct, 2),
                    "message": f"Column '{name}' has {null_cnt} null values ({null_pct:.2f}%) in required field"
                })

        # 2b. Datatype checks -------------------------------------------------
        expected_type = col_spec.get("data_type")
        checker = _DATATYPE_CHECKERS.get(expected_type)
        cast_checker = _DATATYPE_CAST_CHECKERS.get(expected_type)

        if checker and not checker(series):
            # Check if data can be cast to the correct type
            if cast_checker and cast_checker(series):
                # Data can be cast - this is a warning, not an error
                errors.append({
                    "type": "datatype_castable",
                    "column": name,
                    "expected": expected_type,
                    "actual": str(series.dtype),
                    "message": f"Column '{name}' has type {series.dtype} but can be cast to {expected_type}"
                })
            else:
                # Data cannot be cast - this is an error
                errors.append({
                    "type": "datatype_mismatch",
                    "column": name,
                    "expected": expected_type,
                    "actual": str(series.dtype),
                    "message": f"Column '{name}' has type {series.dtype} and cannot be cast to {expected_type}"
                })

        # # 2c. Category values -------------------------------------------------
        # if col_spec.get("is_category_column") and col_spec.get("permissible_values"):
        #     allowed = set(col_spec["permissible_values"])
        #     actual_values = set(series.dropna().unique())

        #     # Check for missing expected values (permissible values not present in data)
        #     missing_values = [v for v in allowed if v not in actual_values]
        #     if missing_values:
        #         errors.append({
        #             "type": "missing_category_values",
        #             "column": name,
        #             "missing_values": missing_values,
        #             "message": f"Column '{name}' is missing expected category values: {missing_values}"
        #         })

    return errors


def validate_table(
    df: pd.DataFrame, table_name: str, spec_dir: str | None = None
) -> List[dict[str, Any]]:
    """Validate *df* using the JSON spec for *table_name*.

    Convenience wrapper combining :pyfunc:`_load_spec` and
    :pyfunc:`validate_dataframe`.
    """

    spec = _load_spec(table_name, spec_dir)
    return validate_dataframe(df, spec)


# ---------------------------------------------------------------------------
# Enhanced validation functions
# ---------------------------------------------------------------------------

def check_required_columns(
    df: pd.DataFrame, 
    column_names: List[str], 
    table_name: str
) -> Dict[str, Any]:
    """
    Validate that required columns are present in the dataframe.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    column_names : List[str]
        List of required column names
    table_name : str
        Name of the table being validated
        
    Returns
    -------
    dict
        Dictionary with validation results including missing columns
    """
    try:
        missing_columns = [col for col in column_names if col not in df.columns]
        
        if missing_columns:
            return {
                "type": "missing_required_columns",
                "table": table_name,
                "missing_columns": missing_columns,
                "status": "error",
                "message": f"Table '{table_name}' is missing required columns: {', '.join(missing_columns)}"
            }
        
        return {
            "type": "missing_required_columns",
            "table": table_name,
            "status": "success",
            "message": f"Table '{table_name}' has all required columns"
        }
        
    except Exception as e:
        return {
            "type": "missing_required_columns",
            "table": table_name,
            "status": "error",
            "error_message": str(e),
            "message": f"Error checking required columns for table '{table_name}': {str(e)}"
        }


def verify_column_dtypes(df: pd.DataFrame, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Ensure columns have correct data types per schema.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    schema : dict
        Schema containing column definitions
        
    Returns
    -------
    List[dict]
        List of datatype mismatch errors
    """
    errors = []
    
    try:
        for col_spec in schema.get("columns", []):
            name = col_spec["name"]
            if name not in df.columns:
                continue
            
            expected_type = col_spec.get("data_type")
            if not expected_type:
                continue
            
            series = df[name]
            checker = _DATATYPE_CHECKERS.get(expected_type)
            cast_checker = _DATATYPE_CAST_CHECKERS.get(expected_type)

            if checker and not checker(series):
                # Check if data can be cast to the correct type
                if cast_checker and cast_checker(series):
                    # Data can be cast - this is a warning, not an error
                    errors.append({
                        "type": "datatype_verification_castable",
                        "column": name,
                        "expected": expected_type,
                        "actual": str(series.dtype),
                        "status": "warning",
                        "message": f"Column '{name}' has type {series.dtype} but can be cast to {expected_type}"
                    })
                else:
                    # Data cannot be cast - this is an error
                    errors.append({
                        "type": "datatype_verification",
                        "column": name,
                        "expected": expected_type,
                        "actual": str(series.dtype),
                        "status": "error",
                        "message": f"Column '{name}' has type {series.dtype} and cannot be cast to {expected_type}"
                    })
                
    except Exception as e:
        errors.append({
            "type": "datatype_verification",
            "status": "error",
            "error_message": str(e),
            "message": f"Error during datatype verification: {str(e)}"
        })
    
    return errors


def validate_datetime_timezone(
    df: pd.DataFrame, 
    datetime_columns: List[str]
) -> List[Dict[str, Any]]:
    """
    Validate that all datetime columns are in UTC format.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    datetime_columns : List[str]
        List of datetime column names
        
    Returns
    -------
    List[dict]
        List of timezone validation results
    """
    results = []
    
    try:
        for col in datetime_columns:
            if col not in df.columns:
                continue
            
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                # Check if timezone-aware
                if df[col].dt.tz is not None:
                    # Check if UTC
                    if str(df[col].dt.tz) != 'UTC':
                        results.append({
                            "type": "datetime_timezone",
                            "column": col,
                            "timezone": str(df[col].dt.tz),
                            "expected": "UTC",
                            "status": "warning",
                            "message": f"Column '{col}' has timezone '{df[col].dt.tz}' but expected 'UTC'"
                        })
                else:
                    # Timezone-naive datetime
                    results.append({
                        "type": "datetime_timezone",
                        "column": col,
                        "timezone": "naive",
                        "expected": "UTC",
                        "status": "info",
                        "message": f"Column '{col}' is timezone-naive, expected UTC timezone"
                    })
                    
    except Exception as e:
        results.append({
            "type": "datetime_timezone",
            "status": "error",
            "error_message": str(e),
            "message": f"Error validating datetime timezones: {str(e)}"
        })
    
    return results


def calculate_missing_stats(
    df: pd.DataFrame,
    format: str = 'long'
) -> pd.DataFrame:
    """
    Report count and percentage of missing values as a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    format : str, default='long'
        Output format:
        - 'long': One row per column (better for many columns)
        - 'wide': Transposed format (better for few columns)

    Returns
    -------
    pd.DataFrame
        Missing data statistics with columns:
        - column: Column name
        - missing_count: Number of missing values
        - missing_percent: Percentage missing
        - total_rows: Total row count (long format only)
    """
    try:
        # Get comprehensive summary from the core function
        summary = report_missing_data_summary(df)

        # Handle error case
        if "error" in summary:
            return pd.DataFrame({'error': [summary['error']]})

        # Convert to DataFrame format
        if format == 'long':
            # Include all columns (even those with no missing data)
            all_columns_data = []

            # Add columns with missing data (already sorted by missing_percent descending)
            for item in summary['columns_with_missing']:
                all_columns_data.append({
                    'column': item['column'],
                    'missing_count': item['missing_count'],
                    'missing_percent': round(item['missing_percent'], 2),
                    'total_rows': summary['total_rows']
                })

            # Add complete columns (no missing data)
            for col in summary['complete_columns']:
                all_columns_data.append({
                    'column': col,
                    'missing_count': 0,
                    'missing_percent': 0.0,
                    'total_rows': summary['total_rows']
                })

            # Create DataFrame
            stats_df = pd.DataFrame(all_columns_data)

        else:  # wide format
            # Create dict for all columns
            missing_counts = {}
            missing_percents = {}

            for item in summary['columns_with_missing']:
                missing_counts[item['column']] = item['missing_count']
                missing_percents[item['column']] = round(item['missing_percent'], 2)

            for col in summary['complete_columns']:
                missing_counts[col] = 0
                missing_percents[col] = 0.0

            stats_df = pd.DataFrame({
                'missing_count': missing_counts,
                'missing_percent': missing_percents
            }).T

        return stats_df

    except Exception as e:
        return pd.DataFrame({'error': [str(e)]})


def report_missing_data_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate comprehensive missing data report.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
        
    Returns
    -------
    dict
        Comprehensive missing data summary
    """
    try:
        total_cells = df.shape[0] * df.shape[1]
        total_missing = df.isnull().sum().sum()
        
        summary = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "total_cells": total_cells,
            "total_missing_cells": int(total_missing),
            "overall_missing_percent": (total_missing / total_cells) * 100 if total_cells > 0 else 0,
            "columns_with_missing": [],
            "complete_columns": []
        }
        
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                summary["columns_with_missing"].append({
                    "column": col,
                    "missing_count": int(missing_count),
                    "missing_percent": (missing_count / len(df)) * 100
                })
            else:
                summary["complete_columns"].append(col)
        
        # Sort columns by missing percentage
        summary["columns_with_missing"] = sorted(
            summary["columns_with_missing"],
            key=lambda x: x["missing_percent"],
            reverse=True
        )
        
        return summary
        
    except Exception as e:
        return {"error": str(e)}


def validate_categorical_values(
    df: pd.DataFrame,
    schema: Dict[str, Any],
    detect_invalid_values: bool = False,
    return_invalid_df: bool = False
) -> List[Dict[str, Any]] | Tuple[List[Dict[str, Any]], pd.DataFrame]:
    """
    Check values against permitted categories.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    schema : dict
        Schema containing category definitions
    detect_invalid_values : bool, optional
        If True, detect and report values in data that are not in permissible_values list.
        If False (default), only check for missing expected values (backward compatible).
    return_invalid_df : bool, optional
        If True and detect_invalid_values=True, return a tuple of (errors, invalid_values_df).
        The invalid_values_df contains all rows with invalid categorical values.
        Default is False for backward compatibility.

    Returns
    -------
    List[dict] or Tuple[List[dict], pd.DataFrame]
        If return_invalid_df=False: List of validation errors (backward compatible)
        If return_invalid_df=True: Tuple of (errors_list, invalid_values_dataframe)

    Notes
    -----
    Categorical matching is case-insensitive (e.g., "Male" matches "MALE").

    For backward compatibility, set detect_invalid_values=False (default).
    This will only report missing expected values, not invalid values in the data.
    """
    errors = []
    invalid_rows_indices = []  # Track rows with invalid values

    try:
        category_columns = schema.get("category_columns") or []

        for col_spec in schema.get("columns", []):
            name = col_spec["name"]
            
            if name not in df.columns or name not in category_columns:
                continue
            
            if col_spec.get("permissible_values"):
                # Original permissible values from schema
                allowed = set(col_spec["permissible_values"])
                # Convert to lowercase for case-insensitive comparison
                allowed_lower = {str(v).lower() for v in allowed}

                # Get actual unique values from data
                unique_values_raw = df[name].dropna().unique()
                unique_values_lower = {str(v).lower() for v in unique_values_raw}

                # 1. Check for missing expected values (case-insensitive)
                # This check is always performed for backward compatibility
                missing_values = [v for v in allowed if str(v).lower() not in unique_values_lower]

                # Report missing expected values
                if missing_values:
                    errors.append({
                                "type": "missing_categorical_values",
                                "column": name,
                                "missing_values": missing_values,
                                "total_missing": len(missing_values),
                                "message": f"Column '{name}' is missing {len(missing_values)} expected category values: {missing_values}"
                            })

                # 2. Check for INVALID values (ONLY if detect_invalid_values=True)
                if detect_invalid_values:
                    invalid_values = []
                    invalid_value_counts = {}

                    # Identify and count occurrences of each invalid value
                    for val in unique_values_raw:
                        if str(val).lower() not in allowed_lower:
                            invalid_values.append(val)
                            # Count how many times this invalid value appears
                            count = int((df[name] == val).sum())
                            invalid_value_counts[str(val)] = count

                            # Track row indices for creating invalid_df later
                            if return_invalid_df:
                                invalid_mask = (df[name] == val)
                                invalid_rows_indices.extend(df[invalid_mask].index.tolist())

                    # Sort invalid values by frequency (most common first)
                    invalid_values_sorted = sorted(invalid_values,
                                                   key=lambda x: invalid_value_counts.get(str(x), 0),
                                                   reverse=True)

                    # Report INVALID values found in data
                    if invalid_values:
                        # Show top 10 most common invalid values with their counts
                        top_invalid_display = []
                        for val in invalid_values_sorted[:10]:
                            count = invalid_value_counts[str(val)]
                            top_invalid_display.append(f"{val} ({count:,} occurrences)")

                        errors.append({
                            "type": "invalid_categorical_values",
                            "column": name,
                            "invalid_values": invalid_values_sorted[:20],  # Store top 20 for reference
                            "invalid_value_counts": invalid_value_counts,  # Full counts for analysis
                            "total_invalid_unique": len(invalid_values),
                            "total_invalid_rows": sum(invalid_value_counts.values()),
                            "permissible_values": list(allowed),  # Include what IS allowed for reference
                            "status": "error",
                            "message": f"Column '{name}' contains {len(invalid_values)} unique invalid categorical values affecting {sum(invalid_value_counts.values()):,} rows. Top invalid: {', '.join(top_invalid_display[:5])}"
                        })
                    
    except Exception as e:
        errors.append({
            "type": "categorical_validation",
            "status": "error",
            "error_message": str(e),
            "message": f"Error validating categorical values: {str(e)}"
        })

    # Return based on parameters
    if return_invalid_df and detect_invalid_values:
        # Create DataFrame of rows with invalid categorical values
        if invalid_rows_indices:
            # Remove duplicates and sort
            unique_indices = sorted(set(invalid_rows_indices))
            invalid_df = df.loc[unique_indices].copy()
        else:
            # Return empty DataFrame with same columns if no invalid rows
            invalid_df = pd.DataFrame(columns=df.columns)

        return errors, invalid_df
    else:
        # Backward compatible return
        return errors


def check_for_duplicates(
    df: pd.DataFrame, 
    composite_keys: List[str]
) -> Dict[str, Any]:
    """
    Validate uniqueness constraints on composite keys.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    composite_keys : List[str]
        List of columns forming the composite key
        
    Returns
    -------
    dict
        Duplicate checking results
    """
    try:
        # Filter to only keys that exist in the dataframe
        existing_keys = [key for key in composite_keys if key in df.columns]
        
        if not existing_keys:
            return {
                "type": "duplicate_check",
                "status": "skipped",
                "message": "No composite key columns found in dataframe"
            }
        
        # Check for duplicates
        duplicated = df.duplicated(subset=existing_keys, keep=False)
        num_duplicates = duplicated.sum()
        
        result = {
            "type": "duplicate_check",
            "composite_keys": existing_keys,
            "total_rows": len(df),
            "duplicate_rows": int(num_duplicates),
            "unique_rows": len(df) - int(num_duplicates),
            "has_duplicates": num_duplicates > 0
        }

        if num_duplicates > 0:
            # Get examples of duplicate keys (limit to 5)
            duplicate_df = df[duplicated]
            duplicate_examples = (
                duplicate_df[existing_keys]
                .drop_duplicates()
                .head(5)
                .to_dict('records')
            )
            result["duplicate_examples"] = duplicate_examples
            result["status"] = "warning"
            result["message"] = f"Found {int(num_duplicates)} duplicate rows out of {len(df)} total rows based on keys: {', '.join(existing_keys)}"
        else:
            result["status"] = "success"
            result["message"] = f"No duplicate rows found based on composite keys: {', '.join(existing_keys)}"
        
        return result
        
    except Exception as e:
        return {
            "type": "duplicate_check",
            "status": "error",
            "error_message": str(e),
            "message": f"Error checking for duplicates: {str(e)}"
        }


def generate_summary_statistics(
    df: pd.DataFrame, 
    numeric_columns: List[str],
    output_path: str = None,
    table_name: str = None
) -> pd.DataFrame:
    """
    Calculate Q1, Q3, median for numeric columns.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    numeric_columns : List[str]
        List of numeric column names
    output_path : str, optional
        Path to save the statistics CSV
    table_name : str, optional
        Table name for file naming
        
    Returns
    -------
    pd.DataFrame
        Summary statistics
    """
    try:
        # Filter to existing numeric columns
        existing_cols = [col for col in numeric_columns if col in df.columns]
        
        if not existing_cols:
            return pd.DataFrame()
        
        # Calculate statistics
        stats = df[existing_cols].describe(percentiles=[0.25, 0.5, 0.75])
        
        # Select specific statistics
        summary_stats = stats.loc[['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']]
        summary_stats = summary_stats.rename(index={'25%': 'Q1', '50%': 'median', '75%': 'Q3'})
        
        # Save to CSV if output path provided
        if output_path and table_name:
            filename = os.path.join(output_path, f'summary_statistics_{table_name}.csv')
            summary_stats.to_csv(filename)
        
        return summary_stats
        
    except Exception as e:
        return pd.DataFrame({'error': [str(e)]})


def analyze_skewed_distributions(
    df: pd.DataFrame,
    output_path: str = None,
    table_name: str = None
) -> pd.DataFrame:
    """
    Identify and report skewed variables.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    output_path : str, optional
        Path to save the analysis CSV
    table_name : str, optional
        Table name for file naming
        
    Returns
    -------
    pd.DataFrame
        Skewness analysis results
    """
    try:
        numeric_df = df.select_dtypes(include=['number'])
        
        if numeric_df.empty:
            return pd.DataFrame()
        
        skewness = numeric_df.skew()
        kurtosis = numeric_df.kurtosis()
        
        analysis = pd.DataFrame({
            'column': skewness.index,
            'skewness': skewness.values,
            'kurtosis': kurtosis.values,
            'skew_interpretation': pd.cut(
                skewness.values,
                bins=[-float('inf'), -1, -0.5, 0.5, 1, float('inf')],
                labels=['Highly left-skewed', 'Moderately left-skewed', 
                       'Approximately symmetric', 'Moderately right-skewed', 
                       'Highly right-skewed']
            )
        })
        
        # Sort by absolute skewness
        analysis['abs_skewness'] = analysis['skewness'].abs()
        analysis = analysis.sort_values('abs_skewness', ascending=False)
        analysis = analysis.drop('abs_skewness', axis=1)
        
        # Save to CSV if output path provided
        if output_path and table_name:
            filename = os.path.join(output_path, f'skewness_analysis_{table_name}.csv')
            analysis.to_csv(filename, index=False)
        
        return analysis
        
    except Exception as e:
        return pd.DataFrame({'error': [str(e)]})


def validate_units(
    df: pd.DataFrame, 
    unit_mappings: Dict[str, Any], 
    table_name: str
) -> List[Dict[str, Any]]:
    """
    Verify units match schema (critical for labs and vitals).
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    unit_mappings : dict
        Expected units for each category
    table_name : str
        Name of the table being validated
        
    Returns
    -------
    List[dict]
        List of unit validation results
    """
    results = []
    
    try:
        # Table-specific unit validation
        if table_name == 'vitals' and 'vital_category' in df.columns:
            # For vitals, check if categories match expected units
            for category, expected_unit in unit_mappings.items():
                category_data = df[df['vital_category'] == category]
                if not category_data.empty:
                    results.append({
                        "type": "unit_validation",
                        "table": table_name,
                        "category": category,
                        "expected_unit": expected_unit,
                        "row_count": len(category_data),
                        "status": "info",
                        "message": f"Table '{table_name}' category '{category}' found with {len(category_data)} rows, expected unit: {expected_unit}"
                    })
                    
        elif table_name == 'labs' and 'lab_category' in df.columns and 'reference_unit' in df.columns:
            # For labs, check if reference units match expected
            for category, expected_units in unit_mappings.items():
                category_data = df[df['lab_category'] == category]
                if not category_data.empty:
                    actual_units = category_data['reference_unit'].dropna().unique()
                    
                    # Check if any unexpected units
                    unexpected_units = [u for u in actual_units if u not in expected_units]
                    
                    if unexpected_units:
                        results.append({
                            "type": "unit_validation",
                            "table": table_name,
                            "category": category,
                            "expected_units": expected_units,
                            "unexpected_units": list(unexpected_units),
                            "status": "warning",
                            "message": f"Table '{table_name}' category '{category}' has unexpected units: {', '.join(unexpected_units)}, expected: {', '.join(expected_units)}"
                        })
                        
    except Exception as e:
        results.append({
            "type": "unit_validation",
            "table": table_name,
            "status": "error",
            "error_message": str(e),
            "message": f"Error validating units for table '{table_name}': {str(e)}"
        })
    
    return results


def calculate_cohort_sizes(
    df: pd.DataFrame, 
    id_columns: List[str]
) -> Dict[str, int]:
    """
    Calculate distinct counts of ID columns.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    id_columns : List[str]
        List of ID column names
        
    Returns
    -------
    dict
        Distinct counts for each ID column
    """
    try:
        cohort_sizes = {}
        
        for col in id_columns:
            if col in df.columns:
                cohort_sizes[col] = df[col].nunique()
                cohort_sizes[f"{col}_with_nulls"] = df[col].isnull().sum()
        
        # Add total row count
        cohort_sizes["total_rows"] = len(df)
        
        return cohort_sizes
        
    except Exception as e:
        return {"error": str(e)}


def get_distinct_counts(
    df: pd.DataFrame, 
    columns: List[str]
) -> Dict[str, int]:
    """
    General distinct count function.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    columns : List[str]
        List of column names
        
    Returns
    -------
    dict
        Distinct counts for each column
    """
    try:
        distinct_counts = {}
        
        for col in columns:
            if col in df.columns:
                distinct_counts[col] = {
                    "distinct_count": df[col].nunique(),
                    "total_count": len(df[col]),
                    "null_count": df[col].isnull().sum(),
                    "distinct_ratio": df[col].nunique() / len(df) if len(df) > 0 else 0
                }
        
        return distinct_counts
        
    except Exception as e:
        return {"error": str(e)}

# ---------------------------------------------------------------------------
# Outlier range validation from outlier_config.yaml
# ---------------------------------------------------------------------------

def load_outlier_config(config_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load outlier configuration from YAML file."""
    try:
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(os.path.dirname(__file__)),
                'schemas',
                'outlier_config.yaml'
            )
        if not os.path.exists(config_path):
            return None
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return None


def _validate_range(df: pd.DataFrame, mask: pd.Series, column: str,
                    min_val: float, max_val: float) -> Tuple[int, int]:
    """Helper to check min/max violations for masked data."""
    data = df.loc[mask, column].dropna()
    if len(data) == 0:
        return 0, 0
    return int((data < min_val).sum()), int((data > max_val).sum())


def _validate_range_polars(df_pl: 'pl.DataFrame', mask_expr: 'pl.Expr', column: str,
                           min_val: float, max_val: float) -> Tuple[int, int]:
    """Helper to check min/max violations for masked data using Polars."""
    data = df_pl.filter(mask_expr).select(pl.col(column).drop_nulls())
    if data.height == 0:
        return 0, 0

    col_data = data[column]
    below = (col_data < min_val).sum()
    above = (col_data > max_val).sum()
    return int(below), int(above)


def _validate_simple_range(
    df: pd.DataFrame,
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Validate simple range pattern: {min: X, max: Y}"""
    results = []
    below, above = _validate_range(df, df.index, col_name, col_config['min'], col_config['max'])
    if below > 0 or above > 0:
        total_values = int(df[col_name].notna().sum())
        total_outliers = below + above

        results.append({
            "type": "outlier_validation",
            "table": table_name,
            "column": col_name,
            "min_expected": col_config['min'],
            "max_expected": col_config['max'],
            "total_values": total_values,
            "below_min_count": below,
            "above_max_count": above,
            "total_outliers": total_outliers,
            "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
            "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
            "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
            "status": "warning",
            "message": f"Column '{col_name}' has {below} values below minimum *{col_config['min']}* & {above} values above maximum *{col_config['max']}*"
        })
    return results


def _validate_simple_range_polars(
    df_pl: 'pl.DataFrame',
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Validate simple range pattern using Polars: {min: X, max: Y}"""
    results = []
    col_data = df_pl.select(pl.col(col_name).drop_nulls())

    if col_data.height == 0:
        return results

    below = int((col_data[col_name] < col_config['min']).sum())
    above = int((col_data[col_name] > col_config['max']).sum())

    if below > 0 or above > 0:
        total_values = col_data.height
        total_outliers = below + above

        results.append({
            "type": "outlier_validation",
            "table": table_name,
            "column": col_name,
            "min_expected": col_config['min'],
            "max_expected": col_config['max'],
            "total_values": total_values,
            "below_min_count": below,
            "above_max_count": above,
            "total_outliers": total_outliers,
            "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
            "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
            "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
            "status": "warning",
            "message": f"Column '{col_name}' has {below} values below minimum *{col_config['min']}* & {above} values above maximum *{col_config['max']}*"
        })
    return results


def _validate_single_category(
    df: pd.DataFrame,
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any],
    category_col: str
) -> List[Dict[str, Any]]:
    """Validate single category pattern: {category: {min: X, max: Y}}"""
    results = []
    for category, ranges in col_config.items():
        mask = df[category_col].astype(str).str.lower() == category.lower()
        if not mask.any():
            continue
        below, above = _validate_range(df, mask, col_name, ranges['min'], ranges['max'])
        if below > 0 or above > 0:
            total_values = int(df.loc[mask, col_name].notna().sum())
            total_outliers = below + above

            results.append({
                "type": "outlier_validation",
                "category": category,
                "column": col_name,
                "min_expected": ranges['min'],
                "max_expected": ranges['max'],
                "total_values": total_values,
                "below_min_count": below,
                "above_max_count": above,
                "total_outliers": total_outliers,
                "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
                "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
                "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
                "status": "warning",
                "message": f"Category {category} for column '{col_name}' has {below} values below minimum *{ranges['min']}* & {above} values above maximum *{ranges['max']}*"
            })
    return results


def _validate_single_category_polars(
    df_pl: 'pl.DataFrame',
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any],
    category_col: str
) -> List[Dict[str, Any]]:
    """Validate single category pattern using Polars: {category: {min: X, max: Y}}"""
    results = []
    for category, ranges in col_config.items():
        mask = pl.col(category_col).cast(pl.Utf8).str.to_lowercase() == category.lower()
        filtered = df_pl.filter(mask)

        if filtered.height == 0:
            continue

        below, above = _validate_range_polars(df_pl, mask, col_name, ranges['min'], ranges['max'])
        if below > 0 or above > 0:
            total_values = int(filtered.select(pl.col(col_name).is_not_null().sum())[0, 0])
            total_outliers = below + above

            results.append({
                "type": "outlier_validation",
                "category": category,
                "column": col_name,
                "min_expected": ranges['min'],
                "max_expected": ranges['max'],
                "total_values": total_values,
                "below_min_count": below,
                "above_max_count": above,
                "total_outliers": total_outliers,
                "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
                "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
                "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
                "status": "warning",
                "message": f"Category {category} for column '{col_name}' has {below} values below minimum *{ranges['min']}* & {above} values above maximum *{ranges['max']}*"
            })
    return results


def _validate_double_category(
    df: pd.DataFrame,
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any],
    primary_col: str,
    secondary_col: str
) -> List[Dict[str, Any]]:
    """Validate double category pattern: {cat1: {cat2: {min: X, max: Y}}}"""
    results = []
    for cat1, sub_config in col_config.items():
        for cat2, ranges in sub_config.items():
            if not isinstance(ranges, dict) or 'min' not in ranges:
                continue
            mask = (
                (df[primary_col].astype(str).str.lower() == cat1.lower()) &
                (df[secondary_col].astype(str).str.lower() == cat2.lower())
            )
            if not mask.any():
                continue
            below, above = _validate_range(df, mask, col_name, ranges['min'], ranges['max'])
            if below > 0 or above > 0:
                total_values = int(df.loc[mask, col_name].notna().sum())
                total_outliers = below + above

                results.append({
                    "type": "outlier_validation",
                    "primary_category": cat1,
                    "secondary_category": cat2,
                    "column": col_name,
                    "min_expected": ranges['min'],
                    "max_expected": ranges['max'],
                    "total_values": total_values,
                    "below_min_count": below,
                    "above_max_count": above,
                    "total_outliers": total_outliers,
                    "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
                    "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
                    "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
                    "status": "warning",
                    "message": f"{cat1} ({cat2}) for column '{col_name}' has {below} values below minimum *{ranges['min']}* & {above} values above maximum *{ranges['max']}*"
                })
    return results


def _validate_double_category_polars(
    df_pl: 'pl.DataFrame',
    table_name: str,
    col_name: str,
    col_config: Dict[str, Any],
    primary_col: str,
    secondary_col: str
) -> List[Dict[str, Any]]:
    """Validate double category pattern using Polars: {cat1: {cat2: {min: X, max: Y}}}"""
    results = []
    for cat1, sub_config in col_config.items():
        for cat2, ranges in sub_config.items():
            if not isinstance(ranges, dict) or 'min' not in ranges:
                continue
            mask = (
                (pl.col(primary_col).cast(pl.Utf8).str.to_lowercase() == cat1.lower()) &
                (pl.col(secondary_col).cast(pl.Utf8).str.to_lowercase() == cat2.lower())
            )
            filtered = df_pl.filter(mask)
            if filtered.height == 0:
                continue

            below, above = _validate_range_polars(df_pl, mask, col_name, ranges['min'], ranges['max'])
            if below > 0 or above > 0:
                total_values = int(filtered.select(pl.col(col_name).is_not_null().sum())[0, 0])
                total_outliers = below + above

                results.append({
                    "type": "outlier_validation",
                    "primary_category": cat1,
                    "secondary_category": cat2,
                    "column": col_name,
                    "min_expected": ranges['min'],
                    "max_expected": ranges['max'],
                    "total_values": total_values,
                    "below_min_count": below,
                    "above_max_count": above,
                    "total_outliers": total_outliers,
                    "outlier_percent": round((total_outliers / total_values * 100), 2) if total_values > 0 else 0.0,
                    "below_min_percent": round((below / total_values * 100), 2) if total_values > 0 else 0.0,
                    "above_max_percent": round((above / total_values * 100), 2) if total_values > 0 else 0.0,
                    "status": "warning",
                    "message": f"{cat1} ({cat2}) for column '{col_name}' has {below} values below minimum *{ranges['min']}* & {above} values above maximum *{ranges['max']}*"
                })
    return results


def _find_category_column(
    df: pd.DataFrame,
    col_config: Dict[str, Any],
    cat_cols: set
) -> Optional[str]:
    """Find which category column matches the config categories."""
    config_cats = set(col_config.keys())
    config_cats_lower = {c.lower() for c in config_cats}
    for cat_col in cat_cols:
        if cat_col in df.columns:
            df_cats = {str(c).lower() for c in df[cat_col].dropna().unique()}
            if config_cats_lower & df_cats:
                return cat_col
    return None


def _find_secondary_category_column(
    df: pd.DataFrame,
    col_config: Dict[str, Any],
    cat_cols: set,
    primary_col: str
) -> Optional[str]:
    """Find the secondary category column for double-nested patterns."""
    first_val = col_config[list(col_config.keys())[0]]
    sec_cats = set(first_val.keys())

    for potential_col in cat_cols:
        if potential_col != primary_col and potential_col in df.columns:
            df_vals = {str(v).lower() for v in df[potential_col].dropna().unique()}
            if sec_cats & {c.lower() for c in df_vals}:
                return potential_col
    return None


def validate_numeric_ranges_from_config(
    df: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    outlier_config: Dict[str, Any],
    use_polars: bool = True,
    chunk_size: Optional[int] = None,
    n_jobs: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Validate numeric ranges using outlier config with automatic pattern detection.

    Automatically handles three patterns:
    - Simple range: {min: X, max: Y}
    - Single-category: {category: {min: X, max: Y}}
    - Double-category: {cat1: {cat2: {min: X, max: Y}}}

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    table_name : str
        Name of the table being validated
    schema : dict
        Table schema with column definitions
    outlier_config : dict
        Outlier configuration from outlier_config.yaml
    use_polars : bool, default=True
        Use Polars for faster processing if available. Falls back to pandas if False or unavailable.
    chunk_size : int, optional
        Process data in chunks of this size. Useful for very large datasets.
        If None, processes entire dataset at once.
    n_jobs : int, optional
        Number of parallel jobs for chunk processing. Defaults to CPU count.
        Only used when chunk_size is specified.

    Returns
    -------
    List[dict]
        List of outlier summaries with type "outlier_summary"

    Examples
    --------
    >>> from clifpy.utils.validator import load_outlier_config, validate_numeric_ranges_from_config
    >>>
    >>> # Load config once
    >>> outlier_config = load_outlier_config()
    >>>
    >>> # Validate multiple tables with Polars (fast)
    >>> vitals_outliers = validate_numeric_ranges_from_config(
    ...     vitals_df, 'vitals', vitals_schema, outlier_config
    ... )
    >>>
    >>> # For very large datasets, use chunking
    >>> large_outliers = validate_numeric_ranges_from_config(
    ...     large_df, 'vitals', vitals_schema, outlier_config,
    ...     chunk_size=100000, n_jobs=4
    ... )
    """
    table_config = outlier_config.get('tables', {}).get(table_name, {})
    if not table_config:
        return []

    # Use chunking for large datasets
    if chunk_size and len(df) > chunk_size:
        return _validate_with_chunking(
            df, table_name, schema, outlier_config, chunk_size, n_jobs, use_polars
        )

    # Use Polars if available and requested
    if use_polars and HAS_POLARS:
        return _validate_numeric_ranges_polars(df, table_name, schema, table_config)
    else:
        return _validate_numeric_ranges_pandas(df, table_name, schema, table_config)


def _validate_numeric_ranges_pandas(
    df: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    table_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Pandas implementation of numeric range validation."""
    results = []

    # Get category columns from schema
    cat_cols = {col['name'] for col in schema.get('columns', [])
                if col.get('is_category_column')}

    # Process each column in config
    for col_name, col_config in table_config.items():
        if col_name not in df.columns:
            continue

        # Pattern 1: Simple range {min: X, max: Y}
        if 'min' in col_config and 'max' in col_config:
            results.extend(_validate_simple_range(df, table_name, col_name, col_config))
            continue

        # Pattern 2 & 3: Category-dependent
        category_col = _find_category_column(df, col_config, cat_cols)
        if not category_col:
            continue

        first_val = col_config[list(col_config.keys())[0]]

        # Pattern 2: Single category {category: {min: X, max: Y}}
        if isinstance(first_val, dict) and 'min' in first_val:
            results.extend(_validate_single_category(
                df, table_name, col_name, col_config, category_col
            ))

        # Pattern 3: Double category {cat1: {cat2: {min: X, max: Y}}}
        else:
            sec_col = _find_secondary_category_column(df, col_config, cat_cols, category_col)
            if sec_col:
                results.extend(_validate_double_category(
                    df, table_name, col_name, col_config, category_col, sec_col
                ))

    return results


def _validate_numeric_ranges_polars(
    df: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    table_config: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """Polars implementation of numeric range validation (much faster)."""
    results = []

    # Convert to Polars DataFrame
    df_pl = pl.from_pandas(df)

    # Get category columns from schema
    cat_cols = {col['name'] for col in schema.get('columns', [])
                if col.get('is_category_column')}

    # Process each column in config
    for col_name, col_config in table_config.items():
        if col_name not in df.columns:
            continue

        # Pattern 1: Simple range {min: X, max: Y}
        if 'min' in col_config and 'max' in col_config:
            results.extend(_validate_simple_range_polars(df_pl, table_name, col_name, col_config))
            continue

        # Pattern 2 & 3: Category-dependent
        category_col = _find_category_column(df, col_config, cat_cols)
        if not category_col:
            continue

        first_val = col_config[list(col_config.keys())[0]]

        # Pattern 2: Single category {category: {min: X, max: Y}}
        if isinstance(first_val, dict) and 'min' in first_val:
            results.extend(_validate_single_category_polars(
                df_pl, table_name, col_name, col_config, category_col
            ))

        # Pattern 3: Double category {cat1: {cat2: {min: X, max: Y}}}
        else:
            sec_col = _find_secondary_category_column(df, col_config, cat_cols, category_col)
            if sec_col:
                results.extend(_validate_double_category_polars(
                    df_pl, table_name, col_name, col_config, category_col, sec_col
                ))

    return results


def _process_chunk(
    chunk: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    outlier_config: Dict[str, Any],
    use_polars: bool
) -> List[Dict[str, Any]]:
    """Process a single chunk of data."""
    return validate_numeric_ranges_from_config(
        chunk, table_name, schema, outlier_config,
        use_polars=use_polars, chunk_size=None
    )


def _validate_with_chunking(
    df: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    outlier_config: Dict[str, Any],
    chunk_size: int,
    n_jobs: Optional[int],
    use_polars: bool
) -> List[Dict[str, Any]]:
    """Process large datasets in chunks with multiprocessing."""
    if n_jobs is None:
        n_jobs = max(1, cpu_count() - 1)

    # Split dataframe into chunks
    chunks = [df[i:i + chunk_size] for i in range(0, len(df), chunk_size)]

    # Process chunks in parallel
    process_func = partial(
        _process_chunk,
        table_name=table_name,
        schema=schema,
        outlier_config=outlier_config,
        use_polars=use_polars
    )

    with Pool(n_jobs) as pool:
        chunk_results = pool.map(process_func, chunks)

    # Merge results from all chunks
    merged_results = {}
    for chunk_result in chunk_results:
        for result in chunk_result:
            # Create unique key for each validation
            if 'category' in result:
                key = (result['column'], result.get('category'))
            elif 'primary_category' in result:
                key = (result['column'], result.get('primary_category'), result.get('secondary_category'))
            else:
                key = (result['column'],)

            # Aggregate counts
            if key in merged_results:
                merged_results[key]['below_min_count'] += result['below_min_count']
                merged_results[key]['above_max_count'] += result['above_max_count']
            else:
                merged_results[key] = result.copy()

    # Update messages with merged counts
    final_results = []
    for result in merged_results.values():
        below = result['below_min_count']
        above = result['above_max_count']

        if 'category' in result:
            category = result['category']
            col_name = result['column']
            result['message'] = f"Category {category} for column '{col_name}' has {below} values below minimum *{result['min_expected']}* & {above} values above maximum *{result['max_expected']}*"
        elif 'primary_category' in result:
            cat1 = result['primary_category']
            cat2 = result['secondary_category']
            col_name = result['column']
            result['message'] = f"{cat1} ({cat2}) for column '{col_name}' has {below} values below minimum *{result['min_expected']}* & {above} values above maximum *{result['max_expected']}*"
        else:
            col_name = result['column']
            result['message'] = f"Column '{col_name}' has {below} values below minimum *{result['min_expected']}* & {above} values above maximum *{result['max_expected']}*"

        final_results.append(result)

    return final_results


def plot_outlier_distribution(
    df: pd.DataFrame,
    table_name: str,
    schema: Dict[str, Any],
    outlier_config: Dict[str, Any],
    save_path: Optional[str] = None,
    show_plot: bool = True,
    figsize: Optional[Tuple[int, int]] = None,
    max_cols: int = 3,
    max_plots_per_figure: Optional[int] = 6
) -> Optional[List[plt.Figure]]:
    """
    Create boxplots showing outlier distributions for numeric columns.

    This function creates visualizations that show:
    - Data distribution via boxplots
    - Expected min/max ranges as red horizontal lines
    - Outliers beyond the expected ranges highlighted

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to visualize
    table_name : str
        Name of the table being validated
    schema : dict
        Table schema with column definitions
    outlier_config : dict
        Outlier configuration from outlier_config.yaml
    save_path : str, optional
        Path to save the figure. If None, figure is not saved.
        If multiple figures are created, saves as 'path_part1.png', 'path_part2.png', etc.
    show_plot : bool, default=True
        Whether to display the plot
    figsize : tuple, optional
        Figure size as (width, height). If None, automatically calculated based on number of plots.
    max_cols : int, default=3
        Maximum number of columns in the grid layout
    max_plots_per_figure : int, optional, default=6
        Maximum number of plots per figure. If more plots exist, splits into multiple figures.
        Set to None to disable splitting. Default creates 2 rows × 3 columns layout.

    Returns
    -------
    List[matplotlib.figure.Figure] or None
        List of figure objects if plots were created, None otherwise

    Examples
    --------
    >>> from clifpy.utils.validator import load_outlier_config, plot_outlier_distribution
    >>>
    >>> outlier_config = load_outlier_config()
    >>> fig = plot_outlier_distribution(
    ...     vitals_df, 'vitals', vitals_schema, outlier_config,
    ...     save_path='vitals_outliers.png'
    ... )
    """
    table_config = outlier_config.get('tables', {}).get(table_name, {})
    if not table_config:
        return None

    # Get category columns from schema
    cat_cols = {col['name'] for col in schema.get('columns', [])
                if col.get('is_category_column')}

    # Collect plot data
    plot_data = []

    for col_name, col_config in table_config.items():
        if col_name not in df.columns:
            continue

        # Pattern 1: Simple range {min: X, max: Y}
        if 'min' in col_config and 'max' in col_config:
            data = df[col_name].dropna()
            if len(data) > 0:
                plot_data.append({
                    'label': col_name,
                    'data': data,
                    'min': col_config['min'],
                    'max': col_config['max']
                })
        else:
            # Pattern 2 & 3: Category-dependent
            category_col = _find_category_column(df, col_config, cat_cols)
            if not category_col:
                continue

            first_val = col_config[list(col_config.keys())[0]]

            # Pattern 2: Single category
            if isinstance(first_val, dict) and 'min' in first_val:
                for category, ranges in col_config.items():
                    mask = df[category_col].astype(str).str.lower() == category.lower()
                    if mask.any():
                        data = df.loc[mask, col_name].dropna()
                        if len(data) > 0:
                            plot_data.append({
                                'label': f"{category}\n({col_name})",
                                'data': data,
                                'min': ranges['min'],
                                'max': ranges['max']
                            })

            # Pattern 3: Double category
            else:
                sec_col = _find_secondary_category_column(df, col_config, cat_cols, category_col)
                if sec_col:
                    for cat1, sub_config in col_config.items():
                        for cat2, ranges in sub_config.items():
                            if not isinstance(ranges, dict) or 'min' not in ranges:
                                continue
                            mask = (
                                (df[category_col].astype(str).str.lower() == cat1.lower()) &
                                (df[sec_col].astype(str).str.lower() == cat2.lower())
                            )
                            if mask.any():
                                data = df.loc[mask, col_name].dropna()
                                if len(data) > 0:
                                    plot_data.append({
                                        'label': f"{cat1}\n{cat2}\n({col_name})",
                                        'data': data,
                                        'min': ranges['min'],
                                        'max': ranges['max']
                                    })

    if not plot_data:
        return None

    # Split into multiple figures if needed
    n_total = len(plot_data)
    if max_plots_per_figure and n_total > max_plots_per_figure:
        n_figures = (n_total + max_plots_per_figure - 1) // max_plots_per_figure
        plot_chunks = [plot_data[i:i + max_plots_per_figure]
                       for i in range(0, n_total, max_plots_per_figure)]
    else:
        n_figures = 1
        plot_chunks = [plot_data]

    figures = []

    for fig_idx, chunk in enumerate(plot_chunks):
        n_plots = len(chunk)
        n_cols = min(max_cols, n_plots)
        n_rows = (n_plots + n_cols - 1) // n_cols  # Ceiling division

        # Auto-calculate figsize if not provided
        if figsize is None:
            width = min(4 * n_cols, 20)  # Cap at 20 inches wide
            height = 4 * n_rows
            current_figsize = (width, height)
        else:
            current_figsize = figsize

        fig, axes = plt.subplots(n_rows, n_cols, figsize=current_figsize, squeeze=False)
        axes = axes.flatten()

        # Hide unused subplots
        for idx in range(n_plots, len(axes)):
            axes[idx].axis('off')

        for idx, item in enumerate(chunk):
            ax = axes[idx]
            data = item['data']

            # Create boxplot
            bp = ax.boxplot([data], widths=0.6, patch_artist=True,
                            boxprops=dict(facecolor='lightblue', alpha=0.7),
                            medianprops=dict(color='darkblue', linewidth=2),
                            flierprops=dict(marker='o', markerfacecolor='red',
                                           markersize=4, alpha=0.5))

            # Add expected range lines
            ax.axhline(y=item['min'], color='red', linestyle='--',
                       linewidth=2, label=f"Min: {item['min']}")
            ax.axhline(y=item['max'], color='red', linestyle='--',
                       linewidth=2, label=f"Max: {item['max']}")

            # Shade the acceptable range
            ax.axhspan(item['min'], item['max'], alpha=0.1, color='green')

            # Count outliers
            below = (data < item['min']).sum()
            above = (data > item['max']).sum()

            # Set labels and title
            ax.set_title(f"{item['label']}\n(n={len(data)})", fontsize=9, pad=10)
            ax.set_ylabel('Value', fontsize=8)
            ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
            ax.legend(fontsize=7, loc='best')
            ax.grid(True, alpha=0.3, axis='y')

            # Add outlier counts as text
            if below > 0 or above > 0:
                outlier_text = f"Below: {below}\nAbove: {above}"
                ax.text(0.02, 0.98, outlier_text, transform=ax.transAxes,
                       fontsize=7, verticalalignment='top',
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        # Add title with part number if multiple figures
        if n_figures > 1:
            fig.suptitle(f'Outlier Distribution: {table_name} (Part {fig_idx + 1}/{n_figures})',
                        fontsize=12, y=0.995)
        else:
            fig.suptitle(f'Outlier Distribution: {table_name}', fontsize=12, y=0.995)

        plt.tight_layout(rect=[0, 0, 1, 0.99])

        # Save with part number if multiple figures
        if save_path:
            if n_figures > 1:
                base, ext = save_path.rsplit('.', 1) if '.' in save_path else (save_path, 'png')
                current_save_path = f"{base}_part{fig_idx + 1}.{ext}"
            else:
                current_save_path = save_path
            fig.savefig(current_save_path, dpi=150, bbox_inches='tight')

        if show_plot:
            plt.show()

        figures.append(fig)

    return figures if figures else None
