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
from typing import List, Dict, Any
import pandas as pd

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
            if null_cnt:
                errors.append({
                    "type": "null_values",
                    "column": name,
                    "count": null_cnt,
                    "message": f"Column '{name}' has {null_cnt} null values in required field"
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
    Report count and percentage of missing values.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to analyze
    format : str
        Output format ('long' or 'wide')
        
    Returns
    -------
    pd.DataFrame
        Missing data statistics
    """
    try:
        missing_count = df.isnull().sum()
        missing_percent = (missing_count / len(df)) * 100
        
        if format == 'long':
            stats_df = pd.DataFrame({
                'column': missing_count.index,
                'missing_count': missing_count.values,
                'missing_percent': missing_percent.values,
                'total_rows': len(df)
            })
            # Sort by missing percentage descending
            stats_df = stats_df.sort_values('missing_percent', ascending=False)
            
        else:  # wide format
            stats_df = pd.DataFrame({
                'missing_count': missing_count,
                'missing_percent': missing_percent
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
    schema: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Check values against permitted categories.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    schema : dict
        Schema containing category definitions
        
    Returns
    -------
    List[dict]
        List of invalid category value errors
    """
    errors = []
    
    try:
        category_columns = schema.get("category_columns", [])
        
        for col_spec in schema.get("columns", []):
            name = col_spec["name"]
            
            if name not in df.columns or name not in category_columns:
                continue
            
            if col_spec.get("permissible_values"):
                allowed = set(col_spec["permissible_values"])
                
                # Get unique values in the column (excluding NaN)
                unique_values = set(df[name].dropna().unique())
                # Check for missing expected values (permissible values not present in data)
                missing_values = [v for v in allowed if v not in unique_values]
                if missing_values:
                    errors.append({
                                "type": "missing_categorical_values",
                                "column": name,
                                "missing_values": missing_values,  
                                "total_missing": len(missing_values),
                                "message": f"Column '{name}' is missing {len(missing_values)} expected category values"
                            })
                    
    except Exception as e:
        errors.append({
            "type": "categorical_validation",
            "status": "error",
            "error_message": str(e),
            "message": f"Error validating categorical values: {str(e)}"
        })
    
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
# Additional validation functions from enhanced_validator
# ---------------------------------------------------------------------------

def validate_numeric_ranges(
    df: pd.DataFrame, 
    ranges: Dict[str, Dict[str, float]],
    table_name: str
) -> List[Dict[str, Any]]:
    """
    Validate that numeric values fall within expected ranges.
    
    Parameters
    ----------
    df : pd.DataFrame
        The dataframe to validate
    ranges : dict
        Dictionary mapping column/category names to min/max ranges
    table_name : str
        Name of the table being validated
        
    Returns
    -------
    List[dict]
        List of range validation results
    """
    results = []
    
    try:
        for category, range_def in ranges.items():
            min_val = range_def.get('min')
            max_val = range_def.get('max')
            
            if min_val is None or max_val is None:
                continue
            
            # For tables with category columns (vitals, labs)
            category_col = None
            value_col = None
            
            if table_name == 'vitals' and 'vital_category' in df.columns:
                category_col = 'vital_category'
                value_col = 'vital_value_numeric'
            elif table_name == 'labs' and 'lab_category' in df.columns:
                category_col = 'lab_category'
                value_col = 'lab_value_numeric'
            
            if category_col and value_col and category_col in df.columns and value_col in df.columns:
                # Filter for this category
                category_data = df[df[category_col] == category]
                if len(category_data) > 0:
                    numeric_data = category_data[value_col].dropna()
                    if len(numeric_data) > 0:
                        below_min = (numeric_data < min_val).sum()
                        above_max = (numeric_data > max_val).sum()
                        
                        if below_min > 0 or above_max > 0:
                            results.append({
                                "type": "numeric_range_violation",
                                "table": table_name,
                                "category": category,
                                "min_expected": min_val,
                                "max_expected": max_val,
                                "below_min_count": int(below_min),
                                "above_max_count": int(above_max),
                                "total_values": len(numeric_data),
                                "status": "warning",
                                "message": f"Table '{table_name}' category '{category}': {int(below_min)} values below minimum ({min_val}), {int(above_max)} values above maximum ({max_val}) out of {len(numeric_data)} total values"
                            })
            elif category in df.columns and pd.api.types.is_numeric_dtype(df[category]):
                # Direct column check
                numeric_data = df[category].dropna()
                if len(numeric_data) > 0:
                    below_min = (numeric_data < min_val).sum()
                    above_max = (numeric_data > max_val).sum()
                    
                    if below_min > 0 or above_max > 0:
                        results.append({
                            "type": "numeric_range_violation",
                            "table": table_name,
                            "column": category,
                            "min_expected": min_val,
                            "max_expected": max_val,
                            "below_min_count": int(below_min),
                            "above_max_count": int(above_max),
                            "total_values": len(numeric_data),
                            "status": "warning",
                            "message": f"Column '{category}': {int(below_min)} values below minimum ({min_val}), {int(above_max)} values above maximum ({max_val}) out of {len(numeric_data)} total values"
                        })
                        
    except Exception as e:
        results.append({
            "type": "numeric_range_validation",
            "table": table_name,
            "status": "error",
            "error_message": str(e),
            "message": f"Error validating numeric ranges for table '{table_name}': {str(e)}"
        })
    
    return results 
