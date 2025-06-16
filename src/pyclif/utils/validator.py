"""Generic table/dataframe validator based on mCIDE JSON specs.

This module replaces heavy Pydantic-based validation with a lightweight, pandas-
based approach which relies on JSON model specifications placed in the
``pyclif/mCIDE`` directory.  Each specification file is named
``<TableName>Model.json`` (e.g. ``PatientModel.json``) and contains keys such as
``columns``, ``required_columns``, etc.

Usage
-----
>>> from pyclif.utils.validator import validate_table
>>> errors = validate_table(df, "patient")
>>> if errors:
...     print("Validation failed", errors)
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

    Returns a list of error dictionaries.  An empty list means success.
    """

    errors: List[dict[str, Any]] = []

    # 1. Required columns present ------------------------------------------------
    req_cols = set(spec.get("required_columns", []))
    missing = req_cols - set(df.columns)
    if missing:
        errors.append({"type": "missing_columns", "columns": sorted(missing)})

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
                errors.append({"type": "null_values", "column": name, "count": null_cnt})

        # 2b. Datatype checks -------------------------------------------------
        expected_type = col_spec.get("data_type")
        checker = _DATATYPE_CHECKERS.get(expected_type)
        if checker and not checker(series):
            errors.append({"type": "datatype_mismatch", "column": name, "expected": expected_type})

        # 2c. Category values -------------------------------------------------
        if col_spec.get("is_category_column") and col_spec.get("permissible_values"):
            allowed = set(col_spec["permissible_values"])
            bad_values = series.dropna()[~series.isin(allowed)].unique().tolist()
            if bad_values:
                errors.append({"type": "invalid_category", "column": name, "values": bad_values})

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


## add code for time conversion in the validator class? 
