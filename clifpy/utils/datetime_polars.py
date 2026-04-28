"""
Polars-based datetime handling utilities for consistent timezone and time unit conversions.

This module provides Polars equivalents of the pandas datetime utilities in io.py,
ensuring consistent handling of timezones, time units, and ambiguous times during
DST transitions.

The functions mirror the behavior of the pandas-based `convert_datetime_columns_to_site_tz()`
in io.py to maintain consistency across the codebase.
"""

import polars as pl
from typing import Union, List, Optional, Tuple
import logging

logger = logging.getLogger('clifpy.utils.datetime_polars')


def standardize_datetime_columns(
    df: Union[pl.DataFrame, pl.LazyFrame],
    target_timezone: str,
    target_time_unit: str = 'ns',
    ambiguous: str = 'earliest',
    non_existent: str = 'null',
    datetime_columns: Optional[List[str]] = None
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Standardize all datetime columns to consistent timezone and time unit.

    This function handles three cases for each datetime column:
    1. **UTC datetime** → Convert to target timezone
    2. **Naive datetime** → Assume it's in target timezone, make it timezone-aware
    3. **Already in target timezone** → No change needed

    Additionally, it standardizes the time unit (default: nanoseconds) to prevent
    precision mismatch errors during joins.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        Input dataframe
    target_timezone : str
        Target timezone (e.g., 'US/Eastern', 'US/Central', 'America/New_York')
    target_time_unit : str, default='ns'
        Target time unit for all datetime columns ('ms', 'us', 'ns')
        Recommended: 'ns' (nanoseconds) for maximum precision
    ambiguous : str, default='earliest'
        How to handle ambiguous times during DST "fall back" transitions:
        - 'earliest': Use earlier occurrence (recommended)
        - 'latest': Use later occurrence
        - 'raise': Raise error
        - 'null': Set ambiguous times to null
    non_existent : str, default='null'
        How to handle non-existent times during DST "spring forward" transitions
        (e.g., 2:30 AM on the day clocks move forward):
        - 'null': Set non-existent times to null (recommended)
        - 'raise': Raise error (for strict validation)
    datetime_columns : list of str, optional
        Specific datetime columns to convert. If None, auto-detects all datetime
        columns (those with 'dttm' in the name or Datetime dtype).

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Dataframe with standardized datetime columns

    Examples
    --------
    >>> # Standardize all datetime columns to US/Central timezone
    >>> df = standardize_datetime_columns(df, target_timezone='US/Central')

    >>> # Convert specific columns only
    >>> df = standardize_datetime_columns(
    ...     df,
    ...     target_timezone='US/Central',
    ...     datetime_columns=['recorded_dttm', 'admin_dttm']
    ... )

    Notes
    -----
    This function is the Polars equivalent of `convert_datetime_columns_to_site_tz()`
    in clifpy/utils/io.py, designed to handle the same edge cases:
    - Naive datetimes are localized to the target timezone
    - UTC datetimes are converted to the target timezone
    - DST transitions are handled with configurable behavior
    """
    is_lazy = isinstance(df, pl.LazyFrame)

    # Get schema to identify datetime columns
    schema = df.schema if not is_lazy else df.collect_schema()

    # Auto-detect datetime columns if not specified
    if datetime_columns is None:
        datetime_columns = []
        for col_name, dtype in schema.items():
            # Check if it's a Datetime type
            if isinstance(dtype, pl.Datetime):
                datetime_columns.append(col_name)
            # Also include columns with 'dttm' in name (CLIF convention)
            elif 'dttm' in col_name.lower() and col_name not in datetime_columns:
                datetime_columns.append(col_name)

    if not datetime_columns:
        logger.debug("No datetime columns found to standardize")
        return df

    logger.info(f"Standardizing {len(datetime_columns)} datetime column(s) to {target_timezone} with time unit {target_time_unit}")

    # Build conversion expressions
    conversions = []

    for col_name in datetime_columns:
        if col_name not in schema:
            logger.warning(f"Column '{col_name}' not found in dataframe, skipping")
            continue

        dtype = schema[col_name]

        # Build conversion expression based on current timezone state
        conversion_expr = _build_datetime_conversion_expr(
            col_name,
            dtype,
            target_timezone,
            target_time_unit,
            ambiguous,
            non_existent
        )

        if conversion_expr is not None:
            conversions.append(conversion_expr)

    if conversions:
        df = df.with_columns(conversions)
        logger.info(f"Successfully standardized {len(conversions)} datetime column(s)")

    return df


def _build_datetime_conversion_expr(
    col_name: str,
    dtype: pl.DataType,
    target_timezone: str,
    target_time_unit: str,
    ambiguous: str,
    non_existent: str
) -> Optional[pl.Expr]:
    """
    Build Polars expression for datetime conversion based on current state.

    Parameters
    ----------
    col_name : str
        Column name
    dtype : pl.DataType
        Current column data type
    target_timezone : str
        Target timezone
    target_time_unit : str
        Target time unit
    ambiguous : str
        Ambiguous time handling strategy for DST "fall back"
    non_existent : str
        Non-existent time handling strategy for DST "spring forward"

    Returns
    -------
    pl.Expr or None
        Polars expression for conversion, or None if column is not a datetime
    """
    # Skip non-datetime columns
    if not isinstance(dtype, pl.Datetime):
        logger.warning(f"Column '{col_name}' is not a datetime column (dtype: {dtype}), skipping")
        return None

    dtype_str = str(dtype)
    current_tz = None

    # Extract current timezone from dtype
    if hasattr(dtype, 'time_zone'):
        current_tz = dtype.time_zone
    elif 'time_zone=' in dtype_str:
        # Fallback: extract from string representation
        tz_start = dtype_str.find('time_zone=') + len('time_zone=')
        tz_value = dtype_str[tz_start:].split(',')[0].split(')')[0].strip('\'"')
        if tz_value != 'None' and tz_value:
            current_tz = tz_value

    # Case 1: Naive datetime (no timezone)
    if current_tz is None:
        logger.debug(f"{col_name}: Naive datetime → Localizing to {target_timezone}")
        expr = (
            pl.col(col_name)
            .cast(pl.Datetime(target_time_unit))  # Standardize time unit first
            .dt.replace_time_zone(target_timezone, ambiguous=ambiguous, non_existent=non_existent)
            .alias(col_name)
        )

    # Case 2: Already in target timezone
    elif current_tz == target_timezone:
        logger.debug(f"{col_name}: Already in {target_timezone} → Standardizing time unit only")
        expr = (
            pl.col(col_name)
            .cast(pl.Datetime(target_time_unit, target_timezone))
            .alias(col_name)
        )

    # Case 3: Different timezone (e.g., UTC → target timezone)
    else:
        logger.debug(f"{col_name}: {current_tz} → Converting to {target_timezone}")
        expr = (
            pl.col(col_name)
            .cast(pl.Datetime(target_time_unit, current_tz))  # Standardize time unit first
            .dt.convert_time_zone(target_timezone)
            .alias(col_name)
        )

    return expr


def ensure_datetime_precision_match(
    df1: Union[pl.DataFrame, pl.LazyFrame],
    df2: Union[pl.DataFrame, pl.LazyFrame],
    df1_datetime_col: str,
    df2_datetime_col: str,
    target_timezone: str,
    target_time_unit: str = 'ns'
) -> Tuple[Union[pl.DataFrame, pl.LazyFrame], Union[pl.DataFrame, pl.LazyFrame]]:
    """
    Ensure two dataframes have matching datetime precision for joins.

    This is specifically useful before join_asof operations which require
    exact datetime type matching.

    Parameters
    ----------
    df1 : pl.DataFrame or pl.LazyFrame
        First dataframe
    df2 : pl.DataFrame or pl.LazyFrame
        Second dataframe
    df1_datetime_col : str
        Datetime column name in df1
    df2_datetime_col : str
        Datetime column name in df2
    target_timezone : str
        Target timezone
    target_time_unit : str, default='ns'
        Target time unit

    Returns
    -------
    tuple
        (df1_standardized, df2_standardized) with matching datetime precision

    Examples
    --------
    >>> # Prepare dataframes for join_asof
    >>> labs, resp = ensure_datetime_precision_match(
    ...     labs, resp,
    ...     'lab_result_dttm', 'recorded_dttm',
    ...     target_timezone='US/Central'
    ... )
    >>> # Now safe to join
    >>> result = labs.join_asof(resp, left_on='lab_result_dttm', right_on='recorded_dttm')
    """
    logger.info(f"Ensuring datetime precision match: {df1_datetime_col} ↔ {df2_datetime_col}")

    # Standardize both dataframes
    df1 = standardize_datetime_columns(
        df1,
        target_timezone=target_timezone,
        target_time_unit=target_time_unit,
        datetime_columns=[df1_datetime_col]
    )

    df2 = standardize_datetime_columns(
        df2,
        target_timezone=target_timezone,
        target_time_unit=target_time_unit,
        datetime_columns=[df2_datetime_col]
    )

    logger.info(f"Datetime precision matched at {target_time_unit} in {target_timezone}")

    return df1, df2


def convert_datetime_columns_to_site_tz(
    df: Union[pl.DataFrame, pl.LazyFrame],
    site_tz_str: str,
    verbose: bool = True
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Convert all datetime columns in the DataFrame to the specified site timezone.

    This is the Polars equivalent of `convert_datetime_columns_to_site_tz()` in
    clifpy/utils/io.py, providing the same functionality for Polars DataFrames.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        Input DataFrame
    site_tz_str : str
        Timezone string, e.g., "America/New_York" or "US/Central"
    verbose : bool, default=True
        Whether to log detailed output

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        DataFrame with datetime columns converted to the specified timezone

    Examples
    --------
    >>> df = convert_datetime_columns_to_site_tz(df, 'US/Central')

    Notes
    -----
    This function mirrors the pandas version in io.py:
    - Auto-detects datetime columns by 'dttm' suffix
    - Handles both timezone-aware and naive datetimes
    - Logs conversion statistics when verbose=True
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    schema = df.schema if not is_lazy else df.collect_schema()

    # Identify datetime-related columns (CLIF convention: columns with 'dttm')
    dttm_columns = [col for col in schema.keys() if 'dttm' in col.lower()]

    if not dttm_columns:
        logger.debug("No datetime columns found in DataFrame")
        return df

    # Track conversion statistics
    converted_cols = []
    already_correct_cols = []
    naive_cols = []

    conversions = []

    for col in dttm_columns:
        dtype = schema[col]

        if not isinstance(dtype, pl.Datetime):
            continue

        current_tz = getattr(dtype, 'time_zone', None)

        if current_tz is None:
            # Naive datetime - localize to site timezone
            conversions.append(
                pl.col(col)
                .dt.replace_time_zone(site_tz_str, ambiguous='earliest', non_existent='null')
                .alias(col)
            )
            naive_cols.append(col)
            logger.warning(f"{col}: Naive datetime localized to {site_tz_str}. Please verify this is correct.")

        elif str(current_tz) == str(site_tz_str):
            # Already in correct timezone
            already_correct_cols.append(col)
            logger.debug(f"{col}: Already in timezone {current_tz}")

        else:
            # Different timezone - convert
            conversions.append(
                pl.col(col)
                .dt.convert_time_zone(site_tz_str)
                .alias(col)
            )
            converted_cols.append(col)
            logger.debug(f"{col}: Converted from {current_tz} to {site_tz_str}")

    if conversions:
        df = df.with_columns(conversions)

    # Log summary based on verbosity
    if verbose and (converted_cols or naive_cols):
        summary_parts = []
        if converted_cols:
            summary_parts.append(f"{len(converted_cols)} converted to {site_tz_str}")
        if already_correct_cols:
            summary_parts.append(f"{len(already_correct_cols)} already correct")
        if naive_cols:
            summary_parts.append(f"{len(naive_cols)} naive dates localized")

        logger.info(f"Timezone processing complete: {', '.join(summary_parts)}")

    return df


def standardize_datetime_for_comparison(
    col: pl.Expr,
    target_timezone: str,
    target_time_unit: str = 'ns'
) -> pl.Expr:
    """
    Standardize a datetime column expression for comparisons in lazy operations.

    Useful for filtering or comparing datetime columns in lazy dataframes.

    Parameters
    ----------
    col : pl.Expr
        Datetime column expression
    target_timezone : str
        Target timezone
    target_time_unit : str, default='ns'
        Target time unit

    Returns
    -------
    pl.Expr
        Standardized datetime expression

    Examples
    --------
    >>> # In a lazy filter operation
    >>> lazy_df = lazy_df.filter(
    ...     standardize_datetime_for_comparison(pl.col('recorded_dttm'), 'US/Central')
    ...     > pl.datetime(2020, 1, 1)
    ... )
    """
    return col.dt.convert_time_zone(target_timezone)

