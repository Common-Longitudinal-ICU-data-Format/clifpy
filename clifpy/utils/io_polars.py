"""
Polars-based I/O utilities for loading CLIF data files.

This module provides Polars equivalents of the pandas loading functions in io.py,
optimized for performance through lazy evaluation and predicate pushdown.

The functions mirror the behavior of `load_data()` in clifpy/utils/io.py
to maintain consistency across the codebase.
"""

import polars as pl
from pathlib import Path
from typing import Optional, List, Dict, Union, Any
import logging

from .datetime_polars import standardize_datetime_columns

logger = logging.getLogger('clifpy.utils.io_polars')


def _cast_id_cols_to_utf8(df: Union[pl.DataFrame, pl.LazyFrame]) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Cast all ID columns (ending with '_id') to Utf8 (string) type.

    This ensures consistent type matching during joins and filters,
    mirroring the pandas `_cast_id_cols_to_string()` in io.py.

    Parameters
    ----------
    df : pl.DataFrame or pl.LazyFrame
        Input dataframe

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Dataframe with ID columns cast to Utf8
    """
    is_lazy = isinstance(df, pl.LazyFrame)
    schema = df.schema if not is_lazy else df.collect_schema()

    id_cols = [col for col in schema.keys() if col.endswith('_id')]

    if not id_cols:
        return df

    cast_exprs = [pl.col(col).cast(pl.Utf8).alias(col) for col in id_cols]
    return df.with_columns(cast_exprs)


def load_parquet_polars(
    file_path: Union[str, Path],
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Load a parquet file using Polars with optional filtering and timezone conversion.

    Parameters
    ----------
    file_path : str or Path
        Path to the parquet file
    columns : list of str, optional
        List of column names to load. If None, loads all columns.
    filters : dict, optional
        Dictionary of column filters to apply.
        Example: {'hospitalization_id': ['H1', 'H2'], 'lab_category': 'creatinine'}
    sample_size : int, optional
        Number of rows to load (applies LIMIT)
    site_tz : str, optional
        Target timezone for datetime conversion (e.g., 'US/Central')
    lazy : bool, default=True
        If True, returns LazyFrame for deferred execution.
        If False, returns collected DataFrame.
    verbose : bool, default=False
        If True, log detailed loading messages.

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Loaded data with optional filtering and timezone conversion

    Examples
    --------
    >>> # Load labs data lazily
    >>> labs = load_parquet_polars('/data/clif_labs.parquet', lazy=True)

    >>> # Load specific columns with filter
    >>> labs = load_parquet_polars(
    ...     '/data/clif_labs.parquet',
    ...     columns=['hospitalization_id', 'lab_category', 'lab_value_numeric'],
    ...     filters={'lab_category': ['creatinine', 'platelet_count']},
    ...     site_tz='US/Central'
    ... )
    """
    file_path = Path(file_path)
    filename = file_path.name

    if verbose:
        logger.info(f"Loading {filename}")

    # Start with lazy scan
    if columns:
        df = pl.scan_parquet(str(file_path)).select(columns)
    else:
        df = pl.scan_parquet(str(file_path))

    # Apply filters (predicate pushdown)
    if filters:
        filter_exprs = []
        for col, val in filters.items():
            if isinstance(val, list):
                filter_exprs.append(pl.col(col).is_in(val))
            else:
                filter_exprs.append(pl.col(col) == val)

        if filter_exprs:
            combined_filter = filter_exprs[0]
            for expr in filter_exprs[1:]:
                combined_filter = combined_filter & expr
            df = df.filter(combined_filter)

    # Apply sample size limit
    if sample_size:
        df = df.limit(sample_size)

    # Cast ID columns to Utf8
    df = _cast_id_cols_to_utf8(df)

    # Apply timezone conversion
    if site_tz:
        df = standardize_datetime_columns(df, target_timezone=site_tz)

    if verbose:
        logger.info(f"Data loaded successfully from {filename}")

    # Return lazy or collected
    if lazy:
        return df
    else:
        return df.collect()


def load_csv_polars(
    file_path: Union[str, Path],
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Load a CSV file using Polars with optional filtering and timezone conversion.

    Parameters
    ----------
    file_path : str or Path
        Path to the CSV file
    columns : list of str, optional
        List of column names to load. If None, loads all columns.
    filters : dict, optional
        Dictionary of column filters to apply.
    sample_size : int, optional
        Number of rows to load (applies LIMIT)
    site_tz : str, optional
        Target timezone for datetime conversion
    lazy : bool, default=True
        If True, returns LazyFrame for deferred execution.
    verbose : bool, default=False
        If True, log detailed loading messages.

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Loaded data with optional filtering and timezone conversion
    """
    file_path = Path(file_path)
    filename = file_path.name

    if verbose:
        logger.info(f"Loading {filename}")

    # Start with lazy scan
    df = pl.scan_csv(str(file_path))

    # Select columns if specified
    if columns:
        df = df.select(columns)

    # Apply filters
    if filters:
        filter_exprs = []
        for col, val in filters.items():
            if isinstance(val, list):
                filter_exprs.append(pl.col(col).is_in(val))
            else:
                filter_exprs.append(pl.col(col) == val)

        if filter_exprs:
            combined_filter = filter_exprs[0]
            for expr in filter_exprs[1:]:
                combined_filter = combined_filter & expr
            df = df.filter(combined_filter)

    # Apply sample size limit
    if sample_size:
        df = df.limit(sample_size)

    # Cast ID columns to Utf8
    df = _cast_id_cols_to_utf8(df)

    # Apply timezone conversion
    if site_tz:
        df = standardize_datetime_columns(df, target_timezone=site_tz)

    if verbose:
        logger.info(f"Data loaded successfully from {filename}")

    if lazy:
        return df
    else:
        return df.collect()


def load_data_polars(
    table_name: str,
    table_path: Union[str, Path],
    table_format_type: str,
    sample_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Load CLIF data from a file using Polars with optional column selection and filters.

    This is the Polars equivalent of `load_data()` in clifpy/utils/io.py,
    optimized for performance through lazy evaluation.

    Parameters
    ----------
    table_name : str
        The name of the table to load (e.g., 'labs', 'vitals', 'respiratory_support')
    table_path : str or Path
        Path to the directory containing the data file
    table_format_type : str
        Format of the data file ('csv' or 'parquet')
    sample_size : int, optional
        Number of rows to load
    columns : list of str, optional
        List of column names to load
    filters : dict, optional
        Dictionary of filters to apply.
        Example: {'hospitalization_id': ['H1', 'H2']}
    site_tz : str, optional
        Timezone string for datetime conversion (e.g., 'US/Central')
    lazy : bool, default=True
        If True, returns LazyFrame for deferred execution.
        If False, returns collected DataFrame.
    verbose : bool, default=False
        If True, show detailed loading messages.

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        DataFrame/LazyFrame containing the requested data

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist
    ValueError
        If an unsupported file format is specified

    Examples
    --------
    >>> # Load labs data lazily
    >>> labs = load_data_polars(
    ...     'labs',
    ...     '/data/clif_tables',
    ...     'parquet',
    ...     site_tz='US/Central'
    ... )

    >>> # Load with filters and specific columns
    >>> vitals = load_data_polars(
    ...     'vitals',
    ...     '/data/clif_tables',
    ...     'parquet',
    ...     columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'],
    ...     filters={'vital_category': ['map', 'spo2']},
    ...     site_tz='US/Central',
    ...     lazy=False  # Collect immediately
    ... )
    """
    # Construct file path following CLIF naming convention
    file_path = Path(table_path) / f"clif_{table_name}.{table_format_type}"

    if not file_path.exists():
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

    if table_format_type == 'parquet':
        return load_parquet_polars(
            file_path,
            columns=columns,
            filters=filters,
            sample_size=sample_size,
            site_tz=site_tz,
            lazy=lazy,
            verbose=verbose
        )
    elif table_format_type == 'csv':
        return load_csv_polars(
            file_path,
            columns=columns,
            filters=filters,
            sample_size=sample_size,
            site_tz=site_tz,
            lazy=lazy,
            verbose=verbose
        )
    else:
        raise ValueError(f"Unsupported filetype '{table_format_type}'. Only 'csv' and 'parquet' are supported.")


def load_clif_table_polars(
    data_directory: Union[str, Path],
    table_name: str,
    filetype: str = 'parquet',
    hospitalization_ids: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Convenience function to load a CLIF table with common filtering patterns.

    Parameters
    ----------
    data_directory : str or Path
        Path to directory containing CLIF data files
    table_name : str
        Name of the table (e.g., 'labs', 'vitals', 'respiratory_support')
    filetype : str, default='parquet'
        File format ('parquet' or 'csv')
    hospitalization_ids : list of str, optional
        List of hospitalization IDs to filter to
    columns : list of str, optional
        List of columns to load
    site_tz : str, optional
        Target timezone for datetime conversion
    lazy : bool, default=True
        If True, returns LazyFrame

    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        Loaded and filtered data

    Examples
    --------
    >>> # Load labs for specific patients
    >>> labs = load_clif_table_polars(
    ...     '/data/clif',
    ...     'labs',
    ...     hospitalization_ids=['H001', 'H002', 'H003'],
    ...     site_tz='US/Central'
    ... )
    """
    filters = None
    if hospitalization_ids:
        filters = {'hospitalization_id': hospitalization_ids}

    return load_data_polars(
        table_name=table_name,
        table_path=data_directory,
        table_format_type=filetype,
        columns=columns,
        filters=filters,
        site_tz=site_tz,
        lazy=lazy
    )

