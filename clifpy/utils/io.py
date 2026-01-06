
import pandas as pd
import os
import duckdb
import pytz
from typing import Dict, List, Optional, Any, Union, Literal, overload
from duckdb import DuckDBPyRelation
import yaml
import logging
from .config import get_config_or_params

# Initialize logger for this module
logger = logging.getLogger('clifpy.utils.io')

# conn = duckdb.connect(database=':memory:')

def _cast_id_cols_to_string(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all columns ending with '_id' to string dtype.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.

    Returns
    -------
    pd.DataFrame
        DataFrame with ID columns cast to string.
    """
    id_cols = [c for c in df.columns if c.endswith("_id")]
    if id_cols:                                   # no-op if none found
        df[id_cols] = df[id_cols].astype("string")
    return df


def _build_tz_converted_select(
    con: duckdb.DuckDBPyConnection,
    file_path: str,
    columns: Optional[List[str]],
    site_tz: Optional[str],
    source_type: str = "parquet"
) -> str:
    """Build SELECT clause with timezone conversion for 'dttm' columns.

    Queries the file schema to identify columns, then builds a SELECT clause
    that applies timezone conversion to columns containing 'dttm' in their name.

    Parameters
    ----------
    con : duckdb.DuckDBPyConnection
        Active DuckDB connection.
    file_path : str
        Path to the data file.
    columns : list of str, optional
        Specific columns to select. If None, selects all columns.
    site_tz : str, optional
        Target timezone string (e.g., 'US/Eastern'). If None, no conversion.
    source_type : str
        Type of source file ('parquet' or 'csv').

    Returns
    -------
    str
        SQL SELECT clause with timezone conversions applied.
    """
    # Get column info from file schema
    if source_type == "parquet":
        schema_query = f"DESCRIBE SELECT * FROM parquet_scan('{file_path}')"
    else:  # csv
        schema_query = f"DESCRIBE SELECT * FROM read_csv_auto('{file_path}')"

    schema_result = con.execute(schema_query).fetchall()
    all_columns = [row[0] for row in schema_result]

    # Filter to requested columns or use all
    target_columns = columns if columns else all_columns

    select_parts = []
    for col in target_columns:
        if 'dttm' in col.lower() and site_tz:
            # Convert UTC to site timezone for datetime columns
            select_parts.append(f"timezone('{site_tz}', {col}) AS {col}")
        else:
            select_parts.append(col)

    return ", ".join(select_parts)


def load_config(file_path: str) -> Dict[str, Any]:
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


@overload
def load_parquet_with_tz(
    file_path: str,
    columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ...,
    site_tz: Optional[str] = ...,
    verbose: bool = ...,
    return_rel: Literal[False] = ...
) -> pd.DataFrame: ...


@overload
def load_parquet_with_tz(
    file_path: str,
    columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    sample_size: Optional[int] = ...,
    site_tz: Optional[str] = ...,
    verbose: bool = ...,
    return_rel: Literal[True] = ...
) -> DuckDBPyRelation: ...


def load_parquet_with_tz(
    file_path: str,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    verbose: bool = False,
    return_rel: bool = False
) -> Union[pd.DataFrame, DuckDBPyRelation]:
    """Load a parquet file with optional timezone conversion for datetime columns.

    Parameters
    ----------
    file_path : str
        Path to the parquet file.
    columns : list of str, optional
        List of column names to load.
    filters : dict, optional
        Dictionary of filters to apply (column: value or column: [values]).
    sample_size : int, optional
        Number of rows to load (LIMIT clause).
    site_tz : str, optional
        Target timezone for datetime columns (e.g., 'US/Eastern').
        If provided, columns with 'dttm' in their name are converted from UTC.
    verbose : bool, optional
        If True, show detailed loading messages.
    return_rel : bool, optional
        If True, return a lazy DuckDBPyRelation instead of DataFrame.
        Uses DuckDB's default connection (no cleanup needed). Default is False.

    Returns
    -------
    pd.DataFrame or DuckDBPyRelation
        If return_rel=False: DataFrame with timezone-converted datetime columns.
        If return_rel=True: DuckDBPyRelation for lazy evaluation.
    """
    filename = os.path.basename(file_path)
    if verbose:
        logger.info(f"Loading {filename}")

    # Use DuckDB's default connection for simplicity (no connection management needed)
    duckdb.execute("SET timezone = 'UTC';")          # read & return in UTC
    duckdb.execute("SET pandas_analyze_sample=0;")   # avoid sampling issues

    # Build SELECT clause with timezone conversion if site_tz is provided
    if site_tz:
        sel = _build_tz_converted_select(duckdb.default_connection(), file_path, columns, site_tz, source_type="parquet")
    else:
        sel = "*" if columns is None else ", ".join(columns)

    query = f"SELECT {sel} FROM parquet_scan('{file_path}')"

    if filters:                                  # optional WHERE clause
        clauses = []
        for col, val in filters.items():
            if isinstance(val, list):
                vals = ", ".join([f"'{v}'" for v in val])
                clauses.append(f"{col} IN ({vals})")
            else:
                clauses.append(f"{col} = '{val}'")
        query += " WHERE " + " AND ".join(clauses)

    if sample_size:
        query += f" LIMIT {sample_size}"

    if return_rel:
        return duckdb.sql(query)  # lazy relation, no connection management
    else:
        df = duckdb.sql(query).df()              # pandas DataFrame
        df = _cast_id_cols_to_string(df)         # cast id columns to string
        return df

@overload
def load_data(
    table_name: str,
    table_path: Optional[str] = ...,
    table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ...,
    columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    site_tz: Optional[str] = ...,
    verbose: bool = ...,
    return_rel: Literal[False] = ...,
    config_path: Optional[str] = ...
) -> pd.DataFrame: ...


@overload
def load_data(
    table_name: str,
    table_path: Optional[str] = ...,
    table_format_type: Optional[str] = ...,
    sample_size: Optional[int] = ...,
    columns: Optional[List[str]] = ...,
    filters: Optional[Dict[str, Union[str, List[str]]]] = ...,
    site_tz: Optional[str] = ...,
    verbose: bool = ...,
    return_rel: Literal[True] = ...,
    config_path: Optional[str] = ...
) -> DuckDBPyRelation: ...


def load_data(
    table_name: str,
    table_path: Optional[str] = None,
    table_format_type: Optional[str] = None,
    sample_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Union[str, List[str]]]] = None,
    site_tz: Optional[str] = None,
    verbose: bool = False,
    return_rel: bool = False,
    config_path: Optional[str] = None
) -> Union[pd.DataFrame, DuckDBPyRelation]:
    """Load data from a file with optional timezone conversion for datetime columns.

    Parameters can be provided directly or loaded from a config file. If table_path
    and table_format_type are not provided, they will be loaded from the config file.

    Parameters
    ----------
    table_name : str
        The name of the table to load (e.g., 'vitals', 'labs', 'adt').
    table_path : str, optional
        Path to the directory containing the data file.
        If None, loaded from config file's 'data_directory'.
    table_format_type : str, optional
        Format of the data file ('csv' or 'parquet').
        If None, loaded from config file's 'filetype'.
    sample_size : int, optional
        Number of rows to load.
    columns : list of str, optional
        List of column names to load.
    filters : dict, optional
        Dictionary of filters to apply.
    site_tz : str, optional
        Target timezone for datetime columns (e.g., 'US/Eastern').
        If None, loaded from config file's 'timezone'.
        Columns with 'dttm' in their name are converted from UTC.
    verbose : bool, optional
        If True, show detailed loading messages. Default is False.
    return_rel : bool, optional
        If True, return a lazy DuckDBPyRelation instead of DataFrame.
        Only supported for parquet files. CSV files will ignore this and return DataFrame.
        Uses DuckDB's default connection (no cleanup needed). Default is False.
    config_path : str, optional
        Path to config file. If None, auto-detects config.json/yaml in current directory.

    Returns
    -------
    pd.DataFrame or DuckDBPyRelation
        If return_rel=False or CSV: DataFrame with timezone-converted datetime columns.
        If return_rel=True and parquet: DuckDBPyRelation for lazy evaluation.

    Examples
    --------
    # Using config file (auto-detected in current directory)
    >>> df = load_data(table_name='vitals')

    # Using config file with lazy evaluation
    >>> rel = load_data(table_name='vitals', return_rel=True)

    # Explicit parameters (no config needed)
    >>> df = load_data('vitals', '/path/to/data', 'parquet', site_tz='US/Eastern')
    """
    # Load config if table_path or table_format_type not provided
    if table_path is None or table_format_type is None:
        config = get_config_or_params(
            config_path=config_path,
            data_directory=table_path,
            filetype=table_format_type,
            timezone=site_tz
        )
        table_path = config['data_directory']
        table_format_type = config['filetype']
        # Use timezone from config if site_tz not explicitly provided
        if site_tz is None:
            site_tz = config.get('timezone')

    file_path = os.path.join(table_path, 'clif_' + table_name + '.' + table_format_type)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

    if table_format_type == 'csv':
        if return_rel:
            logger.warning("return_rel=True is not supported for CSV files. Returning DataFrame instead.")
        if verbose:
            logger.info('Loading CSV file')
        # For CSV, use DuckDB default connection with timezone conversion
        duckdb.execute("SET timezone = 'UTC';")
        duckdb.execute("SET pandas_analyze_sample=0;")

        # Build SELECT clause with timezone conversion if site_tz is provided
        if site_tz:
            select_clause = _build_tz_converted_select(duckdb.default_connection(), file_path, columns, site_tz, source_type="csv")
        else:
            select_clause = "*" if not columns else ", ".join(columns)

        query = f"SELECT {select_clause} FROM read_csv_auto('{file_path}')"

        # Apply filters
        if filters:
            filter_clauses = []
            for column, values in filters.items():
                if isinstance(values, list):
                    values_list = ', '.join(["'" + str(value).replace("'", "''") + "'" for value in values])
                    filter_clauses.append(f"{column} IN ({values_list})")
                else:
                    value = str(values).replace("'", "''")
                    filter_clauses.append(f"{column} = '{value}'")
            if filter_clauses:
                query += " WHERE " + " AND ".join(filter_clauses)

        if sample_size:
            query += f" LIMIT {sample_size}"

        df = duckdb.sql(query).df()

    elif table_format_type == 'parquet':
        # Pass site_tz and return_rel to load_parquet_with_tz for DuckDB-level timezone conversion
        result = load_parquet_with_tz(file_path, columns, filters, sample_size, site_tz, verbose, return_rel)
        if return_rel:
            return result  # DuckDBPyRelation - lazy evaluation
        df = result

    else:
        raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")

    filename = os.path.basename(file_path)
    if verbose:
        logger.info(f"Data loaded successfully from {filename}")

    df = _cast_id_cols_to_string(df)
    return df

def convert_datetime_columns_to_site_tz(
    df: pd.DataFrame,
    site_tz_str: str,
    verbose: bool = True
) -> pd.DataFrame:
    """Convert all datetime columns in the DataFrame to the specified site timezone.

    .. deprecated::
        This function is deprecated. Timezone conversion is now handled at the
        DuckDB SQL level in `load_data()` and `load_parquet_with_tz()` using
        the `site_tz` parameter. This function is kept for backward compatibility.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    site_tz_str : str
        Timezone string, e.g., "America/New_York" or "US/Central".
    verbose : bool
        Whether to print detailed output (default: True).

    Returns
    -------
    pd.DataFrame
        Modified DataFrame with datetime columns converted.
    """
    site_tz = pytz.timezone(site_tz_str)

    # Identify datetime-related columns
    dttm_columns = [col for col in df.columns if 'dttm' in col]

    if not dttm_columns:
        logger.debug("No datetime columns found in DataFrame")
        return df

    # Track conversion statistics
    converted_cols = []
    already_correct_cols = []
    naive_cols = []
    problem_cols = []

    for col in dttm_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            current_tz = df[col].dt.tz
            # Compare timezone names/strings instead of timezone objects
            if str(current_tz) == str(site_tz):
                already_correct_cols.append(col)
                logger.debug(f"{col}: Already in timezone {current_tz}")
            else:
                null_before = df[col].isna().sum()
                df[col] = df[col].dt.tz_convert(site_tz)
                null_after = df[col].isna().sum()
                converted_cols.append(col)
                if null_before != null_after:
                    logger.warning(f"{col}: Null count changed during conversion ({null_before} → {null_after})")
                logger.debug(f"{col}: Converted from {current_tz} to {site_tz}")
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(site_tz, ambiguous=True, nonexistent='shift_forward')
            naive_cols.append(col)
            logger.warning(f"{col}: Naive datetime localized to {site_tz}. Please verify this is correct.")
        else:
            problem_cols.append(col)
            logger.warning(f"{col}: Expected datetime but found {df[col].dtype}")

    # Log summary based on verbosity
    if verbose and (converted_cols or naive_cols or problem_cols):
        summary_parts = []
        if converted_cols:
            summary_parts.append(f"{len(converted_cols)} converted to {site_tz}")
        if already_correct_cols:
            summary_parts.append(f"{len(already_correct_cols)} already correct")
        if naive_cols:
            summary_parts.append(f"{len(naive_cols)} naive dates localized")
        if problem_cols:
            summary_parts.append(f"{len(problem_cols)} problematic")

        logger.info(f"Timezone processing complete: {', '.join(summary_parts)}")

        if logger.isEnabledFor(logging.DEBUG):
            if converted_cols:
                logger.debug(f"Converted columns: {', '.join(converted_cols)}")
            if naive_cols:
                logger.debug(f"Naive columns: {', '.join(naive_cols)}")
            if problem_cols:
                logger.debug(f"Problem columns: {', '.join(problem_cols)}")

    return df
