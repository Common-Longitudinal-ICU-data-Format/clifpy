
import pandas as pd
import os
import duckdb
import pytz
from typing import Dict, Union
import yaml
import logging
from .config import get_config_or_params

# Initialize logger for this module
logger = logging.getLogger('clifpy.utils.io')


class LazyRelation:
    """
    Wrapper around DuckDB relation that keeps the connection alive.

    This class holds both the DuckDB relation and its connection, ensuring
    the connection isn't garbage collected while you're still using the relation.

    All DuckDB relation methods are proxied through, so you can use it
    exactly like a regular DuckDB relation.

    Examples
    --------
    rel = load_data('labs', path, 'parquet', lazy=True)

    # Chain operations (lazy - nothing executed yet)
    result = rel.filter("lab_category = 'sodium'").limit(100)

    # Execute and fetch
    df = result.fetchdf()

    # Clean up when done
    rel.close()
    """

    def __init__(self, relation: duckdb.DuckDBPyRelation, connection: duckdb.DuckDBPyConnection):
        self._relation = relation
        self._connection = connection

    def __getattr__(self, name):
        """Proxy all attribute access to the underlying relation."""
        attr = getattr(self._relation, name)
        if callable(attr):
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                # If the result is a relation, wrap it to keep connection alive
                if isinstance(result, duckdb.DuckDBPyRelation):
                    return LazyRelation(result, self._connection)
                return result
            return wrapper
        return attr

    def close(self):
        """Close the underlying connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def __del__(self):
        """Clean up connection when garbage collected."""
        self.close()

    def __repr__(self):
        return f"LazyRelation({self._relation})"

def _cast_id_cols_to_string(df):
    id_cols = [c for c in df.columns if c.endswith("_id")]
    if id_cols:                                   # no-op if none found
        df[id_cols] = df[id_cols].astype("string")
    return df


def close_lazy_relation(rel: Union['LazyRelation', duckdb.DuckDBPyRelation]) -> None:
    """
    Close the connection associated with a lazy relation.

    Call this when you're done with a lazy relation to free resources.

    Parameters
    ----------
    rel : LazyRelation
        A lazy relation returned from load_data(..., lazy=True)

    Examples
    --------
    rel = load_data('labs', path, 'parquet', lazy=True)
    df = rel.filter("lab_category = 'sodium'").fetchdf()
    close_lazy_relation(rel)  # Or just: rel.close()
    """
    if hasattr(rel, 'close'):
        rel.close()


def fetch_lazy_result(
    rel: Union['LazyRelation', duckdb.DuckDBPyRelation],
    cast_ids: bool = True,
    site_tz: str = None,
    close_connection: bool = True,
    verbose: bool = False
) -> pd.DataFrame:
    """
    Fetch results from a lazy relation and apply standard post-processing.

    This is a convenience function that fetches the DataFrame from a lazy
    relation and applies the same post-processing as eager load_data().

    Parameters
    ----------
    rel : LazyRelation
        A lazy relation from load_data(..., lazy=True)
    cast_ids : bool, optional
        If True (default), cast ID columns to string type.
    site_tz : str, optional
        Timezone string for datetime conversion.
    close_connection : bool, optional
        If True (default), close the connection after fetching.
    verbose : bool, optional
        If True, show detailed messages.

    Returns
    -------
    pd.DataFrame
        The fetched and processed DataFrame.

    Examples
    --------
    # Load lazily, filter, then fetch with post-processing
    rel = load_data('labs', path, 'parquet', lazy=True)
    filtered = rel.filter("lab_category = 'sodium'")
    df = fetch_lazy_result(filtered, site_tz='America/New_York')
    """
    df = rel.fetchdf()

    if cast_ids:
        df = _cast_id_cols_to_string(df)

    if site_tz:
        df = convert_datetime_columns_to_site_tz(df, site_tz, verbose)

    if close_connection:
        close_lazy_relation(rel)

    return df


def load_config(file_path: str) -> Dict:
    with open(file_path, 'r') as file:
        config = yaml.safe_load(file)
    return config


def load_parquet_with_tz(
    file_path,
    columns=None,
    filters=None,
    sample_size=None,
    verbose=False,
    lazy=False
) -> Union[pd.DataFrame, 'LazyRelation']:
    """
    Load a parquet file with timezone handling.

    Parameters
    ----------
    file_path : str
        Path to the parquet file.
    columns : list of str, optional
        List of column names to load.
    filters : dict, optional
        Dictionary of filters to apply.
    sample_size : int, optional
        Number of rows to load.
    verbose : bool, optional
        If True, show detailed loading messages.
    lazy : bool, optional
        If True, return a DuckDB Relation for lazy evaluation.
        If False (default), return a pandas DataFrame.

    Returns
    -------
    pd.DataFrame or DuckDBRelation
        DataFrame when lazy=False, DuckDB Relation when lazy=True.

    Examples
    --------
    # Eager loading (default) - returns DataFrame
    df = load_parquet_with_tz('labs.parquet')

    # Lazy loading - returns DuckDB Relation
    rel = load_parquet_with_tz('labs.parquet', lazy=True)
    # Chain operations (not executed yet)
    result = rel.filter("lab_category = 'sodium'").limit(100)
    # Execute and fetch results
    df = result.fetchdf()
    """
    filename = os.path.basename(file_path)
    if verbose:
        logger.info(f"Loading {filename}" + (" (lazy)" if lazy else ""))

    con = duckdb.connect()
    # DuckDB >=0.9 understands the original zone if we ask for TIMESTAMPTZ
    con.execute("SET timezone = 'UTC';")          # read & return in UTC
    con.execute("SET pandas_analyze_sample=0;")   # avoid sampling issues

    # Build the relation lazily using DuckDB's Relational API
    rel = con.read_parquet(file_path)

    # Apply column selection (lazy)
    if columns:
        rel = rel.select(*columns)

    # Apply filters (lazy)
    if filters:
        for col, val in filters.items():
            if isinstance(val, list):
                vals = ", ".join([f"'{v}'" for v in val])
                rel = rel.filter(f"{col} IN ({vals})")
            else:
                rel = rel.filter(f"{col} = '{val}'")

    # Apply limit (lazy)
    if sample_size:
        rel = rel.limit(sample_size)

    # Return lazy relation or execute and return DataFrame
    if lazy:
        return LazyRelation(rel, con)
    else:
        df = rel.fetchdf()
        con.close()
        df = _cast_id_cols_to_string(df)
        return df

def load_data(
    table_name,
    table_path,
    table_format_type,
    sample_size=None,
    columns=None,
    filters=None,
    site_tz=None,
    verbose=False,
    lazy=False
) -> Union[pd.DataFrame, 'LazyRelation']:
    """
    Load data from a file in the specified directory with the option to select specific columns and apply filters.

    Parameters
    ----------
    table_name : str
        The name of the table to load.
    table_path : str
        Path to the directory containing the data file.
    table_format_type : str
        Format of the data file (e.g., 'csv', 'parquet').
    sample_size : int, optional
        Number of rows to load.
    columns : list of str, optional
        List of column names to load.
    filters : dict, optional
        Dictionary of filters to apply.
    site_tz : str, optional
        Timezone string for datetime conversion, e.g., "America/New_York".
        Note: Not applied when lazy=True (apply after fetching).
    verbose : bool, optional
        If True, show detailed loading messages. Default is False.
    lazy : bool, optional
        If True, return a DuckDB Relation for lazy evaluation instead of a DataFrame.
        This allows chaining operations before execution, which is memory-efficient
        for large datasets (300GB+). Default is False.

    Returns
    -------
    pd.DataFrame or DuckDBRelation
        DataFrame when lazy=False (default), DuckDB Relation when lazy=True.

    Examples
    --------
    # Standard eager loading (default behavior, unchanged)
    df = load_data('labs', '/path/to/data', 'parquet')

    # Lazy loading for large datasets
    rel = load_data('labs', '/path/to/data', 'parquet', lazy=True)

    # Chain operations (nothing executed yet - just builds query plan)
    result = (
        rel
        .filter("lab_category = 'sodium'")
        .filter("lab_value_numeric > 140")
        .aggregate("hospitalization_id, COUNT(*) as count", "hospitalization_id")
    )

    # Execute and fetch only when needed
    df = result.fetchdf()

    # Or fetch in chunks for very large results
    for chunk in result.fetchmany(10000):
        process(chunk)
    """
    # Determine the file path based on the directory and filetype
    file_path = os.path.join(table_path, 'clif_' + table_name + '.' + table_format_type)

    # Load the data based on filetype
    if os.path.exists(file_path):
        if table_format_type == 'csv':
            if verbose:
                logger.info('Loading CSV file' + (' (lazy)' if lazy else ''))

            con = duckdb.connect()

            # Use Relational API for lazy evaluation
            rel = con.read_csv(file_path)

            # Apply column selection (lazy)
            if columns:
                rel = rel.select(*columns)

            # Apply filters (lazy)
            if filters:
                for column, values in filters.items():
                    if isinstance(values, list):
                        values_list = ', '.join(["'" + str(v).replace("'", "''") + "'" for v in values])
                        rel = rel.filter(f"{column} IN ({values_list})")
                    else:
                        value = str(values).replace("'", "''")
                        rel = rel.filter(f"{column} = '{value}'")

            # Apply limit (lazy)
            if sample_size:
                rel = rel.limit(sample_size)

            if lazy:
                return LazyRelation(rel, con)
            else:
                df = rel.fetchdf()
                con.close()

        elif table_format_type == 'parquet':
            result = load_parquet_with_tz(file_path, columns, filters, sample_size, verbose, lazy=lazy)
            if lazy:
                return result
            df = result

        else:
            raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")

        # The following only runs when lazy=False
        filename = os.path.basename(file_path)
        if verbose:
            logger.info(f"Data loaded successfully from {filename}")

        df = _cast_id_cols_to_string(df)

        # Convert datetime columns to site timezone if specified
        if site_tz:
            df = convert_datetime_columns_to_site_tz(df, site_tz, verbose)

        return df
    else:
        raise FileNotFoundError(f"The file {file_path} does not exist in the specified directory.")

def convert_datetime_columns_to_site_tz(df, site_tz_str, verbose=True):
    """
    Convert all datetime columns in the DataFrame to the specified site timezone.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    site_tz_str : str
        Timezone string, e.g., "America/New_York". or "US/Central"
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
