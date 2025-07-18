import pandas as pd
import duckdb
import numpy as np
from datetime import datetime
import os
import re
from typing import List, Dict, Optional, Union
from tqdm import tqdm
import logging

# Set up logging
logger = logging.getLogger(__name__)


def create_wide_dataset(
    clif_instance,
    optional_tables: Optional[List[str]] = None,
    category_filters: Optional[Dict[str, List[str]]] = None,
    sample: bool = False,
    hospitalization_ids: Optional[List[str]] = None,
    output_format: str = 'dataframe',
    save_to_data_location: bool = False,
    output_filename: Optional[str] = None,
    return_dataframe: bool = True,
    base_table_columns: Optional[Dict[str, List[str]]] = None,
    batch_size: int = 1000,
    memory_limit: Optional[str] = None,
    threads: Optional[int] = None,
    show_progress: bool = True
) -> Optional[pd.DataFrame]:
    """
    Create a wide dataset by joining multiple CLIF tables with pivoting support.
    
    Parameters:
        clif_instance: CLIF object with loaded data
        optional_tables: DEPRECATED - use category_filters to specify tables
        category_filters: Dict specifying which categories to include for each table
                         Keys are table names, values are lists of categories to filter
                         Table presence in this dict determines if it will be loaded
        sample: Boolean - if True, randomly select 20 hospitalizations
        hospitalization_ids: List of specific hospitalization IDs to filter
        output_format: 'dataframe', 'csv', or 'parquet'
        save_to_data_location: Boolean - save output to data directory
        output_filename: Custom filename (default: 'wide_dataset_YYYYMMDD_HHMMSS')
        return_dataframe: Boolean - return DataFrame even when saving to file (default=True)
        base_table_columns: DEPRECATED - columns are selected automatically
        batch_size: Number of hospitalizations to process in each batch (default=1000)
        memory_limit: DuckDB memory limit (e.g., '8GB')
        threads: Number of threads for DuckDB to use
        show_progress: Show progress bars for long operations
    
    Returns:
        pd.DataFrame or None (if return_dataframe=False)
    """
    
    print("Starting wide dataset creation...")
    
    # Define tables that need pivoting vs those already wide
    PIVOT_TABLES = ['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments']
    WIDE_TABLES = ['respiratory_support']
    
    # Determine which tables to load from category_filters
    if category_filters is None:
        category_filters = {}
    
    # For backward compatibility with optional_tables
    if optional_tables and not category_filters:
        print("Warning: optional_tables parameter is deprecated. Converting to category_filters format.")
        category_filters = {table: [] for table in optional_tables}
    
    tables_to_load = list(category_filters.keys())
    
    # Create DuckDB connection with optimized settings
    conn_config = {
        'preserve_insertion_order': 'false'
    }
    
    if memory_limit:
        conn_config['memory_limit'] = memory_limit
    if threads:
        conn_config['threads'] = str(threads)
    
    # Use context manager for connection
    with duckdb.connect(':memory:', config=conn_config) as conn:
        # Set additional optimization settings
        conn.execute("SET preserve_insertion_order = false")
        
        # Get hospitalization IDs to process
        hospitalization_df = clif_instance.hospitalization.df.copy()
        
        if hospitalization_ids is not None:
            print(f"Filtering to specific hospitalization IDs: {len(hospitalization_ids)} encounters")
            required_ids = hospitalization_ids
        elif sample:
            print("Sampling 20 random hospitalizations...")
            all_ids = hospitalization_df['hospitalization_id'].unique()
            required_ids = np.random.choice(all_ids, size=min(20, len(all_ids)), replace=False).tolist()
            print(f"Selected {len(required_ids)} hospitalizations for sampling")
        else:
            required_ids = hospitalization_df['hospitalization_id'].unique().tolist()
            print(f"Processing all {len(required_ids)} hospitalizations")
        
        # Filter all base tables by required IDs immediately
        print("\nLoading and filtering base tables...")
        # Only keep required columns from hospitalization table
        hosp_required_cols = ['hospitalization_id', 'patient_id', 'age_at_admission']
        hosp_available_cols = [col for col in hosp_required_cols if col in hospitalization_df.columns]
        hospitalization_df = hospitalization_df[hosp_available_cols]
        hospitalization_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(required_ids)]
        patient_df = clif_instance.patient.df[['patient_id']].copy()
        
        # Get ADT with selected columns
        adt_df = clif_instance.adt.df.copy()
        adt_df = adt_df[adt_df['hospitalization_id'].isin(required_ids)]
        # Remove duplicate columns and _name columns
        adt_cols = [col for col in adt_df.columns if not col.endswith('_name') and col != 'patient_id']
        adt_df = adt_df[adt_cols]
        
        print(f"Base tables filtered - Hospitalization: {len(hospitalization_df)}, Patient: {len(patient_df)}, ADT: {len(adt_df)}")
        
        # Process in batches to avoid memory issues
        if batch_size > 0 and len(required_ids) > batch_size:
            print(f"Processing {len(required_ids)} hospitalizations in batches of {batch_size}")
            return _process_in_batches(
                conn, clif_instance, required_ids, patient_df, hospitalization_df, adt_df,
                tables_to_load, category_filters, PIVOT_TABLES, WIDE_TABLES,
                batch_size, show_progress, save_to_data_location, output_filename,
                output_format, return_dataframe
            )
        else:
            # Process all at once for small datasets
            return _process_hospitalizations(
                conn, clif_instance, required_ids, patient_df, hospitalization_df, adt_df,
                tables_to_load, category_filters, PIVOT_TABLES, WIDE_TABLES,
                show_progress
            )


def _find_alternative_timestamp(table_name: str, columns: List[str]) -> Optional[str]:
    """Find alternative timestamp column if the default is not found."""
    
    alternatives = {
        'labs': ['lab_collect_dttm', 'recorded_dttm', 'lab_order_dttm'],
        'vitals': ['recorded_dttm_min', 'recorded_dttm'],
    }
    
    if table_name in alternatives:
        for alt_col in alternatives[table_name]:
            if alt_col in columns:
                return alt_col
    
    return None


def _save_dataset(
    df: pd.DataFrame,
    data_dir: str,
    output_filename: Optional[str],
    output_format: str
):
    """Save the dataset to file."""
    
    if output_filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f'wide_dataset_{timestamp}'
    
    output_path = os.path.join(data_dir, f'{output_filename}.{output_format}')
    
    if output_format == 'csv':
        df.to_csv(output_path, index=False)
    elif output_format == 'parquet':
        df.to_parquet(output_path, index=False)
    
    print(f"Wide dataset saved to: {output_path}")


def _get_timestamp_column(table_name: str) -> Optional[str]:
    """Get the timestamp column name for each table type."""
    timestamp_mapping = {
        'vitals': 'recorded_dttm',
        'labs': 'lab_result_dttm',
        'medication_admin_continuous': 'admin_dttm',
        'patient_assessments': 'recorded_dttm',
        'respiratory_support': 'recorded_dttm'
    }
    return timestamp_mapping.get(table_name)




def convert_wide_to_hourly(wide_df: pd.DataFrame, aggregation_config: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Convert a wide dataset to hourly aggregation with user-defined aggregation methods.
    
    Parameters:
        wide_df: Wide dataset DataFrame from create_wide_dataset()
        aggregation_config: Dict mapping aggregation methods to list of columns
            Example: {
                'max': ['map', 'temp_c', 'sbp'],
                'mean': ['heart_rate', 'respiratory_rate'],
                'min': ['spo2'],
                'median': ['glucose'],
                'first': ['gcs_total', 'rass'],
                'last': ['assessment_value'],
                'boolean': ['norepinephrine', 'propofol'],
                'one_hot_encode': ['medication_name', 'assessment_category']
            }
    
    Returns:
        pd.DataFrame: Hourly aggregated wide dataset with nth_hour column
    """
    
    print("Starting hourly aggregation of wide dataset...")
    
    # Validate input
    if 'event_time' not in wide_df.columns:
        raise ValueError("wide_df must contain 'event_time' column")
    
    if 'hospitalization_id' not in wide_df.columns:
        raise ValueError("wide_df must contain 'hospitalization_id' column")
    
    if 'day_number' not in wide_df.columns:
        raise ValueError("wide_df must contain 'day_number' column")
    
    # Create a copy to avoid modifying original
    df = wide_df.copy()
    
    # Create hour-truncated datetime (removes minutes/seconds)
    df['event_time_hour'] = df['event_time'].dt.floor('H')
    
    # Calculate nth_hour starting from 0 based on first event time per hospitalization
    print("Calculating nth_hour starting from 0 based on first event...")
    
    # Find first event time for each hospitalization
    first_event_times = df.groupby('hospitalization_id')['event_time_hour'].min().reset_index()
    first_event_times.rename(columns={'event_time_hour': 'first_event_hour'}, inplace=True)
    
    # Merge first event times back to main dataframe
    df = df.merge(first_event_times, on='hospitalization_id', how='left')
    
    # Calculate nth_hour as hours elapsed since first event (starting from 0)
    df['nth_hour'] = ((df['event_time_hour'] - df['first_event_hour']).dt.total_seconds() // 3600).astype(int)
    
    # Extract hour bucket for compatibility
    df['hour_bucket'] = df['event_time_hour'].dt.hour
    
    print(f"Processing {len(df)} records into hourly buckets...")
    
    # Columns to group by - use event_time_hour instead of day_number and hour_bucket
    group_cols = ['hospitalization_id', 'event_time_hour', 'nth_hour']
    
    # Find columns not in aggregation_config and set them to 'first' with '_c' postfix
    all_agg_columns = []
    for columns_list in aggregation_config.values():
        all_agg_columns.extend(columns_list)
    
    # Get list of columns that are not in aggregation_config
    non_agg_columns = [col for col in df.columns 
                      if col not in all_agg_columns 
                      and col not in group_cols
                      and col != 'patient_id'
                      and col != 'day_number'
                      and col != 'first_event_hour'
                      and col != 'hour_bucket'
                      and col != 'event_time']
    
    # Print these columns and add them to 'first' aggregation with '_c' postfix
    if non_agg_columns:
        print("The following columns are not mentioned in aggregation_config, defaulting to 'first' with '_c' postfix:")
        for col in non_agg_columns:
            print(f"  - {col}")
        
        # Add these columns to the config with '_c' postfix instead of 'first_'
        if 'first' not in aggregation_config:
            aggregation_config['first'] = []
            
        aggregation_config['first'].extend(non_agg_columns)
    
    # Initialize result dictionary
    aggregated_data = []
    
    # Track columns we've already warned about to avoid duplicate warnings
    warned_columns = set()
    
    # Process each hospitalization-hour group with tqdm progress bar
    for group_key, group_df in tqdm(df.groupby(group_cols), desc="Aggregating data by hour", unit="group"):
        hosp_id, event_time_hour, nth_hour = group_key
        
        # Start with base info
        row_data = {
            'hospitalization_id': hosp_id,
            'event_time_hour': event_time_hour,
            'nth_hour': nth_hour,
            'hour_bucket': event_time_hour.hour  # Extract hour for backward compatibility
        }
        
        # Add patient_id (should be same for all rows in group)
        if 'patient_id' in group_df.columns:
            row_data['patient_id'] = group_df['patient_id'].iloc[0]
        
        # Add day_number for backward compatibility (should be same for all rows in group)
        if 'day_number' in group_df.columns:
            row_data['day_number'] = group_df['day_number'].iloc[0]
        
        # Apply aggregations based on config
        for agg_method, columns in aggregation_config.items():
            for col in columns:
                if col not in group_df.columns:
                    # Only print warning once per column
                    if col not in warned_columns:
                        print(f"Warning: Column '{col}' not found in wide_df, skipping...")
                        warned_columns.add(col)
                    continue
                
                # Get non-null values for this column
                col_values = group_df[col].dropna()
                
                if agg_method == 'max':
                    row_data[f"{col}_max"] = col_values.max() if len(col_values) > 0 else np.nan
                elif agg_method == 'min':
                    row_data[f"{col}_min"] = col_values.min() if len(col_values) > 0 else np.nan
                elif agg_method == 'mean':
                    row_data[f"{col}_mean"] = col_values.mean() if len(col_values) > 0 else np.nan
                elif agg_method == 'median':
                    row_data[f"{col}_median"] = col_values.median() if len(col_values) > 0 else np.nan
                elif agg_method == 'first':
                    # Check if this is a non-agg column (not originally in agg_config)
                    if col in non_agg_columns:
                        # Use '_c' postfix instead of 'first_'
                        row_data[f"{col}_c"] = col_values.iloc[0] if len(col_values) > 0 else np.nan
                    else:
                        # Use original 'first_' postfix for columns specified in agg_config
                        row_data[f"{col}_first"] = col_values.iloc[0] if len(col_values) > 0 else np.nan
                elif agg_method == 'last':
                    row_data[f"{col}_last"] = col_values.iloc[-1] if len(col_values) > 0 else np.nan
                elif agg_method == 'boolean':
                    # 1 if any non-null value present, 0 otherwise
                    row_data[f"{col}_boolean"] = 1 if len(col_values) > 0 else 0
                elif agg_method == 'one_hot_encode':
                    # Create binary columns for each unique value
                    unique_values = col_values.unique()
                    for val in unique_values:
                        new_col_name = f"{col}_{val}"
                        # Clean column name (remove special characters)
                        new_col_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(new_col_name))
                        row_data[new_col_name] = 1
                else:
                    print(f"Warning: Unknown aggregation method '{agg_method}', skipping...")
        
        aggregated_data.append(row_data)
    
    # Create result DataFrame
    hourly_df = pd.DataFrame(aggregated_data)
    
    # Sort by hospitalization_id and nth_hour for chronological order
    hourly_df = hourly_df.sort_values(['hospitalization_id', 'nth_hour']).reset_index(drop=True)
    
    # Fill in missing one-hot encoded columns with 0
    for agg_method, columns in aggregation_config.items():
        if agg_method == 'one_hot_encode':
            for col in columns:
                # Find all one-hot encoded columns for this base column
                one_hot_cols = [c for c in hourly_df.columns if c.startswith(f"{col}_")]
                for ohc in one_hot_cols:
                    hourly_df[ohc] = hourly_df[ohc].fillna(0).astype(int)
    
    print(f"Hourly aggregation complete: {len(hourly_df)} hourly records from {len(df)} original records")
    print(f"Columns in hourly dataset: {len(hourly_df.columns)}")
    
    return hourly_df


def _process_hospitalizations(
    conn: duckdb.DuckDBPyConnection,
    clif_instance,
    required_ids: List[str],
    patient_df: pd.DataFrame,
    hospitalization_df: pd.DataFrame,
    adt_df: pd.DataFrame,
    tables_to_load: List[str],
    category_filters: Dict[str, List[str]],
    pivot_tables: List[str],
    wide_tables: List[str],
    show_progress: bool
) -> Optional[pd.DataFrame]:
    """Process hospitalizations with pivot-first approach."""
    
    print("\n=== Processing Tables ===")
    
    # Create base cohort
    base_cohort = pd.merge(hospitalization_df, patient_df, on='patient_id', how='inner')
    print(f"Base cohort created with {len(base_cohort)} records")
    
    # Register base tables as proper tables, not views
    conn.register('temp_base', base_cohort)
    conn.execute("CREATE OR REPLACE TABLE base_cohort AS SELECT * FROM temp_base")
    conn.unregister('temp_base')
    
    conn.register('temp_adt', adt_df)
    conn.execute("CREATE OR REPLACE TABLE adt AS SELECT * FROM temp_adt")
    conn.unregister('temp_adt')
    
    # Dictionaries to store table info
    event_time_queries = []
    pivoted_table_names = {}
    raw_table_names = {}
    
    # Add ADT event times
    if 'in_dttm' in adt_df.columns:
        event_time_queries.append("""
            SELECT DISTINCT hospitalization_id, in_dttm AS event_time 
            FROM adt 
            WHERE in_dttm IS NOT NULL
        """)
    
    # Process tables to load
    for table_name in tables_to_load:
        print(f"\nProcessing {table_name}...")
        
        # Get table data
        table_attr = 'lab' if table_name == 'labs' else table_name
        table_obj = getattr(clif_instance, table_attr, None)
        
        if table_obj is None:
            print(f"Warning: {table_name} not loaded in CLIF instance, skipping...")
            continue
            
        # Filter by hospitalization IDs immediately
        table_df = table_obj.df[table_obj.df['hospitalization_id'].isin(required_ids)].copy()
        
        if len(table_df) == 0:
            print(f"No data found in {table_name} for selected hospitalizations")
            continue
        
        # For wide tables (non-pivot), filter columns based on category_filters
        if table_name in wide_tables and table_name in category_filters:
            # For respiratory_support, category_filters contains column names to keep
            required_cols = ['hospitalization_id']  # Always keep hospitalization_id
            timestamp_col = _get_timestamp_column(table_name)
            if timestamp_col:
                required_cols.append(timestamp_col)
            
            # Add the columns specified in category_filters
            specified_cols = category_filters[table_name]
            required_cols.extend(specified_cols)
            
            # Filter to only available columns
            available_cols = [col for col in required_cols if col in table_df.columns]
            missing_cols = [col for col in required_cols if col not in table_df.columns]
            
            if missing_cols:
                print(f"Warning: Columns not found in {table_name}: {missing_cols}")
            
            if available_cols:
                table_df = table_df[available_cols].copy()
                print(f"Filtered {table_name} to {len(available_cols)} columns: {available_cols}")
            
        print(f"Loaded {len(table_df)} records from {table_name}")
        
        # Get timestamp column
        timestamp_col = _get_timestamp_column(table_name)
        if timestamp_col and timestamp_col not in table_df.columns:
            timestamp_col = _find_alternative_timestamp(table_name, table_df.columns)
        
        if not timestamp_col or timestamp_col not in table_df.columns:
            print(f"Warning: No timestamp column found for {table_name}, skipping...")
            continue
        
        # Register raw table as a proper table, not a view
        raw_table_name = f"{table_name}_raw"
        # First register the DataFrame temporarily
        conn.register('temp_df', table_df)
        # Create a proper table from it
        conn.execute(f"CREATE OR REPLACE TABLE {raw_table_name} AS SELECT * FROM temp_df")
        # Clean up the temporary registration
        conn.unregister('temp_df')
        raw_table_names[table_name] = raw_table_name
        
        # Process based on table type
        if table_name in pivot_tables:
            # Pivot the table first
            pivoted_name = _pivot_table_duckdb(conn, table_name, table_df, timestamp_col, category_filters)
            if pivoted_name:
                pivoted_table_names[table_name] = pivoted_name
                # Add event times from the RAW table (not pivoted)
                event_time_queries.append(f"""
                    SELECT DISTINCT hospitalization_id, {timestamp_col} AS event_time 
                    FROM {raw_table_name} 
                    WHERE {timestamp_col} IS NOT NULL
                """)
        else:
            # Wide table - just add event times
            event_time_queries.append(f"""
                SELECT DISTINCT hospitalization_id, {timestamp_col} AS event_time 
                FROM {raw_table_name} 
                WHERE {timestamp_col} IS NOT NULL
            """)
    
    # Now create the union and join
    if event_time_queries:
        print("\n=== Creating wide dataset ===")
        final_df = _create_wide_dataset(
            conn, base_cohort, event_time_queries, 
            pivoted_table_names, raw_table_names, 
            tables_to_load, pivot_tables, 
            category_filters
        )
        return final_df
    else:
        print("No event times found, returning base cohort only")
        return base_cohort


def _pivot_table_duckdb(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    table_df: pd.DataFrame,
    timestamp_col: str,
    category_filters: Dict[str, List[str]]
) -> Optional[str]:
    """Pivot a table and return the pivoted table name."""
    
    # Get column mappings
    category_col_mapping = {
        'vitals': 'vital_category',
        'labs': 'lab_category', 
        'medication_admin_continuous': 'med_category',
        'patient_assessments': 'assessment_category'
    }
    
    value_col_mapping = {
        'vitals': 'vital_value',
        'labs': 'lab_value_numeric',
        'medication_admin_continuous': 'med_dose',
        'patient_assessments': 'assessment_value'
    }
    
    category_col = category_col_mapping.get(table_name)
    value_col = value_col_mapping.get(table_name)
    
    if not category_col or not value_col:
        print(f"Warning: No pivot configuration for {table_name}")
        return None
    
    if category_col not in table_df.columns or value_col not in table_df.columns:
        print(f"Warning: Required columns {category_col} or {value_col} not found in {table_name}")
        return None
    
    # Build filter clause if categories specified
    filter_clause = ""
    if table_name in category_filters and category_filters[table_name]:
        categories_list = "','".join(category_filters[table_name])
        filter_clause = f"AND {category_col} IN ('{categories_list}')"
        print(f"Filtering {table_name} categories to: {category_filters[table_name]}")
    
    # Create pivot query
    pivoted_table_name = f"{table_name}_pivoted"
    pivot_query = f"""
    CREATE OR REPLACE TABLE {pivoted_table_name} AS
    WITH pivot_data AS (
        SELECT DISTINCT 
            {value_col}, 
            {category_col},
            hospitalization_id || '_' || strftime({timestamp_col}, '%Y%m%d%H%M') AS combo_id
        FROM {table_name}_raw 
        WHERE {timestamp_col} IS NOT NULL {filter_clause}
    ) 
    PIVOT pivot_data
    ON {category_col}
    USING first({value_col})
    GROUP BY combo_id
    """
    
    try:
        conn.execute(pivot_query)
        
        # Get stats
        count = conn.execute(f"SELECT COUNT(*) FROM {pivoted_table_name}").fetchone()[0]
        cols = len(conn.execute(f"SELECT * FROM {pivoted_table_name} LIMIT 0").df().columns) - 1
        
        print(f"Pivoted {table_name}: {count} combo_ids with {cols} category columns")
        return pivoted_table_name
        
    except Exception as e:
        print(f"Error pivoting {table_name}: {str(e)}")
        return None


def _create_wide_dataset(
    conn: duckdb.DuckDBPyConnection,
    base_cohort: pd.DataFrame,
    event_time_queries: List[str],
    pivoted_table_names: Dict[str, str],
    raw_table_names: Dict[str, str],
    tables_to_load: List[str],
    pivot_tables: List[str],
    category_filters: Dict[str, List[str]]
) -> pd.DataFrame:
    """Create the final wide dataset by joining all tables."""
    
    # Create union of all event times
    union_query = " UNION ALL ".join(event_time_queries)
    
    # Build the main query
    query = f"""
    WITH all_events AS (
        SELECT DISTINCT hospitalization_id, event_time
        FROM ({union_query}) uni_time
    ),
    expanded_cohort AS (
        SELECT 
            a.*,
            b.event_time,
            a.hospitalization_id || '_' || strftime(b.event_time, '%Y%m%d%H%M') AS combo_id
        FROM base_cohort a
        INNER JOIN all_events b ON a.hospitalization_id = b.hospitalization_id
    )
    SELECT ec.*
    """
    
    # Add ADT columns
    if 'adt' in conn.execute("SHOW TABLES").df()['name'].values:
        adt_cols = [col for col in conn.execute("SELECT * FROM adt LIMIT 0").df().columns 
                   if col not in ['hospitalization_id']]
        if adt_cols:
            adt_col_list = ', '.join([f"adt_combo.{col}" for col in adt_cols])
            query = query.replace("SELECT ec.*", f"SELECT ec.*, {adt_col_list}")
    
    # Add pivoted table columns
    for table_name, pivoted_table_name in pivoted_table_names.items():
        pivot_cols = conn.execute(f"SELECT * FROM {pivoted_table_name} LIMIT 0").df().columns
        pivot_cols = [col for col in pivot_cols if col != 'combo_id']
        
        if pivot_cols:
            pivot_col_list = ', '.join([f"{pivoted_table_name}.{col}" for col in pivot_cols])
            query = query.replace("SELECT ec.*", f"SELECT ec.*, {pivot_col_list}")
    
    # Add non-pivoted table columns (respiratory_support)
    for table_name in tables_to_load:
        if table_name not in pivot_tables and table_name in raw_table_names:
            timestamp_col = _get_timestamp_column(table_name)
            if not timestamp_col:
                continue
                
            raw_cols = conn.execute(f"SELECT * FROM {raw_table_names[table_name]} LIMIT 0").df().columns
            table_cols = [col for col in raw_cols if col not in ['hospitalization_id', timestamp_col]]
            
            if table_cols:
                col_list = ', '.join([f"{table_name}_combo.{col}" for col in table_cols])
                query = query.replace("SELECT ec.*", f"SELECT ec.*, {col_list}")
    
    # Add FROM clause
    query += " FROM expanded_cohort ec"
    
    # Add ADT join
    if 'adt' in conn.execute("SHOW TABLES").df()['name'].values:
        query += """
        LEFT JOIN (
            SELECT 
                hospitalization_id || '_' || strftime(in_dttm, '%Y%m%d%H%M') AS combo_id,
                *
            FROM adt
            WHERE in_dttm IS NOT NULL
        ) adt_combo USING (combo_id)
        """
    
    # Add joins for pivoted tables
    for table_name, pivoted_table_name in pivoted_table_names.items():
        query += f" LEFT JOIN {pivoted_table_name} USING (combo_id)"
    
    # Add joins for non-pivoted tables
    for table_name in tables_to_load:
        if table_name not in pivot_tables and table_name in raw_table_names:
            timestamp_col = _get_timestamp_column(table_name)
            if timestamp_col:
                raw_cols = conn.execute(f"SELECT * FROM {raw_table_names[table_name]} LIMIT 0").df().columns
                if timestamp_col in raw_cols:
                    table_cols = [col for col in raw_cols if col not in ['hospitalization_id', timestamp_col]]
                    if table_cols:
                        col_list = ', '.join(table_cols)
                        query += f"""
                        LEFT JOIN (
                            SELECT 
                                hospitalization_id || '_' || strftime({timestamp_col}, '%Y%m%d%H%M') AS combo_id,
                                {col_list}
                            FROM {raw_table_names[table_name]}
                            WHERE {timestamp_col} IS NOT NULL
                        ) {table_name}_combo USING (combo_id)
                        """
    
    # Execute query
    print("Executing join query...")
    result_df = conn.execute(query).df()
    
    # Remove duplicate columns
    result_df = result_df.loc[:, ~result_df.columns.duplicated()]
    
    # Add day-based columns
    result_df['date'] = pd.to_datetime(result_df['event_time']).dt.date
    result_df = result_df.sort_values(['hospitalization_id', 'event_time']).reset_index(drop=True)
    result_df['day_number'] = result_df.groupby('hospitalization_id')['date'].rank(method='dense').astype(int)
    result_df['hosp_id_day_key'] = (result_df['hospitalization_id'].astype(str) + '_day_' + 
                                    result_df['day_number'].astype(str))
    
    # Add missing columns for requested categories
    _add_missing_columns(result_df, category_filters, tables_to_load)
    
    # Clean up
    columns_to_drop = ['combo_id', 'date']
    result_df = result_df.drop(columns=[col for col in columns_to_drop if col in result_df.columns])
    
    print(f"Wide dataset created: {len(result_df)} records with {len(result_df.columns)} columns")
    
    return result_df


def _add_missing_columns(
    df: pd.DataFrame, 
    category_filters: Dict[str, List[str]], 
    tables_loaded: List[str]
):
    """Add missing columns for categories that were requested but not found in data."""
    
    if not category_filters:
        return
    
    for table_name, categories in category_filters.items():
        if table_name in tables_loaded and categories:
            for category in categories:
                if category not in df.columns:
                    df[category] = np.nan
                    print(f"Added missing column: {category}")


def _process_in_batches(
    conn: duckdb.DuckDBPyConnection,
    clif_instance,
    all_hosp_ids: List[str],
    patient_df: pd.DataFrame,
    hospitalization_df: pd.DataFrame,
    adt_df: pd.DataFrame,
    tables_to_load: List[str],
    category_filters: Dict[str, List[str]],
    pivot_tables: List[str],
    wide_tables: List[str],
    batch_size: int,
    show_progress: bool,
    save_to_data_location: bool,
    output_filename: Optional[str],
    output_format: str,
    return_dataframe: bool
) -> Optional[pd.DataFrame]:
    """Process hospitalizations in batches using the new approach."""
    
    # Split into batches
    batches = [all_hosp_ids[i:i + batch_size] for i in range(0, len(all_hosp_ids), batch_size)]
    batch_results = []
    
    iterator = tqdm(batches, desc="Processing batches") if show_progress else batches
    
    for batch_idx, batch_hosp_ids in enumerate(iterator):
        try:
            print(f"\nProcessing batch {batch_idx + 1}/{len(batches)} ({len(batch_hosp_ids)} hospitalizations)")
            
            # Filter base tables for this batch
            batch_hosp_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(batch_hosp_ids)]
            batch_adt_df = adt_df[adt_df['hospitalization_id'].isin(batch_hosp_ids)]
            
            # Clean up tables from previous batch
            tables_df = conn.execute("SHOW TABLES").df()
            for idx, row in tables_df.iterrows():
                table_name = row['name']
                if table_name not in ['base_cohort', 'adt']:
                    try:
                        # Try to drop as table first
                        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                    except:
                        # If that fails, try to drop as view
                        try:
                            conn.execute(f"DROP VIEW IF EXISTS {table_name}")
                        except:
                            pass
            
            # Process this batch
            batch_result = _process_hospitalizations(
                conn, clif_instance, batch_hosp_ids, patient_df, batch_hosp_df, batch_adt_df,
                tables_to_load, category_filters, pivot_tables, wide_tables,
                show_progress=False
            )
            
            if batch_result is not None and len(batch_result) > 0:
                batch_results.append(batch_result)
                print(f"Batch {batch_idx + 1} completed: {len(batch_result)} records")
            
            # Clean up after batch
            import gc
            gc.collect()
            
        except Exception as e:
            logger.error(f"Error processing batch {batch_idx + 1}: {str(e)}")
            print(f"Warning: Failed to process batch {batch_idx + 1}: {str(e)}")
            continue
    
    # Combine results
    if batch_results:
        print(f"\nCombining {len(batch_results)} batch results...")
        final_df = pd.concat(batch_results, ignore_index=True)
        print(f"Final dataset: {len(final_df)} records with {len(final_df.columns)} columns")
        
        if save_to_data_location:
            _save_dataset(final_df, clif_instance.data_dir, output_filename, output_format)
        
        return final_df if return_dataframe else None
    else:
        print("No data processed successfully")
        return None