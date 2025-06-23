import pandas as pd
import duckdb
import numpy as np
from datetime import datetime
import os
import re
from typing import List, Dict, Optional


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
    base_table_columns: Optional[Dict[str, List[str]]] = None
) -> Optional[pd.DataFrame]:
    """
    Create a wide dataset by joining multiple CLIF tables with pivoting support.
    
    Parameters:
        clif_instance: CLIF object with loaded data
        optional_tables: List of optional tables to include ['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments', 'respiratory_support']
        category_filters: Dict specifying which categories to pivot for each table
        sample: Boolean - if True, randomly select 20 hospitalizations
        hospitalization_ids: List of specific hospitalization IDs to filter
        output_format: 'dataframe', 'csv', or 'parquet'
        save_to_data_location: Boolean - save output to data directory
        output_filename: Custom filename (default: 'wide_dataset_YYYYMMDD_HHMMSS')
        return_dataframe: Boolean - return DataFrame even when saving to file (default=True)
        base_table_columns: Dict specifying which columns to select from base tables {'patient': ['col1', 'col2'], 'hospitalization': ['col1'], 'adt': ['col1']}
    
    Returns:
        pd.DataFrame or None (if return_dataframe=False)
    """
    
    print("Starting wide dataset creation...")
    
    # Get base tables with optional column filtering
    patient_df = _filter_base_table_columns(clif_instance.patient.df.copy(), 'patient', base_table_columns)
    hospitalization_df = _filter_base_table_columns(clif_instance.hospitalization.df.copy(), 'hospitalization', base_table_columns)
    adt_df = _filter_base_table_columns(clif_instance.adt.df.copy(), 'adt', base_table_columns)
    
    print(f"Base tables loaded - Patient: {len(patient_df)}, Hospitalization: {len(hospitalization_df)}, ADT: {len(adt_df)}")
    
    # Apply hospitalization filtering
    if hospitalization_ids is not None:
        print(f"Filtering to specific hospitalization IDs: {len(hospitalization_ids)} encounters")
        hospitalization_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(hospitalization_ids)]
        required_ids = hospitalization_df['hospitalization_id'].unique()
    elif sample:
        print("Sampling 20 random hospitalizations...")
        sampled_ids = hospitalization_df['hospitalization_id'].sample(n=min(20, len(hospitalization_df))).tolist()
        hospitalization_df = hospitalization_df[hospitalization_df['hospitalization_id'].isin(sampled_ids)]
        required_ids = hospitalization_df['hospitalization_id'].unique()
        print(f"Selected {len(required_ids)} hospitalizations for sampling")
    else:
        required_ids = hospitalization_df['hospitalization_id'].unique()
        print(f"Processing all {len(required_ids)} hospitalizations")
    
    # Filter other tables based on required IDs
    adt_df = adt_df[adt_df['hospitalization_id'].isin(required_ids)]
    
    # Create base cohort by merging patient and hospitalization
    base_cohort = pd.merge(hospitalization_df, patient_df, on='patient_id', how='inner')
    print(f"Base cohort created with {len(base_cohort)} records")
    
    # Collect all unique event timestamps
    event_times = []
    
    # Add ADT timestamps
    if 'in_dttm' in adt_df.columns:
        adt_times = adt_df[['hospitalization_id', 'in_dttm']].dropna()
        adt_times.rename(columns={'in_dttm': 'event_time'}, inplace=True)
        event_times.append(adt_times)
    
    # Process optional tables
    optional_data = {}
    if optional_tables:
        for table_name in optional_tables:
            table_attr = table_name if table_name != 'labs' else 'lab'
            table_obj = getattr(clif_instance, table_attr, None)
            
            if table_obj is None:
                print(f"Warning: {table_name} table not loaded, skipping...")
                continue
                
            table_df = table_obj.df.copy()
            table_df = table_df[table_df['hospitalization_id'].isin(required_ids)]
            
            print(f"Processing {table_name}: {len(table_df)} records")
            
            # Get timestamp column name
            timestamp_col = _get_timestamp_column(table_name)
            # Handle case where the expected column doesn't exist, try alternatives
            if timestamp_col and timestamp_col not in table_df.columns:
                if table_name == 'labs':
                    # Try alternative timestamp columns for labs
                    alt_cols = ['lab_collect_dttm', 'recorded_dttm', 'lab_order_dttm']
                    for alt_col in alt_cols:
                        if alt_col in table_df.columns:
                            timestamp_col = alt_col
                            break
                elif table_name == 'vitals':
                    # Try alternative for vitals
                    if 'recorded_dttm_min' in table_df.columns:
                        timestamp_col = 'recorded_dttm_min'
            
            if timestamp_col and timestamp_col in table_df.columns:
                # Add timestamps to event_times
                table_times = table_df[['hospitalization_id', timestamp_col]].dropna()
                table_times.rename(columns={timestamp_col: 'event_time'}, inplace=True)
                event_times.append(table_times)
                
                # Process pivoting if needed
                if table_name in ['vitals', 'labs', 'medication_admin_continuous', 'patient_assessments']:
                    pivoted_df = _pivot_table(table_df, table_name, category_filters)
                    if pivoted_df is not None:
                        optional_data[table_name] = {
                            'data': table_df,
                            'pivoted': pivoted_df,
                            'timestamp_col': timestamp_col
                        }
                else:
                    optional_data[table_name] = {
                        'data': table_df,
                        'timestamp_col': timestamp_col
                    }
    
    # Create unified event times
    if event_times:
        all_event_times = pd.concat(event_times, ignore_index=True).drop_duplicates()
        print(f"Found {len(all_event_times)} unique event timestamps")
    else:
        print("No event timestamps found")
        return base_cohort
    
    # Create expanded cohort with all event times
    expanded_cohort = pd.merge(base_cohort, all_event_times, on='hospitalization_id', how='left')
    print(f"Expanded cohort created with {len(expanded_cohort)} records")
    
    # Join with ADT data
    adt_join_df = adt_df.copy()
    if 'in_dttm' in adt_join_df.columns:
        adt_join_df['combo_id'] = (adt_join_df['hospitalization_id'].astype(str) + '_' + 
                                  adt_join_df['in_dttm'].dt.strftime('%Y%m%d%H%M'))
    
    expanded_cohort['combo_id'] = (expanded_cohort['hospitalization_id'].astype(str) + '_' + 
                                  expanded_cohort['event_time'].dt.strftime('%Y%m%d%H%M'))
    
    # Start with expanded cohort
    final_df = expanded_cohort.copy()
    
    # Join ADT data
    if not adt_join_df.empty and 'combo_id' in adt_join_df.columns:
        adt_cols = [col for col in adt_join_df.columns if col not in final_df.columns or col == 'combo_id']
        final_df = pd.merge(final_df, adt_join_df[adt_cols], on='combo_id', how='left')
        print("Joined ADT data")
    
    # Join optional table data
    for table_name, table_info in optional_data.items():
        if 'pivoted' in table_info:
            # Join pivoted data
            pivoted_df = table_info['pivoted']
            if not pivoted_df.empty:
                final_df = pd.merge(final_df, pivoted_df, on='combo_id', how='left')
                print(f"Joined pivoted {table_name} data")
        else:
            # Join non-pivoted data
            table_df = table_info['data']
            timestamp_col = table_info['timestamp_col']
            if timestamp_col and timestamp_col in table_df.columns:
                table_df['combo_id'] = (table_df['hospitalization_id'].astype(str) + '_' + 
                                       table_df[timestamp_col].dt.strftime('%Y%m%d%H%M'))
                table_cols = [col for col in table_df.columns if col not in final_df.columns or col == 'combo_id']
                final_df = pd.merge(final_df, table_df[table_cols], on='combo_id', how='left')
                print(f"Joined {table_name} data")
    
    # Create day-based aggregation
    final_df['date'] = final_df['event_time'].dt.date
    final_df = final_df.sort_values(['hospitalization_id', 'event_time']).reset_index(drop=True)
    final_df['day_number'] = final_df.groupby('hospitalization_id')['date'].rank(method='dense').astype(int)
    final_df['hosp_id_day_key'] = (final_df['hospitalization_id'].astype(str) + '_day_' + 
                                   final_df['day_number'].astype(str))
    
    print(f"Final wide dataset created with {len(final_df)} records and {len(final_df.columns)} columns")
    
    # Handle missing columns for assessments and medications
    _add_missing_columns(final_df, category_filters, optional_tables)
    
    # Clean up intermediate columns
    columns_to_drop = ['combo_id', 'date']
    final_df = final_df.drop(columns=[col for col in columns_to_drop if col in final_df.columns])
    
    # Save if requested
    if save_to_data_location:
        if output_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f'wide_dataset_{timestamp}'
        
        output_path = os.path.join(clif_instance.data_dir, f'{output_filename}.{output_format}')
        
        if output_format == 'csv':
            final_df.to_csv(output_path, index=False)
        elif output_format == 'parquet':
            final_df.to_parquet(output_path, index=False)
        
        print(f"Wide dataset saved to: {output_path}")
    
    # Return DataFrame unless explicitly requested not to
    if return_dataframe:
        return final_df
    else:
        return None


def _get_timestamp_column(table_name: str) -> Optional[str]:
    """Get the timestamp column name for each table type."""
    timestamp_mapping = {
        'vitals': 'recorded_dttm',
        'labs': 'lab_result_dttm',  # fallback to 'recorded_dttm' if not found
        'medication_admin_continuous': 'admin_dttm',
        'patient_assessments': 'recorded_dttm',
        'respiratory_support': 'recorded_dttm'
    }
    return timestamp_mapping.get(table_name)


def _pivot_table(df: pd.DataFrame, table_name: str, category_filters: Optional[Dict[str, List[str]]] = None) -> Optional[pd.DataFrame]:
    """Pivot tables with category columns."""
    
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
    timestamp_col = _get_timestamp_column(table_name)
    
    # Handle alternative timestamp columns
    if timestamp_col and timestamp_col not in df.columns:
        if table_name == 'labs':
            alt_cols = ['lab_collect_dttm', 'recorded_dttm', 'lab_order_dttm']
            for alt_col in alt_cols:
                if alt_col in df.columns:
                    timestamp_col = alt_col
                    break
        elif table_name == 'vitals':
            if 'recorded_dttm_min' in df.columns:
                timestamp_col = 'recorded_dttm_min'
    
    if not all([category_col, value_col, timestamp_col]) or not all([col in df.columns for col in [category_col, value_col, timestamp_col]]):
        print(f"Warning: Required columns not found for pivoting {table_name}")
        return None
    
    # Filter categories if specified
    if category_filters and table_name in category_filters:
        df = df[df[category_col].isin(category_filters[table_name])]
        print(f"Filtered {table_name} to categories: {category_filters[table_name]}")
    
    if df.empty:
        print(f"No data remaining after filtering {table_name}")
        return None
    
    try:
        # Create combo_id for pivoting
        df['combo_id'] = (df['hospitalization_id'].astype(str) + '_' + 
                         df[timestamp_col].dt.strftime('%Y%m%d%H%M'))
        
        # Use DuckDB for pivoting
        duckdb.register(f'{table_name}_data', df)
        
        pivot_query = f"""
        WITH pivot_data AS (
            SELECT DISTINCT {value_col}, {category_col}, combo_id
            FROM {table_name}_data 
            WHERE {timestamp_col} IS NOT NULL 
        ) 
        PIVOT pivot_data
        ON {category_col}
        USING first({value_col})
        GROUP BY combo_id
        """
        
        pivoted_df = duckdb.sql(pivot_query).df()
        print(f"Pivoted {table_name}: {len(pivoted_df)} records with {len(pivoted_df.columns)-1} category columns")
        
        return pivoted_df
        
    except Exception as e:
        print(f"Error pivoting {table_name}: {str(e)}")
        return None


def _add_missing_columns(df: pd.DataFrame, category_filters: Optional[Dict[str, List[str]]] = None, optional_tables: Optional[List[str]] = None):
    """Add missing columns with NaN values for assessments and medications only if those tables were requested."""
    
    # Only add standard columns if the corresponding tables were requested
    if optional_tables is None:
        optional_tables = []
    
    # Standard assessment columns - only add if patient_assessments was requested
    if 'patient_assessments' in optional_tables:
        assessment_columns = [
            'sbt_delivery_pass_fail', 'sbt_screen_pass_fail', 
            'sat_delivery_pass_fail', 'sat_screen_pass_fail',
            'rass', 'gcs_total'
        ]
        
        for col in assessment_columns:
            if col not in df.columns:
                df[col] = np.nan
                print(f"Added missing assessment column: {col}")
    
    # Standard medication columns - only add if medication_admin_continuous was requested
    if 'medication_admin_continuous' in optional_tables:
        medication_columns = [
            'norepinephrine', 'epinephrine', 'phenylephrine', 'angiotensin',
            'vasopressin', 'dopamine', 'dobutamine', 'milrinone', 'isoproterenol',
            'cisatracurium', 'vecuronium', 'rocuronium', 'fentanyl', 'propofol',
            'lorazepam', 'midazolam', 'hydromorphone', 'morphine'
        ]
        
        for col in medication_columns:
            if col not in df.columns:
                df[col] = np.nan
                print(f"Added missing medication column: {col}")
    
    # Add category-specific columns if filters were specified - only add if table was requested
    if category_filters:
        for table_name, categories in category_filters.items():
            # Only add missing columns if the corresponding table was requested
            if table_name in optional_tables:
                for category in categories:
                    if category not in df.columns:
                        df[category] = np.nan
                        print(f"Added missing {table_name} category column: {category}")
            else:
                print(f"Skipping {table_name} categories - table not in optional_tables")


def _filter_base_table_columns(df: pd.DataFrame, table_name: str, base_table_columns: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
    """Filter base table columns based on user specification while preserving required ID columns."""
    
    if base_table_columns is None or table_name not in base_table_columns:
        # Return all columns if no filtering specified
        return df
    
    # Define required ID columns for each base table
    required_columns = {
        'patient': ['patient_id'],
        'hospitalization': ['hospitalization_id', 'patient_id'],
        'adt': ['hospitalization_id']
    }
    
    # Get user-specified columns
    specified_columns = base_table_columns[table_name]
    
    # Combine required and specified columns, remove duplicates
    required_cols = required_columns.get(table_name, [])
    all_columns = list(set(required_cols + specified_columns))
    
    # Filter to only include columns that exist in the DataFrame
    available_columns = [col for col in all_columns if col in df.columns]
    missing_columns = [col for col in all_columns if col not in df.columns]
    
    if missing_columns:
        print(f"Warning: Requested columns not found in {table_name} table: {missing_columns}")
    
    if available_columns:
        filtered_df = df[available_columns].copy()
        original_cols = len(df.columns)
        filtered_cols = len(filtered_df.columns)
        print(f"Filtered {table_name} table: {original_cols} -> {filtered_cols} columns")
        return filtered_df
    else:
        print(f"Warning: No valid columns found for {table_name} table, returning original")
        return df


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
    
    # Get demographic columns (columns that don't vary within a hospitalization)
    demographic_cols = []
    potential_demo_cols = ['age', 'sex', 'race', 'sex_category', 'race_category', 
                          'admission_dttm', 'discharge_dttm', 'admit_dttm']
    for col in potential_demo_cols:
        if col in df.columns:
            demographic_cols.append(col)
    
    # Columns to group by - use event_time_hour instead of day_number and hour_bucket
    group_cols = ['hospitalization_id', 'event_time_hour', 'nth_hour']
    
    # Initialize result dictionary
    aggregated_data = []
    
    # Process each hospitalization-hour group
    for group_key, group_df in df.groupby(group_cols):
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
        
        # Add demographic columns (should be same for all rows in group)
        for col in demographic_cols:
            row_data[col] = group_df[col].iloc[0]
        
        # Apply aggregations based on config
        for agg_method, columns in aggregation_config.items():
            for col in columns:
                if col not in group_df.columns:
                    print(f"Warning: Column '{col}' not found in wide_df, skipping...")
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