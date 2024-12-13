# src/pyclif/utils/io.py
'''
TO DO - USE THE FUNCTION FROM MOBILIZATION CODE TO LOAD SPECIFIC TABLES
'''
import os
import duckdb
import pandas as pd
import numpy as np

def load_table(file_path, filetype='csv', columns=None, sample_size=None):
    con = duckdb.connect()
    if filetype == 'csv':
        query = f"SELECT {', '.join(columns) if columns else '*'} FROM read_csv_auto('{file_path}')"
    elif filetype == 'parquet':
        query = f"SELECT {', '.join(columns) if columns else '*'} FROM parquet_scan('{file_path}')"
    else:
        raise ValueError("Unsupported filetype. Only 'csv' and 'parquet' are supported.")

    if sample_size:
        query += f" LIMIT {sample_size}"

    df = con.execute(query).fetchdf()
    con.close()
    return df

def process_resp_support(df):
    """
    Process the respiratory support data using waterfall logic.
    
    Parameters:
        df (pd.DataFrame): DataFrame containing respiratory support data.
        
    Returns:
        pd.DataFrame: Processed DataFrame with filled values.
    """
    print("Initiating waterfall processing...")
    
    # Ensure 'recorded_dttm' is in datetime format
    df['recorded_dttm'] = pd.to_datetime(df['recorded_dttm'])
    
    # Convert categories to lowercase to standardize
    df['device_category'] = df['device_category'].str.lower()
    df['mode_category'] = df['mode_category'].str.lower()
    df['device_name'] = df['device_name'].str.lower()
    df['mode_name'] = df['mode_name'].str.lower()
    
    # # Fix out-of-range values
    # print("Fixing out-of-range values for 'fio2_set', 'peep_set', and 'resp_rate_set'...")
    # df['fio2_set'] = df['fio2_set'].where(df['fio2_set'].between(0.21, 1), np.nan)
    # df['peep_set'] = df['peep_set'].where(df['peep_set'].between(0, 50), np.nan)
    # df['resp_rate_set'] = df['resp_rate_set'].where(df['resp_rate_set'].between(0, 60), np.nan)
    
    # Create 'recorded_date' and 'recorded_hour'
    print('Creating recorded_date and recorded_hour...')
    df['recorded_date'] = df['recorded_dttm'].dt.date
    df['recorded_hour'] = df['recorded_dttm'].dt.hour
    
    # Sort data
    print("Sorting data by 'hospitalization_id' and 'recorded_dttm'...")
    df.sort_values(by=['hospitalization_id', 'recorded_dttm'], inplace=True)
    
    # Fix missing 'device_category' and 'device_name' based on 'mode_category'
    print("Fixing missing 'device_category' and 'device_name' based on 'mode_category'...")
    mask = (
        df['device_category'].isna() &
        df['device_name'].isna() &
        df['mode_category'].str.contains('assist control-volume control|simv|pressure control', case=False, na=False)
    )
    df.loc[mask, 'device_category'] = 'imv'
    df.loc[mask, 'device_name'] = 'mechanical ventilator'
    
    # Fix 'device_category' and 'device_name' based on neighboring records
    print("Fixing 'device_category' and 'device_name' based on neighboring records...")
    # Create shifted columns once to avoid multiple shifts
    df['device_category_shifted'] = df['device_category'].shift()
    df['device_category_shifted_neg'] = df['device_category'].shift(-1)
    
    condition_prev = (
        df['device_category'].isna() &
        (df['device_category_shifted'] == 'imv') &
        df['resp_rate_set'].gt(1) &
        df['peep_set'].gt(1)
    )
    condition_next = (
        df['device_category'].isna() &
        (df['device_category_shifted_neg'] == 'imv') &
        df['resp_rate_set'].gt(1) &
        df['peep_set'].gt(1)
    )
    
    condition = condition_prev | condition_next
    df.loc[condition, 'device_category'] = 'imv'
    df.loc[condition, 'device_name'] = 'mechanical ventilator'
    
    # Drop the temporary shifted columns
    df.drop(['device_category_shifted', 'device_category_shifted_neg'], axis=1, inplace=True)
    
    # Handle duplicates and missing data
    print("Handling duplicates and removing rows with all key variables missing...")
    df['n'] = df.groupby(['hospitalization_id', 'recorded_dttm'])['recorded_dttm'].transform('size')
    df = df[~((df['n'] > 1) & (df['device_category'] == 'nippv'))]
    df = df[~((df['n'] > 1) & (df['device_category'].isna()))]
    subset_vars = ['device_category', 'device_name', 'mode_category', 'mode_name', 'fio2_set']
    df.dropna(subset=subset_vars, how='all', inplace=True)
    df.drop_duplicates(subset=['hospitalization_id', 'recorded_dttm'], keep='first', inplace=True)
    df.drop('n', axis=1, inplace=True)  # Drop 'n' as it's no longer needed
    
    # Fill forward 'device_category' within each hospitalization
    print("Filling forward 'device_category' within each hospitalization...")
    df['device_category'] = df.groupby('hospitalization_id')['device_category'].ffill()
    
    # Create 'device_cat_id' based on changes in 'device_category'
    print("Creating 'device_cat_id' to track changes in 'device_category'...")
    df['device_cat_f'] = df['device_category'].fillna('missing').astype('category').cat.codes
    df['device_cat_change'] = df['device_cat_f'] != df.groupby('hospitalization_id')['device_cat_f'].shift()
    df['device_cat_change'] = df['device_cat_change'].astype(int)
    df['device_cat_id'] = df.groupby('hospitalization_id')['device_cat_change'].cumsum()
    df.drop('device_cat_change', axis=1, inplace=True)
    
    # Fill 'device_name' within 'device_cat_id'
    print("Filling 'device_name' within each 'device_cat_id'...")
    df['device_name'] = df.groupby(['hospitalization_id', 'device_cat_id'])['device_name'].ffill().bfill()
    
    # Create 'device_id' based on changes in 'device_name'
    print("Creating 'device_id' to track changes in 'device_name'...")
    df['device_name_f'] = df['device_name'].fillna('missing').astype('category').cat.codes
    df['device_name_change'] = df['device_name_f'] != df.groupby('hospitalization_id')['device_name_f'].shift()
    df['device_name_change'] = df['device_name_change'].astype(int)
    df['device_id'] = df.groupby('hospitalization_id')['device_name_change'].cumsum()
    df.drop('device_name_change', axis=1, inplace=True)
    
    # Fill 'mode_category' within 'device_id'
    print("Filling 'mode_category' within each 'device_id'...")
    df['mode_category'] = df.groupby(['hospitalization_id', 'device_id'])['mode_category'].ffill().bfill()
    
    # Create 'mode_cat_id' based on changes in 'mode_category'
    print("Creating 'mode_cat_id' to track changes in 'mode_category'...")
    df['mode_cat_f'] = df['mode_category'].fillna('missing').astype('category').cat.codes
    df['mode_cat_change'] = df['mode_cat_f'] != df.groupby(['hospitalization_id', 'device_id'])['mode_cat_f'].shift()
    df['mode_cat_change'] = df['mode_cat_change'].astype(int)
    df['mode_cat_id'] = df.groupby(['hospitalization_id', 'device_id'])['mode_cat_change'].cumsum()
    df.drop('mode_cat_change', axis=1, inplace=True)
    
    # Fill 'mode_name' within 'mode_cat_id'
    print("Filling 'mode_name' within each 'mode_cat_id'...")
    df['mode_name'] = df.groupby(['hospitalization_id', 'mode_cat_id'])['mode_name'].ffill().bfill()
    
    # Create 'mode_name_id' based on changes in 'mode_name'
    print("Creating 'mode_name_id' to track changes in 'mode_name'...")
    df['mode_name_f'] = df['mode_name'].fillna('missing').astype('category').cat.codes
    df['mode_name_change'] = df['mode_name_f'] != df.groupby(['hospitalization_id', 'mode_cat_id'])['mode_name_f'].shift()
    df['mode_name_change'] = df['mode_name_change'].astype(int)
    df['mode_name_id'] = df.groupby(['hospitalization_id', 'mode_cat_id'])['mode_name_change'].cumsum()
    df.drop('mode_name_change', axis=1, inplace=True)
    
    # Adjust 'fio2_set' for 'room air' device_category
    print("Adjusting 'fio2_set' for 'room air' device_category...")
    df['fio2_set'] = np.where(df['fio2_set'].isna() & (df['device_category'] == 'room air'), 0.21, df['fio2_set'])
    
    # Adjust 'mode_category' for 't-piece' devices
    print("Adjusting 'mode_category' for 't-piece' devices...")
    mask_tpiece = (
        df['mode_category'].isna() &
        df['device_name'].str.contains('t-piece', case=False, na=False)
    )
    df.loc[mask_tpiece, 'mode_category'] = 'blow by'
    
    # Fill remaining variables within 'mode_name_id'
    print("Filling remaining variables within each 'mode_name_id'...")
    fill_vars = [
        'fio2_set', 'lpm_set', 'peep_set', 'resp_rate_set',
        'resp_rate_obs'
    ]
    df[fill_vars] = df.groupby(['hospitalization_id', 'mode_name_id'])[fill_vars].transform(lambda x: x.ffill().bfill())
    
    # Fill 'tracheostomy' forward within each hospitalization
    print("Filling 'tracheostomy' forward within each hospitalization...")
    df['tracheostomy'] = df.groupby('hospitalization_id')['tracheostomy'].ffill()
    
    # Remove duplicates
    print("Removing duplicates...")
    df.drop_duplicates(inplace=True)
    
    # Select relevant columns
    columns_to_keep = [
        'hospitalization_id', 'recorded_dttm', 'recorded_date', 'recorded_hour',
        'device_category', 'device_name', 'mode_category', 'mode_name',
        'device_cat_id', 'device_id', 'mode_cat_id', 'mode_name_id',
        'fio2_set', 'lpm_set', 'peep_set', 'resp_rate_set',
        'tracheostomy', 'resp_rate_obs'
    ]
    # Ensure columns exist before selecting
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[existing_columns]
    
    print("Waterfall processing completed.")
    return df
