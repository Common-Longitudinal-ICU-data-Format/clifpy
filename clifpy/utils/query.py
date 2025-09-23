import pandas as pd
from typing import List, Dict
import duckdb
from .io import load_data, load_config


# ref:
"""
SELECT m.*
    , v.vital_value as weight_kg
    , v.recorded_dttm as weight_recorded_dttm
    -- rn = 1 for the weight w/ the latest recorded_dttm (and thus most recent)
    , ROW_NUMBER() OVER (
        PARTITION BY m.hospitalization_id, m.admin_dttm, m.med_category
        ORDER BY v.recorded_dttm DESC
        ) as rn
FROM mac m
LEFT JOIN vitals v 
    ON m.hospitalization_id = v.hospitalization_id 
    AND v.vital_category = 'weight_kg' AND v.vital_value IS NOT NULL
    AND v.recorded_dttm <= m.admin_dttm  -- only past weights
QUALIFY (rn = 1) -- OR (weight_kg IS NULL) -- include meds even if no weight found
ORDER BY m.hospitalization_id, m.admin_dttm, m.med_category, rn
"""

def _convert_key_value_pair_to_sql_clause(
    category_key: str, 
    extremal_values: List[str], 
    category_name: str
) -> str:
    """
    Convert a key-value query to a SQL whereclause
    e.g. 'spo2': 'max' -> ' vital_category = 'spo2' AND rn_max = 1'
    """
    extra = set(extremal_values) - {'min', 'max', 'latest'}
    if extra:
        raise ValueError(f"Invalid extremal_values: {extra}. Must be one of {'min', 'max', 'latest'}")
    return ' OR '.join([f"({category_name} = '{category_key}' AND rn_{extremal_value} = 1)" for extremal_value in extremal_values])
    
def _convert_query_dict_to_sql_clause(query_dict: Dict[str, List[str]], category_name: str) -> str:
    """
    Convert a query dictionary to a SQL clause
    """
    return ' OR '.join([_convert_key_value_pair_to_sql_clause(k, v, category_name) for k, v in query_dict.items()])

table_name_to_category_name_mapper = {
    "vitals": "vital_category",
    "labs": "lab_category",
    "medication_admin_continuous": "med_category",
    "patient_assessments": "assessment_category"
}

table_name_to_value_name_mapper = {
    "vitals": "vital_value",
    "labs": "lab_value_numeric",
    "medication_admin_continuous": "med_dose",
    "patient_assessments": "numerical_value"
}

table_name_to_dttm_name_mapper = {
    "vitals": "recorded_dttm",
    "labs": "lab_collect_dttm",
    "medication_admin_continuous": "admin_dttm",
    "patient_assessments": "recorded_dttm"
}

acceptable_table_names = set(table_name_to_category_name_mapper.keys())

def lookup_extremal_values_in_long_table(
    ids_w_dttm: pd.DataFrame, 
    query_dict: Dict[str, List[str]], 
    table_name: str
) -> pd.DataFrame:
    """
    Lookup extremal values in a long table
    
    Args:
    - ids_w_dttm: DataFrame with hospitalization_id and start_dttm and end_dttm
    - query_dict: Dictionary of category names and the extremal values (e.g. 'max', 'latest')
        - e.g. {'weight_kg': ['latest', 'max'], 'spo2': ['max', 'min']}
    - table_name: name of the table to lookup
        
    Returns:
    # - joined: DataFrame with all joined data including row number rankings
    # - qualified: DataFrame filtered to only requested extremal values
    - pivoted: DataFrame with columns like 'spo2_max', 'weight_kg_latest' etc.
    
    Usage:
    """
    missing_columns = {'hospitalization_id', 'start_dttm', 'end_dttm'} - set(ids_w_dttm.columns)
    if missing_columns:
        raise ValueError(f"ids_w_dttm must have columns: {', '.join(missing_columns)}")
    if table_name not in acceptable_table_names:
        raise ValueError(f"table_name must be one of the long tables: {acceptable_table_names}")
    
    category_name = table_name_to_category_name_mapper[table_name]
    value_name = table_name_to_value_name_mapper[table_name]
    dttm_name = table_name_to_dttm_name_mapper[table_name]
    
    categories = list(query_dict.keys())
    # TODO: check if vital_categories is a dictionary
    
    # load cohort data from table
    config = load_config('config/config.yaml')
    df = load_data(
        table_name, config['tables_path'], config['filetype'], 
        filters={'hospitalization_id': ids_w_dttm['hospitalization_id'].tolist()}
        )
    
    categories_sql = ', '.join([f"'{cat}'" for cat in categories])
    query = f"""
    SELECT l.*
        , r.{value_name}
        , r.{dttm_name}
        , r.{category_name}
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.{category_name}
            ORDER BY r.{dttm_name} DESC
            ) as rn_latest
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.{category_name}
            ORDER BY r.{value_name} DESC
            ) as rn_max
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.{category_name}
            ORDER BY r.{value_name} ASC
            ) as rn_min
    FROM ids_w_dttm l
    LEFT JOIN df r
    ON l.hospitalization_id = r.hospitalization_id
        AND l.start_dttm <= r.{dttm_name}
        AND r.{dttm_name} <= l.end_dttm
        AND r.{category_name} IN ({categories_sql})
        AND r.{value_name} IS NOT NULL
    ORDER BY l.hospitalization_id, l.start_dttm, r.{category_name}, rn_latest, rn_max, rn_min
    """
    joined = duckdb.sql(query).to_df()
    
    query = f"""
    SELECT *
    FROM joined
    WHERE {_convert_query_dict_to_sql_clause(query_dict, category_name)}
    """
    qualified = duckdb.sql(query).to_df()
    
    # Create the melted version to identify which extremal values we want
    melted = qualified.melt(
        id_vars=['hospitalization_id', 'start_dttm', 'end_dttm', category_name, value_name],
        value_vars=['rn_max', 'rn_latest', 'rn_min'],
        var_name='extreme',
        value_name='rn'
    ).query('rn == 1').drop(columns=['rn'])
    
    # Create column names like 'spo2_max', 'weight_kg_latest'
    melted['column_name'] = melted[category_name] + '_' + melted['extreme'].str.replace('rn_', '')
    
    # Pivot to get the desired column structure
    pivoted = melted.pivot_table(
        index=['hospitalization_id', 'start_dttm', 'end_dttm'],
        columns='column_name',
        values=value_name
    ).reset_index()
    
    # Flatten column names
    pivoted.columns.name = None
    
    return pivoted

def lookup_extremal_vital_values(
    ids_w_dttm: pd.DataFrame, query_dict: Dict[str, List[str]], vitals_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    # TODO: deprecate this function
    Lookup vital values 
    
    Args:
    - ids_w_dttm: DataFrame with hospitalization_id and start_dttm and end_dttm
    - query_dict: Dictionary of vital categories and the extremal values (e.g. 'max', 'latest')
        - e.g. {'weight_kg': ['latest', 'max'], 'spo2': ['max', 'min']}
    - vitals_df: DataFrame with vital values
        
    Returns:
    - joined: DataFrame with all joined data including row number rankings
    - qualified: DataFrame filtered to only requested extremal values
    - pivoted: DataFrame with columns like 'spo2_max', 'weight_kg_latest' etc.
    
    Usage:
    """
    missing_columns = {'hospitalization_id', 'start_dttm', 'end_dttm'} - set(ids_w_dttm.columns)
    if missing_columns:
        raise ValueError(f"ids_w_dttm must have columns: {', '.join(missing_columns)}")
    
    distinct_hosp_ids = ids_w_dttm['hospitalization_id'].unique()
    vital_categories = list(query_dict.keys())
    # vitals_df = vitals_df[vitals_df['vital_category'].isin(vital_categories)]
    # check if vital_categories is a dictionary
    vital_categories_sql = ', '.join([f"'{cat}'" for cat in vital_categories])
    query = f"""
    SELECT l.*
        , r.vital_value 
        , r.recorded_dttm
        , r.vital_category
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.vital_category
            ORDER BY r.recorded_dttm DESC
            ) as rn_latest
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.vital_category
            ORDER BY r.vital_value DESC
            ) as rn_max
        , ROW_NUMBER() OVER (
            PARTITION BY l.hospitalization_id, l.start_dttm, l.end_dttm, r.vital_category
            ORDER BY r.vital_value ASC
            ) as rn_min
    FROM ids_w_dttm l
    LEFT JOIN vitals_df r
    ON l.hospitalization_id = r.hospitalization_id
        AND l.start_dttm <= r.recorded_dttm
        AND r.recorded_dttm <= l.end_dttm
        AND r.vital_category IN ({vital_categories_sql})
        AND r.vital_value IS NOT NULL
    ORDER BY l.hospitalization_id, l.start_dttm, r.vital_category, rn_latest, rn_max, rn_min
    """
    joined = duckdb.sql(query).to_df()
    
    query = f"""
    SELECT *
    FROM joined
    WHERE {_convert_query_dict_to_sql_clause(query_dict)}
    """
    qualified = duckdb.sql(query).to_df()
    
    # Create the melted version to identify which extremal values we want
    melted = qualified.melt(
        id_vars=['hospitalization_id', 'start_dttm', 'end_dttm', 'vital_category', 'vital_value'],
        value_vars=['rn_max', 'rn_latest', 'rn_min'],
        var_name='extreme',
        value_name='rn'
    ).query('rn == 1').drop(columns=['rn'])
    
    # Create column names like 'spo2_max', 'weight_kg_latest'
    melted['column_name'] = melted['vital_category'] + '_' + melted['extreme'].str.replace('rn_', '')
    
    # Pivot to get the desired column structure
    pivoted = melted.pivot_table(
        index=['hospitalization_id', 'start_dttm', 'end_dttm'],
        columns='column_name',
        values='vital_value'
    ).reset_index()
    
    # Flatten column names
    pivoted.columns.name = None
    
    return pivoted
    

    
    