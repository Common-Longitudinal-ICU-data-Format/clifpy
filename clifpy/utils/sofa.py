import pandas as pd
from typing import Dict, List, Optional
import duckdb

REQUIRED_SOFA_CATEGORIES_BY_TABLE = {
    'labs': ['creatinine','platelet_count','po2_arterial','bilirubin_total'],
    'vitals': ['map','spo2', 'weight_kg'],
    'patient_assessments': ['gcs_total'],
    "medication_admin_continuous": [
        "norepinephrine","epinephrine","phenylephrine","vasopressin", "dopamine","angiotensin",
        "dobutamine","milrinone"
        ],
    'respiratory_support': [
        'device_category','device_name','mode_name','mode_category','peep_set','fio2_set','lpm_set',
        # 'resp_rate_set', 'tracheostomy', 'resp_rate_obs','tidal_volume_set'
    ] 
}

MAX_ITEMS = REQUIRED_SOFA_CATEGORIES_BY_TABLE['medication_admin_continuous'] \
    + ['fio2_set', 'creatinine', 'bilirubin_total']

MIN_ITEMS = ['map', 'spo2', 'po2_arterial', 'platelet_count', 'gcs_total']

DEVICE_RANK_DICT = {
    'IMV': 1,
    'NIPPV': 2,
    'CPAP': 3,
    'High Flow NC': 4,
    'Face Mask': 5,
    'Trach Collar': 6,
    'Nasal Cannula': 7,
    'Other': 8,
    'Room Air': 9
}

# create a mappping df
DEVICE_RANK_MAPPING = pd.DataFrame({
    'device_category': DEVICE_RANK_DICT.keys(),
    'device_rank': DEVICE_RANK_DICT.values()
})

def _agg_extremal_values_by_id(
    wide_df: pd.DataFrame,
    extremal_type: str,  
    id_name: str
) -> pd.DataFrame:
    '''
    extremal_type: 'worst' or 'latest'
    id_name: 'hospitalization_id', 'patient_id', 'encounter_block'
    
    '''
    if extremal_type == 'worst':
        q = f"""
        FROM wide_df
        LEFT JOIN DEVICE_RANK_MAPPING USING (device_category)
        SELECT {id_name}
            , MAX(COLUMNS({MAX_ITEMS}))
            , MIN(COLUMNS({MIN_ITEMS}))
            , device_rank_min: MIN(device_rank)
        GROUP BY {id_name}
        """
        return duckdb.sql(q).df()
    elif extremal_type == 'latest':
        raise NotImplementedError("this is a future feature and currently available")
        # TODO: Future feature - implement latest value extraction
        q = f"""
        FROM wide_df
        LEFT JOIN DEVICE_RANK_MAPPING USING (device_category)
        SELECT {id_name}
            , any_value(COLUMNS({MAX_ITEMS + MIN_ITEMS}) ORDER BY event_time DESC)
            , device_rank_min: MIN(device_rank)
        GROUP BY {id_name}
        """
        return duckdb.sql(q).df()
        
    else:
        raise ValueError(f"Invalid extremal type: {extremal_type}")

def _compute_sofa_from_extremal_values(
    extremal_df: pd.DataFrame,
    id_name: str
):
    q = f"""
    FROM extremal_df df
    LEFT JOIN DEVICE_RANK_MAPPING m on df.device_rank_min = m.device_rank
    SELECT {id_name}
        , p_f: po2_arterial / fio2_set
        , sofa_cv_97: CASE WHEN dopamine > 15 OR epinephrine > 0.1 OR norepinephrine > 0.1 THEN 4
            WHEN dopamine > 5 OR epinephrine <= 0.1 OR norepinephrine <= 0.1 THEN 3
            WHEN dopamine <= 5 OR dobutamine > 0 THEN 2
            WHEN map < 70 THEN 1
            WHEN map >= 70 THEN 0 END
        , sofa_coag: CASE WHEN platelet_count < 20 THEN 4
            WHEN platelet_count < 50 THEN 3
            WHEN platelet_count < 100 THEN 2
            WHEN platelet_count < 150 THEN 1
            WHEN platelet_count >= 150 THEN 0 END 
        , sofa_liver: CASE WHEN bilirubin_total >= 12 THEN 4
            WHEN bilirubin_total < 12 AND bilirubin_total >= 6 THEN 3
            WHEN bilirubin_total < 6 AND bilirubin_total >= 2 THEN 2
            WHEN bilirubin_total < 2 AND bilirubin_total >= 1.2 THEN 1
            WHEN bilirubin_total < 1.2 THEN 0 END
        , sofa_resp: CASE WHEN p_f < 100 AND m.device_category IN ('IMV', 'NIPPV', 'CPAP') THEN 4 
            WHEN p_f >= 100 and p_f < 200 AND m.device_category IN ('IMV', 'NIPPV', 'CPAP') THEN 3
            WHEN p_f >= 200 AND p_f < 300 THEN 2
            WHEN p_f >= 300 AND p_f < 400 THEN 1
            WHEN p_f >= 400 THEN 0 END
        , sofa_cns: CASE WHEN gcs_total < 6 THEN 4
            WHEN gcs_total >= 6 AND gcs_total <= 9 THEN 3
            WHEN gcs_total >= 10 AND gcs_total <= 12 THEN 2
            WHEN gcs_total >= 13 AND gcs_total <= 14 THEN 1
            WHEN gcs_total == 15 THEN 0 END
        , sofa_renal: CASE WHEN creatinine >= 5 THEN 4
            WHEN creatinine < 5 AND creatinine >= 3.5 THEN 3
            WHEN creatinine < 3.5 AND creatinine >= 2 THEN 2
            WHEN creatinine < 2 AND creatinine >= 1.2 THEN 1
            WHEN creatinine < 1.2 THEN 0 END
        , sofa_total: sofa_cv_97 + sofa_coag + sofa_liver + sofa_resp + sofa_renal + sofa_cns
    """
    return duckdb.sql(q).df()

def _fill_na_scores(
    sofa_df: pd.DataFrame
) -> pd.DataFrame:
    # compute the total score ignoring NAs
    subscore_columns = ['sofa_cv_97', 'sofa_coag', 'sofa_renal', 'sofa_liver', 'sofa_resp', 'sofa_cns']
    sofa_df['sofa_total'] = sofa_df[subscore_columns].sum(axis=1, skipna=True)
    
    # fill all the NAs in the subscore columns with 0
    sofa_df[subscore_columns] = sofa_df[subscore_columns].fillna(0)
    return sofa_df

def compute_sofa(
    wide_df: pd.DataFrame,
    cohort_df: Optional[pd.DataFrame] = None,
    extremal_type: str = 'worst',
    id_name: str = 'encounter_block',
    fill_na_scores_with_zero: bool = True
) -> pd.DataFrame:
    """
    Compute SOFA scores from a wide dataset.

    Parameters:
        wide_df: Wide dataset containing all required SOFA variables
        cohort_df: Optional DataFrame with columns ['hospitalization_id', 'start_time', 'end_time']
                  to further filter observations by time windows
        extremal_type: 'worst' or 'latest' (currently only 'worst' is implemented)
        id_name: Column name for grouping (e.g., 'hospitalization_id', 'patient_id', 'encounter_block')

    Returns:
        DataFrame with SOFA component scores and total score for each ID
    """
    # Validate inputs
    if extremal_type not in ['worst', 'latest']:
        raise ValueError(f"extremal_type must be 'worst' or 'latest', got '{extremal_type}'")

    if id_name not in wide_df.columns:
        raise ValueError(f"id_name '{id_name}' not found in wide_df columns")

    # Apply cohort time filtering if provided
    if cohort_df is not None:
        required_cols = [id_name, 'start_time', 'end_time']
        missing_cols = [col for col in required_cols if col not in cohort_df.columns]
        if missing_cols:
            raise ValueError(f"cohort_df must contain columns: {required_cols}. Missing: {missing_cols}")

        q = f"""
        FROM wide_df w
        INNER JOIN cohort_df c
            ON w.{id_name} = c.{id_name}
            AND c.start_time <= w.event_time
            AND c.end_time >= w.event_time
        SELECT w.*
        """
        wide_df = duckdb.sql(q).df()
        
    sofa_scores = (
        _agg_extremal_values_by_id(wide_df, extremal_type, id_name)
        .pipe(_compute_sofa_from_extremal_values, id_name)
    )

    if fill_na_scores_with_zero:
        sofa_scores = _fill_na_scores(sofa_scores)

    return sofa_scores