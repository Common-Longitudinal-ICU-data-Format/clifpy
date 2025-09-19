from .query import lookup_extremal_values_in_long_table
import pandas as pd
from typing import Dict, List
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
        'resp_rate_set','tracheostomy', 'resp_rate_obs','tidal_volume_set'
    ] 
}

MAX_ITEMS = REQUIRED_SOFA_CATEGORIES_BY_TABLE['medication_admin_continuous'] \
    + ['fio2_set', 'creatinine', 'bilirubin_total']

MIN_ITEMS = ['map', 'spo2', 'po2_arterial', 'platelet_count', 'gcs_total']

device_rank_dict = {
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
device_rank_mapping = pd.DataFrame({
    'device_category': device_rank_dict.keys(),
    'device_rank': device_rank_dict.values()
})

def generate_extremal_values_by_id(
    wide_df: pd.DataFrame,
    extremal_type: str # worst or latest
) -> pd.DataFrame:
    if extremal_type == 'worst':
        q = f"""
        FROM wide_df
        LEFT JOIN device_rank_mapping USING (device_category)
        SELECT hospitalization_id
            , MAX(COLUMNS({MAX_ITEMS}))
            , MIN(COLUMNS({MIN_ITEMS}))
            , device_rank_min: MIN(device_rank)
        GROUP BY hospitalization_id
        """
        return duckdb.sql(q).df()
    elif extremal_type == 'latest':
        q = f"""
        FROM wide_df
        LEFT JOIN device_rank_mapping USING (device_category)
        SELECT hospitalization_id
            , any_value(COLUMNS({REQUIRED_SOFA_CATEGORIES_BY_TABLE}) ORDER BY event_time DESC)
            , device_rank_min: MIN(device_rank)
        GROUP BY hospitalization_id
        """
        return duckdb.sql(q).df()
    else:
        raise ValueError(f"Invalid extremal type: {extremal_type}")

def compute_sofa_from_extremal_values(
    extremal_df: pd.DataFrame
):
    q = f"""
    FROM extremal_df df
    LEFT JOIN device_rank_mapping m on df.device_rank_min = m.device_rank
    SELECT hospitalization_id
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
        , sofa_resp: CASE WHEN p_f < 100 AND m.device_category IN ('imv', 'nippv', 'cpap') THEN 4 
            WHEN p_f >= 100 and p_f < 200 AND m.device_category IN ('imv', 'nippv', 'cpap') THEN 3
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
    