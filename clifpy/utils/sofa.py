from .query import lookup_extremal_values_in_long_table
import pandas as pd
from typing import Dict, List
# TODO: apply outlier handlings for all


def _find_extremal_labs_for_sofa(
    ids_w_dttm: pd.DataFrame
) -> pd.DataFrame:
    
    
    query_dict = {
        "creatinine": ["latest", "max"],
        "platelet_count": ["latest", "min"],
        "po2_arterial": ["latest", "min"],
        "bilirubin_total": ["latest", "max"]
    }
    
    return lookup_extremal_values_in_long_table(ids_w_dttm, query_dict, table_name="labs")

def _find_extremal_vitals_for_sofa(
    ids_w_dttm: pd.DataFrame,
    vitals_df: pd.DataFrame,
) -> pd.DataFrame:
    pass

def _find_extremal_meds_for_sofa(
    ids_w_dttm: pd.DataFrame,
    medications_df: pd.DataFrame,
) -> pd.DataFrame:
    pass

def _find_extremal_gcs_for_sofa(
    ids_w_dttm: pd.DataFrame,
    gcs_df: pd.DataFrame,
) -> pd.DataFrame:
    query_dict = {
        "gcs_total": ["latest", "min"]
    }
    return lookup_extremal_values_in_long_table(ids_w_dttm, query_dict, gcs_df, table_name="patient_assessments")



def compute_sofa(
    
):
    pass
    