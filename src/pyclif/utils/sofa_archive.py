"""sofa_score.py 
self-contained module to compute SOFA-97 in the specified window (start_dttm and stop_dttm)

Author: Kaveri Chhikara
Date: April 20, 2025

To-do-- add check that required columns exist in the input DataFrame

Usage
-----
from sofa_score import compute_sofa
res_df = compute_sofa(
    ids_w_dttm,            # DataFrame with id, start_dttm, stop_dttm (UTC)
    tables_path=...,
    use_hospitalization_id=True,
    id_mapping=None,       # DataFrame with hospitalization_id + custom id_col
    output_filepath=None,  # if given, write Parquet
)

Returned
--------
Pandas DataFrame one row / id with worst components + total score.
"""

import pandas as pd, numpy as np, logging, json
import pyCLIF
from typing import Optional

##############################################################################
# Logging setup
##############################################################################
logger = logging.getLogger("sofa_score")
if not logger.handlers:
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

##############################################################################
# Outlier config file
##############################################################################

with open('../config/outlier_config.json', 'r', encoding='utf-8') as f:
    outlier_cfg = json.load(f)


##############################################################################
# main entry
##############################################################################
def compute_sofa(ids_w_dttm:pd.DataFrame,
                 tables_path:str,
                 use_hospitalization_id:bool=True,
                 id_mapping:Optional[pd.DataFrame]=None,
                 output_filepath:Optional[str]=None,
                 helper_module=None)->pd.DataFrame:
    """Compute SOFA using worst value between start/stop for each id"""
    logger.info("Starting SOFA computation for %d rows", len(ids_w_dttm))
    load_data = pyCLIF.load_data

    # ------------------------------------------------------------------ ids
    ids = ids_w_dttm.copy()
    ids.columns = [c.lower() for c in ids.columns]
    if use_hospitalization_id:
        id_col = "hospitalization_id"
        ids[id_col] = ids[id_col].astype(str)
    else:
        if id_mapping is None:
            raise ValueError("id_mapping required when use_hospitalization_id=False")
        ids = ids.merge(id_mapping, on=id_mapping.columns[0], how='left')
        id_col = [c for c in id_mapping.columns if c!='hospitalization_id'][0]

    hosp_list = ids['hospitalization_id'].unique().tolist() if 'hospitalization_id' in ids.columns else None

    #######################################################################
    # 1.  Labs
    #######################################################################
    labs_required_columns = [
        'hospitalization_id',
        'lab_result_dttm',
        'lab_category',
        'lab_value_numeric'
    ]
    lab_categories = ['creatinine','platelet_count','po2_arterial','bilirubin_total']
    labs_filters = {
        'hospitalization_id': hosp_list,
        'lab_category': lab_categories
    }
    labs = pyCLIF.load_data('clif_labs', columns=labs_required_columns, filters=labs_filters)
    labs = pyCLIF.convert_datetime_columns_to_site_tz(labs, pyCLIF.helper['timezone'])
    labs['lab_value_numeric'] = pd.to_numeric(labs['lab_value_numeric'], errors='coerce')
    labs = labs.dropna(subset=['lab_value_numeric'])
    logger.info("Loaded %d lab rows", len(labs))

    # Apply threhsolds and drop NAs 
    lab_thresholds = {
    "creatinine": (0, 20),
    "bilirubin_total": (0, 80),
    "po2_arterial": (30, 700),
    "platelet_count": (0, 2000)
    }
    mask = False
    for lab_cat, (min_val, max_val) in lab_thresholds.items():
        mask = mask | (
            (labs['lab_category'] == lab_cat) & 
            (labs['lab_value_numeric'] >= min_val) & 
            (labs['lab_value_numeric'] <= max_val)
        )

    lab_sub = labs[mask].copy()

    # 2. Filter labs within start and stop times
    # Merge with ids to get start and stop times
    lab_sub = lab_sub.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )
    # Filter labs within time window
    lab_sub = lab_sub[
        (lab_sub['lab_result_dttm'] >= lab_sub['start_dttm']) & 
        (lab_sub['lab_result_dttm'] <= lab_sub['stop_dttm'])
    ]
    # Aggregate by time bucket
    lab_summary = (
        lab_sub
        .groupby([id_col, 'lab_category'])['lab_value_numeric']
        .agg(lambda x: x.min() if x.name in ["po2_arterial", "platelet_count"] else x.max())
        .reset_index()
    )

    lab_summary = lab_summary.pivot(
        index=id_col,
        columns='lab_category',
        values='lab_value_numeric'
    ).reset_index()

    # Rename columns to indicate aggregation function used
    lab_summary = lab_summary.rename(columns={
        "po2_arterial": "po2_arterial_min",
        "platelet_count": "platelet_count_min",
        "creatinine": "creatinine_max",
        "bilirubin_total": "bilirubin_total_max"
    })

    #######################################################################
    # 2. Vitals
    #######################################################################
    vital_cats = ['map','spo2', 'weight_kg']
    vitals = pyCLIF.load_data('clif_vitals',
                    columns=['hospitalization_id','recorded_dttm','vital_category','vital_value'],
                    filters={'hospitalization_id':hosp_list,
                                'vital_category': vital_cats})
    vitals = pyCLIF.convert_datetime_columns_to_site_tz(vitals, pyCLIF.helper['timezone'])
    vitals['vital_value'] = pd.to_numeric(vitals['vital_value'], errors='coerce')
    logger.info("Loaded %d vitals rows", len(vitals))
    vital_thresholds = {
    'map': (30, float('inf')),    # MAP ≥ 30
    'spo2': (60, 100),           # SpO2 ≥ 60 and ≤ 100 (adding upper limit for physiological range)
    }
    # Extract weight measurements into separate dataframe
    vitals_weights = vitals[vitals['vital_category'] == 'weight_kg'].copy()

    # Remove weight measurements from main vitals dataframe 
    vitals = vitals[vitals['vital_category'] != 'weight_kg']

    # Remove weight from vital_thresholds since it's now separate
    # Apply all thresholds at once
    mask = False
    for vital_cat, (min_val, max_val) in vital_thresholds.items():
        mask = mask | (
            (vitals['vital_category'] == vital_cat) & 
            (vitals['vital_value'] >= min_val) & 
            (vitals['vital_value'] <= max_val)
        )
    # Filter vitals and drop NAs
    vital_sub = vitals[mask].copy()
    vital_sub = vital_sub.dropna(subset=['vital_value', 'recorded_dttm'])
    vital_sub = vital_sub.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )

    vital_sub = vital_sub[
        (vital_sub['recorded_dttm'] >= vital_sub['start_dttm']) & 
        (vital_sub['recorded_dttm'] <= vital_sub['stop_dttm'])
    ]
    vital_summary = (
        vital_sub
        .groupby([id_col, 'vital_category'])['vital_value']
        .min()  # using min() for all vitals as we want the worst values
        .reset_index()
    ) 
    # Pivot the table
    vital_summary = vital_summary.pivot(
        index=id_col,
        columns='vital_category',
        values='vital_value'
    ).reset_index()
    # Add _min suffix to all columns except id_col
    vital_summary.columns = [col if col == id_col else f"{col}_min" for col in vital_summary.columns]
    logger.info("Created vitals summary")

    def calc_pao2_vectorized(spo2_series):
        # Create a mask for SpO2 < 97 (we'll use this later)
        valid_spo2_mask = (spo2_series < 97)
        
        # Convert to proportion (0-1)
        s = spo2_series / 100
        
        # Vectorized calculation
        a = 11700 / ((1 / s) - 1)
        b = np.sqrt((50**3) + (a**2))
        pao2 = ((b + a)**(1/3)) - ((b - a)**(1/3))
        
        # Apply masks:
        # 1. Set to NaN where SpO2 >= 97
        # 2. Set to NaN where SpO2 = 100 (to avoid division by zero)
        pao2 = np.where(
            (valid_spo2_mask) & (spo2_series != 100),
            pao2,
            np.nan
        )
        return pao2

    # Add the calculated column
    vital_summary['pao2_imputed_min'] = calc_pao2_vectorized(vital_summary['spo2_min'])

    logger.info("Imputed pao2 from spo2")

    #######################################################################
    # 3. Assessments – GCS total
    #######################################################################
    gcs = pyCLIF.load_data('clif_patient_assessments',
                    columns=['hospitalization_id','recorded_dttm','assessment_category','numerical_value'],
                    filters={'hospitalization_id':hosp_list,
                             'assessment_category':['gcs_total']})
    gcs = pyCLIF.convert_datetime_columns_to_site_tz(gcs, pyCLIF.helper['timezone'])
    gcs['numerical_value'] = pd.to_numeric(gcs['numerical_value'], errors='coerce')
    logger.info("Loaded %d GCS rows", len(gcs))

    # 2. Filter gcs within start and stop times
    # Merge with ids to get start and stop times
    gcs_sub = gcs.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )
    # Filter labs within time window
    gcs_sub = gcs_sub[
        (gcs_sub['recorded_dttm'] >= gcs_sub['start_dttm']) & 
        (gcs_sub['recorded_dttm'] <= gcs_sub['stop_dttm'])
    ]
    gcs_summary = (
        gcs_sub
        .groupby([id_col, 'assessment_category'])['numerical_value']
        .min()  # using min() for gcs as we want the worst values
        .reset_index()
    ) 
    # Pivot the table
    gcs_summary = gcs_summary.pivot(
        index=id_col,
        columns='assessment_category',
        values='numerical_value'
    ).reset_index()

    gcs_summary = gcs_summary.rename(columns={
        "gcs_total": "min_gcs_score"
    }) 

    #######################################################################
    # 4. Meds – vasoactives
    #######################################################################
    vp_meds = ["norepinephrine","epinephrine","phenylephrine","vasopressin",
               "dopamine","angiotensin","dobutamine","milrinone"]
    meds = pyCLIF.load_data('clif_medication_admin_continuous',
                     columns=['hospitalization_id','admin_dttm','med_category',
                              'med_dose','med_dose_unit'],
                     filters={'hospitalization_id':hosp_list,
                              'med_category': vp_meds})
    meds = pyCLIF.convert_datetime_columns_to_site_tz(meds, pyCLIF.helper['timezone'])
    logger.info("Loaded %d med rows", len(meds))

    # get weights from vitals for dose conversion
    vitals_weights = vitals_weights.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )
    vitals_weights = vitals_weights[
        (vitals_weights['recorded_dttm'] >= vitals_weights['start_dttm']) & 
        (vitals_weights['recorded_dttm'] <= vitals_weights['stop_dttm'])
    ]
    vitals_weights_summary = (
        vitals_weights
        .groupby([id_col, 'vital_category'])['vital_value']
        .first()  # keep first non-NA value
        .reset_index()
    ) 
    vitals_weights_pivot = vitals_weights_summary.pivot(index=id_col, 
                                    columns='vital_category', 
                                    values='vital_value'
                                    ).reset_index()
    meds_sub = meds.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )
    meds_sub = meds_sub[
        (meds_sub['admin_dttm'] >= meds_sub['start_dttm']) & 
        (meds_sub['admin_dttm'] <= meds_sub['stop_dttm'])
    ]

    meds_sub = meds_sub.merge(vitals_weights_pivot[[id_col, 'weight_kg']], on=id_col, how='left')
    meds_sub = meds_sub[~meds_sub['weight_kg'].isnull()].copy()
    meds_sub['med_dose_converted'] = meds_sub.apply(pyCLIF.convert_dose, axis=1)
    # Drop rows with NaN in 'med_dose_converted' (unrecognized units)
    meds_sub = meds_sub[~meds_sub['med_dose_converted'].isnull()].copy()

    # Filter doses within acceptable ranges
    meds_sub = meds_sub[meds_sub.apply(pyCLIF.is_dose_within_range, axis=1, args=(outlier_cfg,))].copy()

    meds_summary = (
        meds_sub
        .groupby([id_col, 'med_category'])['med_dose_converted']
        .max()  # using max() for all vasopressor as we want the worst values
        .reset_index()
    ) 
    # Pivot the table
    meds_summary = meds_summary.pivot(
        index=id_col,
        columns='med_category',
        values='med_dose_converted'
    ).reset_index()
    # Add _min suffix to all columns except id_col
    # meds_summary.columns = [col if col == id_col else f"{col}_max" for col in meds_summary.columns]

    #######################################################################
    # 5. Respiratory table (for vent support flag)
    #######################################################################
    resp = pyCLIF.load_data('clif_respiratory_support',
                        columns=['hospitalization_id','recorded_dttm','device_category','device_name','mode_name',
                                'mode_category','peep_set','fio2_set','lpm_set','resp_rate_set','tracheostomy',
                                'resp_rate_obs','tidal_volume_set'],
                        filters={'hospitalization_id':hosp_list})
    resp = pyCLIF.convert_datetime_columns_to_site_tz(resp, pyCLIF.helper['timezone'])
    logger.info("Loaded %d resp rows", len(resp))
    resp = resp.merge(
        ids, 
        on='hospitalization_id', 
        how='inner'
    )
    resp = resp[
        (resp['recorded_dttm'] >= resp['start_dttm']) & 
        (resp['recorded_dttm'] <= resp['stop_dttm'])
    ]
    resp['fio2_set'] = pd.to_numeric(resp['fio2_set'], errors='coerce')
    resp['lpm_set'] = pd.to_numeric(resp['lpm_set'], errors='coerce')
    resp['peep_set'] = pd.to_numeric(resp['peep_set'], errors='coerce')
    resp['tidal_volume_set'] = pd.to_numeric(resp['tidal_volume_set'], errors='coerce')
    resp['resp_rate_set'] = pd.to_numeric(resp['resp_rate_set'], errors='coerce')
    resp['resp_rate_obs'] = pd.to_numeric(resp['resp_rate_obs'], errors='coerce')

    resp['device_category'] = resp['device_category'].str.lower()
    resp['mode_category'] = resp['mode_category'].str.lower()
    fio2_mean = resp['fio2_set'].mean(skipna=True)

    # If the mean is greater than 1, divide 'fio2_set' by 100
    if fio2_mean and fio2_mean > 1.0:
        # Only divide values greater than 1 to avoid re-dividing already correct values
        resp.loc[resp['fio2_set'] > 1, 'fio2_set'] = \
            resp.loc[resp['fio2_set'] > 1, 'fio2_set'] / 100
        print("Updated fio2_set to be between 0.21 and 1")
    else:
        print("FIO2_SET mean=", fio2_mean, "is within the required range")

    pyCLIF.apply_outlier_thresholds(resp, 'fio2_set', *outlier_cfg['fio2_set'])
    pyCLIF.apply_outlier_thresholds(resp, 'peep_set', *outlier_cfg['peep_set'])
    pyCLIF.apply_outlier_thresholds(resp, 'lpm_set',  *outlier_cfg['lpm_set'])

    # processed_resp_support = pyCLIF.process_resp_support(resp)
    # resp_new = processed_resp_support.drop_duplicates()
    resp_new = resp.drop_duplicates()

    # Apply device categorization row by row using apply
    resp_new['device_category'] = resp_new.apply(pyCLIF.categorize_device, axis=1)
    resp_new['fio2_combined'] = resp_new.apply(pyCLIF.refill_fio2, axis=1)
    resp_new= resp_new[[id_col, 'hospitalization_id', 'recorded_dttm', 'mode_category', 
                        'device_category', 'fio2_combined', 'fio2_set', 'tidal_volume_set', 'peep_set', 'lpm_set']]
    # Define device ranking
    device_rank_dict = {
        'imv': 1,
        'nippv': 2,
        'cpap': 3,
        'high flow nc': 4,
        'face mask': 5,
        'trach collar': 6,
        'nasal cannula': 7,
        'other': 8,
        'room air': 9
    }

    # Apply device ranking
    resp_new['device_rank'] = resp_new['device_category'].map(device_rank_dict)

    # Aggregate to get worst FiO2 and device per hospitalization
    resp_summary = resp_new.groupby(id_col).agg(
        device_rank_min=('device_rank', lambda x: np.nan if x.isna().all() else x.min(skipna=True)),
        fio2_max=('fio2_combined', lambda x: np.nan if x.isna().all() else x.max(skipna=True))
    ).reset_index()

    # Map device ranks back to categories
    reverse_device_rank = {v: k for k, v in device_rank_dict.items()}
    resp_summary['resp_support_max'] = resp_summary['device_rank_min'].map(reverse_device_rank)

    # Final columns selection
    resp_summary = resp_summary[[id_col, 'fio2_max', 'resp_support_max']]

    #######################################################################
    # 7. Merge Tables 
    #######################################################################

    # Define tables to merge
    tables = {
        'resp': resp_summary, 
        'vitals': vital_summary,
        'labs': lab_summary,
        'rass_gcs': gcs_summary,
        'meds': meds_summary
    }

    # Start with resp_summary as base
    merged_df = resp_summary

    # Merge each table one by one using left join
    for table_name, table in tables.items():
        if table_name != 'resp':  # Skip resp as it's our base
            merged_df = merged_df.merge(
                table,
                on=id_col,
                how='left'
            )

    #######################################################################
    # 8. SOFA score calculation
    #######################################################################
    merged_df['p_f'] = np.where((merged_df['fio2_max'].notna()) & (merged_df['fio2_max'] != 0) & (merged_df['po2_arterial_min'].notna()),
                          merged_df['po2_arterial_min'] / merged_df['fio2_max'], np.nan)

    merged_df['p_f_imputed'] = np.where((merged_df['fio2_max'].notna()) & (merged_df['fio2_max'] != 0) & (merged_df['pao2_imputed_min'].notna()),
                                    merged_df['pao2_imputed_min'] / merged_df['fio2_max'], np.nan)

    merged_df['s_f'] = np.where((merged_df['fio2_max'].notna()) & (merged_df['fio2_max'] != 0) & (merged_df['spo2_min'].notna()),
                            merged_df['spo2_min'] / merged_df['fio2_max'], np.nan)
    
    print("Missing ratio of p_f (po2_arterial_min / fio2_max): ", merged_df.p_f.isna().sum()/merged_df.shape[0])
    print("Missing ratio of p_f_imputed (pao2_imputed_min / fio2_max): ", merged_df.p_f_imputed.isna().sum()/merged_df.shape[0])
    print("Missing ratio of s_f (spo2_min / fio2_max):", merged_df.s_f.isna().sum()/merged_df.shape[0])

    print(f"\nMost of the missing values in p_f_imputed are caused by pao2_imputed_min, which is set to NaN when spo2>97")

    sofa_df = merged_df.copy()

    #######################################################################
    ######################     SOFA CV  ###################################
    #######################################################################
    # Condition and corresponsding values
    conditions = [
        (sofa_df['dopamine'] > 15) | (sofa_df['epinephrine'] > 0.1) | (sofa_df['norepinephrine'] > 0.1),  #4
        (sofa_df['dopamine'] > 5) | ((sofa_df['epinephrine'] <= 0.1) & (sofa_df['epinephrine'] > 0)) | ((sofa_df['norepinephrine'] <= 0.1) & (sofa_df['norepinephrine'] > 0)),  #3
        ((sofa_df['dopamine'] <= 5) & (sofa_df['dopamine'] > 0)) | (sofa_df['dobutamine'] > 0),  #2
        (sofa_df['map_min'] < 70) #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_cv_97'] = np.select(conditions, values, default=0)

    #######################################################################
    ######################     SOFA COAG  #################################
    #######################################################################

        # Condition and corresponsding values
    conditions = [
        sofa_df['platelet_count_min'] < 20, #4
        (sofa_df['platelet_count_min'] < 50) & (sofa_df['platelet_count_min'] >= 20), #3
        (sofa_df['platelet_count_min'] < 100) & (sofa_df['platelet_count_min'] >= 50), #2
        (sofa_df['platelet_count_min'] < 150) & (sofa_df['platelet_count_min'] >= 100), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_coag'] = np.select(conditions, values, default=0)

    #######################################################################
    ######################     SOFA LIVER  ################################
    #######################################################################

    # Condition and corresponsding values
    conditions = [
        sofa_df['bilirubin_total_max'] >= 12, #4
        (sofa_df['bilirubin_total_max'] >= 6 ) & (sofa_df['bilirubin_total_max'] < 12), #3
        (sofa_df['bilirubin_total_max'] >= 2) & (sofa_df['bilirubin_total_max'] < 6), #2
        (sofa_df['bilirubin_total_max'] >= 1.2) & (sofa_df['bilirubin_total_max'] < 2), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_liver'] = np.select(conditions, values, default=0)

    #######################################################################
    ######################     SOFA RESP  #################################
    #######################################################################


    conditions = [
        (sofa_df['p_f'] < 100) & sofa_df['resp_support_max'].isin(["nippv", "cpap", "imv"]), #4
        (sofa_df['p_f'] < 200) & (sofa_df['p_f'] >= 100) & sofa_df['resp_support_max'].isin(["nippv", "cpap", "imv"]), #3
        (sofa_df['p_f'] < 300) & (sofa_df['p_f'] >= 200), #2
        (sofa_df['p_f'] < 400) & (sofa_df['p_f'] >= 300), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_resp_pf'] = np.select(conditions, values, default=0)


    conditions = [
        (sofa_df['p_f_imputed'] < 100) & sofa_df['resp_support_max'].isin(["nippv", "cpap", "imv"]), #4
        (sofa_df['p_f_imputed'] < 200) & (sofa_df['p_f_imputed'] >= 100) & sofa_df['resp_support_max'].isin(["nippv", "cpap", "imv"]), #3
        (sofa_df['p_f_imputed'] < 300) & (sofa_df['p_f_imputed'] >= 200), #2
        (sofa_df['p_f_imputed'] < 400) & (sofa_df['p_f_imputed'] >= 300), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_resp_pf_imp'] = np.select(conditions, values, default=0)

    # if both column is NaN return Nan
    sofa_df['sofa_resp'] = sofa_df.apply(lambda x: np.nanmax([x['sofa_resp_pf'], x['sofa_resp_pf_imp']]), axis=1)

    #######################################################################
    ######################     SOFA CNS  ##################################
    #######################################################################

    conditions = [
        (sofa_df['min_gcs_score'] < 6), #4
        (sofa_df['min_gcs_score'] >= 6) & (sofa_df['min_gcs_score'] <= 9), #3
        (sofa_df['min_gcs_score'] >= 10) & (sofa_df['min_gcs_score'] <= 12), #2
        (sofa_df['min_gcs_score'] >= 13) & (sofa_df['min_gcs_score'] <= 14), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_cns'] = np.select(conditions, values, default=0)

    #######################################################################
    ######################     SOFA RENAL  ################################
    #######################################################################

    conditions = [
        sofa_df['creatinine_max'] >= 5, #4
        (sofa_df['creatinine_max'] >= 3.5) & (sofa_df['creatinine_max'] < 5), #3
        (sofa_df['creatinine_max'] >= 2) & (sofa_df['creatinine_max'] < 3.5), #2
        (sofa_df['creatinine_max'] >= 1.2) & (sofa_df['creatinine_max'] < 2), #1
    ]

    values = [4, 3, 2, 1]

    # default 0 if doesn't meet any conditions
    sofa_df['sofa_renal'] = np.select(conditions, values, default=0)


    #######################################################################
    #  CRRT THERAPY table (for CRRT FLAG)
    #######################################################################


    try:
        crrt = pyCLIF.load_data('clif_crrt_therapy')
        crrt['hospitalization_id'] = crrt['hospitalization_id'].astype(str)
        crrt = pyCLIF.convert_datetime_columns_to_site_tz(crrt, pyCLIF.helper['timezone'])
        logger.info("Loaded %d CRRT rows", len(crrt))

        # Merge with ids to get start and stop times
        crrt_sub = crrt.merge(
            ids, 
            on='hospitalization_id', 
            how='inner'
        )
        # Filter labs within time window
        crrt_sub = crrt_sub[
            (crrt_sub['recorded_dttm'] >= (crrt_sub['start_dttm'] - pd.Timedelta(hours=72))) & 
            (crrt_sub['recorded_dttm'] <= crrt_sub['stop_dttm'])
        ]

        crrt_final = crrt_sub[[id_col]].drop_duplicates()
        crrt_final['crrt_flag'] = 1

        # Join CRRT flag with SOFA scores
        sofa_df = sofa_df.merge(
            crrt_final,
            on=id_col,
            how='left'
        )

        # Fill NaN CRRT flags with 0 
        sofa_df['crrt_flag'] = sofa_df['crrt_flag'].fillna(0)

        # Where CRRT flag is 1, set SOFA renal score to 4
        sofa_df.loc[sofa_df['crrt_flag'] == 1, 'sofa_renal'] = 4

    except Exception as e:
        logger.error("Error processing CRRT data: %s", str(e))
        sofa_df['crrt_flag'] = 0

    #############################################################

    sofa_columns = ['sofa_cv_97', 'sofa_coag', 'sofa_renal', 'sofa_liver', 'sofa_resp', 'sofa_cns']

    # treat NaN as 0
    sofa_df['sofa_total'] = sofa_df[sofa_columns].sum(axis=1, skipna=True)

    logger.info("Finished computing SOFA for %d ids", len(sofa_df))

    if output_filepath:
        sofa_df.to_parquet(output_filepath,index=False)
        logger.info("Wrote results to %s", output_filepath)
    return sofa_df
