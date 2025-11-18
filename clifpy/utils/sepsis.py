"""
Adult Sepsis Event (ASE) Calculation Utility

This module implements the CDC Adult Sepsis Event criteria for identifying
sepsis cases in hospitalized patients based on the CDC surveillance toolkit.

Reference: https://www.cdc.gov/sepsis/pdfs/sepsis-surveillance-toolkit-mar-2018_508.pdf

ASE requires BOTH:
A. Presumed Infection (blood culture + qualifying antibiotic days)
AND
B. Organ Dysfunction (vasopressors, mechanical ventilation, or lab criteria)
"""

import pandas as pd
from typing import Dict, Optional, List
import duckdb
import logging

# Set up logging - use centralized logger
logger = logging.getLogger('clifpy.utils.sepsis')


def _identify_presumed_infection(
    blood_cultures: pd.DataFrame,
    antibiotics: pd.DataFrame,
    hospitalization: pd.DataFrame,
    patient: Optional[pd.DataFrame] = None,
    window_days: int = 2
) -> pd.DataFrame:
    """
    Identify presumed infection based on CDC ASE criteria.
    
    Presumed infection requires:
    1. Blood culture obtained (any result)
    2. At least 4 Qualifying Antibiotic Days (QAD) within -2 to +6 days of blood culture
       OR 1+ QAD if patient died/transferred within 6 days
    
    Parameters:
        blood_cultures: DataFrame with columns [hospitalization_id, collect_dttm, fluid_category]
                       filtered to blood/buffy coat cultures
        antibiotics: DataFrame with columns [hospitalization_id, admin_dttm, med_group]
                    filtered to CMS_sepsis_qualifying_antibiotics
        hospitalization: DataFrame with columns [hospitalization_id, discharge_dttm, discharge_category]
        patient: Optional DataFrame with columns [patient_id, death_dttm] for censoring
        window_days: Number of days before/after blood culture to look (default: 2)
    
    Returns:
        DataFrame with columns [hospitalization_id, presumed_infection_time]
    """
    # Step 1: Get earliest blood culture per hospitalization within window
    bc_query = """
    SELECT 
        hospitalization_id,
        MIN(collect_dttm) as blood_culture_time
    FROM blood_cultures
    GROUP BY hospitalization_id
    """
    bc_df = duckdb.sql(bc_query).df()
    
    # Step 2: Calculate qualifying antibiotic days (QAD)
    # Relative day calculation: day 0 = blood culture day
    qad_query = f"""
    WITH antibiotic_days AS (
        SELECT 
            a.hospitalization_id,
            bc.blood_culture_time,
            a.admin_dttm,
            FLOOR(DATE_DIFF('hour', bc.blood_culture_time::TIMESTAMP, a.admin_dttm::TIMESTAMP) / 24.0) as relative_day
        FROM antibiotics a
        INNER JOIN bc_df bc ON a.hospitalization_id = bc.hospitalization_id
    ),
    filtered_days AS (
        SELECT 
            hospitalization_id,
            blood_culture_time,
            relative_day
        FROM antibiotic_days
        WHERE relative_day >= -{window_days} AND relative_day <= 6
    ),
    distinct_days AS (
        SELECT DISTINCT 
            hospitalization_id,
            blood_culture_time,
            relative_day
        FROM filtered_days
    ),
    consecutive_runs AS (
        SELECT 
            hospitalization_id,
            blood_culture_time,
            relative_day,
            relative_day - ROW_NUMBER() OVER (
                PARTITION BY hospitalization_id 
                ORDER BY relative_day
            ) as run_group
        FROM distinct_days
    ),
    run_lengths AS (
        SELECT 
            hospitalization_id,
            blood_culture_time,
            run_group,
            COUNT(*) as run_length,
            MAX(relative_day) as end_day
        FROM consecutive_runs
        GROUP BY hospitalization_id, blood_culture_time, run_group
    ),
    max_run_per_hosp AS (
        SELECT 
            hospitalization_id,
            blood_culture_time,
            MAX(run_length) as total_QAD
        FROM run_lengths
        GROUP BY hospitalization_id, blood_culture_time
    )
    SELECT 
        m.hospitalization_id,
        m.blood_culture_time,
        m.total_QAD,
        MAX(r.end_day) as last_qad_day
    FROM max_run_per_hosp m
    LEFT JOIN run_lengths r 
        ON m.hospitalization_id = r.hospitalization_id 
        AND m.blood_culture_time = r.blood_culture_time
        AND m.total_QAD = r.run_length
    GROUP BY m.hospitalization_id, m.blood_culture_time, m.total_QAD
    """
    qad_df = duckdb.sql(qad_query).df()
    
    # Step 3: Determine censoring time for early death/transfer
    if patient is not None:
        censoring_query = """
        SELECT 
            h.hospitalization_id,
            CASE 
                WHEN p.death_dttm IS NOT NULL 
                    AND p.death_dttm <= COALESCE(h.discharge_dttm, p.death_dttm) 
                    THEN p.death_dttm
                WHEN h.discharge_category IN ('Expired', 'Acute Care Hospital', 'Hospice')
                    THEN h.discharge_dttm
                ELSE NULL
            END as censoring_time
        FROM hospitalization h
        LEFT JOIN patient p ON h.patient_id = p.patient_id
        WHERE censoring_time IS NOT NULL
        """
        censoring_df = duckdb.sql(censoring_query).df()
    else:
        censoring_query = """
        SELECT 
            hospitalization_id,
            CASE 
                WHEN discharge_category IN ('Expired', 'Acute Care Hospital', 'Hospice')
                    THEN discharge_dttm
                ELSE NULL
            END as censoring_time
        FROM hospitalization
        WHERE censoring_time IS NOT NULL
        """
        censoring_df = duckdb.sql(censoring_query).df()
    
    # Step 4: Identify presumed infections
    presumed_query = """
    SELECT 
        q.hospitalization_id,
        q.blood_culture_time as presumed_infection_time,
        q.total_QAD
    FROM qad_df q
    LEFT JOIN censoring_df c ON q.hospitalization_id = c.hospitalization_id
    WHERE 
        q.total_QAD >= 4
        OR (q.total_QAD >= 1 AND c.censoring_time IS NOT NULL 
            AND c.censoring_time < q.blood_culture_time + INTERVAL 6 DAYS)
    """
    presumed_df = duckdb.sql(presumed_query).df()
    
    return presumed_df


def _identify_organ_dysfunction_vasopressors(
    continuous_meds: pd.DataFrame,
    presumed_infection: pd.DataFrame,
    window_days: int = 2
) -> pd.DataFrame:
    """
    Identify vasopressor initiation as organ dysfunction.
    
    Parameters:
        continuous_meds: DataFrame with [hospitalization_id, admin_dttm, med_category, med_dose]
                        filtered to vasoactive medications
        presumed_infection: DataFrame with [hospitalization_id, presumed_infection_time]
        window_days: Days before/after infection time (default: 2)
    
    Returns:
        DataFrame with [hospitalization_id, vasopressor_time]
    """
    query = f"""
    SELECT DISTINCT
        c.hospitalization_id,
        MIN(c.admin_dttm) as vasopressor_time
    FROM continuous_meds c
    INNER JOIN presumed_infection p ON c.hospitalization_id = p.hospitalization_id
    WHERE 
        c.med_dose > 0
        AND c.admin_dttm >= p.presumed_infection_time - INTERVAL {window_days} DAYS
        AND c.admin_dttm <= p.presumed_infection_time + INTERVAL {window_days} DAYS
    GROUP BY c.hospitalization_id
    """
    return duckdb.sql(query).df()


def _identify_organ_dysfunction_ventilation(
    respiratory_support: pd.DataFrame,
    presumed_infection: pd.DataFrame,
    window_days: int = 2
) -> pd.DataFrame:
    """
    Identify invasive mechanical ventilation as organ dysfunction.
    
    Parameters:
        respiratory_support: DataFrame with [hospitalization_id, recorded_dttm, device_category]
                           filtered to IMV
        presumed_infection: DataFrame with [hospitalization_id, presumed_infection_time]
        window_days: Days before/after infection time (default: 2)
    
    Returns:
        DataFrame with [hospitalization_id, imv_time]
    """
    query = f"""
    SELECT DISTINCT
        r.hospitalization_id,
        MIN(r.recorded_dttm) as imv_time
    FROM respiratory_support r
    INNER JOIN presumed_infection p ON r.hospitalization_id = p.hospitalization_id
    WHERE 
        r.recorded_dttm >= p.presumed_infection_time - INTERVAL {window_days} DAYS
        AND r.recorded_dttm <= p.presumed_infection_time + INTERVAL {window_days} DAYS
    GROUP BY r.hospitalization_id
    """
    return duckdb.sql(query).df()


def _identify_organ_dysfunction_labs(
    labs: pd.DataFrame,
    presumed_infection: pd.DataFrame,
    window_days: int = 2,
    include_lactate: bool = True
) -> pd.DataFrame:
    """
    Identify lab-based organ dysfunction criteria.
    
    Lab criteria:
    - Creatinine: Doubling from baseline
    - Bilirubin: ≥2.0 mg/dL AND doubled from baseline
    - Platelets: <100 AND ≥50% decline from baseline (baseline must be ≥100)
    - Lactate: ≥2.0 mmol/L (optional)
    
    Parameters:
        labs: DataFrame with [hospitalization_id, lab_category, lab_value_numeric, lab_result_dttm]
        presumed_infection: DataFrame with [hospitalization_id, presumed_infection_time]
        window_days: Days before/after infection time (default: 2)
        include_lactate: Whether to include lactate criterion (default: True)
    
    Returns:
        DataFrame with [hospitalization_id, aki_time, hyperbilirubinemia_time, 
                       thrombocytopenia_time, elevated_lactate_time]
    """
    # Calculate baselines - separate query for each lab type
    baseline_queries = {
        'creatinine': """
            SELECT 
                hospitalization_id,
                'creatinine' as lab_category,
                MIN(lab_value_numeric) as baseline_value
            FROM labs
            WHERE lab_category = 'creatinine'
                AND lab_value_numeric IS NOT NULL
            GROUP BY hospitalization_id
        """,
        'bilirubin_total': """
            SELECT 
                hospitalization_id,
                'bilirubin_total' as lab_category,
                MIN(lab_value_numeric) as baseline_value
            FROM labs
            WHERE lab_category = 'bilirubin_total'
                AND lab_value_numeric IS NOT NULL
            GROUP BY hospitalization_id
        """,
        'platelet_count': """
            SELECT 
                hospitalization_id,
                'platelet_count' as lab_category,
                FIRST(lab_value_numeric ORDER BY lab_result_dttm) as baseline_value
            FROM labs
            WHERE lab_category = 'platelet_count'
                AND lab_value_numeric IS NOT NULL
            GROUP BY hospitalization_id
        """
    }
    
    # Execute all baseline queries and combine
    baseline_dfs = []
    for lab_type, query in baseline_queries.items():
        df = duckdb.sql(query).df()
        if len(df) > 0:
            baseline_dfs.append(df)
    
    if len(baseline_dfs) == 0:
        # No baselines found, return empty
        return pd.DataFrame(columns=['hospitalization_id'])
    
    baselines = pd.concat(baseline_dfs, ignore_index=True)
    
    # Pivot baselines for easier joining
    if len(baselines) > 0:
        baseline_pivot = baselines.pivot(
            index='hospitalization_id',
            columns='lab_category',
            values='baseline_value'
        ).reset_index()
        
        # Ensure all expected columns exist
        expected_cols = ['hospitalization_id', 'bilirubin_total', 'creatinine', 'platelet_count']
        for col in expected_cols:
            if col not in baseline_pivot.columns:
                baseline_pivot[col] = None
        
        # Rename to standard names
        baseline_pivot = baseline_pivot.rename(columns={
            'bilirubin_total': 'baseline_bilirubin',
            'creatinine': 'baseline_creatinine',
            'platelet_count': 'baseline_platelet'
        })
        baseline_pivot = baseline_pivot[['hospitalization_id', 'baseline_bilirubin', 'baseline_creatinine', 'baseline_platelet']]
    else:
        # No baselines, return empty
        return pd.DataFrame(columns=['hospitalization_id'])
    
    # Identify organ dysfunction
    lactate_condition = """
        , CASE 
            WHEN lab_category = 'lactate' 
                AND lab_value_numeric >= 2
            THEN 1 ELSE 0 END AS elevated_lactate
    """ if include_lactate else ""
    
    dysfunction_query = f"""
    WITH lab_with_baseline AS (
        SELECT 
            l.hospitalization_id,
            l.lab_category,
            l.lab_value_numeric,
            l.lab_result_dttm,
            p.presumed_infection_time,
            b.baseline_creatinine,
            b.baseline_bilirubin,
            b.baseline_platelet
        FROM labs l
        INNER JOIN presumed_infection p ON l.hospitalization_id = p.hospitalization_id
        LEFT JOIN baseline_pivot b ON l.hospitalization_id = b.hospitalization_id
        WHERE l.lab_result_dttm >= p.presumed_infection_time - INTERVAL {window_days} DAYS
            AND l.lab_result_dttm <= p.presumed_infection_time + INTERVAL {window_days} DAYS
    )
    SELECT 
        hospitalization_id,
        CASE 
            WHEN lab_category = 'creatinine' 
                AND baseline_creatinine IS NOT NULL
                AND lab_value_numeric >= 2 * baseline_creatinine
            THEN 1 ELSE 0 END AS aki,
        CASE 
            WHEN lab_category = 'bilirubin_total'
                AND baseline_bilirubin IS NOT NULL
                AND lab_value_numeric >= 2.0
                AND lab_value_numeric >= 2 * baseline_bilirubin
            THEN 1 ELSE 0 END AS hyperbilirubinemia,
        CASE 
            WHEN lab_category = 'platelet_count'
                AND baseline_platelet >= 100
                AND lab_value_numeric < 100
                AND lab_value_numeric <= 0.5 * baseline_platelet
            THEN 1 ELSE 0 END AS thrombocytopenia
        {lactate_condition},
        lab_result_dttm
    FROM lab_with_baseline
    """
    
    dysfunction_df = duckdb.sql(dysfunction_query).df()
    
    # Get earliest time for each dysfunction type
    summary_cols = ['aki', 'hyperbilirubinemia', 'thrombocytopenia']
    if include_lactate:
        summary_cols.append('elevated_lactate')
    
    summary_query = f"""
    SELECT 
        hospitalization_id,
        MIN(CASE WHEN aki = 1 THEN lab_result_dttm END) as aki_time,
        MIN(CASE WHEN hyperbilirubinemia = 1 THEN lab_result_dttm END) as hyperbilirubinemia_time,
        MIN(CASE WHEN thrombocytopenia = 1 THEN lab_result_dttm END) as thrombocytopenia_time
        {"" if not include_lactate else ", MIN(CASE WHEN elevated_lactate = 1 THEN lab_result_dttm END) as elevated_lactate_time"}
    FROM dysfunction_df
    GROUP BY hospitalization_id
    HAVING MAX(aki) = 1 OR MAX(hyperbilirubinemia) = 1 OR MAX(thrombocytopenia) = 1
        {"" if not include_lactate else "OR MAX(elevated_lactate) = 1"}
    """
    
    return duckdb.sql(summary_query).df()


def compute_sepsis(
    blood_cultures: pd.DataFrame,
    antibiotics: pd.DataFrame,
    hospitalization: pd.DataFrame,
    labs: pd.DataFrame,
    continuous_meds: Optional[pd.DataFrame] = None,
    respiratory_support: Optional[pd.DataFrame] = None,
    patient: Optional[pd.DataFrame] = None,
    window_days: int = 2,
    include_lactate: bool = True
) -> pd.DataFrame:
    """
    Compute Adult Sepsis Event (ASE) flags using CDC criteria.
    
    ASE requires BOTH:
    A. Presumed Infection (blood culture + qualifying antibiotic days)
    AND
    B. Organ Dysfunction (at least one of: vasopressors, mechanical ventilation, labs)
    
    Parameters:
        blood_cultures: DataFrame with [hospitalization_id, collect_dttm, fluid_category]
                       Should be pre-filtered to fluid_category == 'blood/buffy coat'
        antibiotics: DataFrame with [hospitalization_id, admin_dttm, med_group]
                    Should be pre-filtered to med_group == 'CMS_sepsis_qualifying_antibiotics'
        hospitalization: DataFrame with [hospitalization_id, patient_id, discharge_dttm, discharge_category]
        labs: DataFrame with [hospitalization_id, lab_category, lab_value_numeric, lab_result_dttm]
             Should include: creatinine, bilirubin_total, platelet_count, lactate
        continuous_meds: Optional DataFrame with [hospitalization_id, admin_dttm, med_category, med_dose]
                        Should be pre-filtered to vasoactive medications
        respiratory_support: Optional DataFrame with [hospitalization_id, recorded_dttm, device_category]
                           Should be pre-filtered to device_category == 'IMV'
        patient: Optional DataFrame with [patient_id, death_dttm] for censoring logic
        window_days: Number of days before/after blood culture (default: 2)
        include_lactate: Whether to include lactate as organ dysfunction criterion (default: True)
    
    Returns:
        DataFrame with columns:
        - hospitalization_id
        - ase_flag: 1 if sepsis criteria met, 0 otherwise
        - presumed_infection_time: Time of blood culture
        - first_organ_dysfunction_time: Earliest organ dysfunction time
        - organ_dysfunction_type: Type of first organ dysfunction
        - vasopressor_time, imv_time, aki_time, etc. (individual dysfunction times)
    
    Example:
        >>> sepsis_df = compute_sepsis(
        ...     blood_cultures=bc_df,
        ...     antibiotics=abx_df,
        ...     hospitalization=hosp_df,
        ...     labs=labs_df,
        ...     continuous_meds=meds_df,
        ...     respiratory_support=resp_df
        ... )
    """
    logger.info("Computing Adult Sepsis Event (ASE) criteria")
    
    # Step 1: Identify presumed infection
    logger.info("Identifying presumed infections...")
    presumed_infection = _identify_presumed_infection(
        blood_cultures=blood_cultures,
        antibiotics=antibiotics,
        hospitalization=hospitalization,
        patient=patient,
        window_days=window_days
    )
    
    if len(presumed_infection) == 0:
        logger.warning("No presumed infections identified")
        return pd.DataFrame(columns=[
            'hospitalization_id', 'ase_flag', 'presumed_infection_time',
            'first_organ_dysfunction_time', 'organ_dysfunction_type'
        ])
    
    logger.info(f"Found {len(presumed_infection)} presumed infections")
    
    # Step 2: Identify organ dysfunction components
    organ_dysfunction_dfs = []
    
    # Vasopressors
    if continuous_meds is not None and len(continuous_meds) > 0:
        logger.info("Checking vasopressor criteria...")
        vaso_df = _identify_organ_dysfunction_vasopressors(
            continuous_meds, presumed_infection, window_days
        )
        if len(vaso_df) > 0:
            vaso_df['dysfunction_type'] = 'vasopressor'
            vaso_df = vaso_df.rename(columns={'vasopressor_time': 'dysfunction_time'})
            organ_dysfunction_dfs.append(vaso_df)
            logger.info(f"Found {len(vaso_df)} with vasopressor dysfunction")
    
    # Mechanical Ventilation
    if respiratory_support is not None and len(respiratory_support) > 0:
        logger.info("Checking mechanical ventilation criteria...")
        imv_df = _identify_organ_dysfunction_ventilation(
            respiratory_support, presumed_infection, window_days
        )
        if len(imv_df) > 0:
            imv_df['dysfunction_type'] = 'invasive_mechanical_ventilation'
            imv_df = imv_df.rename(columns={'imv_time': 'dysfunction_time'})
            organ_dysfunction_dfs.append(imv_df)
            logger.info(f"Found {len(imv_df)} with IMV dysfunction")
    
    # Lab-based organ dysfunction
    logger.info("Checking lab-based organ dysfunction criteria...")
    lab_dysfunction = _identify_organ_dysfunction_labs(
        labs, presumed_infection, window_days, include_lactate
    )
    
    # Convert lab dysfunction to long format
    if len(lab_dysfunction) > 0:
        lab_types = {
            'aki_time': 'aki',
            'hyperbilirubinemia_time': 'hyperbilirubinemia',
            'thrombocytopenia_time': 'thrombocytopenia'
        }
        if include_lactate:
            lab_types['elevated_lactate_time'] = 'elevated_lactate'
        
        for time_col, dysfunction_name in lab_types.items():
            if time_col in lab_dysfunction.columns:
                temp_df = lab_dysfunction[['hospitalization_id', time_col]].dropna()
                if len(temp_df) > 0:
                    temp_df['dysfunction_type'] = dysfunction_name
                    temp_df = temp_df.rename(columns={time_col: 'dysfunction_time'})
                    organ_dysfunction_dfs.append(temp_df)
                    logger.info(f"Found {len(temp_df)} with {dysfunction_name} dysfunction")
    
    # Step 3: Combine all organ dysfunction
    if len(organ_dysfunction_dfs) == 0:
        logger.warning("No organ dysfunction criteria met")
        # Return presumed infections without ASE flag
        result = presumed_infection.copy()
        result['ase_flag'] = 0
        result['first_organ_dysfunction_time'] = None
        result['organ_dysfunction_type'] = None
        return result
    
    all_dysfunction = pd.concat(organ_dysfunction_dfs, ignore_index=True)
    
    # Find earliest dysfunction per hospitalization
    earliest_dysfunction = all_dysfunction.loc[
        all_dysfunction.groupby('hospitalization_id')['dysfunction_time'].idxmin()
    ][['hospitalization_id', 'dysfunction_time', 'dysfunction_type']]
    
    # Step 4: Create final ASE dataset
    ase_query = """
    SELECT 
        p.hospitalization_id,
        1 as ase_flag,
        p.presumed_infection_time,
        d.dysfunction_time as first_organ_dysfunction_time,
        d.dysfunction_type as organ_dysfunction_type
    FROM presumed_infection p
    INNER JOIN earliest_dysfunction d ON p.hospitalization_id = d.hospitalization_id
    """
    ase_df = duckdb.sql(ase_query).df()
    
    logger.info(f"Identified {len(ase_df)} Adult Sepsis Events")
    
    return ase_df
