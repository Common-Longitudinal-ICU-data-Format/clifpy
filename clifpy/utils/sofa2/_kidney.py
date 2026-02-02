"""Kidney subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec, creatinine in mg/dL):
- Creatinine ≤ 1.20 mg/dL: 0 points
- Creatinine > 1.20 mg/dL: 1 point
- Creatinine > 2.0 mg/dL: 2 points
- Creatinine > 3.50 mg/dL: 3 points
- RRT or meets RRT criteria: 4 points

Special rules:
- Footnote p: Score 4 if patient meets RRT criteria even without RRT
- RRT criteria: creatinine > 1.2 AND (potassium >= 6.0 OR (pH <= 7.20 AND bicarbonate <= 12.0))
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config, _flag_rrt

# Lab categories for kidney subscore
KIDNEY_LAB_CATEGORIES = ['creatinine', 'potassium', 'ph_arterial', 'ph_venous', 'bicarbonate']


def _calculate_kidney_subscore(
    cohort_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    crrt_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    include_intermediates: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 kidney subscore based on creatinine with RRT override.

    Implements pre-window lookback with fallback pattern for ALL kidney labs
    (creatinine, potassium, pH, bicarbonate).
    Also implements footnote p: RRT criteria fallback for patients meeting criteria.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs)
    crrt_rel : DuckDBPyRelation
        CRRT therapy table (CLIF crrt_therapy)
    cfg : SOFA2Config
        Configuration with kidney_lookback_hours
    include_intermediates : bool, default False
        If True, return (result, intermediates_dict) for QA

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, creatinine, has_rrt, rrt_criteria_met, kidney]
        Where kidney is the subscore (0-4) or NULL if no creatinine data
    """
    lookback_hours = cfg.kidney_lookback_hours
    lab_categories = KIDNEY_LAB_CATEGORIES

    # Step 1: Get RRT flag (in-window only)
    rrt_flag = _flag_rrt(cohort_rel, crrt_rel)

    # Step 2: Get in-window lab values for kidney
    # MAX creatinine/potassium = worst, MIN pH/bicarbonate = worst
    labs_in_window = duckdb.sql(f"""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , MAX(lab_value_numeric) FILTER(lab_category = 'creatinine') AS creatinine
            , MAX(lab_value_numeric) FILTER(lab_category = 'potassium') AS potassium
            , MIN(lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) AS ph_min
            , MIN(lab_value_numeric) FILTER(lab_category = 'bicarbonate') AS bicarbonate
        WHERE t.lab_category IN {tuple(lab_categories)}
        GROUP BY t.hospitalization_id, c.start_dttm
    """)

    # Step 3: Get pre-window lab values using CROSS JOIN + ASOF JOIN pattern
    # This is more compact than separate ASOF JOINs per lab category
    labs_pre_window_long = duckdb.sql(f"""
        WITH lab_cats AS (
            SELECT UNNEST({lab_categories}::VARCHAR[]) AS lab_category
        ),
        cohort_labs AS (
            FROM cohort_rel c
            CROSS JOIN lab_cats
            SELECT c.hospitalization_id, c.start_dttm, lab_category
        )
        FROM cohort_labs cl
        ASOF LEFT JOIN labs_rel t
            ON cl.hospitalization_id = t.hospitalization_id
            AND cl.lab_category = t.lab_category
            AND cl.start_dttm > t.lab_collect_dttm
        SELECT
            cl.hospitalization_id
            , cl.start_dttm
            , cl.lab_category
            , t.lab_value_numeric AS lab_value
            , cl.start_dttm - t.lab_collect_dttm AS time_gap
        WHERE t.hospitalization_id IS NOT NULL
            AND time_gap <= INTERVAL '{lookback_hours} hours'
    """)

    # Step 4: Pivot pre-window labs to wide format
    # Combine ph_arterial and ph_venous into ph_min (take the value we got, since ASOF returns one)
    labs_pre_window = duckdb.sql("""
        PIVOT labs_pre_window_long
        ON lab_category
        USING ANY_VALUE(lab_value)        
        GROUP BY hospitalization_id, start_dtt
    """)

    # Step 5: Apply fallback pattern for ALL labs
    # Use pre-window value only if no in-window value exists for that specific lab
    labs_with_fallback = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN labs_in_window l USING (hospitalization_id, start_dttm)
        LEFT JOIN labs_pre_window p USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            -- Apply fallback per-lab: use pre-window only if in-window is NULL
            , COALESCE(l.creatinine, p.creatinine) AS creatinine
            , COALESCE(l.potassium, p.potassium) AS potassium
            , COALESCE(l.ph_min, LEAST(p.ph_arterial, p.ph_venous)) AS ph_min
            , COALESCE(l.bicarbonate, p.bicarbonate) AS bicarbonate
    """)

    # Step 6: Calculate subscore with RRT override and RRT criteria fallback
    kidney_score = duckdb.sql("""
        FROM labs_with_fallback l
        LEFT JOIN rrt_flag r USING (hospitalization_id, start_dttm)
        SELECT
            l.hospitalization_id
            , l.start_dttm
            , l.creatinine
            , l.potassium
            , l.ph_min
            , l.bicarbonate
            , COALESCE(r.has_rrt, 0) AS has_rrt
            -- Footnote p: RRT criteria met (window-level, no concurrency check)
            , rrt_criteria_met: CASE
                WHEN l.creatinine > 1.2
                     AND (l.potassium >= 6.0
                          OR (l.ph_min <= 7.20 AND l.bicarbonate <= 12.0))
                THEN 1 ELSE 0 END
            , kidney: CASE
                WHEN r.has_rrt = 1 THEN 4
                WHEN rrt_criteria_met = 1 THEN 4  -- Footnote p fallback
                WHEN l.creatinine > 3.50 THEN 3
                WHEN l.creatinine > 2.0 THEN 2
                WHEN l.creatinine > 1.20 THEN 1
                WHEN l.creatinine <= 1.20 THEN 0
                ELSE NULL
            END
    """)

    if include_intermediates:
        return kidney_score, {
            'rrt_flag': rrt_flag,
            'labs_in_window': labs_in_window,
            'labs_pre_window_long': labs_pre_window_long,
            'labs_pre_window': labs_pre_window,
            'labs_with_fallback': labs_with_fallback,
        }

    return kidney_score
