"""Shared utilities and configuration for SOFA-2 scoring.

This module contains:
- SOFA2Config: Configuration dataclass for customizing calculation parameters
- Aggregation queries for in-window data (no lookback)
- Flag queries for RRT, delirium drug detection
"""

from dataclasses import dataclass

import duckdb
from duckdb import DuckDBPyRelation


@dataclass
class SOFA2Config:
    """
    Configuration for SOFA-2 calculation parameters.

    All time-based parameters can be customized for different clinical contexts.
    Defaults are based on the SOFA-2 specification.

    Attributes
    ----------
    resp_lookback_hours : float
        Pre-window lookback for respiratory data (FiO2, PaO2, SpO2). Default 6.0.
    liver_lookback_hours : float
        Pre-window lookback for bilirubin. Default 24.0.
    kidney_lookback_hours : float
        Pre-window lookback for creatinine. Default 12.0.
    hemo_lookback_hours : float
        Pre-window lookback for platelets. Default 12.0.
    pressor_min_duration_minutes : int
        Minimum vasopressor infusion duration to count (footnote j). Default 60.
    pf_sf_tolerance_hours : float
        Maximum time gap between O2 measurement and FiO2 for concurrent ratio. Default 4.0.
    include_timestamps : bool
        If True, include ARGMAX/ARGMIN timestamps for determining measurements. Default False.
    """

    # Pre-window lookback (hours) - per subscore type
    resp_lookback_hours: float = 6.0
    liver_lookback_hours: float = 24.0
    kidney_lookback_hours: float = 12.0
    hemo_lookback_hours: float = 12.0

    # CV subscore
    pressor_min_duration_minutes: int = 60

    # Resp subscore
    pf_sf_tolerance_hours: float = 4.0

    # QA options
    include_timestamps: bool = False


# =============================================================================
# Aggregation Queries (in-window only, no lookback)
# =============================================================================

def _agg_labs(cohort_rel: DuckDBPyRelation, labs_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Aggregate lab values within each scoring window.

    Returns one row per (hospitalization_id, start_dttm) with:

    - platelet_count (MIN - worse when lower)

    - po2_arterial (MIN)

    - creatinine (MAX - worse when higher)

    - bilirubin_total (MAX)

    - potassium, ph_min, bicarbonate (for RRT criteria fallback, footnote p)

    Each lab value includes a corresponding `*_dttm_offset` column (interval from start_dttm).
    Positive offset = in-window measurement.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs)

    Returns
    -------
    DuckDBPyRelation
        Aggregated lab values per window with offset timestamps
    """
    # Offset = lab_collect_dttm - start_dttm (positive for in-window)
    return duckdb.sql("""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm  -- window identity
            -- MIN aggregations (worse = lower) with offsets
            , MIN(lab_value_numeric) FILTER(lab_category = 'platelet_count') AS platelet_count
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'platelet_count') - c.start_dttm AS platelet_dttm_offset
            , MIN(lab_value_numeric) FILTER(lab_category = 'po2_arterial') AS po2_arterial
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'po2_arterial') - c.start_dttm AS po2_arterial_dttm_offset
            -- MAX aggregations (worse = higher) with offsets
            , MAX(lab_value_numeric) FILTER(lab_category = 'creatinine') AS creatinine
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'creatinine') - c.start_dttm AS creatinine_dttm_offset
            , MAX(lab_value_numeric) FILTER(lab_category = 'bilirubin_total') AS bilirubin_total
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'bilirubin_total') - c.start_dttm AS bilirubin_dttm_offset
            -- Footnote p: RRT criteria fallback labs with offsets
            , MAX(lab_value_numeric) FILTER(lab_category = 'potassium') AS potassium
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'potassium') - c.start_dttm AS potassium_dttm_offset
            , MIN(lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) AS ph_min
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) - c.start_dttm AS ph_dttm_offset
            , MIN(lab_value_numeric) FILTER(lab_category = 'bicarbonate') AS bicarbonate
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'bicarbonate') - c.start_dttm AS bicarbonate_dttm_offset
        WHERE t.lab_category IN (
            'platelet_count', 'creatinine', 'bilirubin_total', 'po2_arterial',
            'potassium', 'ph_arterial', 'ph_venous', 'bicarbonate'
        )
        GROUP BY t.hospitalization_id, c.start_dttm
    """)


def _agg_gcs(cohort_rel: DuckDBPyRelation, assessments_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Aggregate GCS (min) within each scoring window for brain subscore.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    assessments_rel : DuckDBPyRelation
        Patient assessments table (CLIF patient_assessments)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, gcs_min]
    """
    return duckdb.sql("""
        FROM assessments_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm  -- window identity
            , MIN(numerical_value) AS gcs_min
        WHERE assessment_category = 'gcs_total'
        GROUP BY t.hospitalization_id, c.start_dttm
    """)


def _agg_map(cohort_rel: DuckDBPyRelation, vitals_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Aggregate MAP (min) within each scoring window for CV subscore.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    vitals_rel : DuckDBPyRelation
        Vitals table (CLIF vitals)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, map_min, map_min_dttm_offset]
        Offset is interval from start_dttm (always positive for in-window)
    """
    return duckdb.sql("""
        FROM vitals_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm  -- window identity
            , MIN(vital_value) AS map_min
            , ARG_MIN(t.recorded_dttm, vital_value) - c.start_dttm AS map_min_dttm_offset
        WHERE vital_category = 'map'
        GROUP BY t.hospitalization_id, c.start_dttm
    """)


# =============================================================================
# Flag Queries
# =============================================================================

def _flag_rrt(cohort_rel: DuckDBPyRelation, crrt_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Detect RRT administration during each scoring window for kidney subscore.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    crrt_rel : DuckDBPyRelation
        CRRT therapy table (CLIF crrt_therapy)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, has_rrt]
        Only includes rows where RRT was detected (has_rrt = 1)
    """
    return duckdb.sql("""
        FROM crrt_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT
            t.hospitalization_id
            , c.start_dttm
            , 1 AS has_rrt
    """)


def _flag_delirium_drug(cohort_rel: DuckDBPyRelation, meds_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Detect delirium drug administration during each scoring window.

    Per footnote e: dexmedetomidine infusion -> brain subscore minimum 1 point.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, has_delirium_drug]
        Only includes rows where delirium drug was detected (has_delirium_drug = 1)
    """
    return duckdb.sql("""
        FROM meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT DISTINCT
            t.hospitalization_id
            , c.start_dttm  -- window identity
            , 1 AS has_delirium_drug
        WHERE t.med_category = 'dexmedetomidine'
            AND t.med_dose > 0  -- actively infusing
    """)


# =============================================================================
# Window Expansion
# =============================================================================

def _expand_to_daily_windows(cohort_rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """
    Expand arbitrary duration windows into complete 24-hour periods.

    Takes input windows of any duration >= 24 hours and breaks them into
    complete 24-hour chunks. Partial days at the end are dropped.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm].
        Windows can be any duration >= 24 hours.

    Returns
    -------
    DuckDBPyRelation
        Expanded cohort with one row per complete 24-hour period:

        - hospitalization_id

        - start_dttm (start of the 24h period)

        - end_dttm (end of the 24h period)

        - nth_day (1-indexed: 1, 2, 3, ...)

    Notes
    -----
    - Windows < 24 hours produce no output rows

    - Partial days at the end are dropped

    - Example: 47 hours → 1 row (nth_day=1); 49 hours → 2 rows (nth_day=1, 2)
    """
    return duckdb.sql("""
        WITH window_info AS (
            -- Calculate number of complete 24h periods per window
            FROM cohort_rel
            SELECT
                hospitalization_id
                , start_dttm AS original_start_dttm
                , end_dttm
                -- Number of complete 24h periods (floor division)
                , FLOOR(DATEDIFF('hour', start_dttm, end_dttm) / 24)::INTEGER AS num_complete_days
        ),
        expanded AS (
            -- Generate one row per complete 24h period
            FROM window_info w
            -- UNNEST with generate_series creates rows [0, 1, 2, ..., num_complete_days - 1]
            SELECT
                w.hospitalization_id
                , w.original_start_dttm
                , UNNEST(generate_series(0, w.num_complete_days - 1)) AS day_offset
            WHERE w.num_complete_days >= 1  -- Filter out windows < 24h
        )
        -- Calculate start/end for each 24h period
        FROM expanded
        SELECT
            hospitalization_id
            , original_start_dttm + (day_offset * INTERVAL '24 hours') AS start_dttm
            , original_start_dttm + ((day_offset + 1) * INTERVAL '24 hours') AS end_dttm
            , (day_offset + 1)::INTEGER AS nth_day  -- 1-indexed
    """)
