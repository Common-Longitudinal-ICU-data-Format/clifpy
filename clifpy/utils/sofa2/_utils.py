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
    post_sedation_gcs_invalidate_hours : float
        Hours after sedation ends where GCS measurements remain invalid (footnote c). Default 1.0.
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

    # Brain subscore (footnote c)
    post_sedation_gcs_invalidate_hours: float = 12.0


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
        Columns: [hospitalization_id, start_dttm, has_rrt, rrt_dttm_offset]
        Only includes rows where RRT was detected (has_rrt = 1)
    """
    return duckdb.sql("""
        FROM crrt_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , 1 AS has_rrt
            , MIN(t.recorded_dttm) - c.start_dttm AS rrt_dttm_offset
        GROUP BY t.hospitalization_id, c.start_dttm
    """)


CONT_DELIRIUM_DRUGS = ['dexmedetomidine']
INTM_DELIRIUM_DRUGS = ['haloperidol', 'quetiapine', 'ziprasidone', 'olanzapine']


def _flag_delirium_drug(
    cohort_rel: DuckDBPyRelation,
    cont_meds_rel: DuckDBPyRelation,
    intm_meds_rel: DuckDBPyRelation,
) -> DuckDBPyRelation:
    """
    Detect delirium drug administration during each scoring window.

    Per footnote e: delirium drug administration -> brain subscore minimum 1 point.

    Continuous delirium drugs (dexmedetomidine): pre-window ASOF + in-window from
    medication_admin_continuous.

    Intermittent delirium drugs (haloperidol, quetiapine, ziprasidone, olanzapine):
    in-window only from medication_admin_intermittent, filtered by
    med_dose > 0 AND mar_action_category != 'not_given'.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    cont_meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    intm_meds_rel : DuckDBPyRelation
        Intermittent medication administrations (CLIF medication_admin_intermittent)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, has_delirium_drug, delirium_drug_dttm_offset]
        Only includes rows where delirium drug was detected (has_delirium_drug = 1).
        delirium_drug_dttm_offset = interval from start_dttm to earliest admin.
    """
    cont_delirium_list = CONT_DELIRIUM_DRUGS
    cont_delirium_tuple = tuple(CONT_DELIRIUM_DRUGS)
    intm_delirium_tuple = tuple(INTM_DELIRIUM_DRUGS)

    # --- Continuous delirium drugs (dexmedetomidine): pre-window ASOF + in-window ---

    # Pre-window: ASOF JOIN to detect drugs already infusing at window start
    cont_pre_window = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({cont_delirium_list}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN cont_meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm > t.admin_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            , t.admin_dttm
        WHERE t.hospitalization_id IS NOT NULL
            AND t.mar_action_category != 'stop'
            AND t.med_dose > 0
    """)

    # In-window continuous delirium drugs
    cont_in_window = duckdb.sql(f"""
        FROM cont_meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm
        WHERE t.med_category IN {cont_delirium_tuple}
            AND t.med_dose > 0
    """)

    # --- Intermittent delirium drugs: in-window only ---

    intm_in_window = duckdb.sql(f"""
        FROM intm_meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm
        WHERE t.med_category IN {intm_delirium_tuple}
            AND t.med_dose > 0
            AND t.mar_action_category != 'not_given'
    """)

    return duckdb.sql("""
        FROM (
            FROM cont_pre_window SELECT *
            UNION ALL
            FROM cont_in_window SELECT *
            UNION ALL
            FROM intm_in_window SELECT *
        )
        SELECT
            hospitalization_id
            , start_dttm
            , 1 AS has_delirium_drug
            , MIN(admin_dttm) - start_dttm AS delirium_drug_dttm_offset
        GROUP BY hospitalization_id, start_dttm
    """)


def _flag_mechanical_cv_support(
    cohort_rel: DuckDBPyRelation, ecmo_rel: DuckDBPyRelation
) -> DuckDBPyRelation:
    """
    Detect mechanical cardiovascular support during each scoring window.

    Per footnote n: mechanical CV support -> CV subscore 4 points.
    Devices include:
    - Non-VV ECMO: ecmo_configuration_category IN ('va', 'va_v', 'vv_a')
    - IABP: mcs_group = 'iabp'
    - LV assist / microaxial flow pump: mcs_group IN ('impella_lvad', 'temporary_lvad', 'durable_lvad')
    - RV assist: mcs_group = 'temporary_rvad'

    NOTE: VV-ECMO (ecmo_configuration_category = 'vv') is excluded per footnote i
    (VV-ECMO is respiratory only, does not score in CV).

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    ecmo_rel : DuckDBPyRelation
        ECMO/MCS table (CLIF ecmo_mcs)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, has_mechanical_cv_support, mechanical_cv_dttm_offset]
        Only includes rows where mechanical CV support was detected (has_mechanical_cv_support = 1)
    """
    return duckdb.sql("""
        FROM ecmo_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , 1 AS has_mechanical_cv_support
            , MIN(t.recorded_dttm) - c.start_dttm AS mechanical_cv_dttm_offset
        WHERE
            -- Non-VV ECMO configurations (footnote i CV part)
            t.ecmo_configuration_category IN ('va', 'va_v', 'vv_a')
            -- IABP (footnote n)
            OR t.mcs_group = 'iabp'
            -- LV assist devices / microaxial flow pumps (footnote n)
            OR t.mcs_group IN ('impella_lvad', 'temporary_lvad', 'durable_lvad')
            -- RV assist devices (footnote n)
            OR t.mcs_group = 'temporary_rvad'
        GROUP BY t.hospitalization_id, c.start_dttm
    """)


# =============================================================================
# Sedation Detection (for brain subscore)
# =============================================================================

SEDATION_DRUGS = [
    'propofol',
    'dexmedetomidine',
    'ketamine',
    'midazolam',
    'fentanyl',
    'hydromorphone',
    'morphine',
    'remifentanil',
    'pentobarbital',
    'lorazepam',
]


# NOTE: DEPRECATED — retained for reference. Use _detect_sedation_episodes() instead.
# The earliest-onset approach was simpler but clinicians preferred episode-based
# GCS invalidation with post_sedation_gcs_invalidate_hours.
def _find_earliest_sedation(
    cohort_rel: DuckDBPyRelation,
    cont_meds_rel: DuckDBPyRelation,
    intm_meds_rel: DuckDBPyRelation,
) -> DuckDBPyRelation:
    """
    DEPRECATED: Use _detect_sedation_episodes() instead. Retained for reference only.

    Find earliest sedation drug administration per scoring window.

    Checks three sources:
    1. Pre-window ASOF for continuous sedation drugs (already infusing at window start)
    2. In-window continuous sedation drugs (admin_dttm within [start_dttm, end_dttm])
    3. In-window intermittent sedation drugs (med_dose > 0 AND mar_action_category != 'not_given')

    For source 1 (pre-window ASOF), if a continuous sedation drug was already active at
    window start, we set earliest_sedation_dttm = start_dttm (the drug was on before the
    window began, so ALL GCS in-window are invalidated).

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    cont_meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    intm_meds_rel : DuckDBPyRelation
        Intermittent medication administrations (CLIF medication_admin_intermittent)

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, earliest_sedation_dttm]
        One row per window that has any sedation. Windows without sedation are absent.
    """
    sedation_drugs_list = SEDATION_DRUGS
    sedation_drugs_tuple = tuple(SEDATION_DRUGS)

    # Source 1: Pre-window ASOF for continuous drugs already infusing at window start
    cont_pre_window = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({sedation_drugs_list}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN cont_meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm > t.admin_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            -- Actual pre-window admin time (negative offset from start_dttm)
            , t.admin_dttm AS sedation_dttm
        WHERE t.hospitalization_id IS NOT NULL
            AND t.mar_action_category != 'stop'
            AND t.med_dose > 0
    """)

    # Source 2: In-window continuous sedation drugs
    cont_in_window = duckdb.sql(f"""
        FROM cont_meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm AS sedation_dttm
        WHERE t.med_category IN {sedation_drugs_tuple}
            AND t.med_dose > 0
    """)

    # Source 3: In-window intermittent sedation drugs
    intm_in_window = duckdb.sql(f"""
        FROM intm_meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm AS sedation_dttm
        WHERE t.med_category IN {sedation_drugs_tuple}
            AND t.med_dose > 0
            AND t.mar_action_category != 'not_given'
    """)

    # UNION ALL → MIN per window
    return duckdb.sql("""
        FROM (
            FROM cont_pre_window SELECT *
            UNION ALL
            FROM cont_in_window SELECT *
            UNION ALL
            FROM intm_in_window SELECT *
        )
        SELECT
            hospitalization_id
            , start_dttm
            , MIN(sedation_dttm) AS earliest_sedation_dttm
        GROUP BY hospitalization_id, start_dttm
    """)


def _detect_sedation_episodes(
    cohort_rel: DuckDBPyRelation,
    cont_meds_rel: DuckDBPyRelation,
    post_sedation_gcs_invalidate_hours: float = 1.0,
) -> DuckDBPyRelation:
    """
    Detect sedation episodes within each scoring window for brain subscore.

    Uses the window-bounded episode pattern: pre-window ASOF + in-window +
    forward-fill at end_dttm. This captures sedation that started before the
    window and extends episodes to the window boundary if the drug is still running.

    Episode detection uses LAG + cumulative SUM (same as CV subscore).
    Sedation episodes invalidate GCS measurements during the episode and for
    post_sedation_gcs_invalidate_hours after the episode ends.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    cont_meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    post_sedation_gcs_invalidate_hours : float
        Hours after sedation episode ends where GCS measurements remain invalid.
        Default 1.0.

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, sedation_start, sedation_end_extended]
        One row per sedation episode per window.
        sedation_end_extended = episode_end + post_sedation_gcs_invalidate_hours
    """
    sedation_drugs_list = SEDATION_DRUGS
    sedation_drugs_tuple = tuple(SEDATION_DRUGS)

    # -------------------------------------------------------------------------
    # Collect sedation events from 3 temporal slices
    # -------------------------------------------------------------------------

    # Pre-window: ASOF JOIN to detect drugs already infusing at window start
    sedation_at_start = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({sedation_drugs_list}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN cont_meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm > t.admin_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            , t.admin_dttm
            , cm.med_category
            , t.med_dose
            , t.mar_action_category
        WHERE t.hospitalization_id IS NOT NULL
    """)

    # In-window: standard events during [start_dttm, end_dttm]
    sedation_in_window = duckdb.sql(f"""
        FROM cont_meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm
            , t.med_category
            , t.med_dose
            , t.mar_action_category
        WHERE t.med_category IN {sedation_drugs_tuple}
    """)

    # At-end: forward-fill drug state AT end_dttm (window-bounded, NOT after)
    # Uses end_dttm as synthetic admin_dttm to extend episodes to window boundary
    sedation_at_end = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({sedation_drugs_list}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN cont_meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.end_dttm >= t.admin_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            , cm.end_dttm AS admin_dttm  -- synthetic forward-fill at window boundary
            , cm.med_category
            , t.med_dose
            , t.mar_action_category
        WHERE t.hospitalization_id IS NOT NULL
    """)

    # Combine all temporal slices
    sedation_events = duckdb.sql("""
        FROM sedation_at_start SELECT *
        UNION ALL
        FROM sedation_in_window SELECT *
        UNION ALL
        FROM sedation_at_end SELECT *
    """)

    # -------------------------------------------------------------------------
    # Dedup + episode detection (unchanged pipeline)
    # -------------------------------------------------------------------------

    return duckdb.sql(f"""
        WITH deduped AS (
            -- MAR deduplication (same pattern as CV subscore)
            FROM sedation_events
            SELECT *
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY hospitalization_id, start_dttm, admin_dttm, med_category
                ORDER BY
                    CASE WHEN mar_action_category IS NULL THEN 10
                        WHEN mar_action_category IN ('verify', 'not_given') THEN 9
                        WHEN mar_action_category = 'stop' THEN 8
                        WHEN mar_action_category = 'going' THEN 7
                        ELSE 1 END,
                    CASE WHEN med_dose > 0 THEN 1 ELSE 2 END,
                    med_dose DESC
            ) = 1
        ),
        any_sedation AS (
            -- Collapse to single "is_sedated" flag per timestamp
            -- A patient is sedated if ANY sedation drug is actively infusing
            FROM deduped
            SELECT
                hospitalization_id
                , start_dttm
                , admin_dttm
                , MAX(CASE WHEN med_dose > 0 AND mar_action_category != 'stop' THEN 1 ELSE 0 END) AS is_sedated
            GROUP BY hospitalization_id, start_dttm, admin_dttm
        ),
        with_lag AS (
            FROM any_sedation
            SELECT
                *
                , LAG(is_sedated) OVER w AS prev_is_sedated
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        episode_groups AS (
            -- Detect episode transitions: is_sedated=1 AND prev_is_sedated=0
            FROM with_lag
            SELECT
                *
                , SUM(CASE WHEN is_sedated = 1 AND COALESCE(prev_is_sedated, 0) = 0 THEN 1 ELSE 0 END) OVER w AS sedation_episode
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        with_episode_bounds AS (
            FROM episode_groups
            SELECT
                *
                , FIRST_VALUE(admin_dttm) OVER episode_w AS episode_start
                , LAST_VALUE(admin_dttm) OVER episode_w AS episode_end
            WINDOW episode_w AS (
                PARTITION BY hospitalization_id, start_dttm, sedation_episode
                ORDER BY admin_dttm
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        )
        -- Return distinct episodes with extended end time
        FROM with_episode_bounds
        SELECT DISTINCT
            hospitalization_id
            , start_dttm
            , episode_start AS sedation_start
            , episode_end
            , episode_end + INTERVAL '{post_sedation_gcs_invalidate_hours} hours' AS sedation_end_extended
        WHERE is_sedated = 1
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
