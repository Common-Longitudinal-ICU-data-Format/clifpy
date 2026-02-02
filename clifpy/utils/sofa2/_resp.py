"""Respiratory subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec):
P/F ratio cutoffs:
- P/F > 300: 0 points
- P/F ≤ 300: 1 point
- P/F ≤ 225: 2 points
- P/F ≤ 150 + advanced ventilatory support: 3 points
- P/F ≤ 75 + advanced ventilatory support: 4 points

S/F ratio cutoffs (when PaO2 unavailable and SpO2 < 98%):
- S/F > 300: 0 points
- S/F ≤ 300: 1 point
- S/F ≤ 250: 2 points
- S/F ≤ 200 + advanced ventilatory support: 3 points
- S/F ≤ 120 + advanced ventilatory support: 4 points

Special rules:
- ECMO for respiratory failure: 4 points regardless of ratio
- Advanced ventilatory support includes: IMV, NIPPV, CPAP, High Flow NC
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config


def _calculate_resp_subscore(
    cohort_rel: DuckDBPyRelation,
    resp_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    vitals_rel: DuckDBPyRelation,
    ecmo_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    include_intermediates: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 respiratory subscore based on P/F or S/F ratio.

    Implements:
    - FiO2 imputation from device/LPM
    - Pre-window fallback for FiO2, PaO2, SpO2
    - Concurrent P/F and S/F ratio calculation with tolerance window
    - S/F only when PaO2 unavailable and SpO2 < 98%
    - ECMO override

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    resp_rel : DuckDBPyRelation
        Respiratory support table (CLIF respiratory_support)
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs) - for PaO2
    vitals_rel : DuckDBPyRelation
        Vitals table (CLIF vitals) - for SpO2
    ecmo_rel : DuckDBPyRelation
        ECMO/MCS table (CLIF ecmo_mcs)
    cfg : SOFA2Config
        Configuration with resp_lookback_hours, pf_sf_tolerance_hours
    include_intermediates : bool, default False
        If True, return (result, intermediates_dict) for QA

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, ratio, ratio_type, has_advanced_support, resp]
    """
    lookback_hours = cfg.resp_lookback_hours
    tolerance_minutes = int(cfg.pf_sf_tolerance_hours * 60)

    # =========================================================================
    # Step 1: FiO2 with imputation and pre-window fallback
    # =========================================================================

    # Get in-window FiO2 measurements
    fio2_in_window = duckdb.sql("""
        FROM resp_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.recorded_dttm
            , t.device_category
            , t.fio2_set
            , t.lpm_set
    """)

    # Get pre-window FiO2 measurement (ASOF JOIN)
    fio2_pre_window = duckdb.sql(f"""
        FROM cohort_rel c
        ASOF LEFT JOIN resp_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.recorded_dttm
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , t.recorded_dttm
            , t.device_category
            , t.fio2_set
            , t.lpm_set
            , c.start_dttm - t.recorded_dttm AS time_gap
        WHERE t.hospitalization_id IS NOT NULL
            AND time_gap <= INTERVAL '{lookback_hours} hours'
    """)

    # Apply fallback and FiO2 imputation
    fio2_imputed = duckdb.sql("""
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM fio2_in_window
        ),
        pre_window_fallback AS (
            FROM fio2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT hospitalization_id, start_dttm, recorded_dttm, device_category, fio2_set, lpm_set
        ),
        combined AS (
            FROM fio2_in_window SELECT *
            UNION ALL
            FROM pre_window_fallback SELECT *
        )
        FROM combined t
        SELECT
            t.hospitalization_id
            , t.start_dttm
            , t.recorded_dttm
            , t.device_category
            -- Impute FiO2 based on device
            , CASE
                WHEN t.fio2_set IS NOT NULL AND t.fio2_set > 0 THEN t.fio2_set
                WHEN LOWER(t.device_category) = 'room air' THEN 0.21
                -- Nasal cannula: impute from lpm_set
                WHEN LOWER(t.device_category) = 'nasal cannula' THEN
                    CASE WHEN t.lpm_set <= 1 THEN 0.24
                         WHEN t.lpm_set <= 2 THEN 0.28
                         WHEN t.lpm_set <= 3 THEN 0.32
                         WHEN t.lpm_set <= 4 THEN 0.36
                         WHEN t.lpm_set <= 5 THEN 0.40
                         WHEN t.lpm_set <= 6 THEN 0.44
                         ELSE 0.50 END
                ELSE t.fio2_set
            END AS fio2_imputed
            -- Flag for advanced respiratory support (scores 3-4 require this)
            , CASE WHEN LOWER(t.device_category) IN ('imv', 'nippv', 'cpap', 'high flow nc')
                   THEN 1 ELSE 0 END AS is_advanced_support
        WHERE t.fio2_set IS NOT NULL OR t.device_category IS NOT NULL
    """)

    # =========================================================================
    # Step 2: PaO2 measurements with pre-window fallback
    # =========================================================================

    pao2_in_window = duckdb.sql("""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_result_dttm >= c.start_dttm
            AND t.lab_result_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.lab_result_dttm
            , t.lab_value_numeric AS pao2
        WHERE t.lab_category = 'po2_arterial'
            AND t.lab_value_numeric IS NOT NULL
    """)

    pao2_pre_window = duckdb.sql(f"""
        FROM cohort_rel c
        ASOF LEFT JOIN labs_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.lab_result_dttm
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , t.lab_result_dttm
            , t.lab_value_numeric AS pao2
            , c.start_dttm - t.lab_result_dttm AS time_gap
        WHERE t.hospitalization_id IS NOT NULL
            AND t.lab_category = 'po2_arterial'
            AND t.lab_value_numeric IS NOT NULL
            AND time_gap <= INTERVAL '{lookback_hours} hours'
    """)

    pao2_measurements = duckdb.sql("""
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM pao2_in_window
        ),
        pre_window_fallback AS (
            FROM pao2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT hospitalization_id, start_dttm, lab_result_dttm, pao2
        )
        FROM pao2_in_window SELECT *
        UNION ALL
        FROM pre_window_fallback SELECT *
    """)

    # =========================================================================
    # Step 3: SpO2 measurements with pre-window fallback
    # =========================================================================

    spo2_in_window = duckdb.sql("""
        FROM vitals_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.recorded_dttm
            , t.vital_value AS spo2
        WHERE t.vital_category = 'spo2'
            AND t.vital_value IS NOT NULL
            AND t.vital_value < 98  -- Only use SpO2 < 98% per spec
    """)

    spo2_pre_window = duckdb.sql(f"""
        FROM cohort_rel c
        ASOF LEFT JOIN vitals_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.recorded_dttm
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , t.recorded_dttm
            , t.vital_value AS spo2
            , c.start_dttm - t.recorded_dttm AS time_gap
        WHERE t.hospitalization_id IS NOT NULL
            AND t.vital_category = 'spo2'
            AND t.vital_value IS NOT NULL
            AND t.vital_value < 98  -- Only use SpO2 < 98% per spec
            AND time_gap <= INTERVAL '{lookback_hours} hours'
    """)

    spo2_measurements = duckdb.sql("""
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM spo2_in_window
        ),
        pre_window_fallback AS (
            FROM spo2_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT hospitalization_id, start_dttm, recorded_dttm, spo2
        )
        FROM spo2_in_window SELECT *
        UNION ALL
        FROM pre_window_fallback SELECT *
    """)

    # =========================================================================
    # Step 4: Concurrent P/F ratio (ASOF JOIN with tolerance)
    # =========================================================================

    concurrent_pf = duckdb.sql(f"""
        FROM pao2_measurements p
        ASOF JOIN fio2_imputed f
            ON p.hospitalization_id = f.hospitalization_id
            AND p.start_dttm = f.start_dttm  -- same window
            AND f.recorded_dttm <= p.lab_result_dttm
        SELECT
            p.hospitalization_id
            , p.start_dttm
            , p.pao2
            , p.lab_result_dttm AS pao2_dttm
            , f.fio2_imputed
            , f.recorded_dttm AS fio2_dttm
            , f.device_category
            , f.is_advanced_support
            , p.lab_result_dttm - f.recorded_dttm AS pf_time_gap
            , p.pao2 / f.fio2_imputed AS pf_ratio
        WHERE f.fio2_imputed IS NOT NULL
            AND f.fio2_imputed > 0
            AND pf_time_gap <= INTERVAL '{tolerance_minutes} minutes'
    """)

    # =========================================================================
    # Step 5: Concurrent S/F ratio (ASOF JOIN with tolerance)
    # =========================================================================

    concurrent_sf = duckdb.sql(f"""
        FROM spo2_measurements s
        ASOF JOIN fio2_imputed f
            ON s.hospitalization_id = f.hospitalization_id
            AND s.start_dttm = f.start_dttm  -- same window
            AND f.recorded_dttm <= s.recorded_dttm
        SELECT
            s.hospitalization_id
            , s.start_dttm
            , s.spo2
            , s.recorded_dttm AS spo2_dttm
            , f.fio2_imputed
            , f.recorded_dttm AS fio2_dttm
            , f.device_category
            , f.is_advanced_support
            , s.recorded_dttm - f.recorded_dttm AS sf_time_gap
            , s.spo2 / f.fio2_imputed AS sf_ratio
        WHERE f.fio2_imputed IS NOT NULL
            AND f.fio2_imputed > 0
            AND sf_time_gap <= INTERVAL '{tolerance_minutes} minutes'
    """)

    # =========================================================================
    # Step 6: Aggregate (worst P/F, worst S/F, P/F takes priority)
    # =========================================================================

    resp_agg = duckdb.sql("""
        WITH pf_worst AS (
            -- Worst P/F ratio per window
            FROM concurrent_pf
            SELECT
                hospitalization_id
                , start_dttm
                , MIN(pf_ratio) AS ratio
                , ARG_MIN(is_advanced_support, pf_ratio) AS has_advanced_support
                , ARG_MIN(device_category, pf_ratio) AS device_category
                , ARG_MIN(pao2, pf_ratio) AS pao2_at_worst
                , ARG_MIN(fio2_imputed, pf_ratio) AS fio2_at_worst
                , 'pf' AS ratio_type
            GROUP BY hospitalization_id, start_dttm
        ),
        sf_worst AS (
            -- Worst S/F ratio per window (only for windows WITHOUT P/F data)
            FROM concurrent_sf
            ANTI JOIN (SELECT DISTINCT hospitalization_id, start_dttm FROM concurrent_pf) p
                USING (hospitalization_id, start_dttm)
            SELECT
                hospitalization_id
                , start_dttm
                , MIN(sf_ratio) AS ratio
                , ARG_MIN(is_advanced_support, sf_ratio) AS has_advanced_support
                , ARG_MIN(device_category, sf_ratio) AS device_category
                , ARG_MIN(spo2, sf_ratio) AS spo2_at_worst
                , ARG_MIN(fio2_imputed, sf_ratio) AS fio2_at_worst
                , 'sf' AS ratio_type
            GROUP BY hospitalization_id, start_dttm
        )
        -- Combine: P/F takes priority, S/F only for windows without P/F
        FROM pf_worst
        SELECT hospitalization_id, start_dttm, ratio, has_advanced_support, device_category, ratio_type,
               pao2_at_worst, NULL AS spo2_at_worst, fio2_at_worst
        UNION ALL
        FROM sf_worst
        SELECT hospitalization_id, start_dttm, ratio, has_advanced_support, device_category, ratio_type,
               NULL AS pao2_at_worst, spo2_at_worst, fio2_at_worst
    """)

    # =========================================================================
    # Step 7: ECMO flag
    # =========================================================================

    ecmo_flag = duckdb.sql("""
        FROM ecmo_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT DISTINCT
            t.hospitalization_id
            , c.start_dttm
            , 1 AS has_ecmo
    """)

    # =========================================================================
    # Step 8: Calculate respiratory score
    # =========================================================================

    resp_score = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN resp_agg r USING (hospitalization_id, start_dttm)
        LEFT JOIN ecmo_flag e USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , r.ratio
            , r.ratio_type
            , r.has_advanced_support
            , r.device_category
            , r.pao2_at_worst
            , r.spo2_at_worst
            , r.fio2_at_worst
            , COALESCE(e.has_ecmo, 0) AS has_ecmo
            , resp: CASE
                -- ECMO override: 4 points regardless of ratio
                WHEN COALESCE(e.has_ecmo, 0) = 1 THEN 4
                -- P/F ratio cutoffs
                WHEN r.ratio_type = 'pf' AND r.ratio <= 75 AND r.has_advanced_support = 1 THEN 4
                WHEN r.ratio_type = 'pf' AND r.ratio <= 150 AND r.has_advanced_support = 1 THEN 3
                WHEN r.ratio_type = 'pf' AND r.ratio <= 225 THEN 2
                WHEN r.ratio_type = 'pf' AND r.ratio <= 300 THEN 1
                WHEN r.ratio_type = 'pf' AND r.ratio > 300 THEN 0
                -- S/F ratio cutoffs (different thresholds per SOFA-2 spec)
                WHEN r.ratio_type = 'sf' AND r.ratio <= 120 AND r.has_advanced_support = 1 THEN 4
                WHEN r.ratio_type = 'sf' AND r.ratio <= 200 AND r.has_advanced_support = 1 THEN 3
                WHEN r.ratio_type = 'sf' AND r.ratio <= 250 THEN 2
                WHEN r.ratio_type = 'sf' AND r.ratio <= 300 THEN 1
                WHEN r.ratio_type = 'sf' AND r.ratio > 300 THEN 0
                ELSE NULL
            END
    """)

    if include_intermediates:
        return resp_score, {
            'fio2_in_window': fio2_in_window,
            'fio2_pre_window': fio2_pre_window,
            'fio2_imputed': fio2_imputed,
            'pao2_in_window': pao2_in_window,
            'pao2_pre_window': pao2_pre_window,
            'pao2_measurements': pao2_measurements,
            'spo2_in_window': spo2_in_window,
            'spo2_pre_window': spo2_pre_window,
            'spo2_measurements': spo2_measurements,
            'concurrent_pf': concurrent_pf,
            'concurrent_sf': concurrent_sf,
            'resp_agg': resp_agg,
            'ecmo_flag': ecmo_flag,
        }

    return resp_score
