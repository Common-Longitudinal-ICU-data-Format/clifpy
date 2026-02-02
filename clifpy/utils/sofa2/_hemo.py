"""Hemostasis subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec):
- Platelets > 150 × 10³/µL: 0 points
- Platelets ≤ 150 × 10³/µL: 1 point
- Platelets ≤ 100 × 10³/µL: 2 points
- Platelets ≤ 80 × 10³/µL: 3 points
- Platelets ≤ 50 × 10³/µL: 4 points

Reviewers:4
- Zewei (Whiskey) on 2026-02-01.
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.hemo')


def _calculate_hemo_subscore(
    cohort_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    dev: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 hemostasis subscore based on platelet count.

    Implements pre-window lookback with fallback pattern:
    - First tries to use in-window platelet values
    - Falls back to pre-window value (within lookback hours) if no in-window data

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs)
    cfg : SOFA2Config
        Configuration with hemo_lookback_hours
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, platelet_count, platelet_dttm_offset, hemo]
        Where hemo is the subscore (0-4) or NULL if no platelet data.
        Offset is interval from start_dttm (negative = pre-window, positive = in-window)
    """
    logger.info("Calculating hemostasis subscore...")

    lookback_hours = cfg.hemo_lookback_hours
    logger.info(f"hemo_lookback_hours={lookback_hours}")

    # Step 1: Get in-window platelet values (MIN = worst) with offset from start_dttm
    # Offset = lab_collect_dttm - start_dttm (positive for in-window)
    logger.info("Collecting worst in-window platelet count for coagulation assessment...")
    platelet_in_window = duckdb.sql("""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , MIN(lab_value_numeric) AS platelet_count
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) - c.start_dttm AS platelet_dttm_offset
        WHERE t.lab_category = 'platelet_count'
        GROUP BY t.hospitalization_id, c.start_dttm
    """)

    # Step 2: Get pre-window platelet value (ASOF JOIN with cutoff in WHERE)
    # ASOF returns single closest record, so offset is directly available
    # Offset = lab_collect_dttm - start_dttm (negative for pre-window)
    logger.info("Looking back for pre-window platelet to handle missing data...")
    platelet_pre_window = duckdb.sql(f"""
        FROM cohort_rel c
        ASOF LEFT JOIN labs_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.lab_collect_dttm
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , t.lab_value_numeric AS platelet_count
            , platelet_dttm_offset: t.lab_collect_dttm - c.start_dttm
        WHERE t.lab_category = 'platelet_count'
            AND platelet_dttm_offset >= -INTERVAL '{lookback_hours} hours'
    """)

    # Step 3: Apply fallback pattern (use pre-window only if no in-window data)
    logger.info("Applying fallback: use pre-window platelet only when in-window missing...")
    platelet_with_fallback = duckdb.sql("""
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM platelet_in_window
        ),
        pre_window_fallback AS (
            FROM platelet_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT hospitalization_id, start_dttm, platelet_count, platelet_dttm_offset
        )
        FROM platelet_in_window SELECT hospitalization_id, start_dttm, platelet_count, platelet_dttm_offset
        UNION ALL
        FROM pre_window_fallback SELECT *
    """)

    # Step 4: Calculate subscore
    logger.info("Scoring hemostasis subscore based on platelet thresholds...")
    hemo_score = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN platelet_with_fallback p USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , p.platelet_count
            , p.platelet_dttm_offset
            , hemo: CASE
                WHEN p.platelet_count IS NULL THEN NULL
                WHEN p.platelet_count <= 50 THEN 4
                WHEN p.platelet_count <= 80 THEN 3
                WHEN p.platelet_count <= 100 THEN 2
                WHEN p.platelet_count <= 150 THEN 1
                ELSE 0
            END
    """)

    logger.info("Hemostasis subscore complete")

    if dev:
        return hemo_score, {
            'platelet_in_window': platelet_in_window,
            'platelet_pre_window': platelet_pre_window,
            'platelet_with_fallback': platelet_with_fallback,
        }

    return hemo_score
