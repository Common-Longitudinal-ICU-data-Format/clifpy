"""Liver subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec, bilirubin in mg/dL):
- Bilirubin ≤ 1.2 mg/dL: 0 points
- Bilirubin ≤ 3.0 mg/dL: 1 point
- Bilirubin ≤ 6.0 mg/dL: 2 points
- Bilirubin ≤ 12.0 mg/dL: 3 points
- Bilirubin > 12.0 mg/dL: 4 points

Reviewers:
- Zewei (Whiskey) on 2026-02-01.
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config


def _calculate_liver_subscore(
    cohort_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    include_intermediates: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 liver subscore based on total bilirubin.

    Implements pre-window lookback with fallback pattern:
    - First tries to use in-window bilirubin values
    - Falls back to pre-window value (within lookback hours) if no in-window data

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs)
    cfg : SOFA2Config
        Configuration with liver_lookback_hours
    include_intermediates : bool, default False
        If True, return (result, intermediates_dict) for QA

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, bilirubin_total, bilirubin_dttm_offset, liver]
        Where liver is the subscore (0-4) or NULL if no bilirubin data.
        Offset is interval from start_dttm (negative = pre-window, positive = in-window)
    """
    lookback_hours = cfg.liver_lookback_hours

    # Step 1: Get in-window bilirubin values (MAX = worst) with offset from start_dttm
    # Offset = lab_collect_dttm - start_dttm (positive for in-window)
    bilirubin_in_window = duckdb.sql("""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , MAX(lab_value_numeric) AS bilirubin_total
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) - c.start_dttm AS bilirubin_dttm_offset
        WHERE t.lab_category = 'bilirubin_total'
        GROUP BY t.hospitalization_id, c.start_dttm
    """)

    # Step 2: Get pre-window bilirubin value (ASOF JOIN with cutoff in WHERE)
    # ASOF returns single closest record, so offset is directly available
    # Offset = lab_collect_dttm - start_dttm (negative for pre-window)
    bilirubin_pre_window = duckdb.sql(f"""
        FROM cohort_rel c
        ASOF LEFT JOIN labs_rel t
            ON c.hospitalization_id = t.hospitalization_id
            AND c.start_dttm > t.lab_collect_dttm
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , t.lab_value_numeric AS bilirubin_total
            , bilirubin_dttm_offset: t.lab_collect_dttm - c.start_dttm
        WHERE t.lab_category = 'bilirubin_total'
            AND bilirubin_dttm_offset >= -INTERVAL '{lookback_hours} hours'
    """)

    # Step 3: Apply fallback pattern (use pre-window only if no in-window data)
    bilirubin_with_fallback = duckdb.sql("""
        WITH windows_with_data AS (
            SELECT DISTINCT hospitalization_id, start_dttm
            FROM bilirubin_in_window
        ),
        pre_window_fallback AS (
            FROM bilirubin_pre_window p
            ANTI JOIN windows_with_data w USING (hospitalization_id, start_dttm)
            SELECT hospitalization_id, start_dttm, bilirubin_total, bilirubin_dttm_offset
        )
        FROM bilirubin_in_window SELECT hospitalization_id, start_dttm, bilirubin_total, bilirubin_dttm_offset
        UNION ALL
        FROM pre_window_fallback SELECT *
    """)

    # Step 4: Calculate subscore
    liver_score = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN bilirubin_with_fallback b USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , b.bilirubin_total
            , b.bilirubin_dttm_offset
            , liver: CASE
                WHEN b.bilirubin_total <= 1.2 THEN 0
                WHEN b.bilirubin_total <= 3.0 THEN 1
                WHEN b.bilirubin_total <= 6.0 THEN 2
                WHEN b.bilirubin_total <= 12.0 THEN 3
                WHEN b.bilirubin_total > 12.0 THEN 4
                ELSE NULL
            END
    """)

    if include_intermediates:
        return liver_score, {
            'bilirubin_in_window': bilirubin_in_window,
            'bilirubin_pre_window': bilirubin_pre_window,
            'bilirubin_with_fallback': bilirubin_with_fallback,
        }

    return liver_score
