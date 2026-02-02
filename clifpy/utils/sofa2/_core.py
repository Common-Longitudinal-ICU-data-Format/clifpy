"""Core orchestration functions for SOFA-2 scoring.

This module contains the main public functions:
- calculate_sofa2: Calculate SOFA-2 scores for a cohort with time windows
- calculate_sofa2_daily: Calculate daily SOFA-2 scores with carry-forward logic
"""

from __future__ import annotations

import pandas as pd
import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config, _expand_to_daily_windows
from ._brain import _calculate_brain_subscore
from ._resp import _calculate_resp_subscore
from ._cv import _calculate_cv_subscore
from ._liver import _calculate_liver_subscore
from ._kidney import _calculate_kidney_subscore
from ._hemo import _calculate_hemo_subscore
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.core')


def calculate_sofa2(
    cohort_df: pd.DataFrame | DuckDBPyRelation,
    clif_config_path: str | None = None,
    return_rel: bool = False,
    dev: bool = False,
    *,
    sofa2_config: SOFA2Config | None = None,
) -> pd.DataFrame | DuckDBPyRelation | tuple:
    """
    Calculate SOFA-2 scores for a cohort with time windows.

    Parameters
    ----------
    cohort_df : pd.DataFrame | DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm].
        One row per scoring window.
    clif_config_path : str, optional
        Path to CLIF config file for data loading.
    return_rel : bool, default False
        If True, return DuckDB relation for lazy evaluation.
    dev : bool, default False
        If True, return (results, intermediates) where intermediates is a dict
        of DuckDBPyRelation objects. Call .df() on any intermediate to materialize.
    sofa2_config : SOFA2Config, optional
        Configuration object with calculation parameters.
        If None, uses default values.

    Returns
    -------
    pd.DataFrame | DuckDBPyRelation | tuple
        If dev=False: DataFrame or relation with columns:
            - hospitalization_id, start_dttm, end_dttm (from input)
            - brain, resp, cv, liver, kidney, hemo (subscores 0-4)
            - sofa_total (sum of subscores, 0-24)
        If dev=True: (results, intermediates_dict)
    """
    from clifpy import load_data

    cfg = sofa2_config or SOFA2Config()
    intermediates = {} if dev else None

    logger.info("Starting SOFA-2 calculation...")
    logger.info(f"Config: {cfg}")

    # Convert cohort to relation if needed
    if isinstance(cohort_df, pd.DataFrame):
        cohort_rel = duckdb.sql("SELECT * FROM cohort_df")
    else:
        cohort_rel = cohort_df

    # =========================================================================
    # Load CLIF tables
    # =========================================================================
    logger.info("Loading CLIF tables (vitals, labs, meds, respiratory_support, crrt_therapy)...")
    labs_rel = load_data('labs', config_path=clif_config_path, return_rel=True)
    crrt_rel = load_data('crrt_therapy', config_path=clif_config_path, return_rel=True)
    assessments_rel = load_data('patient_assessments', config_path=clif_config_path, return_rel=True)
    vitals_rel = load_data('vitals', config_path=clif_config_path, return_rel=True)
    meds_rel = load_data('medication_admin_continuous', config_path=clif_config_path, return_rel=True)
    resp_rel = load_data('respiratory_support', config_path=clif_config_path, return_rel=True)
    ecmo_rel = load_data('ecmo_mcs', config_path=clif_config_path, return_rel=True)

    # =========================================================================
    # Calculate subscores
    # =========================================================================
    logger.info("Calculating all 6 organ subscores in sequence...")

    # Brain subscore
    if dev:
        brain_score, brain_intermediates = _calculate_brain_subscore(
            cohort_rel, assessments_rel, meds_rel, dev=True
        )
        intermediates.update({f'brain_{k}': v for k, v in brain_intermediates.items()})
        intermediates['brain_score'] = brain_score
    else:
        brain_score = _calculate_brain_subscore(cohort_rel, assessments_rel, meds_rel)

    # Respiratory subscore
    if dev:
        resp_score, resp_intermediates = _calculate_resp_subscore(
            cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg, dev=True
        )
        intermediates.update({f'resp_{k}': v for k, v in resp_intermediates.items()})
        intermediates['resp_score'] = resp_score
    else:
        resp_score = _calculate_resp_subscore(
            cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg
        )

    # Cardiovascular subscore
    if dev:
        cv_score, cv_intermediates = _calculate_cv_subscore(
            cohort_rel, meds_rel, vitals_rel, cfg, dev=True
        )
        intermediates.update({f'cv_{k}': v for k, v in cv_intermediates.items()})
        intermediates['cv_score'] = cv_score
    else:
        cv_score = _calculate_cv_subscore(cohort_rel, meds_rel, vitals_rel, cfg)

    # Liver subscore
    if dev:
        liver_score, liver_intermediates = _calculate_liver_subscore(
            cohort_rel, labs_rel, cfg, dev=True
        )
        intermediates.update({f'liver_{k}': v for k, v in liver_intermediates.items()})
        intermediates['liver_score'] = liver_score
    else:
        liver_score = _calculate_liver_subscore(cohort_rel, labs_rel, cfg)

    # Kidney subscore
    if dev:
        kidney_score, kidney_intermediates = _calculate_kidney_subscore(
            cohort_rel, labs_rel, crrt_rel, cfg, dev=True
        )
        intermediates.update({f'kidney_{k}': v for k, v in kidney_intermediates.items()})
        intermediates['kidney_score'] = kidney_score
    else:
        kidney_score = _calculate_kidney_subscore(cohort_rel, labs_rel, crrt_rel, cfg)

    # Hemostasis subscore
    if dev:
        hemo_score, hemo_intermediates = _calculate_hemo_subscore(
            cohort_rel, labs_rel, cfg, dev=True
        )
        intermediates.update({f'hemo_{k}': v for k, v in hemo_intermediates.items()})
        intermediates['hemo_score'] = hemo_score
    else:
        hemo_score = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg)

    # =========================================================================
    # Combine all subscores
    # =========================================================================
    logger.info("Combining subscores into final SOFA-2 total (0-24 scale)...")
    sofa_scores = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN hemo_score h USING (hospitalization_id, start_dttm)
        LEFT JOIN liver_score li USING (hospitalization_id, start_dttm)
        LEFT JOIN kidney_score k USING (hospitalization_id, start_dttm)
        LEFT JOIN brain_score b USING (hospitalization_id, start_dttm)
        LEFT JOIN cv_score cv USING (hospitalization_id, start_dttm)
        LEFT JOIN resp_score resp USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , c.end_dttm
            -- 6 subscores
            , h.hemo
            , li.liver
            , k.kidney
            , b.brain
            , cv.cv
            , resp.resp
            -- SOFA Total (sum of all 6 components)
            , sofa_total: COALESCE(h.hemo, 0)
                + COALESCE(li.liver, 0)
                + COALESCE(k.kidney, 0)
                + COALESCE(b.brain, 0)
                + COALESCE(cv.cv, 0)
                + COALESCE(resp.resp, 0)
    """)

    logger.info("SOFA-2 calculation complete")

    # Return based on options
    if dev:
        if return_rel:
            return sofa_scores, intermediates
        else:
            return sofa_scores.df(), intermediates

    if return_rel:
        return sofa_scores
    else:
        return sofa_scores.df()


def calculate_sofa2_daily(
    cohort_df: pd.DataFrame | DuckDBPyRelation,
    clif_config_path: str | None = None,
    return_rel: bool = False,
    *,
    sofa2_config: SOFA2Config | None = None,
) -> pd.DataFrame | DuckDBPyRelation:
    """
    Calculate daily SOFA-2 scores with carry-forward for missing data.

    Accepts input windows of any duration >= 24 hours and automatically breaks
    them into complete 24-hour chunks. Partial days at the end are dropped.

    Implements footnote b: "for missing data after day 1, carry forward
    the last observation, the rationale being that nonmeasurement suggests stability."

    - Day 1 missing values -> score as 0

    - Day 2+ missing values -> forward-fill from last observation

    Parameters
    ----------
    cohort_df : pd.DataFrame | DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm].
        Windows can be any duration >= 24 hours; will be broken into
        complete 24-hour chunks (partial days dropped).
    config_path : str, optional
        Path to CLIF config file for data loading.
    return_rel : bool, default False
        If True, return DuckDB relation for lazy evaluation.
    sofa2_config : SOFA2Config, optional
        Configuration object with calculation parameters.

    Returns
    -------
    pd.DataFrame | DuckDBPyRelation
        Columns: hospitalization_id, start_dttm, end_dttm, nth_day,
        hemo, liver, kidney, brain, cv, resp, sofa_total.

    Notes
    -----
    - Windows < 24 hours produce no output rows

    - Partial days at the end are dropped

    - Example: 47h window → 1 row (nth_day=1); 49h window → 2 rows (nth_day=1, 2)
    """
    logger.info("Starting daily SOFA-2 calculation...")

    # Convert to relation if needed
    if isinstance(cohort_df, pd.DataFrame):
        cohort_rel = duckdb.sql("SELECT * FROM cohort_df")
    else:
        cohort_rel = cohort_df

    # Step 1: Expand arbitrary windows to complete 24h periods
    logger.debug("Expanding windows to 24h periods...")
    expanded_cohort = _expand_to_daily_windows(cohort_rel)

    # Step 2: Calculate raw scores for each 24h window
    raw_scores = calculate_sofa2(
        expanded_cohort,
        clif_config_path=clif_config_path,
        return_rel=True,
        dev=False,
        sofa2_config=sofa2_config,
    )

    # Step 3: Join back to get nth_day and apply carry-forward logic
    logger.debug("Applying carry-forward logic...")
    filled_scores = duckdb.sql("""
        WITH with_nth_day AS (
            -- Join raw scores with expanded cohort to get nth_day
            FROM raw_scores r
            JOIN expanded_cohort e USING (hospitalization_id, start_dttm)
            SELECT
                r.hospitalization_id
                , r.start_dttm
                , r.end_dttm
                , e.nth_day
                , r.hemo
                , r.liver
                , r.kidney
                , r.brain
                , r.cv
                , r.resp
        ),
        with_carryforward AS (
            FROM with_nth_day
            SELECT *
                -- For each subscore, fill NULLs with last non-null value (day 2+)
                -- Day 1 NULLs become 0 at the end
                , CASE WHEN nth_day = 1 THEN hemo
                       ELSE COALESCE(hemo, LAST_VALUE(hemo IGNORE NULLS) OVER w)
                  END AS hemo_filled
                , CASE WHEN nth_day = 1 THEN liver
                       ELSE COALESCE(liver, LAST_VALUE(liver IGNORE NULLS) OVER w)
                  END AS liver_filled
                , CASE WHEN nth_day = 1 THEN kidney
                       ELSE COALESCE(kidney, LAST_VALUE(kidney IGNORE NULLS) OVER w)
                  END AS kidney_filled
                , CASE WHEN nth_day = 1 THEN brain
                       ELSE COALESCE(brain, LAST_VALUE(brain IGNORE NULLS) OVER w)
                  END AS brain_filled
                , CASE WHEN nth_day = 1 THEN cv
                       ELSE COALESCE(cv, LAST_VALUE(cv IGNORE NULLS) OVER w)
                  END AS cv_filled
                , CASE WHEN nth_day = 1 THEN resp
                       ELSE COALESCE(resp, LAST_VALUE(resp IGNORE NULLS) OVER w)
                  END AS resp_filled
            WINDOW w AS (
                PARTITION BY hospitalization_id
                ORDER BY start_dttm
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )
        -- Fill day 1 NULLs with 0, recalculate total
        FROM with_carryforward
        SELECT
            hospitalization_id
            , start_dttm
            , end_dttm
            , nth_day
            , COALESCE(hemo_filled, 0) AS hemo
            , COALESCE(liver_filled, 0) AS liver
            , COALESCE(kidney_filled, 0) AS kidney
            , COALESCE(brain_filled, 0) AS brain
            , COALESCE(cv_filled, 0) AS cv
            , COALESCE(resp_filled, 0) AS resp
            , sofa_total: COALESCE(hemo_filled, 0)
                + COALESCE(liver_filled, 0)
                + COALESCE(kidney_filled, 0)
                + COALESCE(brain_filled, 0)
                + COALESCE(cv_filled, 0)
                + COALESCE(resp_filled, 0)
    """)

    logger.info("Daily SOFA-2 calculation complete")

    if return_rel:
        return filled_scores
    else:
        return filled_scores.df()
