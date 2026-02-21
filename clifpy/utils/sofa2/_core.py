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
from ._perf import StepTimer, NoOpTimer, _cleanup_temp_tables
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.core')


def _load_ecmo_optional(clif_config_path: str | None) -> DuckDBPyRelation:
    """Try to load ecmo_mcs table; return empty sentinel if unavailable.

    Many sites have not built the ECMO/MCS table yet. Since ECMO scoring
    only affects a small subset of patients (score 4 in resp and/or CV),
    the pipeline should gracefully skip ECMO scoring when the table is
    unavailable rather than crashing.

    Returns an empty DuckDB relation with the correct schema when:
    1. The file doesn't exist (FileNotFoundError)
    2. The file exists but is missing required columns

    The empty relation flows harmlessly through the downstream LEFT JOIN +
    COALESCE pattern, resulting in all ECMO-related flags defaulting to 0.
    """
    from clifpy import load_data

    REQUIRED_COLS = {'hospitalization_id', 'recorded_dttm', 'mcs_group', 'ecmo_configuration_category'}
    EMPTY_ECMO = duckdb.sql("""
        SELECT
            NULL::VARCHAR AS hospitalization_id
            , NULL::TIMESTAMP AS recorded_dttm
            , NULL::VARCHAR AS mcs_group
            , NULL::VARCHAR AS ecmo_configuration_category
        WHERE false
    """)

    try:
        rel = load_data('ecmo_mcs', config_path=clif_config_path, return_rel=True)
    except Exception as e:
        logger.warning(f"ECMO/MCS table not available ({e}). ECMO scoring will be skipped.")
        return EMPTY_ECMO

    # Validate required columns exist
    missing = REQUIRED_COLS - set(rel.columns)
    if missing:
        logger.warning(
            f"ECMO/MCS table missing required columns: {missing}. ECMO scoring will be skipped."
        )
        return EMPTY_ECMO

    return rel


def _load_intm_meds_optional(clif_config_path: str | None) -> DuckDBPyRelation:
    """Try to load medication_admin_intermittent table; return empty sentinel if unavailable.

    Many sites may not have the intermittent medication table. It is used for:
    - Sedation detection (intermittent sedation drugs)
    - Delirium drug detection (haloperidol, quetiapine, ziprasidone, olanzapine)

    The empty relation flows harmlessly — no intermittent drugs are detected,
    and only continuous drug data is used for scoring.
    """
    from clifpy import load_data

    REQUIRED_COLS = {'hospitalization_id', 'admin_dttm', 'med_category', 'med_dose', 'mar_action_category'}
    EMPTY_INTM = duckdb.sql("""
        SELECT
            NULL::VARCHAR AS hospitalization_id
            , NULL::TIMESTAMP AS admin_dttm
            , NULL::VARCHAR AS med_category
            , NULL::DOUBLE AS med_dose
            , NULL::VARCHAR AS mar_action_category
        WHERE false
    """)

    try:
        rel = load_data('medication_admin_intermittent', config_path=clif_config_path, return_rel=True)
    except Exception as e:
        logger.warning(f"Intermittent meds table not available ({e}). Only continuous meds will be used.")
        return EMPTY_INTM

    # Validate required columns exist
    missing = REQUIRED_COLS - set(rel.columns)
    if missing:
        logger.warning(
            f"Intermittent meds table missing required columns: {missing}. Only continuous meds will be used."
        )
        return EMPTY_INTM

    return rel


def calculate_sofa2(
    cohort_df: pd.DataFrame | DuckDBPyRelation,
    clif_config_path: str | None = None,
    return_rel: bool = False,
    dev: bool = False,
    *,
    sofa2_config: SOFA2Config | None = None,
    perf_profile: bool = False,
) -> pd.DataFrame | DuckDBPyRelation | tuple:
    """
    Calculate SOFA-2 scores for a cohort with time windows.

    Parameters
    ----------
    cohort_df : pd.DataFrame | DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm].
        One row per scoring window. Note that windows for the same hospitalization_id must be non-overlapping.
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
            - sofa2_total (sum of subscores, 0-24)
            - sofa2_brain, sofa2_resp, sofa2_cv, sofa2_liver, sofa2_kidney, sofa2_hemo
            - Brain: gcs_min, gcs_type, gcs_min_dttm_offset, has_sedation,
              sedation_start_dttm_offset, sedation_end_dttm_offset,
              has_delirium_drug, delirium_drug_dttm_offset
            - Resp: pf_ratio, sf_ratio, has_advanced_support, device_category,
              pao2_at_worst, pao2_dttm_offset, spo2_at_worst, spo2_dttm_offset,
              fio2_at_worst, fio2_dttm_offset, has_ecmo, ecmo_dttm_offset
            - CV: map_min, map_min_dttm_offset, norepi_epi_maxsum,
              norepi_epi_maxsum_dttm_offset, dopa_max, dopa_max_dttm_offset,
              has_other_non_dopa, has_other_vaso, has_mechanical_cv_support,
              mechanical_cv_dttm_offset
            - Liver: bilirubin_total, bilirubin_dttm_offset
            - Kidney: creatinine, creatinine_dttm_offset, potassium,
              potassium_dttm_offset, ph, ph_type, ph_dttm_offset, bicarbonate,
              bicarbonate_dttm_offset, has_rrt, rrt_dttm_offset, rrt_criteria_met
            - Hemo: platelet_count, platelet_dttm_offset
        If dev=True: (results, intermediates_dict)
    """
    from clifpy import load_data

    cfg = sofa2_config or SOFA2Config()
    intermediates = {} if dev else None
    timer = StepTimer() if perf_profile else NoOpTimer()

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
    with timer.step("load_tables"):
        logger.info("Loading CLIF tables (vitals, labs, meds, respiratory_support, crrt_therapy)...")
        labs_rel = load_data('labs', config_path=clif_config_path, return_rel=True)
        crrt_rel = load_data('crrt_therapy', config_path=clif_config_path, return_rel=True)
        assessments_rel = load_data('patient_assessments', config_path=clif_config_path, return_rel=True)
        vitals_rel = load_data('vitals', config_path=clif_config_path, return_rel=True)
        cont_meds_rel = load_data('medication_admin_continuous', config_path=clif_config_path, return_rel=True)
        resp_rel = load_data('respiratory_support', config_path=clif_config_path, return_rel=True)
        ecmo_rel = _load_ecmo_optional(clif_config_path)
        intm_meds_rel = _load_intm_meds_optional(clif_config_path)

    # =========================================================================
    # Calculate subscores
    # =========================================================================
    logger.info("Calculating all 6 organ subscores in sequence...")

    # Brain subscore
    cv_timer = StepTimer() if perf_profile else None
    with timer.step("brain"):
        if dev:
            brain_score, brain_intermediates = _calculate_brain_subscore(
                cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg, dev=True
            )
            intermediates.update({f'brain_{k}': v for k, v in brain_intermediates.items()})
            intermediates['brain_score'] = brain_score
        else:
            brain_score = _calculate_brain_subscore(
                cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg
            )

    # Respiratory subscore
    with timer.step("resp"):
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
    with timer.step("cv"):
        if dev:
            cv_score, cv_intermediates = _calculate_cv_subscore(
                cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, cfg, dev=True,
                _timer=cv_timer,
            )
            intermediates.update({f'cv_{k}': v for k, v in cv_intermediates.items()})
            intermediates['cv_score'] = cv_score
        else:
            cv_score = _calculate_cv_subscore(
                cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, cfg,
                _timer=cv_timer,
            )

    # Liver subscore
    with timer.step("liver"):
        if dev:
            liver_score, liver_intermediates = _calculate_liver_subscore(
                cohort_rel, labs_rel, cfg, dev=True
            )
            intermediates.update({f'liver_{k}': v for k, v in liver_intermediates.items()})
            intermediates['liver_score'] = liver_score
        else:
            liver_score = _calculate_liver_subscore(cohort_rel, labs_rel, cfg)

    # Kidney subscore
    with timer.step("kidney"):
        if dev:
            kidney_score, kidney_intermediates = _calculate_kidney_subscore(
                cohort_rel, labs_rel, crrt_rel, cfg, dev=True
            )
            intermediates.update({f'kidney_{k}': v for k, v in kidney_intermediates.items()})
            intermediates['kidney_score'] = kidney_score
        else:
            kidney_score = _calculate_kidney_subscore(cohort_rel, labs_rel, crrt_rel, cfg)

    # Hemostasis subscore
    with timer.step("hemo"):
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
    with timer.step("assembly"):
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
                -- SOFA-2 total and subscores
                , sofa2_total: COALESCE(b.brain, 0)
                    + COALESCE(resp.resp, 0)
                    + COALESCE(cv.cv, 0)
                    + COALESCE(li.liver, 0)
                    + COALESCE(k.kidney, 0)
                    + COALESCE(h.hemo, 0)
                , sofa2_brain: b.brain
                , sofa2_resp: resp.resp
                , sofa2_cv: cv.cv
                , sofa2_liver: li.liver
                , sofa2_kidney: k.kidney
                , sofa2_hemo: h.hemo
                -- Brain scoring variables
                , b.gcs_min
                , b.gcs_type
                , b.gcs_min_dttm_offset
                , b.has_sedation
                , b.sedation_start_dttm_offset
                , b.sedation_end_dttm_offset
                , b.has_delirium_drug
                , b.delirium_drug_dttm_offset
                -- Respiratory scoring variables
                , resp.pf_ratio
                , resp.sf_ratio
                , resp.has_advanced_support
                , resp.device_category
                , resp.pao2_at_worst
                , resp.pao2_dttm_offset
                , resp.spo2_at_worst
                , resp.spo2_dttm_offset
                , resp.fio2_at_worst
                , resp.fio2_dttm_offset
                , resp.has_ecmo
                , resp.ecmo_dttm_offset
                -- Cardiovascular scoring variables
                , cv.map_min
                , cv.map_min_dttm_offset
                , cv.norepi_epi_maxsum
                , cv.norepi_epi_maxsum_dttm_offset
                , cv.dopa_max
                , cv.dopa_max_dttm_offset
                , cv.has_other_non_dopa
                , cv.has_other_vaso
                , cv.has_mechanical_cv_support
                , cv.mechanical_cv_dttm_offset
                -- Liver scoring variables
                , li.bilirubin_total
                , li.bilirubin_dttm_offset
                -- Kidney scoring variables
                , k.creatinine
                , k.creatinine_dttm_offset
                , k.potassium
                , k.potassium_dttm_offset
                , k.ph
                , k.ph_type
                , k.ph_dttm_offset
                , k.bicarbonate
                , k.bicarbonate_dttm_offset
                , k.has_rrt
                , k.rrt_dttm_offset
                , k.rrt_criteria_met
                -- Hemostasis scoring variables
                , h.platelet_count
                , h.platelet_dttm_offset
        """)

    logger.info("SOFA-2 calculation complete")

    # Return based on options
    # NOTE: Cleanup temp tables after .df() forces evaluation of the lazy DAG.
    # When return_rel=True, skip cleanup — caller owns evaluation lifetime.
    if dev:
        if return_rel:
            result = sofa_scores, intermediates
        else:
            result = sofa_scores.df(), intermediates
            _cleanup_temp_tables()
    elif return_rel:
        result = sofa_scores
    else:
        result = sofa_scores.df()
        _cleanup_temp_tables()

    if perf_profile:
        return result, timer, cv_timer
    return result


def calculate_sofa2_daily(
    cohort_df: pd.DataFrame | DuckDBPyRelation,
    clif_config_path: str | None = None,
    return_rel: bool = False,
    *,
    sofa2_config: SOFA2Config | None = None,
    perf_profile: bool = False,
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
        Same columns as calculate_sofa2 output, plus nth_day.
        Column order: hospitalization_id, start_dttm, end_dttm, nth_day,
        sofa2_total, sofa2_brain, sofa2_resp, sofa2_cv, sofa2_liver,
        sofa2_kidney, sofa2_hemo, then all scoring variables.
        Subscores are carried forward; scoring variables are raw per-window.

    Notes
    -----
    - Windows < 24 hours produce no output rows

    - Partial days at the end are dropped

    - Example: 47h window → 1 row (nth_day=1); 49h window → 2 rows (nth_day=1, 2)
    """
    logger.info("Starting daily SOFA-2 calculation...")
    timer = StepTimer() if perf_profile else NoOpTimer()

    # Convert to relation if needed
    if isinstance(cohort_df, pd.DataFrame):
        cohort_rel = duckdb.sql("SELECT * FROM cohort_df")
    else:
        cohort_rel = cohort_df

    # Step 1: Expand arbitrary windows to complete 24h periods
    with timer.step("expand_windows"):
        logger.info("Expanding windows to 24h periods...")
        # Materialize expansion to DataFrame so calculate_sofa2() gets
        # concrete cardinality — prevents DuckDB optimizer from choking on
        # deeply nested lazy subqueries (observed 5x slowdown without this).
        expanded_cohort = _expand_to_daily_windows(cohort_rel).df()
        logger.info(f"Expanded cohort: {len(expanded_cohort)} rows (from input cohort)")

    # Step 2: Calculate raw scores for each 24h window
    with timer.step("calculate_sofa2"):
        if perf_profile:
            raw_result, inner_timer, inner_cv_timer = calculate_sofa2(
                expanded_cohort,
                clif_config_path=clif_config_path,
                return_rel=False,
                dev=False,
                sofa2_config=sofa2_config,
                perf_profile=True,
            )
            raw_scores = raw_result
        else:
            raw_scores = calculate_sofa2(
                expanded_cohort,
                clif_config_path=clif_config_path,
                return_rel=False,
                dev=False,
                sofa2_config=sofa2_config,
            )
    logger.info(f"Raw scores: {len(raw_scores)} rows")

    # Step 3: Join back to get nth_day and apply carry-forward logic
    logger.info("Applying carry-forward logic...")
    filled_scores = duckdb.sql("""
        WITH with_nth_day AS (
            -- Join raw scores with expanded cohort to get nth_day
            FROM raw_scores r
            LEFT JOIN expanded_cohort e USING (hospitalization_id, start_dttm)
            SELECT
                r.*
                , e.nth_day
        ),
        with_carryforward AS (
            FROM with_nth_day
            SELECT *
                -- For each subscore, fill NULLs with last non-null value (day 2+)
                -- Day 1 NULLs become 0 at the end
                , CASE WHEN nth_day = 1 THEN sofa2_brain
                       ELSE COALESCE(sofa2_brain, LAST_VALUE(sofa2_brain IGNORE NULLS) OVER w)
                  END AS sofa2_brain_filled
                , CASE WHEN nth_day = 1 THEN sofa2_resp
                       ELSE COALESCE(sofa2_resp, LAST_VALUE(sofa2_resp IGNORE NULLS) OVER w)
                  END AS sofa2_resp_filled
                , CASE WHEN nth_day = 1 THEN sofa2_cv
                       ELSE COALESCE(sofa2_cv, LAST_VALUE(sofa2_cv IGNORE NULLS) OVER w)
                  END AS sofa2_cv_filled
                , CASE WHEN nth_day = 1 THEN sofa2_liver
                       ELSE COALESCE(sofa2_liver, LAST_VALUE(sofa2_liver IGNORE NULLS) OVER w)
                  END AS sofa2_liver_filled
                , CASE WHEN nth_day = 1 THEN sofa2_kidney
                       ELSE COALESCE(sofa2_kidney, LAST_VALUE(sofa2_kidney IGNORE NULLS) OVER w)
                  END AS sofa2_kidney_filled
                , CASE WHEN nth_day = 1 THEN sofa2_hemo
                       ELSE COALESCE(sofa2_hemo, LAST_VALUE(sofa2_hemo IGNORE NULLS) OVER w)
                  END AS sofa2_hemo_filled
            WINDOW w AS (
                PARTITION BY hospitalization_id
                ORDER BY start_dttm
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            )
        )
        -- Fill day 1 NULLs with 0, recalculate total from carried-forward subscores
        FROM with_carryforward
        SELECT
            hospitalization_id
            , start_dttm
            , end_dttm
            , nth_day
            -- SOFA-2 total and subscores (carried forward)
            , sofa2_total: COALESCE(sofa2_brain_filled, 0)
                + COALESCE(sofa2_resp_filled, 0)
                + COALESCE(sofa2_cv_filled, 0)
                + COALESCE(sofa2_liver_filled, 0)
                + COALESCE(sofa2_kidney_filled, 0)
                + COALESCE(sofa2_hemo_filled, 0)
            , sofa2_brain: COALESCE(sofa2_brain_filled, 0)
            , sofa2_resp: COALESCE(sofa2_resp_filled, 0)
            , sofa2_cv: COALESCE(sofa2_cv_filled, 0)
            , sofa2_liver: COALESCE(sofa2_liver_filled, 0)
            , sofa2_kidney: COALESCE(sofa2_kidney_filled, 0)
            , sofa2_hemo: COALESCE(sofa2_hemo_filled, 0)
            -- Brain scoring variables
            , gcs_min
            , gcs_type
            , gcs_min_dttm_offset
            , has_sedation
            , sedation_start_dttm_offset
            , sedation_end_dttm_offset
            , has_delirium_drug
            , delirium_drug_dttm_offset
            -- Respiratory scoring variables
            , pf_ratio
            , sf_ratio
            , has_advanced_support
            , device_category
            , pao2_at_worst
            , pao2_dttm_offset
            , spo2_at_worst
            , spo2_dttm_offset
            , fio2_at_worst
            , fio2_dttm_offset
            , has_ecmo
            , ecmo_dttm_offset
            -- Cardiovascular scoring variables
            , map_min
            , map_min_dttm_offset
            , norepi_epi_maxsum
            , norepi_epi_maxsum_dttm_offset
            , dopa_max
            , dopa_max_dttm_offset
            , has_other_non_dopa
            , has_other_vaso
            , has_mechanical_cv_support
            , mechanical_cv_dttm_offset
            -- Liver scoring variables
            , bilirubin_total
            , bilirubin_dttm_offset
            -- Kidney scoring variables
            , creatinine
            , creatinine_dttm_offset
            , potassium
            , potassium_dttm_offset
            , ph
            , ph_type
            , ph_dttm_offset
            , bicarbonate
            , bicarbonate_dttm_offset
            , has_rrt
            , rrt_dttm_offset
            , rrt_criteria_met
            -- Hemostasis scoring variables
            , platelet_count
            , platelet_dttm_offset
    """)

    logger.info("Daily SOFA-2 calculation complete")

    if return_rel:
        result = filled_scores
    else:
        result = filled_scores.df()
        _cleanup_temp_tables()

    if perf_profile:
        return result, timer, inner_timer, inner_cv_timer
    return result
