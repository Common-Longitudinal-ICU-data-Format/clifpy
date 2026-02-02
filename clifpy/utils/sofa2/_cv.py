"""Cardiovascular subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec):
- MAP ≥ 70 mmHg, no vasopressors: 0 points
- MAP < 70 mmHg, no vasopressors: 1 point
- Low-dose vasopressor (norepi+epi ≤ 0.2 mcg/kg/min) or any other pressor: 2 points
- Medium-dose (norepi+epi > 0.2 to ≤ 0.4) or low-dose with other pressor: 3 points
- High-dose (norepi+epi > 0.4) or medium-dose with other pressor: 4 points

Special rules:
- Footnote j: Vasopressors only count if infused for ≥ 60 minutes
- Footnote l: Dopamine-only has different cutoffs (≤20 → 2, >20-40 → 3, >40 → 4)
- MAR action deduplication to handle simultaneous entries
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config, _agg_map
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.cv')


# Default vasopressor list with preferred units
PRESSOR_PREFERRED_UNITS = {
    'norepinephrine': 'mcg/kg/min',
    'epinephrine': 'mcg/kg/min',
    'dopamine': 'mcg/kg/min',
    'dobutamine': 'mcg/kg/min',
    'vasopressin': 'u/min',
    'phenylephrine': 'mcg/kg/min',
    'milrinone': 'mcg/kg/min',
    'angiotensin': 'ng/kg/min',
    'isoproterenol': 'mcg/kg/min',
}

COHORT_VASOPRESSORS = list(PRESSOR_PREFERRED_UNITS.keys())


def _calculate_cv_subscore(
    cohort_rel: DuckDBPyRelation,
    meds_rel: DuckDBPyRelation,
    vitals_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    dev: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 cardiovascular subscore.

    Implements:
    - ASOF JOINs for pre-window vasopressor state (always forward-fill, no cutoff)
    - 60-minute duration validation (footnote j)
    - MAR deduplication
    - Unit conversion via clifpy.utils.unit_converter
    - Dopamine-only scoring (footnote l)

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    vitals_rel : DuckDBPyRelation
        Vitals table (CLIF vitals) - for MAP and weight
    cfg : SOFA2Config
        Configuration with pressor_min_duration_minutes
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, map_min, map_min_dttm_offset,
                  norepi_epi_maxsum, norepi_epi_maxsum_dttm_offset,
                  dopa_max, dopa_max_dttm_offset, ..., cv]
        Offset columns are intervals from start_dttm (always positive for in-window).
    """
    logger.info("Calculating cardiovascular subscore...")

    from clifpy.utils.unit_converter import convert_dose_units_by_med_category

    min_duration = cfg.pressor_min_duration_minutes
    cohort_vasopressors = COHORT_VASOPRESSORS
    logger.info(f"pressor_min_duration_minutes={min_duration}, cohort_vasopressors={cohort_vasopressors}")

    # =========================================================================
    # Step 1: Get MAP aggregation (in-window only)
    # =========================================================================
    logger.info("Aggregating in-window MAP to identify hypotension without pressors...")
    map_agg = _agg_map(cohort_rel, vitals_rel)

    # Get vitals for unit conversion (weight for mcg/kg/min)
    cohort_vitals = duckdb.sql("""
        FROM vitals_rel t
        JOIN cohort_rel c ON t.hospitalization_id = c.hospitalization_id
        SELECT *
    """)

    # =========================================================================
    # Step 2: Get vasopressor events (pre-window, in-window, post-window)
    # =========================================================================
    logger.info("Collecting vasopressor events across pre/in/post windows for episode tracking...")

    # Pre-window: Forward-filled event AT start_dttm for each vasopressor
    pressor_at_start = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({cohort_vasopressors}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.start_dttm > t.admin_dttm  -- include events ONLY BEFORE start_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            , t.admin_dttm
            , cm.med_category
            , t.mar_action_category
            , t.med_dose
            , t.med_dose_unit
        WHERE t.hospitalization_id IS NOT NULL
            AND t.mar_action_category != 'stop'
            AND t.med_dose > 0  -- only if actively infusing
    """)

    # In-window: Events during [start_dttm, end_dttm]
    pressor_in_window = duckdb.sql(f"""
        FROM meds_rel t
        JOIN cohort_rel c ON
            t.hospitalization_id = c.hospitalization_id
            AND t.admin_dttm >= c.start_dttm
            AND t.admin_dttm <= c.end_dttm
        SELECT
            t.hospitalization_id
            , c.start_dttm
            , t.admin_dttm
            , t.med_category
            , t.mar_action_category
            , t.med_dose
            , t.med_dose_unit
        WHERE t.med_category IN {tuple(cohort_vasopressors)}
    """)

    # Post-window: Forward-filled event AT end_dttm (for duration calculation)
    pressor_at_end = duckdb.sql(f"""
        WITH med_cats AS (
            SELECT UNNEST({cohort_vasopressors}::VARCHAR[]) AS med_category
        ),
        cohort_meds AS (
            FROM cohort_rel c
            CROSS JOIN med_cats m
            SELECT c.hospitalization_id, c.start_dttm, c.end_dttm, m.med_category
        )
        FROM cohort_meds cm
        ASOF LEFT JOIN meds_rel t
            ON cm.hospitalization_id = t.hospitalization_id
            AND cm.med_category = t.med_category
            AND cm.end_dttm < t.admin_dttm  -- first event AFTER end_dttm
        SELECT
            cm.hospitalization_id
            , cm.start_dttm
            , t.admin_dttm
            , cm.med_category
            , t.mar_action_category
            , t.med_dose
            , t.med_dose_unit
        WHERE t.hospitalization_id IS NOT NULL
    """)

    # Combine all pressor events
    pressor_events_raw = duckdb.sql("""
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_at_start
        UNION ALL
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_in_window
        UNION ALL
        SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose, med_dose_unit, mar_action_category
        FROM pressor_at_end
    """)

    # =========================================================================
    # Step 3: MAR deduplication
    # =========================================================================
    logger.info("Deduplicating MAR actions to resolve simultaneous entries...")
    pressor_events_deduped = duckdb.sql("""
        FROM pressor_events_raw t
        SELECT
            t.hospitalization_id
            , t.start_dttm
            , t.admin_dttm
            , t.med_category
            , t.mar_action_category
            , t.med_dose
            , t.med_dose_unit
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY t.hospitalization_id, t.start_dttm, t.admin_dttm, t.med_category
            ORDER BY
                CASE WHEN t.mar_action_category IS NULL THEN 10
                    WHEN t.mar_action_category IN ('verify', 'not_given') THEN 9
                    WHEN t.mar_action_category = 'stop' THEN 8
                    WHEN t.mar_action_category = 'going' THEN 7
                    ELSE 1 END,
                CASE WHEN t.med_dose > 0 THEN 1 ELSE 2 END,
                t.med_dose DESC
        ) = 1
    """)

    # =========================================================================
    # Step 4: Unit conversion
    # =========================================================================
    logger.info("Converting pressor doses to standardized weight-based units...")
    pressor_events, _ = convert_dose_units_by_med_category(
        med_df=pressor_events_deduped,
        vitals_df=cohort_vitals,
        return_rel=True,
        preferred_units=PRESSOR_PREFERRED_UNITS,
        override=True
    )

    # =========================================================================
    # Step 5: Pivot epi+norepi to wide format
    # =========================================================================
    logger.info("Pivoting epi+norepi to wide format for concurrent dosage calculation...")
    epi_ne_wide = duckdb.sql("""
        FROM pressor_events
        PIVOT (
            ANY_VALUE(med_dose)
            FOR med_category IN (
                'norepinephrine' AS norepi_raw,
                'epinephrine' AS epi_raw
            )
            GROUP BY hospitalization_id, start_dttm, admin_dttm
        )
    """)

    logger.info("Forward-filling epi+norepi to get continuous dosage values...")
    # Forward-fill epi+norepi
    epi_ne_filled = duckdb.sql("""
        FROM epi_ne_wide
        SELECT
            hospitalization_id
            , start_dttm
            , admin_dttm
            , COALESCE(LAST_VALUE(norepi_raw IGNORE NULLS) OVER w, 0) AS norepi
            , COALESCE(LAST_VALUE(epi_raw IGNORE NULLS) OVER w, 0) AS epi
        WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    """)

    # =========================================================================
    # Step 6: Episode detection and 60-min duration validation for epi+norepi
    # =========================================================================
    logger.info("Detecting episodes and validating 60-min duration per footnote j...")
    epi_ne_duration = duckdb.sql(f"""
        WITH with_lag AS (
            FROM epi_ne_filled
            SELECT
                *
                , LAG(norepi) OVER w AS prev_norepi
                , LAG(epi) OVER w AS prev_epi
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        episode_groups AS (
            FROM with_lag
            SELECT
                *
                , SUM(CASE WHEN norepi > 0 AND COALESCE(prev_norepi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS norepi_episode
                , SUM(CASE WHEN epi > 0 AND COALESCE(prev_epi, 0) = 0 THEN 1 ELSE 0 END) OVER w AS epi_episode
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm ORDER BY admin_dttm)
        ),
        with_episode_bounds AS (
            FROM episode_groups
            SELECT
                *
                , FIRST_VALUE(admin_dttm) OVER norepi_w AS norepi_episode_start
                , LAST_VALUE(admin_dttm) OVER norepi_w AS norepi_episode_end
                , FIRST_VALUE(admin_dttm) OVER epi_w AS epi_episode_start
                , LAST_VALUE(admin_dttm) OVER epi_w AS epi_episode_end
            WINDOW norepi_w AS (PARTITION BY hospitalization_id, start_dttm, norepi_episode
                                ORDER BY admin_dttm ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                   epi_w AS (PARTITION BY hospitalization_id, start_dttm, epi_episode
                             ORDER BY admin_dttm ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        )
        FROM with_episode_bounds
        SELECT
            hospitalization_id
            , start_dttm
            , admin_dttm
            , norepi
            , epi
            , norepi_episode_start
            , norepi_episode_end
            , epi_episode_start
            , epi_episode_end
            , DATEDIFF('minute', norepi_episode_start, norepi_episode_end) AS norepi_episode_duration
            , DATEDIFF('minute', epi_episode_start, epi_episode_end) AS epi_episode_duration
            -- Validated doses: ALL events in {min_duration}+ min episodes are valid
            , CASE WHEN norepi > 0 AND norepi_episode_duration >= {min_duration}
                   THEN norepi ELSE 0 END AS norepi_valid
            , CASE WHEN epi > 0 AND epi_episode_duration >= {min_duration}
                   THEN epi ELSE 0 END AS epi_valid
    """)

    # Aggregate epi+norepi with offset at max dose
    epi_ne_agg = duckdb.sql("""
        FROM epi_ne_duration
        SELECT
            hospitalization_id
            , start_dttm
            , MAX(norepi_valid + epi_valid) AS norepi_epi_maxsum
            , ARG_MAX(admin_dttm, norepi_valid + epi_valid) - start_dttm AS norepi_epi_maxsum_dttm_offset
            , MAX(norepi_valid) AS norepi_max
            , MAX(epi_valid) AS epi_max
        GROUP BY hospitalization_id, start_dttm
    """)

    # =========================================================================
    # Step 7: Duration validation for dopamine + other pressors
    # =========================================================================
    logger.info("Validating duration for dopamine and other pressors per footnote l...")
    other_pressor_duration = duckdb.sql(f"""
        WITH filtered AS (
            FROM pressor_events
            SELECT hospitalization_id, start_dttm, admin_dttm, med_category, med_dose
            WHERE med_category NOT IN ('norepinephrine', 'epinephrine')
        ),
        with_lag AS (
            FROM filtered
            SELECT
                *
                , LAG(med_dose) OVER w AS prev_dose
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm, med_category ORDER BY admin_dttm)
        ),
        episode_groups AS (
            FROM with_lag
            SELECT
                *
                , SUM(CASE WHEN med_dose > 0 AND COALESCE(prev_dose, 0) = 0 THEN 1 ELSE 0 END) OVER w AS episode_id
            WINDOW w AS (PARTITION BY hospitalization_id, start_dttm, med_category ORDER BY admin_dttm)
        ),
        with_episode_bounds AS (
            FROM episode_groups
            SELECT
                *
                , FIRST_VALUE(admin_dttm) OVER episode_w AS episode_start
                , LAST_VALUE(admin_dttm) OVER episode_w AS episode_end
            WINDOW episode_w AS (PARTITION BY hospitalization_id, start_dttm, med_category, episode_id
                                 ORDER BY admin_dttm ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
        )
        FROM with_episode_bounds
        SELECT
            hospitalization_id
            , start_dttm
            , admin_dttm
            , med_category
            , med_dose
            , episode_start
            , episode_end
            , DATEDIFF('minute', episode_start, episode_end) AS episode_duration
            , CASE WHEN med_dose > 0 AND episode_duration >= {min_duration}
                   THEN med_dose ELSE 0 END AS dose_valid
    """)

    # Aggregate dopamine + others with offset at max dose
    other_pressor_agg = duckdb.sql("""
        FROM other_pressor_duration
        SELECT
            hospitalization_id
            , start_dttm
            , MAX(dose_valid) FILTER (WHERE med_category = 'dopamine') AS dopa_max
            , ARG_MAX(admin_dttm, dose_valid) FILTER (WHERE med_category = 'dopamine') - start_dttm AS dopa_max_dttm_offset
            , CASE WHEN MAX(dose_valid) FILTER (WHERE med_category != 'dopamine') > 0
                   THEN 1 ELSE 0 END AS has_other_non_dopa
        GROUP BY hospitalization_id, start_dttm
    """)

    # Combine pressor aggregations
    pressor_agg = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN epi_ne_agg ne USING (hospitalization_id, start_dttm)
        LEFT JOIN other_pressor_agg op USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , COALESCE(ne.norepi_epi_maxsum, 0) AS norepi_epi_maxsum
            , ne.norepi_epi_maxsum_dttm_offset
            , COALESCE(op.dopa_max, 0) AS dopa_max
            , op.dopa_max_dttm_offset
            , COALESCE(op.has_other_non_dopa, 0) AS has_other_non_dopa
    """)

    # =========================================================================
    # Step 8: Calculate CV score
    # =========================================================================
    logger.info("Applying CV scoring rules based on norepi+epi dose tiers and pressor combinations...")
    cv_score = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN map_agg m USING (hospitalization_id, start_dttm)
        LEFT JOIN pressor_agg v USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , m.map_min
            , m.map_min_dttm_offset
            , COALESCE(v.norepi_epi_maxsum, 0) AS norepi_epi_maxsum
            , v.norepi_epi_maxsum_dttm_offset
            , COALESCE(v.dopa_max, 0) AS dopa_max
            , v.dopa_max_dttm_offset
            , COALESCE(v.has_other_non_dopa, 0) AS has_other_non_dopa
            -- Composite "other" flag (dopamine OR other non-dopa)
            , CASE WHEN COALESCE(v.dopa_max, 0) > 0 OR COALESCE(v.has_other_non_dopa, 0) = 1
                   THEN 1 ELSE 0 END AS has_other_vaso
            , cv: CASE
                -- Footnote l: Dopamine-only scoring (no norepi/epi, no other pressors)
                WHEN norepi_epi_maxsum = 0 AND has_other_non_dopa = 0 AND dopa_max > 40 THEN 4
                WHEN norepi_epi_maxsum = 0 AND has_other_non_dopa = 0 AND dopa_max > 20 THEN 3
                WHEN norepi_epi_maxsum = 0 AND has_other_non_dopa = 0 AND dopa_max > 0 THEN 2
                -- Standard norepi/epi scoring
                WHEN norepi_epi_maxsum > 0.4 THEN 4
                WHEN norepi_epi_maxsum > 0.2 AND has_other_vaso = 1 THEN 4
                WHEN norepi_epi_maxsum > 0.2 THEN 3
                WHEN norepi_epi_maxsum > 0 AND has_other_vaso = 1 THEN 3
                WHEN norepi_epi_maxsum > 0 THEN 2
                WHEN has_other_vaso = 1 THEN 2
                -- No pressor
                WHEN map_min < 70 THEN 1
                WHEN map_min >= 70 THEN 0
                ELSE NULL
            END
    """)

    logger.info("Cardiovascular subscore complete")

    if dev:
        return cv_score, {
            'map_agg': map_agg,
            'pressor_at_start': pressor_at_start,
            'pressor_in_window': pressor_in_window,
            'pressor_at_end': pressor_at_end,
            'pressor_events_raw': pressor_events_raw,
            'pressor_events_deduped': pressor_events_deduped,
            'pressor_events': pressor_events,
            'epi_ne_wide': epi_ne_wide,
            'epi_ne_filled': epi_ne_filled,
            'epi_ne_duration': epi_ne_duration,
            'epi_ne_agg': epi_ne_agg,
            'other_pressor_duration': other_pressor_duration,
            'other_pressor_agg': other_pressor_agg,
            'pressor_agg': pressor_agg,
        }

    return cv_score
