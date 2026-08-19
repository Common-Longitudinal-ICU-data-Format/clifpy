"""Rolling hemostasis subscore calculation for SOFA-2.

Event-driven approach: processes all platelet observations chronologically,
scores each, and emits rows only when:
1. The subscore changes (score_change)
2. A new worst-ever platelet value is observed (new_worst_value)
3. The score expires due to no new observations (expiry)

Scoring thresholds (same as windowed, shared via _hemo_score_sql):
- Platelets > 150 × 10³/µL: 0 points
- Platelets ≤ 150 × 10³/µL: 1 point
- Platelets ≤ 100 × 10³/µL: 2 points
- Platelets ≤ 80 × 10³/µL: 3 points
- Platelets ≤ 50 × 10³/µL: 4 points
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from .._utils import _hemo_score_sql
from ._config import RollingSOFA2Config
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.rolling.hemo')


def _calculate_rolling_hemo(
    cohort_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    cfg: RollingSOFA2Config,
    *,
    dev: bool = False,
    id_name: str = 'hospitalization_id',
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """Calculate event-driven rolling hemostasis subscore.

    Processes all platelet_count observations chronologically per
    hospitalization. Emits a row when the subscore changes, when a
    new worst-ever platelet value is observed (even if the score stays
    the same), or when the score expires after hemo_expiry_hours with
    no new observation.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Observation periods with columns [id_name, start_dttm, end_dttm].
        One row per hospitalization defining the temporal bounds.
        Observations outside [start_dttm, end_dttm] are ignored.
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs) with lab_category = 'platelet_count'.
    cfg : RollingSOFA2Config
        Configuration with hemo_expiry_hours.
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging.
    id_name : str
        Identity column name. Default 'hospitalization_id'.

    Returns
    -------
    DuckDBPyRelation
        Columns: [id_name, event_dttm, platelet_count, platelet_worst_ever,
                  sofa2_hemo, emit_reason,
                  sofa2_resp(NULL), sofa2_cv(NULL), sofa2_brain(NULL),
                  sofa2_liver(NULL), sofa2_kidney(NULL)]
        Sorted by (id_name, event_dttm).
        Only rows where score changed, worst-ever updated, or expiry fired.
    """
    logger.info("Calculating rolling hemostasis subscore...")
    expiry_hours = cfg.hemo_expiry_hours
    logger.info(f"hemo_expiry_hours={expiry_hours}")

    # Step 1: Filter platelet observations to cohort bounds
    logger.info("Filtering platelet observations to cohort bounds...")
    filtered_obs = duckdb.sql(f"""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.{id_name} = c.{id_name}
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.{id_name}
            , t.lab_collect_dttm AS event_dttm
            , t.lab_value_numeric AS platelet_count
            , c.end_dttm
        WHERE t.lab_category = 'platelet_count'
    """)

    # Step 2: Pre-aggregate same-timestamp observations (MIN = worst)
    logger.info("Pre-aggregating same-timestamp platelet observations...")
    aggregated_obs = duckdb.sql(f"""
        FROM filtered_obs
        SELECT
            {id_name}
            , event_dttm
            , MIN(platelet_count) AS platelet_count
            , MAX(end_dttm) AS end_dttm
        GROUP BY {id_name}, event_dttm
    """)

    # Step 3: Score each observation + compute running worst-ever
    logger.info("Scoring observations and tracking worst-ever values...") # TODO: review this clause
    scored_obs = duckdb.sql(f"""
        FROM aggregated_obs
        SELECT
            {id_name}
            , event_dttm
            , platelet_count
            , MIN(platelet_count) OVER (
                PARTITION BY {id_name}
                ORDER BY event_dttm
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS platelet_worst_ever
            , {_hemo_score_sql('platelet_count')}
            , end_dttm
    """)

    # Step 4: Generate synthetic expiry events (if expiry is enabled) TODO: review this clause
    if expiry_hours is not None:
        logger.info(f"Generating expiry events (expiry after {expiry_hours}h)...")
        expiry_events = duckdb.sql(f"""
            FROM (
                FROM scored_obs
                SELECT
                    {id_name}
                    , event_dttm
                    , platelet_worst_ever
                    , end_dttm
                    , event_dttm + INTERVAL '{expiry_hours} hours' AS expiry_dttm
                    , LEAD(event_dttm) OVER (
                        PARTITION BY {id_name}
                        ORDER BY event_dttm
                    ) AS next_event_dttm
            )
            SELECT
                {id_name}
                , expiry_dttm AS event_dttm
                , NULL::DOUBLE AS platelet_count
                , platelet_worst_ever
                , NULL::INTEGER AS sofa2_hemo
                , end_dttm
            WHERE expiry_dttm <= end_dttm
                AND (next_event_dttm IS NULL OR expiry_dttm < next_event_dttm)
        """)

        # Step 5: UNION ALL real + expiry events
        logger.info("Combining real observations and expiry events...")
        all_events = duckdb.sql(f"""
            FROM scored_obs
            SELECT {id_name}, event_dttm, platelet_count, platelet_worst_ever, sofa2_hemo, end_dttm
            UNION ALL
            FROM expiry_events
            SELECT {id_name}, event_dttm, platelet_count, platelet_worst_ever, sofa2_hemo, end_dttm
        """)
    else:
        logger.info("Expiry disabled, using scored observations directly...")
        all_events = duckdb.sql(f"""
            FROM scored_obs
            SELECT {id_name}, event_dttm, platelet_count, platelet_worst_ever, sofa2_hemo, end_dttm
        """)

    # Step 6-8: Change detection (dual trigger) + emit_reason classification
    logger.info("Detecting score changes and worst-ever updates...")
    result = duckdb.sql(f"""
        WITH with_lag AS (
            FROM all_events
            SELECT
                {id_name}
                , event_dttm
                , platelet_count
                , platelet_worst_ever
                , sofa2_hemo
                , LAG(sofa2_hemo) OVER w AS prev_score
                , LAG(platelet_worst_ever) OVER w AS prev_worst
            WINDOW w AS (PARTITION BY {id_name} ORDER BY event_dttm)
        ),
        filtered AS (
            FROM with_lag
            SELECT *
            WHERE sofa2_hemo IS DISTINCT FROM prev_score
               OR platelet_worst_ever IS DISTINCT FROM prev_worst
        )
        FROM filtered
        SELECT
            {id_name}
            , event_dttm
            , platelet_count
            , platelet_worst_ever
            , sofa2_hemo
            , emit_reason: CASE
                WHEN prev_score IS NULL THEN 'first_obs'
                WHEN platelet_count IS NULL THEN 'expiry'
                WHEN sofa2_hemo IS DISTINCT FROM prev_score THEN 'score_change'
                ELSE 'new_worst_value'
            END
            , NULL::INTEGER AS sofa2_resp
            , NULL::INTEGER AS sofa2_cv
            , NULL::INTEGER AS sofa2_brain
            , NULL::INTEGER AS sofa2_liver
            , NULL::INTEGER AS sofa2_kidney
        ORDER BY {id_name}, event_dttm
    """)

    logger.info("Rolling hemostasis subscore complete")

    if dev:
        intermediates = {
            'filtered_obs': filtered_obs,
            'aggregated_obs': aggregated_obs,
            'scored_obs': scored_obs,
            'all_events': all_events,
        }
        if expiry_hours is not None:
            intermediates['expiry_events'] = expiry_events
        return result, intermediates

    return result
