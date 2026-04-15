"""Kidney subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec, creatinine in mg/dL, UO in mL/kg/h):
- Creatinine ≤ 1.20 mg/dL: 0 points
- Creatinine > 1.20 mg/dL OR UO < 0.5 for 6-12h: 1 point
- Creatinine > 2.0 mg/dL OR UO < 0.5 for ≥12h: 2 points
- Creatinine > 3.50 mg/dL OR UO < 0.3 for ≥24h OR anuria ≥12h: 3 points
- RRT or meets RRT criteria: 4 points

Final kidney score = MAX(creatinine_score, uo_score), with RRT override to 4.

Special rules:
- Footnote p: Score 4 if patient meets RRT criteria even without RRT
- RRT criteria: (creatinine > 1.2 OR oliguria <0.3 for >6h)
    AND (potassium >= 6.0 OR (pH <= 7.20 AND bicarbonate <= 12.0))

UO rate calculation (adapted from MIMIC reference):
- Net UO = output (urine) - input (flush_irrigation_urine)
- Trailing rates at each measurement point using self-join (MIMIC pattern)
- Rate = volume / weight / observation_hours (valid only when obs_hours >= threshold)
- Weight matched via ASOF JOIN to most recent weight_kg

Reviewers:
- Zewei (Whiskey) on 2026-02-01.
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config, _flag_rrt
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.kidney')

# Lab categories for kidney subscore
KIDNEY_LAB_CATEGORIES = ['creatinine', 'potassium', 'ph_arterial', 'ph_venous', 'bicarbonate']


def _calculate_uo_score(
    cohort_rel: DuckDBPyRelation,
    output_rel: DuckDBPyRelation,
    input_rel: DuckDBPyRelation,
    weight_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    id_name: str = 'hospitalization_id',
    dev: bool = False,
) -> DuckDBPyRelation | None | tuple[DuckDBPyRelation, dict]:
    """
    Calculate urine output-based kidney score (0-3) per scoring window.

    Adapted from MIMIC reference SQL. Uses self-join trailing accumulation
    to compute UO rates over 6h/12h/24h periods at each measurement point.

    LAG is computed globally per patient (not per scoring window) to ensure
    correct tm_since_last_uo at window boundaries. Rows are only associated
    with scoring windows after LAG has been computed.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with [id_name, start_dttm, end_dttm]
    output_rel : DuckDBPyRelation
        CLIF output table filtered to output_group='urine'
    input_rel : DuckDBPyRelation
        CLIF input table filtered to input_category='flush_irrigation_urine'
    weight_rel : DuckDBPyRelation
        Vitals filtered to vital_category='weight_kg' with [id_name, recorded_dttm, vital_value]
    cfg : SOFA2Config
        Configuration with uo_first_measurement_baseline and uo_prewindow_lookback_hours
    id_name : str
        Identity column name
    dev : bool, default False
        If True, return (uo_scored, intermediates_dict) for inspecting per-anchor
        intermediate relations (net_uo, with_tm, per_window_events, trailing_uo,
        with_weight).

    Returns
    -------
    DuckDBPyRelation or None or tuple
        Columns: [id_name, start_dttm, uo_score, uo_rate_6hr, uo_rate_12hr,
                  uo_rate_24hr, has_uo_oliguria, weight_at_uo]
        Returns None if output_rel has no data (UO scoring skipped).
        Returns tuple when dev=True.
    """
    # Short-circuit if no output data available
    if output_rel.count('*').fetchone()[0] == 0:
        logger.info("No urine output data available — UO scoring skipped")
        return None

    baseline = cfg.uo_first_measurement_baseline
    if baseline not in ('window_start', 'first_measurement'):
        raise ValueError(f"Invalid uo_first_measurement_baseline: {baseline!r}")

    lookback_hours = cfg.uo_prewindow_lookback_hours
    if lookback_hours <= 24:
        raise ValueError(
            f"uo_prewindow_lookback_hours must be > 24 to ensure boundary "
            f"rows have a real LAG predecessor, got {lookback_hours}"
        )

    # Per-patient cohort span — used to bound the UO loading window.
    # Each patient gets: [min(start_dttm) - lookback, max(end_dttm)]
    # The lookback provides room for LAG to find real predecessors at boundaries.
    logger.info(
        f"Computing per-patient cohort span with {lookback_hours}h prewindow lookback..."
    )
    patient_span = duckdb.sql(f"""
        FROM cohort_rel
        SELECT
            {id_name}
            , MIN(start_dttm) AS earliest_start_dttm
            , MAX(end_dttm) AS latest_end_dttm
            , MIN(start_dttm) - INTERVAL '{lookback_hours} hours' AS load_lower_bound
        GROUP BY {id_name}
    """)

    # Step A (refactored): Collect per-patient UO events from the loading window.
    # NO cohort join yet — we operate on patient-level data to compute LAG globally.
    # Net = output_volume - input_volume (irrigation flushes negate UO).
    logger.info("Collecting per-patient UO events (output - irrigation) ...")
    uo_events = duckdb.sql(f"""
        FROM output_rel t
        JOIN patient_span p USING ({id_name})
        SELECT
            t.{id_name}
            , p.earliest_start_dttm
            , t.recorded_dttm
            , t.output_volume AS volume
        WHERE t.recorded_dttm >= p.load_lower_bound
            AND t.recorded_dttm <= p.latest_end_dttm

        UNION ALL

        FROM input_rel t
        JOIN patient_span p USING ({id_name})
        SELECT
            t.{id_name}
            , p.earliest_start_dttm
            , t.recorded_dttm
            , -t.input_volume AS volume
        WHERE t.recorded_dttm >= p.load_lower_bound
            AND t.recorded_dttm <= p.latest_end_dttm
    """)

    # Step B (refactored): Net volume per (patient, timestamp).
    # No start_dttm in GROUP BY — this is patient-level, pre-window-association.
    logger.info("Aggregating net UO volume per (patient, timestamp)...")
    net_uo = duckdb.sql(f"""
        FROM uo_events
        SELECT
            {id_name}
            , earliest_start_dttm
            , recorded_dttm
            , SUM(volume) AS net_output
        GROUP BY {id_name}, earliest_start_dttm, recorded_dttm
    """)

    # Step C (refactored): Global per-patient LAG for tm_since_last_uo.
    # Partition by id_name only (NOT by scoring window) — the patient's first
    # row gets the baseline, every subsequent row gets correct LAG regardless
    # of which window it will eventually belong to.
    if baseline == 'window_start':
        # tm from patient's earliest cohort start, clamped to 0 if negative
        # (pre-cohort buffer rows shouldn't contribute "negative" observation time)
        first_uo_expr = (
            "GREATEST(DATEDIFF('minute', earliest_start_dttm, recorded_dttm), 0)"
        )
    else:  # 'first_measurement'
        first_uo_expr = "0"

    logger.info(f"Computing global LAG-based tm_since_last_uo (baseline={baseline})...")
    with_tm = duckdb.sql(f"""
        FROM net_uo
        SELECT
            {id_name}
            , recorded_dttm
            , tm_since_last_uo_min: CASE
                WHEN LAG(recorded_dttm) OVER w IS NULL
                    THEN {first_uo_expr}
                ELSE DATEDIFF('minute', LAG(recorded_dttm) OVER w, recorded_dttm)
            END
            -- Zero volume for first measurement when tm = 0 (unknowable collection period).
            -- The timestamp is preserved as a LAG predecessor for the next row.
            -- When window_start gives tm > 0 (in-window first row), volume is kept.
            , net_output: CASE
                WHEN LAG(recorded_dttm) OVER w IS NULL AND {first_uo_expr} = 0
                    THEN 0
                ELSE net_output
            END
        WINDOW w AS (PARTITION BY {id_name} ORDER BY recorded_dttm)
    """)

    # Step C2: Associate rows with scoring windows.
    # Each (patient, recorded_dttm) row joins to all windows for that patient
    # where recorded_dttm falls within [start_dttm - 24h, end_dttm]. The 24h
    # slack is the actual trailing-bucket cutoff (Step D's DATEDIFF ≤ 23).
    # Rows between (start - lookback_hours) and (start - 24h) exist only as
    # LAG predecessors and are dropped here.
    logger.info("Associating rows with scoring windows...")
    per_window_events = duckdb.sql(f"""
        FROM with_tm t
        JOIN cohort_rel c ON
            t.{id_name} = c.{id_name}
            AND t.recorded_dttm >= c.start_dttm - INTERVAL '24 hours'
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.{id_name}
            , c.start_dttm
            , c.end_dttm
            , t.recorded_dttm
            , t.net_output
            , t.tm_since_last_uo_min
    """)

    # Step D: Self-join trailing accumulation (MIMIC pattern)
    # For each in-window measurement, sum volumes + observation times over trailing 6h/12h/24h.
    # NOTE: Uses N-1 hours (5/11/23) following MIMIC convention — each measurement represents
    # ~1h of collection, so a 6h window uses <= 5h difference.
    # Passthrough columns: io.net_output and io.tm_since_last_uo_min are preserved as
    # per-anchor values (constant within each group) to support intermediate testing
    # of Step B/C outputs without a separate join.
    logger.info("Computing trailing 6h/12h/24h UO volumes and observation times...")
    trailing_uo = duckdb.sql(f"""
        FROM per_window_events io
        LEFT JOIN per_window_events iosum
            ON io.{id_name} = iosum.{id_name}
            AND io.start_dttm = iosum.start_dttm
            AND io.recorded_dttm >= iosum.recorded_dttm
            AND io.recorded_dttm <= iosum.recorded_dttm + INTERVAL '23 hours'
        SELECT
            io.{id_name}
            , io.start_dttm
            , io.end_dttm
            , io.recorded_dttm
            , io.net_output
            , io.tm_since_last_uo_min
            -- 6h trailing
            , SUM(CASE WHEN DATEDIFF('hour', iosum.recorded_dttm, io.recorded_dttm) <= 5
                THEN iosum.net_output END) AS uo_vol_6hr
            , SUM(CASE WHEN DATEDIFF('hour', iosum.recorded_dttm, io.recorded_dttm) <= 5
                THEN iosum.tm_since_last_uo_min END) / 60.0 AS uo_tm_6hr
            -- 12h trailing
            , SUM(CASE WHEN DATEDIFF('hour', iosum.recorded_dttm, io.recorded_dttm) <= 11
                THEN iosum.net_output END) AS uo_vol_12hr
            , SUM(CASE WHEN DATEDIFF('hour', iosum.recorded_dttm, io.recorded_dttm) <= 11
                THEN iosum.tm_since_last_uo_min END) / 60.0 AS uo_tm_12hr
            -- 24h trailing
            , SUM(iosum.net_output) AS uo_vol_24hr
            , SUM(iosum.tm_since_last_uo_min) / 60.0 AS uo_tm_24hr
        WHERE io.recorded_dttm >= io.start_dttm
            AND io.recorded_dttm <= io.end_dttm
        GROUP BY io.{id_name}, io.start_dttm, io.end_dttm, io.recorded_dttm,
                 io.net_output, io.tm_since_last_uo_min
    """)

    # Step E: ASOF JOIN to weight for mL/kg/h calculation
    logger.info("Matching weight via ASOF JOIN for rate calculation...")
    with_weight = duckdb.sql(f"""
        FROM trailing_uo tr
        ASOF LEFT JOIN weight_rel w
            ON tr.{id_name} = w.{id_name}
            AND w.recorded_dttm <= tr.recorded_dttm
        SELECT
            tr.*
            , w.vital_value AS weight_kg
            -- Rates: only valid when observation_hours >= threshold AND weight > 0
            , CASE WHEN uo_tm_6hr >= 6 AND w.vital_value > 0
                THEN uo_vol_6hr / w.vital_value / uo_tm_6hr END AS uo_rate_6hr
            , CASE WHEN uo_tm_12hr >= 12 AND w.vital_value > 0
                THEN uo_vol_12hr / w.vital_value / uo_tm_12hr END AS uo_rate_12hr
            , CASE WHEN uo_tm_24hr >= 24 AND w.vital_value > 0
                THEN uo_vol_24hr / w.vital_value / uo_tm_24hr END AS uo_rate_24hr
    """)

    # Step F: Per-window aggregation — take worst (min) rates, score 0-3
    logger.info("Aggregating per-window UO scores...")
    uo_scored = duckdb.sql(f"""
        WITH uo_per_window AS (
            FROM with_weight
            SELECT
                {id_name}
                , start_dttm
                -- Worst rates across all in-window measurement points
                , MIN(uo_rate_6hr) AS min_rate_6hr
                , MIN(uo_rate_12hr) AS min_rate_12hr
                , MIN(uo_rate_24hr) AS min_rate_24hr
                -- Anuria check: minimum 12h trailing volume (valid observations only)
                , MIN(uo_vol_12hr) FILTER (uo_tm_12hr >= 12) AS min_uo_vol_12hr
                -- Weight at the measurement with worst 6h rate (for output)
                , ARG_MIN(weight_kg, COALESCE(uo_rate_6hr, 999)) AS weight_at_uo
            GROUP BY {id_name}, start_dttm
        )
        FROM uo_per_window
        SELECT
            {id_name}
            , start_dttm
            , uo_score: CASE
                -- Score 3: UO < 0.3 mL/kg/h for ≥24h
                WHEN min_rate_24hr IS NOT NULL AND min_rate_24hr < 0.3 THEN 3
                -- Score 3: anuria (0 mL) for ≥12h
                WHEN min_uo_vol_12hr IS NOT NULL AND min_uo_vol_12hr <= 0 THEN 3
                -- Score 2: UO < 0.5 mL/kg/h for ≥12h
                WHEN min_rate_12hr IS NOT NULL AND min_rate_12hr < 0.5 THEN 2
                -- Score 1: UO < 0.5 mL/kg/h for 6-12h (meets 6h but NOT 12h threshold)
                WHEN min_rate_6hr IS NOT NULL AND min_rate_6hr < 0.5
                     AND (min_rate_12hr IS NULL OR min_rate_12hr >= 0.5) THEN 1
                ELSE 0
            END
            -- Footnote p: oliguria (<0.3 mL/kg/h for >6h)
            , has_uo_oliguria: CASE
                WHEN min_rate_6hr IS NOT NULL AND min_rate_6hr < 0.3 THEN 1
                ELSE 0
            END
            , min_rate_6hr AS uo_rate_6hr
            , min_rate_12hr AS uo_rate_12hr
            , min_rate_24hr AS uo_rate_24hr
            , weight_at_uo
    """)

    if dev:
        return uo_scored, {
            'patient_span': patient_span,
            'net_uo': net_uo,
            'with_tm': with_tm,
            'per_window_events': per_window_events,
            'trailing_uo': trailing_uo,
            'with_weight': with_weight,
        }

    return uo_scored


def _calculate_kidney_subscore(
    cohort_rel: DuckDBPyRelation,
    labs_rel: DuckDBPyRelation,
    crrt_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    output_rel: DuckDBPyRelation | None = None,
    input_rel: DuckDBPyRelation | None = None,
    weight_rel: DuckDBPyRelation | None = None,
    dev: bool = False,
    id_name: str = 'hospitalization_id',
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 kidney subscore based on creatinine, urine output, and RRT.

    Implements pre-window lookback with fallback pattern for ALL kidney labs
    (creatinine, potassium, pH, bicarbonate).
    Also implements urine output-based scoring (scores 1-3) and
    footnote p: RRT criteria fallback including oliguria.

    Final score = MAX(creatinine_score, uo_score), with RRT override to 4.

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [id_name, start_dttm, end_dttm]
    labs_rel : DuckDBPyRelation
        Labs table (CLIF labs)
    crrt_rel : DuckDBPyRelation
        CRRT therapy table (CLIF crrt_therapy)
    cfg : SOFA2Config
        Configuration with kidney_lookback_hours, uo_first_measurement_baseline
    output_rel : DuckDBPyRelation, optional
        CLIF output table filtered to urine. If None, UO scoring skipped.
    input_rel : DuckDBPyRelation, optional
        CLIF input table filtered to flush_irrigation_urine. If None, no irrigation subtraction.
    weight_rel : DuckDBPyRelation, optional
        Vitals filtered to weight_kg. Required for UO rate calculation (mL/kg/h).
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging
    id_name : str
        Identity column name. Default 'hospitalization_id'.

    Returns
    -------
    DuckDBPyRelation
        Columns: [id_name, start_dttm, creatinine, creatinine_dttm_offset, ...,
                  uo_score, uo_rate_6hr, uo_rate_12hr, uo_rate_24hr,
                  has_uo_oliguria, weight_at_uo, sofa2_kidney]
        Where sofa2_kidney = MAX(creatinine_score, uo_score) with RRT override.
    """
    logger.info("Calculating kidney subscore...")

    lookback_hours = cfg.kidney_lookback_hours
    lab_categories = KIDNEY_LAB_CATEGORIES
    logger.info(f"kidney_lookback_hours={lookback_hours}")

    # Step 1: Get RRT flag (in-window only)
    logger.info("Flagging RRT for automatic score 4 override...")
    rrt_flag = _flag_rrt(cohort_rel, crrt_rel, id_name=id_name)

    # Step 2: Get in-window lab values for kidney with offsets from start_dttm
    # MAX creatinine/potassium = worst, MIN pH/bicarbonate = worst
    # Offset = lab_collect_dttm - start_dttm (positive for in-window)
    logger.info("Collecting in-window creatinine, potassium, pH, bicarbonate...")
    labs_in_window = duckdb.sql(f"""
        FROM labs_rel t
        JOIN cohort_rel c ON
            t.{id_name} = c.{id_name}
            AND t.lab_collect_dttm >= c.start_dttm
            AND t.lab_collect_dttm <= c.end_dttm
        SELECT
            t.{id_name}
            , c.start_dttm
            , MAX(lab_value_numeric) FILTER(lab_category = 'creatinine') AS creatinine
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'creatinine') - c.start_dttm AS creatinine_dttm_offset
            , MAX(lab_value_numeric) FILTER(lab_category = 'potassium') AS potassium
            , ARG_MAX(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'potassium') - c.start_dttm AS potassium_dttm_offset
            , MIN(lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) AS ph
            , ARG_MIN(lab_category, lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) AS ph_type
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category IN ('ph_arterial', 'ph_venous')) - c.start_dttm AS ph_dttm_offset
            , MIN(lab_value_numeric) FILTER(lab_category = 'bicarbonate') AS bicarbonate
            , ARG_MIN(lab_collect_dttm, lab_value_numeric) FILTER(lab_category = 'bicarbonate') - c.start_dttm AS bicarbonate_dttm_offset
        WHERE t.lab_category IN {tuple(lab_categories)}
        GROUP BY t.{id_name}, c.start_dttm
    """)

    # Step 3: Get pre-window lab values using CROSS JOIN + ASOF JOIN pattern
    # This is more compact than separate ASOF JOINs per lab category
    # ASOF returns single closest record, so offset is directly available
    # Offset = lab_collect_dttm - start_dttm (negative for pre-window)
    logger.info("Looking back for pre-window labs to handle missing in-window data...")
    labs_pre_window_long = duckdb.sql(f"""
        WITH lab_cats AS (
            SELECT UNNEST({lab_categories}::VARCHAR[]) AS lab_category
        ),
        cohort_labs AS (
            FROM cohort_rel c
            CROSS JOIN lab_cats
            SELECT c.{id_name}, c.start_dttm, lab_category
        )
        FROM cohort_labs cl
        ASOF LEFT JOIN labs_rel t
            ON cl.{id_name} = t.{id_name}
            AND cl.lab_category = t.lab_category
            AND cl.start_dttm > t.lab_collect_dttm
        SELECT
            cl.{id_name}
            , cl.start_dttm
            , cl.lab_category
            , t.lab_value_numeric AS lab_value
            , lab_dttm_offset: t.lab_collect_dttm - cl.start_dttm
        WHERE t.{id_name} IS NOT NULL
            AND lab_dttm_offset >= -INTERVAL '{lookback_hours} hours'
    """)

    # Step 4: Pivot pre-window labs to wide format using conditional aggregation
    # NOTE: Dynamic PIVOT (ON lab_category) only creates columns for values present
    # in the data. If a category has no rows, the column is missing → Binder Error.
    # Explicit FILTER aggregation always creates all columns (NULL when no data).
    logger.info("Pivoting pre-window labs to enable per-lab fallback pattern...")
    labs_pre_window = duckdb.sql(f"""
        FROM labs_pre_window_long
        SELECT
            {id_name}
            , start_dttm
            , ANY_VALUE(lab_value) FILTER(lab_category = 'creatinine') AS creatinine_val
            , ANY_VALUE(lab_dttm_offset) FILTER(lab_category = 'creatinine') AS creatinine_dttm_offset
            , ANY_VALUE(lab_value) FILTER(lab_category = 'potassium') AS potassium_val
            , ANY_VALUE(lab_dttm_offset) FILTER(lab_category = 'potassium') AS potassium_dttm_offset
            , ANY_VALUE(lab_value) FILTER(lab_category = 'ph_arterial') AS ph_arterial_val
            , ANY_VALUE(lab_dttm_offset) FILTER(lab_category = 'ph_arterial') AS ph_arterial_dttm_offset
            , ANY_VALUE(lab_value) FILTER(lab_category = 'ph_venous') AS ph_venous_val
            , ANY_VALUE(lab_dttm_offset) FILTER(lab_category = 'ph_venous') AS ph_venous_dttm_offset
            , ANY_VALUE(lab_value) FILTER(lab_category = 'bicarbonate') AS bicarbonate_val
            , ANY_VALUE(lab_dttm_offset) FILTER(lab_category = 'bicarbonate') AS bicarbonate_dttm_offset
        GROUP BY {id_name}, start_dttm
    """)

    # Step 5: Apply fallback pattern for ALL labs (values and offsets)
    # Use pre-window value only if no in-window value exists for that specific lab
    logger.info("Applying fallback: use pre-window only when in-window is missing...")

    labs_with_fallback = duckdb.sql(f"""
        FROM cohort_rel c
        LEFT JOIN labs_in_window l USING ({id_name}, start_dttm)
        LEFT JOIN labs_pre_window p USING ({id_name}, start_dttm)
        SELECT
            c.{id_name}
            , c.start_dttm
            -- Apply fallback per-lab: use pre-window only if in-window is NULL
            , COALESCE(l.creatinine, p.creatinine_val) AS creatinine
            , COALESCE(l.creatinine_dttm_offset, p.creatinine_dttm_offset) AS creatinine_dttm_offset
            , COALESCE(l.potassium, p.potassium_val) AS potassium
            , COALESCE(l.potassium_dttm_offset, p.potassium_dttm_offset) AS potassium_dttm_offset
            , COALESCE(l.ph, LEAST(p.ph_arterial_val, p.ph_venous_val)) AS ph
            , COALESCE(l.ph_type,
                CASE WHEN p.ph_arterial_val IS NULL AND p.ph_venous_val IS NULL THEN NULL
                     WHEN p.ph_arterial_val <= p.ph_venous_val OR p.ph_venous_val IS NULL
                     THEN 'ph_arterial' ELSE 'ph_venous' END) AS ph_type
            , COALESCE(l.ph_dttm_offset,
                CASE WHEN p.ph_arterial_val IS NULL AND p.ph_venous_val IS NULL THEN NULL
                     WHEN p.ph_arterial_val <= p.ph_venous_val OR p.ph_venous_val IS NULL
                     THEN p.ph_arterial_dttm_offset ELSE p.ph_venous_dttm_offset END) AS ph_dttm_offset
            , COALESCE(l.bicarbonate, p.bicarbonate_val) AS bicarbonate
            , COALESCE(l.bicarbonate_dttm_offset, p.bicarbonate_dttm_offset) AS bicarbonate_dttm_offset
    """)

    # Step 6: Calculate UO score (if output data available)
    uo_result = None
    uo_intermediates: dict | None = None
    if output_rel is not None and weight_rel is not None:
        logger.info("Calculating urine output-based score...")
        # Create empty input_rel sentinel if input table not available
        if input_rel is None:
            input_rel = duckdb.sql(f"""
                SELECT
                    NULL::VARCHAR AS {id_name}
                    , NULL::TIMESTAMP AS recorded_dttm
                    , NULL::DOUBLE AS input_volume
                    , NULL::VARCHAR AS input_category
                WHERE false
            """)
        if dev:
            uo_out = _calculate_uo_score(
                cohort_rel, output_rel, input_rel, weight_rel, cfg,
                id_name=id_name, dev=True,
            )
            if uo_out is not None:
                uo_result, uo_intermediates = uo_out
        else:
            uo_result = _calculate_uo_score(
                cohort_rel, output_rel, input_rel, weight_rel, cfg, id_name=id_name,
            )
    else:
        logger.info("UO scoring skipped (no output or weight data)")

    # Step 7: Combine creatinine + UO + RRT into final kidney score
    # kidney_score = MAX(creatinine_score, uo_score) with RRT override to 4
    # Footnote p updated: (creatinine > 1.2 OR oliguria) AND (K >= 6.0 OR acidosis)
    logger.info("Scoring kidney with creatinine + UO + RRT override + footnote p...")

    if uo_result is not None:
        kidney_score = duckdb.sql(f"""
            FROM labs_with_fallback l
            LEFT JOIN rrt_flag r USING ({id_name}, start_dttm)
            LEFT JOIN uo_result uo USING ({id_name}, start_dttm)
            SELECT
                l.{id_name}
                , l.start_dttm
                , l.creatinine
                , l.creatinine_dttm_offset
                , l.potassium
                , l.potassium_dttm_offset
                , l.ph
                , l.ph_type
                , l.ph_dttm_offset
                , l.bicarbonate
                , l.bicarbonate_dttm_offset
                , COALESCE(r.has_rrt, 0) AS has_rrt
                , r.rrt_dttm_offset
                -- UO columns
                , uo.uo_score
                , uo.uo_rate_6hr
                , uo.uo_rate_12hr
                , uo.uo_rate_24hr
                , COALESCE(uo.has_uo_oliguria, 0) AS has_uo_oliguria
                , uo.weight_at_uo
                -- Footnote p: RRT criteria met — now includes oliguria path
                , rrt_criteria_met: CASE
                    WHEN (l.creatinine > 1.2 OR COALESCE(uo.has_uo_oliguria, 0) = 1)
                         AND (l.potassium >= 6.0
                              OR (l.ph <= 7.20 AND l.bicarbonate <= 12.0))
                    THEN 1 ELSE 0 END
                -- Creatinine-based score (intermediate, for GREATEST with UO)
                , creat_score: CASE
                    WHEN l.creatinine > 3.50 THEN 3
                    WHEN l.creatinine > 2.0 THEN 2
                    WHEN l.creatinine > 1.20 THEN 1
                    WHEN l.creatinine <= 1.20 THEN 0
                    ELSE NULL
                END
                -- Final kidney score: RRT > GREATEST(creatinine, UO)
                , sofa2_kidney: CASE
                    WHEN r.has_rrt = 1 THEN 4
                    WHEN rrt_criteria_met = 1 THEN 4
                    ELSE GREATEST(creat_score, COALESCE(uo.uo_score, 0))
                END
        """)
    else:
        # No UO data: creatinine-only scoring (existing behavior)
        kidney_score = duckdb.sql(f"""
            FROM labs_with_fallback l
            LEFT JOIN rrt_flag r USING ({id_name}, start_dttm)
            SELECT
                l.{id_name}
                , l.start_dttm
                , l.creatinine
                , l.creatinine_dttm_offset
                , l.potassium
                , l.potassium_dttm_offset
                , l.ph
                , l.ph_type
                , l.ph_dttm_offset
                , l.bicarbonate
                , l.bicarbonate_dttm_offset
                , COALESCE(r.has_rrt, 0) AS has_rrt
                , r.rrt_dttm_offset
                -- UO columns (all NULL when no UO data)
                , NULL::INTEGER AS uo_score
                , NULL::DOUBLE AS uo_rate_6hr
                , NULL::DOUBLE AS uo_rate_12hr
                , NULL::DOUBLE AS uo_rate_24hr
                , 0::INTEGER AS has_uo_oliguria
                , NULL::DOUBLE AS weight_at_uo
                -- Footnote p: creatinine-only (no oliguria data)
                , rrt_criteria_met: CASE
                    WHEN l.creatinine > 1.2
                         AND (l.potassium >= 6.0
                              OR (l.ph <= 7.20 AND l.bicarbonate <= 12.0))
                    THEN 1 ELSE 0 END
                -- Creatinine-based score (same formula as UO path, for column parity)
                , creat_score: CASE
                    WHEN l.creatinine > 3.50 THEN 3
                    WHEN l.creatinine > 2.0 THEN 2
                    WHEN l.creatinine > 1.20 THEN 1
                    WHEN l.creatinine <= 1.20 THEN 0
                    ELSE NULL
                END
                , sofa2_kidney: CASE
                    WHEN r.has_rrt = 1 THEN 4
                    WHEN rrt_criteria_met = 1 THEN 4
                    ELSE creat_score
                END
        """)

    logger.info("Kidney subscore complete")

    if dev:
        intermediates = {
            'rrt_flag': rrt_flag,
            'labs_in_window': labs_in_window,
            'labs_pre_window_long': labs_pre_window_long,
            'labs_pre_window': labs_pre_window,
            'labs_with_fallback': labs_with_fallback,
        }
        if uo_result is not None:
            intermediates['uo_result'] = uo_result
        if uo_intermediates is not None:
            # Namespace the UO intermediates under 'uo_*' keys
            for k, v in uo_intermediates.items():
                intermediates[f'uo_{k}'] = v
        return kidney_score, intermediates

    return kidney_score
