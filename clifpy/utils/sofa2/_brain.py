"""Brain subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec):
- GCS 15: 0 points (unless delirium drug -> 1 point per footnote e)
- GCS 13-14: 1 point
- GCS 9-12: 2 points
- GCS 6-8: 3 points
- GCS 3-5: 4 points

Special rules:
- Footnote c: For sedated patients, GCS measurements during sedation episodes
  (plus post_sedation_gcs_invalidate_hours) are invalid. If no valid GCS and
  sedation present, score 0. Uses continuous meds only.
- Footnote d: gcs_motor scoring with mapping:
  motor=6 -> 0, motor=5 -> 1, motor=4 -> 2, motor=3 -> 3, motor∈[0,1,2] -> 4
  Default: both gcs_total and gcs_motor scored independently, worst subscore used.
  With deprioritize_gcs_motor=True: gcs_motor only used as fallback when gcs_total unavailable.
- Footnote e: If patient is receiving drug therapy for delirium, score 1 point
  even if GCS is 15. Continuous (dexmedetomidine) via pre-window ASOF + in-window;
  intermittent (haloperidol, quetiapine, ziprasidone, olanzapine) via in-window only.
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import SOFA2Config, _flag_delirium_drug, _detect_sedation_episodes
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.brain')


def _calculate_brain_subscore(
    cohort_rel: DuckDBPyRelation,
    assessments_rel: DuckDBPyRelation,
    cont_meds_rel: DuckDBPyRelation,
    intm_meds_rel: DuckDBPyRelation,
    cfg: SOFA2Config,
    *,
    dev: bool = False,
    id_name: str = 'hospitalization_id',
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 brain subscore based on GCS and delirium drug administration.

    Implements:
    - Footnote c: Sedation-aware GCS (invalidate GCS during sedation episodes)
    - Footnote d: GCS motor scoring (fallback-only or both-source, per cfg)
    - Footnote e: Delirium drug -> minimum 1 point (cont + intm)

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [id_name, start_dttm, end_dttm]
    assessments_rel : DuckDBPyRelation
        Patient assessments table (CLIF patient_assessments)
    cont_meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    intm_meds_rel : DuckDBPyRelation
        Intermittent medication administrations (CLIF medication_admin_intermittent)
    cfg : SOFA2Config
        Configuration with post_sedation_gcs_invalidate_hours and deprioritize_gcs_motor
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging
    id_name : str
        Identity column name. Default 'hospitalization_id'.

    Returns
    -------
    DuckDBPyRelation
        Columns: [id_name, start_dttm, gcs_min, gcs_type,
                  gcs_min_dttm_offset, has_sedation, sedation_start_dttm_offset,
                  sedation_end_dttm_offset, has_delirium_drug,
                  delirium_drug_dttm_offset, sofa2_brain]
        Where sofa2_brain is the subscore (0-4), 0 if sedated with no valid GCS,
        or NULL if no GCS data and no sedation.
    """
    logger.info("Calculating brain subscore...")
    post_sed_hours = cfg.post_sedation_gcs_invalidate_hours
    logger.info(f"post_sedation_gcs_invalidate_hours={post_sed_hours}")

    # =========================================================================
    # Step 1: Detect sedation episodes (footnote c) — continuous meds only
    # =========================================================================
    logger.info("Detecting sedation episodes within scoring windows...")
    sedation_episodes = _detect_sedation_episodes(
        cohort_rel, cont_meds_rel,
        post_sedation_gcs_invalidate_hours=post_sed_hours,
        id_name=id_name,
    )

    # Flag windows that have any sedation + episode boundary offsets
    has_sedation_flag = duckdb.sql(f"""
        FROM sedation_episodes
        SELECT
            {id_name}
            , start_dttm
            , 1 AS has_sedation
            , MIN(sedation_start) - start_dttm AS sedation_start_dttm_offset
            , MAX(sedation_end) - start_dttm AS sedation_end_dttm_offset
        GROUP BY {id_name}, start_dttm
    """)

    # =========================================================================
    # Step 2: Get all GCS measurements within window (both gcs_total and gcs_motor)
    # =========================================================================
    logger.info("Collecting GCS measurements (gcs_total and gcs_motor) within windows...")
    gcs_in_window = duckdb.sql(f"""
        FROM assessments_rel t
        JOIN cohort_rel c ON
            t.{id_name} = c.{id_name}
            AND t.recorded_dttm >= c.start_dttm
            AND t.recorded_dttm <= c.end_dttm
        SELECT
            t.{id_name}
            , c.start_dttm
            , t.recorded_dttm
            , t.assessment_category
            , t.numerical_value AS gcs_value
            , t.recorded_dttm - c.start_dttm AS dttm_offset
        WHERE t.assessment_category IN ('gcs_total', 'gcs_motor')
    """)

    # =========================================================================
    # Step 3: Mark GCS as valid/invalid based on sedation episodes
    # =========================================================================
    logger.info("Filtering GCS measurements to exclude those during sedation episodes...")
    # A GCS is invalid if it falls within ANY sedation episode's [sedation_start, sedation_end_extended]
    gcs_with_validity = duckdb.sql(f"""
        FROM gcs_in_window g
        LEFT JOIN sedation_episodes s ON
            g.{id_name} = s.{id_name}
            AND g.start_dttm = s.start_dttm
            AND g.recorded_dttm >= s.sedation_start
            AND g.recorded_dttm <= s.sedation_end_extended
        SELECT
            g.{id_name}
            , g.start_dttm
            , g.recorded_dttm
            , g.assessment_category
            , g.gcs_value
            , g.dttm_offset
            -- is_valid = 0 if GCS falls within any sedation episode, 1 otherwise
            , CASE WHEN s.{id_name} IS NOT NULL THEN 0 ELSE 1 END AS is_valid
    """)

    # Keep only valid GCS
    valid_gcs = duckdb.sql("""
        FROM gcs_with_validity
        SELECT *
        WHERE is_valid = 1
    """)

    # =========================================================================
    # Step 4: Aggregate valid GCS (prefer gcs_total, fallback to gcs_motor)
    # =========================================================================
    logger.info("Aggregating worst valid GCS...")
    logger.info(f"deprioritize_gcs_motor={cfg.deprioritize_gcs_motor}")
    # Aggregate MIN for each type separately
    gcs_agg_by_type = duckdb.sql(f"""
        FROM valid_gcs
        SELECT
            {id_name}
            , start_dttm
            , MIN(gcs_value) FILTER (WHERE assessment_category = 'gcs_total') AS gcs_total_min
            , ARG_MIN(dttm_offset, gcs_value) FILTER (WHERE assessment_category = 'gcs_total') AS gcs_total_dttm_offset
            , MIN(gcs_value) FILTER (WHERE assessment_category = 'gcs_motor') AS gcs_motor_min
            , ARG_MIN(dttm_offset, gcs_value) FILTER (WHERE assessment_category = 'gcs_motor') AS gcs_motor_dttm_offset
        GROUP BY {id_name}, start_dttm
    """)

    if cfg.deprioritize_gcs_motor:
        # Fallback-only: prefer gcs_total, use gcs_motor only when gcs_total unavailable
        gcs_combined = duckdb.sql(f"""
            FROM gcs_agg_by_type
            SELECT
                {id_name}
                , start_dttm
                , gcs_total_min
                , gcs_motor_min
                , COALESCE(gcs_total_min, gcs_motor_min) AS gcs_min
                , CASE
                    WHEN gcs_total_min IS NOT NULL THEN 'gcs_total'
                    WHEN gcs_motor_min IS NOT NULL THEN 'gcs_motor'
                    ELSE NULL
                  END AS gcs_type
                , CASE
                    WHEN gcs_total_min IS NOT NULL THEN gcs_total_dttm_offset
                    WHEN gcs_motor_min IS NOT NULL THEN gcs_motor_dttm_offset
                    ELSE NULL
                  END AS gcs_min_dttm_offset
        """)
    else:
        # Both-source: pass through both values; scoring deferred to Step 6
        gcs_combined = duckdb.sql(f"""
            FROM gcs_agg_by_type
            SELECT
                {id_name}
                , start_dttm
                , gcs_total_min
                , gcs_total_dttm_offset
                , gcs_motor_min
                , gcs_motor_dttm_offset
        """)

    # =========================================================================
    # Step 5: Flag delirium drug administration (footnote e)
    # =========================================================================
    logger.info("Flagging delirium drug administration for footnote e...")
    delirium_flag = _flag_delirium_drug(
        cohort_rel, cont_meds_rel, intm_meds_rel, id_name=id_name
    )

    # =========================================================================
    # Step 6: Calculate final brain subscore
    # =========================================================================
    logger.info("Calculating final brain subscore with footnotes c, d, e...")
    if cfg.deprioritize_gcs_motor:
        # Fallback-only: score based on pre-resolved gcs_min/gcs_type from Step 4
        brain_score = duckdb.sql(f"""
            FROM cohort_rel c
            LEFT JOIN gcs_combined g USING ({id_name}, start_dttm)
            LEFT JOIN has_sedation_flag s USING ({id_name}, start_dttm)
            LEFT JOIN delirium_flag d USING ({id_name}, start_dttm)
            SELECT
                c.{id_name}
                , c.start_dttm
                , g.gcs_min
                , g.gcs_type
                , g.gcs_min_dttm_offset
                , COALESCE(s.has_sedation, 0) AS has_sedation
                , s.sedation_start_dttm_offset
                , s.sedation_end_dttm_offset
                , COALESCE(d.has_delirium_drug, 0) AS has_delirium_drug
                , d.delirium_drug_dttm_offset
                , sofa2_brain: CASE
                    WHEN g.gcs_type = 'gcs_total' THEN
                        CASE
                            WHEN g.gcs_min >= 15 AND COALESCE(d.has_delirium_drug, 0) = 1 THEN 1
                            WHEN g.gcs_min >= 15 THEN 0
                            WHEN g.gcs_min >= 13 THEN 1
                            WHEN g.gcs_min >= 9 THEN 2
                            WHEN g.gcs_min >= 6 THEN 3
                            WHEN g.gcs_min >= 3 THEN 4
                            ELSE NULL
                        END
                    WHEN g.gcs_type = 'gcs_motor' THEN
                        CASE
                            WHEN g.gcs_min = 6 AND COALESCE(d.has_delirium_drug, 0) = 1 THEN 1
                            WHEN g.gcs_min = 6 THEN 0
                            WHEN g.gcs_min = 5 THEN 1
                            WHEN g.gcs_min = 4 THEN 2
                            WHEN g.gcs_min = 3 THEN 3
                            WHEN g.gcs_min IN (0, 1, 2) THEN 4
                            ELSE NULL
                        END
                    WHEN COALESCE(s.has_sedation, 0) = 1 THEN 0
                    ELSE NULL
                END
        """)
    else:
        # Both-source: score gcs_total and gcs_motor independently, keep worst
        brain_score = duckdb.sql(f"""
            FROM cohort_rel c
            LEFT JOIN gcs_combined g USING ({id_name}, start_dttm)
            LEFT JOIN has_sedation_flag s USING ({id_name}, start_dttm)
            LEFT JOIN delirium_flag d USING ({id_name}, start_dttm)
            SELECT
                c.{id_name}
                , c.start_dttm
                -- Score gcs_total independently
                , subscore_total: CASE
                    WHEN g.gcs_total_min IS NULL THEN NULL
                    WHEN g.gcs_total_min >= 15 AND COALESCE(d.has_delirium_drug, 0) = 1 THEN 1
                    WHEN g.gcs_total_min >= 15 THEN 0
                    WHEN g.gcs_total_min >= 13 THEN 1
                    WHEN g.gcs_total_min >= 9 THEN 2
                    WHEN g.gcs_total_min >= 6 THEN 3
                    WHEN g.gcs_total_min >= 3 THEN 4
                    ELSE NULL
                  END
                -- Score gcs_motor independently
                , subscore_motor: CASE
                    WHEN g.gcs_motor_min IS NULL THEN NULL
                    WHEN g.gcs_motor_min = 6 AND COALESCE(d.has_delirium_drug, 0) = 1 THEN 1
                    WHEN g.gcs_motor_min = 6 THEN 0
                    WHEN g.gcs_motor_min = 5 THEN 1
                    WHEN g.gcs_motor_min = 4 THEN 2
                    WHEN g.gcs_motor_min = 3 THEN 3
                    WHEN g.gcs_motor_min IN (0, 1, 2) THEN 4
                    ELSE NULL
                  END
                -- Pick worst subscore; on tie prefer gcs_total
                , gcs_min: CASE
                    WHEN subscore_motor IS NOT NULL AND subscore_motor > COALESCE(subscore_total, -1) THEN g.gcs_motor_min
                    WHEN subscore_total IS NOT NULL THEN g.gcs_total_min
                    WHEN subscore_motor IS NOT NULL THEN g.gcs_motor_min
                    ELSE NULL
                  END
                , gcs_type: CASE
                    WHEN subscore_motor IS NOT NULL AND subscore_motor > COALESCE(subscore_total, -1) THEN 'gcs_motor'
                    WHEN subscore_total IS NOT NULL THEN 'gcs_total'
                    WHEN subscore_motor IS NOT NULL THEN 'gcs_motor'
                    ELSE NULL
                  END
                , gcs_min_dttm_offset: CASE
                    WHEN subscore_motor IS NOT NULL AND subscore_motor > COALESCE(subscore_total, -1) THEN g.gcs_motor_dttm_offset
                    WHEN subscore_total IS NOT NULL THEN g.gcs_total_dttm_offset
                    WHEN subscore_motor IS NOT NULL THEN g.gcs_motor_dttm_offset
                    ELSE NULL
                  END
                , COALESCE(s.has_sedation, 0) AS has_sedation
                , s.sedation_start_dttm_offset
                , s.sedation_end_dttm_offset
                , COALESCE(d.has_delirium_drug, 0) AS has_delirium_drug
                , d.delirium_drug_dttm_offset
                -- Final brain subscore: worst of both sources
                , sofa2_brain: CASE
                    WHEN subscore_total IS NOT NULL AND subscore_motor IS NOT NULL
                        THEN GREATEST(subscore_total, subscore_motor)
                    WHEN subscore_total IS NOT NULL THEN subscore_total
                    WHEN subscore_motor IS NOT NULL THEN subscore_motor
                    -- Footnote c: no valid GCS + sedation -> 0
                    WHEN COALESCE(s.has_sedation, 0) = 1 THEN 0
                    ELSE NULL
                  END
        """)

    logger.info("Brain subscore complete")

    if dev:
        return brain_score, {
            'sedation_episodes': sedation_episodes,
            'has_sedation_flag': has_sedation_flag,
            'gcs_in_window': gcs_in_window,
            'gcs_with_validity': gcs_with_validity,
            'valid_gcs': valid_gcs,
            'gcs_agg_by_type': gcs_agg_by_type,
            'gcs_combined': gcs_combined,
            'delirium_flag': delirium_flag,
        }

    return brain_score
