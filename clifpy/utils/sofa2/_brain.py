"""Brain subscore calculation for SOFA-2.

Scoring (per SOFA-2 spec):
- GCS 15: 0 points (unless delirium drug -> 1 point per footnote e)
- GCS 13-14: 1 point
- GCS 9-12: 2 points
- GCS 6-8: 3 points
- GCS 3-5: 4 points

Special rules:
- Footnote e: If patient is receiving drug therapy for delirium, score 1 point even if GCS is 15.
  Currently only dexmedetomidine is included.
"""

from __future__ import annotations

import duckdb
from duckdb import DuckDBPyRelation

from ._utils import _agg_gcs, _flag_delirium_drug
from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.sofa2.brain')


def _calculate_brain_subscore(
    cohort_rel: DuckDBPyRelation,
    assessments_rel: DuckDBPyRelation,
    meds_rel: DuckDBPyRelation,
    *,
    dev: bool = False,
) -> DuckDBPyRelation | tuple[DuckDBPyRelation, dict]:
    """
    Calculate SOFA-2 brain subscore based on GCS and delirium drug administration.

    This subscore uses in-window data only (no pre-window lookback).

    Parameters
    ----------
    cohort_rel : DuckDBPyRelation
        Cohort with columns [hospitalization_id, start_dttm, end_dttm]
    assessments_rel : DuckDBPyRelation
        Patient assessments table (CLIF patient_assessments)
    meds_rel : DuckDBPyRelation
        Continuous medication administrations (CLIF medication_admin_continuous)
    dev : bool, default False
        If True, return (result, intermediates_dict) for debugging

    Returns
    -------
    DuckDBPyRelation
        Columns: [hospitalization_id, start_dttm, gcs_min, has_delirium_drug, brain]
        Where brain is the subscore (0-4) or NULL if no GCS data
    """
    logger.info("Calculating brain subscore...")

    # Step 1: Aggregate GCS within window
    logger.info("Aggregating worst GCS components within scoring window...")
    gcs_agg = _agg_gcs(cohort_rel, assessments_rel)

    # Step 2: Flag delirium drug administration
    logger.info("Flagging sedative/delirium drugs to identify unassessable patients...")
    delirium_flag = _flag_delirium_drug(cohort_rel, meds_rel)

    # Step 3: Calculate subscore with footnote e logic
    logger.info("Applying footnote e: imputing GCS=15 when sedated without prior assessment...")
    brain_score = duckdb.sql("""
        FROM cohort_rel c
        LEFT JOIN gcs_agg g USING (hospitalization_id, start_dttm)
        LEFT JOIN delirium_flag d USING (hospitalization_id, start_dttm)
        SELECT
            c.hospitalization_id
            , c.start_dttm
            , g.gcs_min
            , COALESCE(d.has_delirium_drug, 0) AS has_delirium_drug
            , brain: CASE
                -- Footnote e: delirium drug with GCS 15 -> minimum 1 point
                WHEN g.gcs_min >= 15 AND COALESCE(d.has_delirium_drug, 0) = 1 THEN 1
                WHEN g.gcs_min >= 15 THEN 0
                WHEN g.gcs_min >= 13 THEN 1
                WHEN g.gcs_min >= 9 THEN 2
                WHEN g.gcs_min >= 6 THEN 3
                WHEN g.gcs_min >= 3 THEN 4
                ELSE NULL
            END
    """)

    logger.info("Brain subscore complete")

    if dev:
        return brain_score, {
            'gcs_agg': gcs_agg,
            'delirium_flag': delirium_flag,
        }

    return brain_score
