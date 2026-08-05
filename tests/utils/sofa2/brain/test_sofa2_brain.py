"""Tests for SOFA-2 brain subscore calculation.

Test cases (documented in brain_expected.csv notes column):
- 101-105: Score boundaries (GCS 15/14/10/7/4)
- 106: No data -> NULL
- 107: Dexmed dual-role: GCS=15 before sedation + delirium -> 1
- 108: GCS<13 + dexmed: delirium doesn't override -> 2
- 109: Haloperidol (intermittent) + GCS=15 -> 1
- 110: All GCS during sedation -> invalidated -> 0
- 111: Valid GCS before sedation + invalid GCS during -> valid used
- 112: Post-sedation window (+5hr, within 12hr) -> invalidated -> 0
- 113: Post-sedation window (+13hr, outside 12hr) -> valid -> 1
- 114-116: Motor fallback (footnote d)
- 117: Multi-window deterioration
- 118: Daily / missing data (3 consecutive days, GCS only on day 1)
- 119: Sedation present, no GCS at all -> 0
- 120: Delirium drug present, no GCS -> NULL

Custom post_sedation_gcs_invalidate_hours cases:
- short_post_sed: post_sedation_gcs_invalidate_hours=2.0
- long_post_sed: post_sedation_gcs_invalidate_hours=14.0

Custom deprioritize_gcs_motor cases:
- deprioritize: deprioritize_gcs_motor=True (legacy fallback-only behavior)
"""

import pytest
from pathlib import Path
import duckdb

from clifpy.utils.sofa2._brain import _calculate_brain_subscore
from clifpy.utils.sofa2._utils import SOFA2Config
from tests.utils.sofa2.conftest import load_expected, assert_columns_match


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

BRAIN_COLUMNS = [
    ('sofa2_brain', 'Int64'),
    ('gcs_min', 'Float64'),
    ('gcs_type', 'str'),
    ('has_sedation', 'Int64'),
    ('has_delirium_drug', 'Int64'),
    ('gcs_min_dttm_offset', 'offset'),
    ('sedation_start_dttm_offset', 'offset'),
    ('sedation_end_dttm_offset', 'offset'),
    ('delirium_drug_dttm_offset', 'offset'),
]

POST_SED_CONFIGS = {
    'short_post_sed': SOFA2Config(post_sedation_gcs_invalidate_hours=2.0),
    'long_post_sed': SOFA2Config(post_sedation_gcs_invalidate_hours=14.0),
}

DEPRIORITIZE_CONFIGS = {
    'deprioritize': SOFA2Config(deprioritize_gcs_motor=True),
}


@pytest.fixture
def cohort_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_cohort.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def assessments_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_patient_assessments.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def cont_meds_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_medication_admin_continuous.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def intm_meds_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_medication_admin_intermittent.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def expected_df():
    return load_expected(FIXTURES_DIR, 'brain_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel):
    cfg = SOFA2Config()
    result = _calculate_brain_subscore(
        cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg
    )
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_brain_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_columns_match(result_df, expected_df, BRAIN_COLUMNS)


def test_brain_intermediates(cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_brain_subscore(
        cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg, dev=True
    )

    expected_keys = {
        'sedation_episodes',
        'has_sedation_flag',
        'gcs_in_window',
        'gcs_with_validity',
        'valid_gcs',
        'gcs_agg_by_type',
        'gcs_combined',
        'delirium_flag',
    }
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


@pytest.mark.parametrize('case', ['short_post_sed', 'long_post_sed'])
def test_brain_custom_post_sed(
    cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, case
):
    """Verify custom post_sedation_gcs_invalidate_hours produces expected results."""
    cfg = POST_SED_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'brain_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_brain_subscore(
        cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg
    )
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, BRAIN_COLUMNS)


@pytest.mark.parametrize('case', ['deprioritize'])
def test_brain_deprioritize_gcs_motor(
    cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, case
):
    """Verify deprioritize_gcs_motor=True produces legacy fallback behavior."""
    cfg = DEPRIORITIZE_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'brain_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_brain_subscore(
        cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg
    )
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, BRAIN_COLUMNS)
