"""Tests for SOFA-2 cardiovascular subscore calculation.

Test cases (documented in cv_expected.csv notes column):
- 201-202: MAP-only boundaries (>= 70 -> 0, < 70 -> 1)
- 203-205: ne+epi dose tiers (<= 0.2, 0.2-0.4, > 0.4)
- 206-208: Dopamine-only scoring (footnote l: <= 20, 20-40, > 40)
- 209-211: Combination scoring (other vaso, low ne+other, medium ne+other)
- 212: No data -> NULL
- 213-214: Duration validation (footnote j: < 60 min ignored, >= 60 min counted)
- 215: ne+epi concurrent SUM (0.15 + 0.1 = 0.25 > 0.2)
- 216-218: Mechanical CV support (VA-ECMO, IABP, VV-ECMO excluded)
- 219: Pre-window IABP not counted (INNER JOIN)
- 220: Pre-window pressor extends episode (unlimited forward-fill)
- 221-223: MAR deduplication (action priority, active vs stop, dose tiebreaker)
- 224: Pre-window stop not carried forward
- 225: Multi-window (stable -> deterioration)
- 226: 3-day pattern (MAP in-window only, no lookback)

Custom duration cases (case column in expected CSV):
- short_duration: pressor_min_duration_minutes=30
- long_duration: pressor_min_duration_minutes=120
"""

import pytest
from pathlib import Path

from clifpy.utils.sofa2._cv import _calculate_cv_subscore
from clifpy.utils.sofa2._utils import SOFA2Config
from tests.utils.sofa2.conftest import load_expected, assert_columns_match, load_csv_fixture


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

CV_COLUMNS = [
    ('sofa2_cv', 'Int64'),
    ('map_min', 'Float64'),
    ('map_min_dttm_offset', 'offset'),
    ('norepi_epi_maxsum', 'Float64'),
    ('norepi_epi_maxsum_dttm_offset', 'offset'),
    ('dopa_max', 'Float64'),
    ('dopa_max_dttm_offset', 'offset'),
    ('has_other_non_dopa', 'Int64'),
    ('has_other_vaso', 'Int64'),
    ('has_mechanical_cv_support', 'Int64'),
    ('mechanical_cv_dttm_offset', 'offset'),
]

DURATION_CONFIGS = {
    'short_duration': SOFA2Config(pressor_min_duration_minutes=30),
    'long_duration': SOFA2Config(pressor_min_duration_minutes=120),
}


@pytest.fixture
def cohort_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_cohort.csv', ['start_dttm', 'end_dttm'])


@pytest.fixture
def vitals_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_vitals.csv', ['recorded_dttm'])


@pytest.fixture
def cont_meds_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_medication_admin_continuous.csv', ['admin_dttm'])


@pytest.fixture
def ecmo_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_ecmo_mcs.csv', ['recorded_dttm'])


@pytest.fixture
def expected_df():
    return load_expected(FIXTURES_DIR, 'cv_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel):
    cfg = SOFA2Config()
    result = _calculate_cv_subscore(
        cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, cfg,
    )
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_cv_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_columns_match(result_df, expected_df, CV_COLUMNS)


def test_cv_intermediates(cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_cv_subscore(
        cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, cfg, dev=True,
    )

    expected_keys = {
        'map_agg', 'pressor_at_start', 'pressor_in_window', 'pressor_at_end',
        'pressor_events_raw', 'pressor_events_deduped', 'pressor_events',
        'epi_ne_wide', 'epi_ne_filled', 'epi_ne_duration', 'epi_ne_agg',
        'other_pressor_duration', 'other_pressor_agg', 'pressor_agg',
        'mech_cv_flag',
    }
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


@pytest.mark.parametrize('case', ['short_duration', 'long_duration'])
def test_cv_custom_duration(cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, case):
    """Verify custom duration config produces expected results from CSV."""
    cfg = DURATION_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'cv_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_cv_subscore(
        cohort_rel, cont_meds_rel, vitals_rel, ecmo_rel, cfg,
    )
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, CV_COLUMNS)
