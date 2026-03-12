"""Tests for SOFA-2 kidney subscore calculation.

Test cases (documented in kidney_expected.csv notes column):
- 201-205: Score boundaries (1.20, 2.0, 3.50 creatinine thresholds + RRT)
- 206: No data -> NULL
- 207: Pre-window fallback when no in-window data
- 208: Pre-window ignored when in-window exists
- 209: Pre-window outside 12hr lookback -> NULL
- 210-213: RRT criteria (footnote p) — potassium path, pH+bicarb path, not met
- 214: Multi-window same hosp_id (stable -> deterioration)
- 215: Daily / missing data (3 consecutive days, lab only on day 1)
- 216: pH type selection — arterial preferred (LEAST)
- 217: pH type selection — venous used when arterial absent
- 218: RRT override with normal creatinine

Custom lookback cases (case column in expected CSV):
- short_lookback: kidney_lookback_hours=1.0
- long_lookback: kidney_lookback_hours=36.0
"""

import pytest
from pathlib import Path
import duckdb

from clifpy.utils.sofa2._kidney import _calculate_kidney_subscore
from clifpy.utils.sofa2._utils import SOFA2Config
from tests.utils.sofa2.conftest import load_expected, assert_columns_match


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

KIDNEY_COLUMNS = [
    ('sofa2_kidney', 'Int64'),
    ('creatinine', 'Float64'),
    ('creatinine_dttm_offset', 'offset'),
    ('has_rrt', 'Int64'),
    ('rrt_dttm_offset', 'offset'),
    ('rrt_criteria_met', 'Int64'),
]

LOOKBACK_CONFIGS = {
    'short_lookback': SOFA2Config(kidney_lookback_hours=1.0),
    'long_lookback': SOFA2Config(kidney_lookback_hours=36.0),
}


@pytest.fixture
def cohort_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_cohort.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def labs_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_labs.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def crrt_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_crrt_therapy.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def expected_df():
    return load_expected(FIXTURES_DIR, 'kidney_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, labs_rel, crrt_rel):
    cfg = SOFA2Config()
    result = _calculate_kidney_subscore(cohort_rel, labs_rel, crrt_rel, cfg)
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_kidney_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_columns_match(result_df, expected_df, KIDNEY_COLUMNS)


def test_kidney_intermediates(cohort_rel, labs_rel, crrt_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_kidney_subscore(
        cohort_rel, labs_rel, crrt_rel, cfg, dev=True,
    )

    expected_keys = {
        'rrt_flag', 'labs_in_window', 'labs_pre_window_long',
        'labs_pre_window', 'labs_with_fallback',
    }
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


@pytest.mark.parametrize('case', ['short_lookback', 'long_lookback'])
def test_kidney_custom_lookback(cohort_rel, labs_rel, crrt_rel, case):
    """Verify custom lookback config produces expected results from CSV."""
    cfg = LOOKBACK_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'kidney_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_kidney_subscore(cohort_rel, labs_rel, crrt_rel, cfg)
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, KIDNEY_COLUMNS)
