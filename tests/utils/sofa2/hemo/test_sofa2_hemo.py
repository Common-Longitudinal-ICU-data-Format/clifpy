"""Tests for SOFA-2 hemostasis (hemo) subscore calculation.

Test cases (documented in hemo_expected.csv notes column):
- 501-505: Score boundaries (150, 100, 80, 50 thresholds)
- 506: No data -> NULL
- 507: Pre-window fallback when no in-window data
- 508: Pre-window ignored when in-window exists
- 509: Pre-window outside 24hr lookback -> NULL
- 510: Multi-window same hosp_id (stable -> deterioration)
- 511: Daily / missing data (3 consecutive days, lab only on day 1)

Custom lookback cases (case column in expected CSV):
- short_lookback: hemo_lookback_hours=1.0
- long_lookback: hemo_lookback_hours=36.0
"""

import pytest
from pathlib import Path
import duckdb

from clifpy.utils.sofa2._hemo import _calculate_hemo_subscore
from clifpy.utils.sofa2._utils import SOFA2Config
from tests.utils.sofa2.conftest import load_expected, assert_columns_match


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

HEMO_COLUMNS = [
    ('sofa2_hemo', 'Int64'),
    ('platelet_count', 'Float64'),
    ('platelet_dttm_offset', 'offset'),
]

LOOKBACK_CONFIGS = {
    'short_lookback': SOFA2Config(hemo_lookback_hours=1.0),
    'long_lookback': SOFA2Config(hemo_lookback_hours=36.0),
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
def expected_df():
    return load_expected(FIXTURES_DIR, 'hemo_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, labs_rel):
    cfg = SOFA2Config()
    result = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg)
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_hemo_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_columns_match(result_df, expected_df, HEMO_COLUMNS)


def test_hemo_intermediates(cohort_rel, labs_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg, dev=True)

    expected_keys = {'platelet_in_window', 'platelet_pre_window', 'platelet_with_fallback'}
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


@pytest.mark.parametrize('case', ['short_lookback', 'long_lookback'])
def test_hemo_custom_lookback(cohort_rel, labs_rel, case):
    """Verify custom lookback config produces expected results from CSV."""
    cfg = LOOKBACK_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'hemo_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg)
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, HEMO_COLUMNS)
