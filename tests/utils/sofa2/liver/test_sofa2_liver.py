"""Tests for SOFA-2 liver subscore calculation.

Test cases (documented in liver_expected.csv notes column):
- 301-305: Score boundaries (1.2, 3.0, 6.0, 12.0 thresholds)
- 306: No data -> NULL
- 307: Pre-window fallback when no in-window data
- 308: Pre-window ignored when in-window exists
- 309: Pre-window outside 24hr lookback -> NULL
- 310: Multi-window same hosp_id (stable -> deterioration)
- 311: Multi-window same hosp_id (critical -> recovery)

Custom lookback cases (case column in expected CSV):
- short_lookback: liver_lookback_hours=6.0
- long_lookback: liver_lookback_hours=36.0
"""

import pytest
from pathlib import Path
import pandas as pd
import duckdb

from clifpy.utils.sofa2._liver import _calculate_liver_subscore
from clifpy.utils.sofa2._utils import SOFA2Config


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

LOOKBACK_CONFIGS = {
    'short_lookback': SOFA2Config(liver_lookback_hours=6.0),
    'long_lookback': SOFA2Config(liver_lookback_hours=36.0),
}


def _to_total_seconds(x):
    """Convert timedelta/interval to total seconds for comparison."""
    if pd.isna(x):
        return None
    if isinstance(x, pd.Timedelta):
        return x.total_seconds()
    if isinstance(x, str):
        negative = x.startswith('-')
        time_str = x.lstrip('-')
        parts = time_str.split(':')
        hours, mins, secs = int(parts[0]), int(parts[1]), float(parts[2])
        total = hours * 3600 + mins * 60 + secs
        return -total if negative else total
    return None


def _load_expected(case: str) -> pd.DataFrame:
    """Load expected CSV filtered to a specific case."""
    df = pd.read_csv(
        str(FIXTURES_DIR / 'liver_expected.csv'),
        dtype={'hospitalization_id': str},
    )
    return df[df['case'] == case].sort_values(SORT_COLS).reset_index(drop=True)


@pytest.fixture
def cohort_rel():
    """Load cohort fixture as DuckDBPyRelation."""
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_cohort.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def labs_rel():
    """Load labs fixture as DuckDBPyRelation."""
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_labs.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def expected_df():
    """Load expected output for default case as DataFrame."""
    return _load_expected('default')


@pytest.fixture
def result_df(cohort_rel, labs_rel):
    """Run the liver subscore and return sorted result DataFrame."""
    cfg = SOFA2Config()
    result = _calculate_liver_subscore(cohort_rel, labs_rel, cfg)
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_liver_row_count(result_df, expected_df):
    """Verify output has one row per cohort window."""
    assert len(result_df) == len(expected_df), (
        f"Expected {len(expected_df)} rows, got {len(result_df)}"
    )


def test_liver_scores(result_df, expected_df):
    """Verify liver scores match expected (all cases from CSV)."""
    pd.testing.assert_series_equal(
        result_df['liver'].astype('Int64'),
        expected_df['liver'].astype('Int64'),
        check_names=False,
    )


def test_liver_bilirubin_total(result_df, expected_df):
    """Verify bilirubin_total values match expected."""
    pd.testing.assert_series_equal(
        result_df['bilirubin_total'].astype('Float64'),
        expected_df['bilirubin_total'].astype('Float64'),
        check_names=False,
    )


def test_liver_dttm_offset(result_df, expected_df):
    """Verify bilirubin_dttm_offset values match expected."""
    result_seconds = result_df['bilirubin_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected_df['bilirubin_dttm_offset'].apply(_to_total_seconds)

    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False,
    )


def test_liver_intermediates(cohort_rel, labs_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_liver_subscore(cohort_rel, labs_rel, cfg, dev=True)

    expected_keys = {'bilirubin_in_window', 'bilirubin_pre_window', 'bilirubin_with_fallback'}
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


# --- Custom lookback tests (data-driven from expected CSV case column) ---


@pytest.mark.parametrize('case', ['short_lookback', 'long_lookback'])
def test_liver_custom_lookback(cohort_rel, labs_rel, case):
    """Verify custom lookback config produces expected results from CSV."""
    cfg = LOOKBACK_CONFIGS[case]
    expected = _load_expected(case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_liver_subscore(cohort_rel, labs_rel, cfg)
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert len(result_df) == len(expected), (
        f"case={case}: expected {len(expected)} rows, got {len(result_df)}"
    )
    pd.testing.assert_series_equal(
        result_df['liver'].astype('Int64'),
        expected['liver'].astype('Int64'),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        result_df['bilirubin_total'].astype('Float64'),
        expected['bilirubin_total'].astype('Float64'),
        check_names=False,
    )
    result_seconds = result_df['bilirubin_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected['bilirubin_dttm_offset'].apply(_to_total_seconds)
    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False,
    )
