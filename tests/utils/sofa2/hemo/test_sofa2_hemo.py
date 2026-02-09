"""Tests for SOFA-2 hemostasis (hemo) subscore calculation.

Test cases (documented in hemo_expected.csv notes column):
- 501-505: Score boundaries (150, 100, 80, 50 thresholds)
- 506: No data -> NULL
- 507: Pre-window fallback when no in-window data
- 508: Pre-window ignored when in-window exists
- 509: Pre-window outside 12hr lookback -> NULL
- 510: Multi-window same hosp_id (stable -> deterioration)
- 511: Multi-window same hosp_id (critical -> recovery)
"""

import pytest
from pathlib import Path
import pandas as pd
import duckdb

from clifpy.utils.sofa2._hemo import _calculate_hemo_subscore
from clifpy.utils.sofa2._utils import SOFA2Config


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']


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


@pytest.fixture
def cohort_rel():
    """Load cohort fixture as DuckDBPyRelation."""
    return duckdb.read_csv(str(FIXTURES_DIR / 'clif_cohort.csv'))


@pytest.fixture
def labs_rel():
    """Load labs fixture as DuckDBPyRelation."""
    return duckdb.read_csv(str(FIXTURES_DIR / 'clif_labs.csv'))


@pytest.fixture
def expected_df():
    """Load expected output as DataFrame."""
    return pd.read_csv(str(FIXTURES_DIR / 'hemo_expected.csv'))


@pytest.fixture
def result_df(cohort_rel, labs_rel):
    """Run the hemo subscore and return sorted result DataFrame."""
    cfg = SOFA2Config()
    result = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg)
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_hemo_row_count(result_df, expected_df):
    """Verify output has one row per cohort window."""
    assert len(result_df) == len(expected_df), (
        f"Expected {len(expected_df)} rows, got {len(result_df)}"
    )


def test_hemo_scores(result_df, expected_df):
    """Verify hemo scores match expected (all cases from CSV)."""
    expected_df = expected_df.sort_values(SORT_COLS).reset_index(drop=True)

    pd.testing.assert_series_equal(
        result_df['hemo'].astype('Int64'),
        expected_df['hemo'].astype('Int64'),
        check_names=False
    )


def test_hemo_platelet_count(result_df, expected_df):
    """Verify platelet_count values match expected."""
    expected_df = expected_df.sort_values(SORT_COLS).reset_index(drop=True)

    pd.testing.assert_series_equal(
        result_df['platelet_count'].astype('Float64'),
        expected_df['platelet_count'].astype('Float64'),
        check_names=False
    )


def test_hemo_dttm_offset(result_df, expected_df):
    """Verify platelet_dttm_offset values match expected."""
    expected_df = expected_df.sort_values(SORT_COLS).reset_index(drop=True)

    result_seconds = result_df['platelet_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected_df['platelet_dttm_offset'].apply(_to_total_seconds)

    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False
    )


def test_hemo_intermediates(cohort_rel, labs_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_hemo_subscore(cohort_rel, labs_rel, cfg, dev=True)

    expected_keys = {'platelet_in_window', 'platelet_pre_window', 'platelet_with_fallback'}
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"
