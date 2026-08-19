"""Tests for rolling SOFA-2 hemostasis (hemo) subscore calculation.

Test cases (documented in rolling_hemo_expected.csv notes column):
- 601: Single observation, no expiry within bounds
- 602: Single observation, expiry fires at +48h
- 603: Two observations same score, no new worst (200, 250) → suppressed
- 604: Score worsens (200 → 60)
- 605: Score improves (60 → 200)
- 606: No observations → 0 events
- 607: Expiry then new observation (gap > 48h)
- 608: No expiry because next obs within 48h
- 609: Same-timestamp duplicate observations → aggregated to MIN
- 610: Expiry would exceed cohort end_dttm → suppressed
- 611: Worsens, stays same (no new worst), worsens again → middle suppressed
- 612: Two observations same score, NEW worst (145, 130) → new_worst_value emit
- 613: Score change then new worst w/o score change (200 → 90 → 85)

Custom expiry cases (case column in expected CSV):
- short_expiry: hemo_expiry_hours=12.0
- no_expiry: hemo_expiry_hours=None
"""

import pytest
from pathlib import Path
import pandas as pd
import duckdb

from clifpy.utils.sofa2.rolling._hemo import _calculate_rolling_hemo
from clifpy.utils.sofa2.rolling._config import RollingSOFA2Config


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'event_dttm']


def load_rolling_expected(fixtures_dir: Path, filename: str, case: str) -> pd.DataFrame:
    """Load expected CSV filtered to a specific case, sorted by rolling sort columns."""
    df = pd.read_csv(
        str(fixtures_dir / filename),
        dtype={'hospitalization_id': str},
    )
    return df[df['case'] == case].sort_values(SORT_COLS).reset_index(drop=True)


def assert_rolling_columns_match(result_df, expected_df, column_specs):
    """Check multiple columns against expected, collecting all failures.

    Adapted from conftest.assert_columns_match for rolling output
    (uses event_dttm + notes for error context).
    """
    assert len(result_df) == len(expected_df), (
        f"Row count mismatch: expected {len(expected_df)}, got {len(result_df)}\n"
        f"Expected hosp_ids: {expected_df['hospitalization_id'].tolist()}\n"
        f"Result hosp_ids: {result_df['hospitalization_id'].tolist()}"
    )

    failures = {}
    for col, ctype in column_specs:
        if ctype == 'str':
            r = result_df[col].fillna('')
            e = expected_df[col].fillna('')
        else:
            r = result_df[col].astype(ctype)
            e = expected_df[col].astype(ctype)

        both_na = r.isna() & e.isna()
        equal = r == e
        mismatch_mask = ~(both_na | equal)

        if mismatch_mask.any():
            ctx = expected_df[['hospitalization_id', 'event_dttm', 'notes']].copy()
            ctx['expected'] = expected_df[col]
            ctx['actual'] = result_df[col]
            failures[col] = ctx[mismatch_mask]

    if failures:
        parts = [f"{len(failures)} column(s) have mismatches:\n"]
        for col, mismatch_df in failures.items():
            n = len(mismatch_df)
            parts.append(f"  Column '{col}' — {n} mismatch(es):")
            parts.append(
                mismatch_df.to_string(index=False, max_colwidth=60)
            )
            parts.append('')
        assert False, '\n'.join(parts)


ROLLING_HEMO_COLUMNS = [
    ('sofa2_hemo', 'Int64'),
    ('platelet_count', 'Float64'),
    ('platelet_worst_ever', 'Float64'),
    ('emit_reason', 'str'),
]

EXPIRY_CONFIGS = {
    'short_expiry': RollingSOFA2Config(hemo_expiry_hours=12.0),
    'no_expiry': RollingSOFA2Config(hemo_expiry_hours=None),
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
    return load_rolling_expected(FIXTURES_DIR, 'rolling_hemo_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, labs_rel):
    cfg = RollingSOFA2Config()
    result = _calculate_rolling_hemo(cohort_rel, labs_rel, cfg)
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_rolling_hemo_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_rolling_columns_match(result_df, expected_df, ROLLING_HEMO_COLUMNS)


def test_rolling_hemo_no_events_for_empty(cohort_rel, labs_rel):
    """Verify hosp_id 606 (no observations) produces 0 events."""
    cfg = RollingSOFA2Config()
    result = _calculate_rolling_hemo(cohort_rel, labs_rel, cfg)
    result_df = result.df()
    events_606 = result_df[result_df['hospitalization_id'] == '606']
    assert len(events_606) == 0, f"Expected 0 events for 606, got {len(events_606)}"


def test_rolling_hemo_intermediates(cohort_rel, labs_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = RollingSOFA2Config()
    result, intermediates = _calculate_rolling_hemo(cohort_rel, labs_rel, cfg, dev=True)

    expected_keys = {'filtered_obs', 'aggregated_obs', 'scored_obs', 'all_events', 'expiry_events'}
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


def test_rolling_hemo_intermediates_no_expiry(cohort_rel, labs_rel):
    """Verify dev=True with no expiry does not include expiry_events."""
    cfg = RollingSOFA2Config(hemo_expiry_hours=None)
    result, intermediates = _calculate_rolling_hemo(cohort_rel, labs_rel, cfg, dev=True)

    expected_keys = {'filtered_obs', 'aggregated_obs', 'scored_obs', 'all_events'}
    assert set(intermediates.keys()) == expected_keys


@pytest.mark.parametrize('case', ['short_expiry', 'no_expiry'])
def test_rolling_hemo_custom_expiry(cohort_rel, labs_rel, case):
    """Verify custom expiry config produces expected results from CSV."""
    cfg = EXPIRY_CONFIGS[case]
    expected = load_rolling_expected(FIXTURES_DIR, 'rolling_hemo_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_rolling_hemo(cohort_rel, labs_rel, cfg)
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_rolling_columns_match(result_df, expected, ROLLING_HEMO_COLUMNS)
