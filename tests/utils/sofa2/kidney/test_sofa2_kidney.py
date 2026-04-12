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

Urine output cases (case='uo' in expected CSV, all irregular intervals):
- 219: Score 1 — late drop, 12h rate barely >= 0.5 (edge case)
- 220: Score 2 — sustained moderate oliguria, 24h rate just above 0.3
- 221: Score 3 — very low output, 24h rate < 0.3
- 222: Score 3 — anuria after 15:00 (healthy then zero for 17h)
- 223: Score 0 — sparse but healthy (5 measurements, 5-7h gaps, tests validity gate)
- 224: Score 3 — irrigation subtraction drops net UO below threshold
- 225: Score 1 — high early output buffers 12h rate (between-window gap, by design)
- 226: Pre-window UO data (tests global LAG fix); covered only by intermediate test

Intermediate per-anchor test (test_urine_output_rate_intermediate):
Verifies per-anchor intermediate values (net_output, tm_since_last_uo_min,
uo_vol/tm/rate for 6h/12h/24h buckets) against a hand-computed expected CSV.
Currently covers patients 219, 223, 225, 226. Uses dev=True to expose the
`with_weight` intermediate from _calculate_uo_score(). Assumes weight=80 kg.

Custom lookback cases (case column in expected CSV):
- short_lookback: kidney_lookback_hours=1.0
- long_lookback: kidney_lookback_hours=36.0
"""

import pytest
from pathlib import Path
import duckdb

from clifpy.utils.sofa2._kidney import _calculate_kidney_subscore, _calculate_uo_score
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
    ('uo_score', 'Int64'),
    ('uo_rate_6hr', 'Float64'),
    ('uo_rate_12hr', 'Float64'),
    ('uo_rate_24hr', 'Float64'),
    ('has_uo_oliguria', 'Int64'),
    ('weight_at_uo', 'Float64'),
]

INTERMEDIATE_COLUMNS = [
    ('net_output', 'Float64'),
    ('tm_since_last_uo_min', 'Float64'),
    ('uo_vol_6hr', 'Float64'),
    ('uo_tm_6hr', 'Float64'),
    ('uo_rate_6hr', 'Float64'),
    ('uo_vol_12hr', 'Float64'),
    ('uo_tm_12hr', 'Float64'),
    ('uo_rate_12hr', 'Float64'),
    ('uo_vol_24hr', 'Float64'),
    ('uo_tm_24hr', 'Float64'),
    ('uo_rate_24hr', 'Float64'),
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
def output_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_output.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def input_rel():
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_input.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def weight_rel():
    vitals = duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_vitals.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )
    return duckdb.sql("""
        FROM vitals SELECT * WHERE vital_category = 'weight_kg' AND vital_value IS NOT NULL
    """)


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


@pytest.mark.skip(reason="awaiting hand-computed expected values from user (Excel)")
def test_urine_output_rate_intermediate(
    cohort_rel, output_rel, input_rel, weight_rel,
):
    """Verify per-anchor intermediate values from the UO rate pipeline.

    Uses the shared end-to-end fixtures (same patients 219-226 as
    test_kidney_with_uo) and calls _calculate_uo_score(dev=True) to expose
    the `with_weight` intermediate — one row per in-window anchor with
    trailing-bucket vol, obs time, and rate.

    Compares against `net_output_rates.csv`, hand-computed by the user in
    Excel from the verified `net_output.csv`. Weight = 100 kg for all.

    Verification chain:
      clif_output + clif_input → net_output.csv (verified by user)
      → net_output_rates.csv (user computes in Excel)
      → kidney_expected.csv (user derives final scores)

    To unskip: populate all value columns in net_output_rates.csv and remove
    the @pytest.mark.skip decorator.
    """
    import pandas as pd

    cfg = SOFA2Config()
    uo_out = _calculate_uo_score(
        cohort_rel, output_rel, input_rel, weight_rel, cfg, dev=True,
    )
    assert uo_out is not None
    assert isinstance(uo_out, tuple)
    _uo_scored, intermediates = uo_out
    with_weight = intermediates['with_weight']

    expected = pd.read_csv(
        str(FIXTURES_DIR / 'net_output_rates.csv'),
        dtype={'hospitalization_id': str},
    )
    expected['start_dttm'] = pd.to_datetime(expected['start_dttm'], format='mixed')
    expected['recorded_dttm'] = pd.to_datetime(expected['recorded_dttm'], format='mixed')

    intermediate_sort_cols = ['hospitalization_id', 'start_dttm', 'recorded_dttm']

    actual = (
        with_weight.df()
        .assign(recorded_dttm=lambda df: pd.to_datetime(df['recorded_dttm']))
        .sort_values(intermediate_sort_cols)
        .reset_index(drop=True)
    )
    expected = expected.sort_values(intermediate_sort_cols).reset_index(drop=True)

    assert len(actual) == len(expected), (
        f"Row count mismatch: expected {len(expected)}, got {len(actual)}"
    )
    assert_columns_match(actual, expected, INTERMEDIATE_COLUMNS)


@pytest.mark.skip(reason="uo-case expected values blanked for re-derivation at weight=100 kg")
def test_kidney_with_uo(cohort_rel, labs_rel, crrt_rel, output_rel, input_rel, weight_rel):
    """Verify UO-based kidney scoring for patients 219-224 (irregular intervals).

    All patients have creatinine=1.0 (score 0), so kidney score = uo_score.
    Tests GREATEST(creatinine_score, uo_score) integration:
    - 219: UO score 1 — late drop, 12h rate barely >= 0.5
    - 220: UO score 2 — sustained moderate oliguria
    - 221: UO score 3 — very low output (24h rate < 0.3)
    - 222: UO score 3 — anuria after 15:00
    - 223: UO score 0 — sparse but healthy, validity gate
    - 224: UO score 3 — irrigation subtraction changes score
    - 225: UO score 1 — high early output buffers 12h rate (between-window gap)
    """
    cfg = SOFA2Config()
    expected = load_expected(FIXTURES_DIR, 'kidney_expected.csv', 'uo')
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_kidney_subscore(
        cohort_rel, labs_rel, crrt_rel, cfg,
        output_rel=output_rel, input_rel=input_rel, weight_rel=weight_rel,
    )
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, KIDNEY_COLUMNS)
