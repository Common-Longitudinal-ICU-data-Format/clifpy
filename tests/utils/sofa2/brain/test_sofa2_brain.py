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
- 117-118: Multi-window deterioration/recovery
- 119: Sedation present, no GCS at all -> 0
- 120: Delirium drug present, no GCS -> NULL

Custom post_sedation_gcs_invalidate_hours cases:
- short_post_sed: post_sedation_gcs_invalidate_hours=2.0
- long_post_sed: post_sedation_gcs_invalidate_hours=14.0
"""

import pytest
from pathlib import Path
import pandas as pd
import duckdb

from clifpy.utils.sofa2._brain import _calculate_brain_subscore
from clifpy.utils.sofa2._utils import SOFA2Config


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

POST_SED_CONFIGS = {
    'short_post_sed': SOFA2Config(post_sedation_gcs_invalidate_hours=2.0),
    'long_post_sed': SOFA2Config(post_sedation_gcs_invalidate_hours=14.0),
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
        str(FIXTURES_DIR / 'brain_expected.csv'),
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
def assessments_rel():
    """Load patient assessments fixture as DuckDBPyRelation."""
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_patient_assessments.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def cont_meds_rel():
    """Load continuous meds fixture as DuckDBPyRelation."""
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_medication_admin_continuous.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def intm_meds_rel():
    """Load intermittent meds fixture as DuckDBPyRelation."""
    return duckdb.read_csv(
        str(FIXTURES_DIR / 'clif_medication_admin_intermittent.csv'),
        dtype={'hospitalization_id': 'VARCHAR'},
    )


@pytest.fixture
def expected_df():
    """Load expected output for default case as DataFrame."""
    return _load_expected('default')


@pytest.fixture
def result_df(cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel):
    """Run the brain subscore and return sorted result DataFrame."""
    cfg = SOFA2Config()
    result = _calculate_brain_subscore(
        cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, cfg
    )
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_brain_row_count(result_df, expected_df):
    """Verify output has one row per cohort window."""
    assert len(result_df) == len(expected_df), (
        f"Expected {len(expected_df)} rows, got {len(result_df)}"
    )


def test_brain_scores(result_df, expected_df):
    """Verify brain scores match expected (all cases from CSV)."""
    pd.testing.assert_series_equal(
        result_df['sofa2_brain'].astype('Int64'),
        expected_df['sofa2_brain'].astype('Int64'),
        check_names=False,
    )


def test_brain_gcs_min(result_df, expected_df):
    """Verify gcs_min values match expected."""
    pd.testing.assert_series_equal(
        result_df['gcs_min'].astype('Float64'),
        expected_df['gcs_min'].astype('Float64'),
        check_names=False,
    )


def test_brain_gcs_type(result_df, expected_df):
    """Verify gcs_type values match expected (important for motor fallback)."""
    # Fill NaN with empty string for comparison since both should be NaN/None
    # for rows without GCS data
    result_type = result_df['gcs_type'].fillna('')
    expected_type = expected_df['gcs_type'].fillna('')
    pd.testing.assert_series_equal(
        result_type,
        expected_type,
        check_names=False,
    )


def test_brain_flags(result_df, expected_df):
    """Verify has_sedation and has_delirium_drug flags match expected."""
    pd.testing.assert_series_equal(
        result_df['has_sedation'].astype('Int64'),
        expected_df['has_sedation'].astype('Int64'),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        result_df['has_delirium_drug'].astype('Int64'),
        expected_df['has_delirium_drug'].astype('Int64'),
        check_names=False,
    )


def test_brain_dttm_offset(result_df, expected_df):
    """Verify gcs_min_dttm_offset values match expected."""
    result_seconds = result_df['gcs_min_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected_df['gcs_min_dttm_offset'].apply(_to_total_seconds)

    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False,
    )


def test_brain_sedation_offsets(result_df, expected_df):
    """Verify sedation_start_dttm_offset and sedation_end_dttm_offset match expected."""
    for col in ['sedation_start_dttm_offset', 'sedation_end_dttm_offset']:
        result_seconds = result_df[col].apply(_to_total_seconds)
        expected_seconds = expected_df[col].apply(_to_total_seconds)
        pd.testing.assert_series_equal(
            result_seconds,
            expected_seconds,
            check_names=False,
        )


def test_brain_delirium_offset(result_df, expected_df):
    """Verify delirium_drug_dttm_offset values match expected."""
    result_seconds = result_df['delirium_drug_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected_df['delirium_drug_dttm_offset'].apply(_to_total_seconds)
    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False,
    )


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


# --- Custom post_sedation_gcs_invalidate_hours tests ---


@pytest.mark.parametrize('case', ['short_post_sed', 'long_post_sed'])
def test_brain_custom_post_sed(
    cohort_rel, assessments_rel, cont_meds_rel, intm_meds_rel, case
):
    """Verify custom post_sedation_gcs_invalidate_hours produces expected results."""
    cfg = POST_SED_CONFIGS[case]
    expected = _load_expected(case)
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

    assert len(result_df) == len(expected), (
        f"case={case}: expected {len(expected)} rows, got {len(result_df)}"
    )
    pd.testing.assert_series_equal(
        result_df['sofa2_brain'].astype('Int64'),
        expected['sofa2_brain'].astype('Int64'),
        check_names=False,
    )
    pd.testing.assert_series_equal(
        result_df['gcs_min'].astype('Float64'),
        expected['gcs_min'].astype('Float64'),
        check_names=False,
    )
    result_seconds = result_df['gcs_min_dttm_offset'].apply(_to_total_seconds)
    expected_seconds = expected['gcs_min_dttm_offset'].apply(_to_total_seconds)
    pd.testing.assert_series_equal(
        result_seconds,
        expected_seconds,
        check_names=False,
    )
