"""Tests for SOFA-2 respiratory subscore calculation.

Test cases (documented in resp_expected.csv notes column):
- 401-405: P/F ratio score boundaries (>300, <=300, <=225, <=150+adv, <=75+adv)
- 406-408: S/F ratio score boundaries (>300, <=250, <=120+adv)
- 409-410: Advanced support gate (P/F <=150/<=75 without advanced -> capped at 2)
- 411-413: FiO2 imputation (room air, NC+LPM, explicit overrides LPM)
- 414: ECMO override -> 4
- 415: P/F priority over S/F
- 416: No data -> NULL
- 417: Pre-window FiO2 within lookback+tolerance
- 418: Tolerance exceeded -> NULL
- 419: Device heuristic: mode -> imv inferred
- 420: Multi-window (stable -> deterioration)

Custom tolerance cases (case column in expected CSV):
- short_tolerance: pf_sf_tolerance_hours=1.0
- long_tolerance: pf_sf_tolerance_hours=8.0
"""

import pytest
from pathlib import Path

from clifpy.utils.sofa2._resp import _calculate_resp_subscore
from clifpy.utils.sofa2._utils import SOFA2Config
from tests.utils.sofa2.conftest import load_expected, assert_columns_match, load_csv_fixture


FIXTURES_DIR = Path(__file__).parent
SORT_COLS = ['hospitalization_id', 'start_dttm']

RESP_COLUMNS = [
    ('sofa2_resp', 'Int64'),
    ('pf_ratio', 'Float64'),
    ('sf_ratio', 'Float64'),
    ('has_advanced_support', 'Int64'),
    ('has_ecmo', 'Int64'),
    ('fio2_at_worst', 'Float64'),
    ('device_category', 'str'),
    ('pao2_at_worst', 'Float64'),
    ('pao2_dttm_offset', 'offset'),
    ('spo2_at_worst', 'Float64'),
    ('spo2_dttm_offset', 'offset'),
    ('fio2_dttm_offset', 'offset'),
    ('pf_sf_dttm_offset', 'offset'),
    ('ecmo_dttm_offset', 'offset'),
]

TOLERANCE_CONFIGS = {
    'short_tolerance': SOFA2Config(pf_sf_tolerance_hours=1.0),
    'long_tolerance': SOFA2Config(pf_sf_tolerance_hours=8.0),
}


@pytest.fixture
def cohort_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_cohort.csv', ['start_dttm', 'end_dttm'])


@pytest.fixture
def resp_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_respiratory_support.csv', ['recorded_dttm'])


@pytest.fixture
def labs_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_labs.csv', ['lab_collect_dttm'])


@pytest.fixture
def vitals_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_vitals.csv', ['recorded_dttm'])


@pytest.fixture
def ecmo_rel():
    return load_csv_fixture(FIXTURES_DIR / 'clif_ecmo_mcs.csv', ['recorded_dttm'])


@pytest.fixture
def expected_df():
    return load_expected(FIXTURES_DIR, 'resp_expected.csv', 'default')


@pytest.fixture
def result_df(cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel):
    cfg = SOFA2Config()
    result = _calculate_resp_subscore(
        cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg,
    )
    return result.df().sort_values(SORT_COLS).reset_index(drop=True)


def test_resp_default(result_df, expected_df):
    """Verify all output columns match expected for default case."""
    assert_columns_match(result_df, expected_df, RESP_COLUMNS)


def test_resp_intermediates(cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel):
    """Verify dev=True returns intermediate relations."""
    cfg = SOFA2Config()
    result, intermediates = _calculate_resp_subscore(
        cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg, dev=True,
    )

    expected_keys = {
        'imv_inferred_from_mode', 'fio2_ffilled', 'fio2_in_window',
        'fio2_pre_window', 'fio2_imputed', 'pao2_in_window',
        'pao2_pre_window', 'pao2_measurements', 'spo2_in_window',
        'spo2_pre_window', 'spo2_measurements', 'concurrent_pf',
        'concurrent_sf', 'resp_agg', 'ecmo_flag',
    }
    assert set(intermediates.keys()) == expected_keys

    for key in expected_keys:
        assert hasattr(intermediates[key], 'df'), f"{key} is not a DuckDBPyRelation"


@pytest.mark.parametrize('case', ['short_tolerance', 'long_tolerance'])
def test_resp_custom_tolerance(cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, case):
    """Verify custom tolerance config produces expected results from CSV."""
    cfg = TOLERANCE_CONFIGS[case]
    expected = load_expected(FIXTURES_DIR, 'resp_expected.csv', case)
    hosp_ids = expected['hospitalization_id'].tolist()

    result = _calculate_resp_subscore(
        cohort_rel, resp_rel, labs_rel, vitals_rel, ecmo_rel, cfg,
    )
    result_df = (
        result.df()
        .query('hospitalization_id in @hosp_ids')
        .sort_values(SORT_COLS)
        .reset_index(drop=True)
    )

    assert_columns_match(result_df, expected, RESP_COLUMNS)
