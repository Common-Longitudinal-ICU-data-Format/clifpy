"""Tests for the versatile patient-weight lookup (`attach_weight`).

Each fallback strategy maps to one of the weight-availability scenarios in
dev/weighted-unit-impact/_weight_scenarios.py:

    1  weight before the dose              -> 'asof'
    2  only after the dose, same stay      -> 'same_hosp_earliest'
    3  only in an earlier stay             -> 'prior_hosp_latest'
    4  only in a later stay                -> 'later_hosp_earliest'
    5  none anywhere                       -> NULL

Style follows tests/utils/med_unit_converter/test_unit_converter.py: small
in-test pandas literals, one behaviour per test, values asserted rather than
just statuses.
"""

import duckdb
import numpy as np
import pandas as pd
import pytest

from clifpy.utils.unit_converter import (
    WEIGHT_FALLBACKS,
    attach_weight,
    convert_dose_units_by_med_category,
    find_most_recent_weight,
)

T0 = pd.Timestamp('2024-03-10 10:00')  # dose time used by most rows
WEIGHT_MISSING_UNWEIGHTED = 'cannot convert unweighted to weighted: weight_kg is missing'


def _ts(s: str) -> pd.Timestamp:
    return pd.Timestamp(s)


def _vitals(rows) -> pd.DataFrame:
    """`rows`: iterable of (hospitalization_id, recorded_dttm, weight)."""
    return pd.DataFrame(
        [(h, _ts(t), 'weight_kg', v) for h, t, v in rows],
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value'],
    )


def _hosp(rows) -> pd.DataFrame:
    """`rows`: iterable of (hospitalization_id, patient_id, admission_dttm)."""
    return pd.DataFrame(
        [(h, p, _ts(a)) for h, p, a in rows],
        columns=['hospitalization_id', 'patient_id', 'admission_dttm'],
    )


def _meds(rows) -> pd.DataFrame:
    """`rows`: iterable of (hospitalization_id, admin_dttm); adds an `rn` key."""
    return pd.DataFrame({
        'rn': range(len(rows)),
        'hospitalization_id': [h for h, _ in rows],
        'admin_dttm': [_ts(t) for _, t in rows],
        'med_category': 'propofol',
    })


def _attach(med_df, vitals_df, **kwargs) -> pd.DataFrame:
    """Run attach_weight and return a DataFrame sorted back into input order."""
    return (
        attach_weight(med_df, vitals_df, **kwargs)
        .df()
        .sort_values('rn')
        .reset_index(drop=True)
    )


@pytest.fixture
def scenario_data():
    """One stay per scenario, plus the stays the cross-stay strategies read.

    Patients and stays:

    - P1 / H_ASOF: weights 08:00 (70), 09:00 (72), 11:00 (80); dose 10:00
    - P2 / H_AFTER: weights only after the dose, 11:00 (75), 13:00 (77)
    - P3 / H_P3_OLD (Jan) has weights; H_PRIOR (Mar) has none
    - P4 / H_LATER (Mar) has none; H_P4_NEW (May) has weights
    - P5 / H_NONE: no weight in any stay
    """
    vitals = _vitals([
        ('H_ASOF', '2024-03-10 08:00', 70.0),
        ('H_ASOF', '2024-03-10 09:00', 72.0),
        ('H_ASOF', '2024-03-10 11:00', 80.0),
        ('H_AFTER', '2024-03-10 11:00', 75.0),
        ('H_AFTER', '2024-03-10 13:00', 77.0),
        ('H_P3_OLD', '2024-01-02 09:00', 68.0),
        ('H_P3_OLD', '2024-01-03 09:00', 69.0),
        ('H_P4_NEW', '2024-05-02 09:00', 90.0),
        ('H_P4_NEW', '2024-05-05 09:00', 91.0),
    ])
    hosp = _hosp([
        ('H_ASOF', 'P1', '2024-03-09 00:00'),
        ('H_AFTER', 'P2', '2024-03-09 00:00'),
        ('H_P3_OLD', 'P3', '2024-01-01 00:00'),
        ('H_PRIOR', 'P3', '2024-03-09 00:00'),
        ('H_LATER', 'P4', '2024-03-09 00:00'),
        ('H_P4_NEW', 'P4', '2024-05-01 00:00'),
        ('H_NONE', 'P5', '2024-03-09 00:00'),
    ])
    meds = _meds([
        ('H_ASOF', '2024-03-10 10:00'),
        ('H_AFTER', '2024-03-10 10:00'),
        ('H_PRIOR', '2024-03-10 10:00'),
        ('H_LATER', '2024-03-10 10:00'),
        ('H_NONE', '2024-03-10 10:00'),
    ])
    return meds, vitals, hosp


# ===========================================
# One strategy per scenario
# ===========================================

def test_scenario1_asof_picks_latest_prior_weight(scenario_data):
    meds, vitals, _ = scenario_data
    out = _attach(meds, vitals)
    row = out.loc[0]
    assert row['weight_kg'] == 72.0          # 09:00, not 08:00 or the later 11:00
    assert row['_weight_source'] == 'asof'
    assert row['_weight_recorded_dttm'] == _ts('2024-03-10 09:00')
    assert row['_weight_age_hours'] == pytest.approx(1.0)
    # asof alone recovers nothing for the other four scenarios
    assert out.loc[1:, 'weight_kg'].isna().all()
    assert out.loc[1:, '_weight_source'].isna().all()


def test_scenario2_same_hosp_earliest(scenario_data):
    meds, vitals, _ = scenario_data
    out = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    row = out.loc[1]
    assert row['weight_kg'] == 75.0
    assert row['_weight_source'] == 'same_hosp_earliest'
    # charted an hour AFTER the dose -> negative age
    assert row['_weight_age_hours'] == pytest.approx(-1.0)


def test_scenario3_prior_hosp_latest(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(
        meds, vitals, fallbacks=('asof', 'prior_hosp_latest'), hospitalization_df=hosp,
    )
    row = out.loc[2]
    assert row['weight_kg'] == 69.0          # latest of the earlier stay's two
    assert row['_weight_source'] == 'prior_hosp_latest'
    assert row['_weight_recorded_dttm'] == _ts('2024-01-03 09:00')
    expected_hours = (_ts('2024-03-10 10:00') - _ts('2024-01-03 09:00')) / pd.Timedelta(hours=1)
    assert row['_weight_age_hours'] == pytest.approx(expected_hours)
    # a later stay's weight is not a prior stay's weight
    assert pd.isna(out.loc[3, 'weight_kg'])


def test_scenario4_later_hosp_earliest(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(
        meds, vitals, fallbacks=('asof', 'later_hosp_earliest'), hospitalization_df=hosp,
    )
    row = out.loc[3]
    assert row['weight_kg'] == 90.0          # earliest of the later stay's two
    assert row['_weight_source'] == 'later_hosp_earliest'
    assert row['_weight_age_hours'] < 0
    # an earlier stay's weight is not a later stay's weight
    assert pd.isna(out.loc[2, 'weight_kg'])


def test_scenario5_none_anywhere(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    row = out.loc[4]
    assert pd.isna(row['weight_kg'])
    assert pd.isna(row['_weight_source'])
    assert pd.isna(row['_weight_recorded_dttm'])
    assert pd.isna(row['_weight_age_hours'])
    # and the full chain resolves every other scenario to its own strategy
    assert out.loc[:3, '_weight_source'].tolist() == [
        'asof', 'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest',
    ]


# ===========================================
# Chain order
# ===========================================

def test_first_available_strategy_wins():
    """A row eligible for several strategies takes the first in the chain."""
    vitals = _vitals([
        ('H_OLD', '2024-01-02 09:00', 60.0),   # prior stay
        ('H_CUR', '2024-03-10 12:00', 65.0),   # same stay, after the dose
    ])
    hosp = _hosp([
        ('H_OLD', 'P1', '2024-01-01'),
        ('H_CUR', 'P1', '2024-03-09'),
    ])
    meds = _meds([('H_CUR', '2024-03-10 10:00')])

    same_first = _attach(
        meds, vitals, hospitalization_df=hosp,
        fallbacks=('asof', 'same_hosp_earliest', 'prior_hosp_latest'),
    )
    prior_first = _attach(
        meds, vitals, hospitalization_df=hosp,
        fallbacks=('asof', 'prior_hosp_latest', 'same_hosp_earliest'),
    )
    assert same_first.loc[0, ['weight_kg', '_weight_source']].tolist() == [65.0, 'same_hosp_earliest']
    assert prior_first.loc[0, ['weight_kg', '_weight_source']].tolist() == [60.0, 'prior_hosp_latest']


def test_asof_need_not_come_first(scenario_data):
    """The chain is caller-ordered: earliest-in-stay can outrank asof."""
    meds, vitals, _ = scenario_data
    out = _attach(meds, vitals, fallbacks=('same_hosp_earliest', 'asof'))
    assert out.loc[0, 'weight_kg'] == 70.0   # 08:00, the stay's first weight
    assert out.loc[0, '_weight_source'] == 'same_hosp_earliest'


def test_equal_admission_counts_as_later_not_prior():
    """Two stays admitted at the same instant are 'later' to each other."""
    vitals = _vitals([('H_TWIN', '2024-03-09 02:00', 81.0)])
    hosp = _hosp([
        ('H_CUR', 'P1', '2024-03-09 00:00'),
        ('H_TWIN', 'P1', '2024-03-09 00:00'),
    ])
    meds = _meds([('H_CUR', '2024-03-09 01:00')])
    prior = _attach(meds, vitals, fallbacks=('prior_hosp_latest',), hospitalization_df=hosp)
    later = _attach(meds, vitals, fallbacks=('later_hosp_earliest',), hospitalization_df=hosp)
    assert pd.isna(prior.loc[0, 'weight_kg'])
    assert later.loc[0, 'weight_kg'] == 81.0


# ===========================================
# Staleness, bad weights, ties
# ===========================================

def test_max_staleness_demotes_asof():
    """A 10-day-old asof weight is dropped at a 7-day limit: to NULL alone,
    to the next strategy when one is configured."""
    vitals = _vitals([
        ('H_CUR', '2024-03-01 10:00', 70.0),   # 10 days before the dose
        ('H_CUR', '2024-03-11 14:00', 80.0),   # same stay, 4 h after the dose
        ('H_NEW', '2024-06-02 09:00', 88.0),   # later stay
    ])
    hosp = _hosp([
        ('H_CUR', 'P1', '2024-02-28'),
        ('H_NEW', 'P1', '2024-06-01'),
    ])
    meds = _meds([('H_CUR', '2024-03-11 10:00')])

    unlimited = _attach(meds, vitals)
    assert unlimited.loc[0, 'weight_kg'] == 70.0
    assert unlimited.loc[0, '_weight_age_hours'] == pytest.approx(240.0)

    loose = _attach(meds, vitals, max_staleness_days=10)
    assert loose.loc[0, 'weight_kg'] == 70.0  # exactly at the limit is kept

    alone = _attach(meds, vitals, max_staleness_days=7)
    assert pd.isna(alone.loc[0, 'weight_kg'])
    assert pd.isna(alone.loc[0, '_weight_source'])

    # same_hosp_earliest is gated too: it skips the stale 70 (the stay's
    # earliest) and takes the first eligible weight, charted after the dose.
    chained = _attach(
        meds, vitals, max_staleness_days=7,
        fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert chained.loc[0, 'weight_kg'] == 80.0
    assert chained.loc[0, '_weight_source'] == 'same_hosp_earliest'
    assert chained.loc[0, '_weight_age_hours'] == pytest.approx(-4.0)
    uncapped_earliest = _attach(meds, vitals, fallbacks=('same_hosp_earliest',))
    assert uncapped_earliest.loc[0, 'weight_kg'] == 70.0

    # later_hosp_earliest is never gated
    later = _attach(
        meds, vitals, max_staleness_days=7, hospitalization_df=hosp,
        fallbacks=('asof', 'later_hosp_earliest'),
    )
    assert later.loc[0, 'weight_kg'] == 88.0
    assert later.loc[0, '_weight_source'] == 'later_hosp_earliest'


def test_staleness_same_hosp_earliest_is_per_row():
    """Under a cap the earliest ELIGIBLE weight moves with each dose."""
    vitals = _vitals([
        ('H1', '2024-03-01 09:00', 60.0),
        ('H1', '2024-03-08 09:00', 65.0),
        ('H1', '2024-03-20 09:00', 70.0),
    ])
    meds = _meds([
        ('H1', '2024-03-05 09:00'),   # 60 is 4 d old -> eligible
        ('H1', '2024-03-12 09:00'),   # 60 is 11 d old; 65 is 4 d old
        ('H1', '2024-03-18 09:00'),   # both stale; 70 is after the dose
    ])
    capped = _attach(meds, vitals, fallbacks=('same_hosp_earliest',), max_staleness_days=7)
    assert capped['weight_kg'].tolist() == [60.0, 65.0, 70.0]
    assert capped['_weight_age_hours'].tolist() == pytest.approx([96.0, 96.0, -48.0])

    uncapped = _attach(meds, vitals, fallbacks=('same_hosp_earliest',))
    assert uncapped['weight_kg'].tolist() == [60.0, 60.0, 60.0]


def test_staleness_gates_prior_hosp_latest():
    vitals = _vitals([('H_OLD', '2024-02-01 10:00', 66.0)])  # 30 d before the dose
    hosp = _hosp([
        ('H_OLD', 'P1', '2024-01-30'),
        ('H_CUR', 'P1', '2024-03-01'),
    ])
    meds = _meds([('H_CUR', '2024-03-02 10:00')])
    kw = dict(fallbacks=('prior_hosp_latest',), hospitalization_df=hosp)
    assert _attach(meds, vitals, **kw).loc[0, 'weight_kg'] == 66.0
    assert pd.isna(_attach(meds, vitals, max_staleness_days=7, **kw).loc[0, 'weight_kg'])
    assert _attach(meds, vitals, max_staleness_days=30, **kw).loc[0, 'weight_kg'] == 66.0


@pytest.mark.parametrize('fallbacks', [WEIGHT_FALLBACKS, WEIGHT_FALLBACKS[::-1]])
def test_unbounding_cap_matches_no_cap(scenario_data, fallbacks):
    """A cap nothing can exceed must reproduce the uncapped result exactly,
    even though same_hosp_earliest switches to a per-row join under a cap."""
    meds, vitals, hosp = scenario_data
    kw = dict(fallbacks=fallbacks, hospitalization_df=hosp)
    pd.testing.assert_frame_equal(
        _attach(meds, vitals, **kw),
        _attach(meds, vitals, max_staleness_days=100_000, **kw),
    )


def test_zero_and_negative_weights_ignored():
    vitals = _vitals([
        ('H_MIX', '2024-03-10 07:00', 70.0),
        ('H_MIX', '2024-03-10 08:00', 0.0),
        ('H_MIX', '2024-03-10 09:00', -5.0),
        ('H_ZERO', '2024-03-10 08:00', 0.0),
    ])
    meds = _meds([('H_MIX', '2024-03-10 10:00'), ('H_ZERO', '2024-03-10 10:00')])
    out = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    assert out.loc[0, 'weight_kg'] == 70.0   # skips past the 08:00 / 09:00 junk
    assert out.loc[0, '_weight_recorded_dttm'] == _ts('2024-03-10 07:00')
    assert pd.isna(out.loc[1, 'weight_kg'])  # a stay with only a zero has nothing


def test_same_instant_duplicates_are_averaged():
    vitals = _vitals([
        ('H_DUP', '2024-03-10 09:00', 70.0),
        ('H_DUP', '2024-03-10 09:00', 74.0),
    ])
    meds = _meds([('H_DUP', '2024-03-10 10:00')])
    runs = [_attach(meds, vitals).loc[0, 'weight_kg'] for _ in range(3)]
    assert runs == [72.0, 72.0, 72.0]
    capped = _attach(meds, vitals, fallbacks=('same_hosp_earliest',), max_staleness_days=1)
    assert capped.loc[0, 'weight_kg'] == 72.0


def test_cross_stay_same_instant_tie_is_averaged():
    """Two prior stays charting at the same instant resolve by average."""
    vitals = _vitals([
        ('H_A', '2024-01-05 09:00', 60.0),
        ('H_B', '2024-01-05 09:00', 64.0),
    ])
    hosp = _hosp([
        ('H_A', 'P1', '2024-01-01'),
        ('H_B', 'P1', '2024-01-04'),
        ('H_CUR', 'P1', '2024-03-01'),
    ])
    meds = _meds([('H_CUR', '2024-03-02 10:00')])
    out = _attach(meds, vitals, fallbacks=('prior_hosp_latest',), hospitalization_df=hosp)
    assert out.loc[0, 'weight_kg'] == 62.0


# ===========================================
# Shape: row count, input columns, lazy relation
# ===========================================

@pytest.mark.parametrize('max_staleness_days', [None, 3])
def test_row_count_and_input_columns_preserved(max_staleness_days):
    """No fan-out even with duplicate med rows, several stays per patient and
    a duplicated hospitalization row; input columns come back untouched."""
    vitals = _vitals(
        [('H_OLD', f'2024-01-0{d} 09:00', 60.0 + d) for d in range(1, 6)]
        + [('H_CUR', f'2024-03-1{d} 09:00', 70.0 + d) for d in range(0, 5)]
        + [('H_NEW', f'2024-06-0{d} 09:00', 80.0 + d) for d in range(1, 6)]
    )
    hosp = _hosp([
        ('H_OLD', 'P1', '2024-01-01'),
        ('H_CUR', 'P1', '2024-03-09'),
        ('H_CUR', 'P1', '2024-03-09'),   # duplicated row must not duplicate output
        ('H_NEW', 'P1', '2024-06-01'),
    ])
    meds = _meds([
        ('H_CUR', '2024-03-09 12:00'),
        ('H_CUR', '2024-03-09 12:00'),   # exact duplicate med row
        ('H_CUR', '2024-03-12 12:00'),
        ('H_OLD', '2024-01-03 12:00'),
    ])
    meds['med_dose'] = [1.0, 1.0, 2.0, 3.0]
    meds['note'] = ['a', 'b', None, 'd']

    rel = attach_weight(
        meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp,
        max_staleness_days=max_staleness_days,
    )
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    assert rel.columns == list(meds.columns) + [
        'weight_kg', '_weight_source', '_weight_recorded_dttm', '_weight_age_hours',
    ]
    out = rel.df().sort_values('rn').reset_index(drop=True)
    assert len(out) == len(meds)
    pd.testing.assert_frame_equal(out[list(meds.columns)], meds, check_dtype=False)


def test_custom_time_col():
    vitals = _vitals([('H1', '2024-03-10 09:00', 72.0)])
    meds = _meds([('H1', '2024-03-10 10:00')]).rename(columns={'admin_dttm': 'event_dttm'})
    out = _attach(meds, vitals, time_col='event_dttm')
    assert out.loc[0, 'weight_kg'] == 72.0
    assert 'admin_dttm' not in out.columns


# ===========================================
# Timezones: instants in == instants out
# ===========================================

@pytest.fixture
def tokyo_default_tz():
    """Pin the default connection to a zone far from both input and UTC."""
    before = duckdb.sql("SELECT current_setting('TimeZone')").fetchone()[0]
    duckdb.execute("SET TimeZone='Asia/Tokyo'")
    try:
        yield
    finally:
        duckdb.execute(f"SET TimeZone='{before}'")


def test_tz_aware_pandas_instants_preserved(tokyo_default_tz):
    tz = 'US/Eastern'
    meds = _meds([('H1', '2024-03-10 10:00'), ('H1', '2024-11-03 01:30')])
    meds['admin_dttm'] = meds['admin_dttm'].dt.tz_localize(tz, ambiguous=True)
    vitals = _vitals([('H1', '2024-03-10 08:00', 72.0)])
    vitals['recorded_dttm'] = vitals['recorded_dttm'].dt.tz_localize(tz)

    rel = attach_weight(meds, vitals)
    assert dict(zip(rel.columns, map(str, rel.types)))['admin_dttm'] == 'TIMESTAMP WITH TIME ZONE'
    out = rel.df().sort_values('rn').reset_index(drop=True)

    # Compare instants, not dtypes: TIMESTAMPTZ is microsecond-resolution, so
    # the round-trip comes back as datetime64[us] even though no value moved.
    assert (
        out['admin_dttm'].dt.tz_convert('UTC').tolist()
        == meds['admin_dttm'].dt.tz_convert('UTC').tolist()
    )
    assert out.loc[0, '_weight_recorded_dttm'] == vitals.loc[0, 'recorded_dttm']
    # 08:00 -> 10:00 on the US spring-forward day is still 2 wall-hours and 2 real ones
    assert out.loc[0, '_weight_age_hours'] == pytest.approx(2.0)


@pytest.mark.parametrize('fallbacks', [('asof',), ('same_hosp_earliest',)])
def test_staleness_uses_elapsed_time_across_dst(tokyo_default_tz, fallbacks):
    """US spring-forward: 10:00 EST to 10:00 EDT the next day is 24 wall-clock
    hours but 23 real ones. The cap must see 23."""
    tz = 'US/Eastern'
    meds = _meds([('H1', '2024-03-10 10:00')])
    meds['admin_dttm'] = meds['admin_dttm'].dt.tz_localize(tz)
    vitals = _vitals([('H1', '2024-03-09 10:00', 72.0)])
    vitals['recorded_dttm'] = vitals['recorded_dttm'].dt.tz_localize(tz)

    kept = _attach(meds, vitals, fallbacks=fallbacks, max_staleness_days=23.5 / 24)
    assert kept.loc[0, 'weight_kg'] == 72.0
    assert kept.loc[0, '_weight_age_hours'] == pytest.approx(23.0)
    dropped = _attach(meds, vitals, fallbacks=fallbacks, max_staleness_days=22.5 / 24)
    assert pd.isna(dropped.loc[0, 'weight_kg'])


def test_timestamptz_relation_input(tokyo_default_tz):
    meds = duckdb.sql("""
        SELECT 0 AS rn, 'H1' AS hospitalization_id
            , TIMESTAMPTZ '2024-03-10 15:00:00+00' AS admin_dttm
    """)
    vitals = duckdb.sql("""
        SELECT 'H1' AS hospitalization_id
            , TIMESTAMPTZ '2024-03-10 12:30:00+00' AS recorded_dttm
            , 'weight_kg' AS vital_category, 80.0 AS vital_value
    """)
    rel = attach_weight(meds, vitals)
    types = dict(zip(rel.columns, map(str, rel.types)))
    assert types['admin_dttm'] == 'TIMESTAMP WITH TIME ZONE'
    assert types['_weight_recorded_dttm'] == 'TIMESTAMP WITH TIME ZONE'

    out = rel.df()
    assert out.loc[0, 'admin_dttm'] == pd.Timestamp('2024-03-10 15:00', tz='UTC')
    assert out.loc[0, '_weight_recorded_dttm'] == pd.Timestamp('2024-03-10 12:30', tz='UTC')
    assert out.loc[0, '_weight_age_hours'] == pytest.approx(2.5)


# ===========================================
# Validation
# ===========================================

@pytest.mark.parametrize('fallbacks, match', [
    (('asof', 'nearest'), 'unknown weight fallback'),
    ((), 'at least one strategy'),
    ('asof', 'not a str'),
    (('prior_hosp_latest',), 'need hospitalization_df'),
    (('asof', 'later_hosp_earliest'), 'need hospitalization_df'),
])
def test_invalid_fallbacks_raise(scenario_data, fallbacks, match):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError, match=match):
        attach_weight(meds, vitals, fallbacks=fallbacks)


def test_unknown_fallback_message_lists_choices(scenario_data):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError) as exc:
        attach_weight(meds, vitals, fallbacks=('nearest',))
    for name in WEIGHT_FALLBACKS:
        assert name in str(exc.value)


def test_hospitalization_df_missing_columns_raise(scenario_data):
    meds, vitals, hosp = scenario_data
    with pytest.raises(ValueError, match='admission_dttm'):
        attach_weight(
            meds, vitals, fallbacks=('prior_hosp_latest',),
            hospitalization_df=hosp.drop(columns='admission_dttm'),
        )


def test_missing_input_columns_raise(scenario_data):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError, match='med_df'):
        attach_weight(meds.drop(columns='admin_dttm'), vitals)
    with pytest.raises(ValueError, match='vitals_df'):
        attach_weight(meds, vitals.drop(columns='recorded_dttm'))
    with pytest.raises(ValueError, match='already has'):
        attach_weight(meds.assign(weight_kg=70.0), vitals)


@pytest.mark.parametrize('bad', [-1, float('nan'), float('inf'), 'seven', True])
def test_invalid_staleness_raises(scenario_data, bad):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError, match='max_staleness_days'):
        attach_weight(meds, vitals, max_staleness_days=bad)


# ===========================================
# find_most_recent_weight: legacy contract
# ===========================================

def test_find_most_recent_weight_legacy_contract(scenario_data):
    meds, vitals, _ = scenario_data
    legacy_cols = list(meds.columns) + ['weight_kg', '_weight_recorded_dttm', '_weight_source']

    plain = find_most_recent_weight(meds, vitals)
    assert plain.columns == legacy_cols
    plain = plain.df()
    by_id = plain.set_index('hospitalization_id')
    assert by_id.loc['H_ASOF', '_weight_source'] == 'asof'
    assert pd.isna(by_id.loc['H_AFTER', '_weight_source'])

    fb = find_most_recent_weight(meds, vitals, fallback_on_earliest=True)
    assert fb.columns == legacy_cols
    fb = fb.df()
    by_id = fb.set_index('hospitalization_id')
    assert by_id.loc['H_ASOF', '_weight_source'] == 'asof'
    assert by_id.loc['H_AFTER', '_weight_source'] == 'earliest_fallback'
    assert by_id.loc['H_AFTER', 'weight_kg'] == 75.0
    assert pd.isna(by_id.loc['H_NONE', '_weight_source'])
    # legacy ORDER BY hospitalization_id, admin_dttm, med_category
    assert fb['hospitalization_id'].tolist() == sorted(meds['hospitalization_id'])


# ===========================================
# convert_dose_units_by_med_category integration
# ===========================================

@pytest.fixture
def lagged_weight_case():
    """A propofol `mg` dose whose stay's only weight is charted after it."""
    med_df = pd.DataFrame({
        'rn': [0],
        'hospitalization_id': ['H_LAGGED'],
        'admin_dttm': [_ts('2024-01-01 09:00')],
        'med_category': ['propofol'],
        'med_dose': [10.0],
        'med_dose_unit': ['mg'],
    })
    vitals_df = _vitals([('H_LAGGED', '2024-01-01 09:30', 72.0)])
    return med_df, vitals_df


def test_converter_weight_fallbacks_recover_lagged_weight(lagged_weight_case):
    med_df, vitals_df = lagged_weight_case
    prefs = {'propofol': 'mcg/kg'}

    without, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units=prefs, override=True,
    )
    assert without.loc[0, '_convert_status'] == WEIGHT_MISSING_UNWEIGHTED

    with_fb, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units=prefs, override=True,
        show_intermediate=True, weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert with_fb.loc[0, '_convert_status'] == 'success'
    assert with_fb.loc[0, '_weight_source'] == 'same_hosp_earliest'
    assert '_weight_age_hours' not in with_fb.columns
    # 10 mg = 10,000 mcg over 72 kg
    assert with_fb.loc[0, 'med_dose_converted'] == pytest.approx(10_000 / 72.0, rel=1e-9)
    assert with_fb.loc[0, 'med_dose_unit_converted'] == 'mcg/kg'


def test_converter_prefilled_weight_wins_over_fallbacks(lagged_weight_case):
    med_df, vitals_df = lagged_weight_case
    out, _ = convert_dose_units_by_med_category(
        med_df.assign(weight_kg=50.0), vitals_df=vitals_df,
        preferred_units={'propofol': 'mcg/kg'}, override=True,
        weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert out.loc[0, 'med_dose_converted'] == pytest.approx(10_000 / 50.0, rel=1e-9)


def test_converter_rejects_conflicting_weight_args(lagged_weight_case):
    med_df, vitals_df = lagged_weight_case
    prefs = {'propofol': 'mcg/kg'}
    with pytest.raises(ValueError, match='not both'):
        convert_dose_units_by_med_category(
            med_df, vitals_df=vitals_df, preferred_units=prefs,
            fallback_on_earliest=True, weight_fallbacks=('asof',),
        )
    with pytest.raises(ValueError, match='requires weight_fallbacks'):
        convert_dose_units_by_med_category(
            med_df, vitals_df=vitals_df, preferred_units=prefs,
            weight_max_staleness_days=7,
        )
    with pytest.raises(ValueError, match='unknown weight fallback'):
        convert_dose_units_by_med_category(
            med_df, vitals_df=vitals_df, preferred_units=prefs,
            weight_fallbacks=('closest',),
        )
