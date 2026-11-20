"""Tests for the versatile patient-weight lookup (`attach_weight`).

Each fallback strategy maps to one weight-availability scenario:

    1  weight before the dose              -> 'asof'
    2  only after the dose, same stay      -> 'same_hosp_earliest'
    3  only in an earlier stay             -> 'prior_hosp_latest'
    4  only in a later stay                -> 'later_hosp_earliest'
    5  none anywhere                       -> NULL

Style follows tests/utils/med_unit_converter/test_unit_converter.py: small
in-test polars literals, one behaviour per test, values asserted rather than
just statuses.
"""

import warnings
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from fractions import Fraction

import duckdb
import numpy as np
import polars as pl
import polars.selectors as cs
import pytest
from polars.testing import assert_frame_equal

from clifpy.utils.unit_converter import (
    WEIGHT_FALLBACKS,
    attach_weight,
    convert_dose_units_by_med_category,
    find_most_recent_weight,
)
from clifpy.utils.unit_converter._columns import (
    PAIR_QA_COLUMNS,
    PIPELINE_OWNED_COLUMNS,
    WEIGHT_OUTPUT_COLUMNS,
    pair_column_name,
)
from clifpy.utils.unit_converter._weight import _coerce_max_staleness_days

T0 = datetime(2024, 3, 10, 10, 0)  # dose time used by most rows
WEIGHT_MISSING_UNWEIGHTED = 'cannot convert unweighted to weighted: weight_kg is missing'
WEIGHT_MISSING_WEIGHTED = 'cannot convert weighted to unweighted: weight_kg is missing'


def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s)


def _localize(df: pl.DataFrame, col: str, tz: str = 'US/Eastern') -> pl.DataFrame:
    """Copy of `df` (eager or lazy) with its naive `col` localized to `tz`."""
    return df.with_columns(pl.col(col).dt.replace_time_zone(tz))


def _vitals(rows) -> pl.DataFrame:
    """`rows`: iterable of (hospitalization_id, recorded_dttm, weight)."""
    return pl.DataFrame(
        [(h, _ts(t), 'weight_kg', float(v)) for h, t, v in rows],
        schema={
            'hospitalization_id': pl.String, 'recorded_dttm': pl.Datetime('us'),
            'vital_category': pl.String, 'vital_value': pl.Float64,
        },
        orient='row',
    )


def _vitals_relation(rows) -> duckdb.DuckDBPyRelation:
    """Like `_vitals`, but a DuckDB relation built in SQL, with DOUBLE weights."""
    values = ', '.join(f"('{h}', TIMESTAMP '{t}', '{v}'::DOUBLE)" for h, t, v in rows)
    return duckdb.sql(f"""
        FROM (VALUES {values}) v(hospitalization_id, recorded_dttm, vital_value)
        SELECT hospitalization_id, recorded_dttm, vital_category: 'weight_kg', vital_value
    """)


def _hosp(rows) -> pl.DataFrame:
    """`rows`: iterable of (hospitalization_id, patient_id, admission_dttm)."""
    return pl.DataFrame(
        [(h, p, _ts(a)) for h, p, a in rows],
        schema={
            'hospitalization_id': pl.String, 'patient_id': pl.String,
            'admission_dttm': pl.Datetime('us'),
        },
        orient='row',
    )


def _meds(rows) -> pl.DataFrame:
    """`rows`: iterable of (hospitalization_id, admin_dttm); adds an `rn` key."""
    return pl.DataFrame(
        {
            'rn': list(range(len(rows))),
            'hospitalization_id': [h for h, _ in rows],
            'admin_dttm': [_ts(t) for _, t in rows],
            'med_category': ['propofol'] * len(rows),
        },
        schema={
            'rn': pl.Int64, 'hospitalization_id': pl.String,
            'admin_dttm': pl.Datetime('us'), 'med_category': pl.String,
        },
    )


def _attach(med_df, vitals_df, **kwargs) -> pl.DataFrame:
    """Run attach_weight (polars output) and sort it back into input order."""
    return attach_weight(med_df, vitals_df, **kwargs).sort('rn')


def _rows_by(frame: pl.DataFrame, key: str) -> dict:
    """{value of `key`: row as a dict}, for frames whose `key` values are unique."""
    return {row[key]: row for row in frame.iter_rows(named=True)}


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
    row = out.row(0, named=True)
    assert row['weight_kg'] == 72.0          # 09:00, not 08:00 or the later 11:00
    assert row['_weight_source'] == 'asof'
    assert row['_weight_recorded_dttm'] == _ts('2024-03-10 09:00')
    assert row['_weight_age_hours'] == pytest.approx(1.0)
    # asof alone recovers nothing for the other four scenarios
    assert out['weight_kg'][1:].is_null().all()
    assert out['_weight_source'][1:].is_null().all()


def test_scenario2_same_hosp_earliest(scenario_data):
    meds, vitals, _ = scenario_data
    out = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    row = out.row(1, named=True)
    assert row['weight_kg'] == 75.0
    assert row['_weight_source'] == 'same_hosp_earliest'
    # charted an hour AFTER the dose -> negative age
    assert row['_weight_age_hours'] == pytest.approx(-1.0)


def test_scenario3_prior_hosp_latest(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(
        meds, vitals, fallbacks=('asof', 'prior_hosp_latest'), hospitalization_df=hosp,
    )
    row = out.row(2, named=True)
    assert row['weight_kg'] == 69.0          # latest of the earlier stay's two
    assert row['_weight_source'] == 'prior_hosp_latest'
    assert row['_weight_recorded_dttm'] == _ts('2024-01-03 09:00')
    expected_hours = (_ts('2024-03-10 10:00') - _ts('2024-01-03 09:00')) / timedelta(hours=1)
    assert row['_weight_age_hours'] == pytest.approx(expected_hours)
    # a later stay's weight is not a prior stay's weight
    assert out['weight_kg'][3] is None


def test_scenario4_later_hosp_earliest(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(
        meds, vitals, fallbacks=('asof', 'later_hosp_earliest'), hospitalization_df=hosp,
    )
    row = out.row(3, named=True)
    assert row['weight_kg'] == 90.0          # earliest of the later stay's two
    assert row['_weight_source'] == 'later_hosp_earliest'
    assert row['_weight_age_hours'] < 0
    # an earlier stay's weight is not a later stay's weight
    assert out['weight_kg'][2] is None


def test_scenario5_none_anywhere(scenario_data):
    meds, vitals, hosp = scenario_data
    out = _attach(meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    row = out.row(4, named=True)
    assert row['weight_kg'] is None
    assert row['_weight_source'] is None
    assert row['_weight_recorded_dttm'] is None
    assert row['_weight_age_hours'] is None
    # and the full chain resolves every other scenario to its own strategy
    assert out['_weight_source'][:4].to_list() == [
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
    assert list(same_first.select('weight_kg', '_weight_source').row(0)) == [65.0, 'same_hosp_earliest']
    assert list(prior_first.select('weight_kg', '_weight_source').row(0)) == [60.0, 'prior_hosp_latest']


def test_asof_need_not_come_first(scenario_data):
    """The chain is caller-ordered: earliest-in-stay can outrank asof."""
    meds, vitals, _ = scenario_data
    out = _attach(meds, vitals, fallbacks=('same_hosp_earliest', 'asof'))
    assert out['weight_kg'][0] == 70.0   # 08:00, the stay's first weight
    assert out['_weight_source'][0] == 'same_hosp_earliest'


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
    assert prior['weight_kg'][0] is None
    assert later['weight_kg'][0] == 81.0


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
    assert unlimited['weight_kg'][0] == 70.0
    assert unlimited['_weight_age_hours'][0] == pytest.approx(240.0)

    loose = _attach(meds, vitals, max_staleness_days=10)
    assert loose['weight_kg'][0] == 70.0  # exactly at the limit is kept

    alone = _attach(meds, vitals, max_staleness_days=7)
    assert alone['weight_kg'][0] is None
    assert alone['_weight_source'][0] is None

    # same_hosp_earliest is gated too: it skips the stale 70 (the stay's
    # earliest) and takes the first eligible weight, charted after the dose.
    chained = _attach(
        meds, vitals, max_staleness_days=7,
        fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert chained['weight_kg'][0] == 80.0
    assert chained['_weight_source'][0] == 'same_hosp_earliest'
    assert chained['_weight_age_hours'][0] == pytest.approx(-4.0)
    uncapped_earliest = _attach(meds, vitals, fallbacks=('same_hosp_earliest',))
    assert uncapped_earliest['weight_kg'][0] == 70.0

    # later_hosp_earliest is never gated
    later = _attach(
        meds, vitals, max_staleness_days=7, hospitalization_df=hosp,
        fallbacks=('asof', 'later_hosp_earliest'),
    )
    assert later['weight_kg'][0] == 88.0
    assert later['_weight_source'][0] == 'later_hosp_earliest'


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
    assert capped['weight_kg'].to_list() == [60.0, 65.0, 70.0]
    assert capped['_weight_age_hours'].to_list() == pytest.approx([96.0, 96.0, -48.0])

    uncapped = _attach(meds, vitals, fallbacks=('same_hosp_earliest',))
    assert uncapped['weight_kg'].to_list() == [60.0, 60.0, 60.0]


def test_staleness_gates_prior_hosp_latest():
    vitals = _vitals([('H_OLD', '2024-02-01 10:00', 66.0)])  # 30 d before the dose
    hosp = _hosp([
        ('H_OLD', 'P1', '2024-01-30'),
        ('H_CUR', 'P1', '2024-03-01'),
    ])
    meds = _meds([('H_CUR', '2024-03-02 10:00')])
    kw = dict(fallbacks=('prior_hosp_latest',), hospitalization_df=hosp)
    assert _attach(meds, vitals, **kw)['weight_kg'][0] == 66.0
    assert _attach(meds, vitals, max_staleness_days=7, **kw)['weight_kg'][0] is None
    assert _attach(meds, vitals, max_staleness_days=30, **kw)['weight_kg'][0] == 66.0


@pytest.mark.parametrize('fallbacks', [WEIGHT_FALLBACKS, WEIGHT_FALLBACKS[::-1]])
def test_unbounding_cap_matches_no_cap(scenario_data, fallbacks):
    """A cap nothing can exceed must reproduce the uncapped result exactly,
    even though same_hosp_earliest switches to a per-row join under a cap."""
    meds, vitals, hosp = scenario_data
    kw = dict(fallbacks=fallbacks, hospitalization_df=hosp)
    assert_frame_equal(
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
    assert out['weight_kg'][0] == 70.0   # skips past the 08:00 / 09:00 junk
    assert out['_weight_recorded_dttm'][0] == _ts('2024-03-10 07:00')
    assert out['weight_kg'][1] is None  # a stay with only a zero has nothing


@pytest.mark.parametrize('make_vitals', [_vitals, _vitals_relation], ids=['polars', 'duckdb'])
def test_non_finite_weights_ignored(make_vitals):
    """NaN and +inf weights are junk like zero: skipped, never attached.

    `vital_value > 0` alone let them through (DuckDB sorts NaN above every
    number), so a NaN in a DuckDB relation, or +inf from any input, became
    the patient's weight."""
    vitals = make_vitals([
        ('H_MIX', '2024-03-10 07:00', 70.0),
        ('H_MIX', '2024-03-10 08:00', float('inf')),
        ('H_MIX', '2024-03-10 09:00', float('nan')),
        ('H_INF', '2024-03-10 08:00', float('inf')),
    ])
    meds = _meds([('H_MIX', '2024-03-10 10:00'), ('H_INF', '2024-03-10 10:00')])
    out = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    assert out['weight_kg'][0] == 70.0
    assert out['_weight_recorded_dttm'][0] == _ts('2024-03-10 07:00')
    assert out['weight_kg'][1] is None          # a stay with only +inf has nothing
    assert out['_weight_source'][1] is None


def test_same_instant_duplicates_are_averaged():
    vitals = _vitals([
        ('H_DUP', '2024-03-10 09:00', 70.0),
        ('H_DUP', '2024-03-10 09:00', 74.0),
    ])
    meds = _meds([('H_DUP', '2024-03-10 10:00')])
    runs = [_attach(meds, vitals)['weight_kg'][0] for _ in range(3)]
    assert runs == [72.0, 72.0, 72.0]
    capped = _attach(meds, vitals, fallbacks=('same_hosp_earliest',), max_staleness_days=1)
    assert capped['weight_kg'][0] == 72.0


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
    assert out['weight_kg'][0] == 62.0


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
    meds = meds.with_columns(pl.Series('med_dose', [1.0, 1.0, 2.0, 3.0]))
    meds = meds.with_columns(pl.Series('note', ['a', 'b', None, 'd']))

    rel = attach_weight(
        meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp,
        max_staleness_days=max_staleness_days, return_format='duckdb',
    )
    assert isinstance(rel, duckdb.DuckDBPyRelation)
    assert rel.columns == meds.columns + [
        'weight_kg', '_weight_source', '_weight_recorded_dttm', '_weight_age_hours',
    ]
    out = rel.pl().sort('rn')
    assert out.height == meds.height
    assert_frame_equal(out.select(meds.columns), meds, check_dtypes=False)


def test_custom_time_col():
    vitals = _vitals([('H1', '2024-03-10 09:00', 72.0)])
    meds = _meds([('H1', '2024-03-10 10:00')]).rename({'admin_dttm': 'event_dttm'})
    out = _attach(meds, vitals, time_col='event_dttm')
    assert out['weight_kg'][0] == 72.0
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


@pytest.mark.parametrize('kind', ['polars', 'pandas'])
def test_tz_aware_instants_preserved(tokyo_default_tz, kind):
    """polars input, and pandas through the Arrow boundary: zone and instants kept.

    _weight_recorded_dttm comes from vitals through DuckDB, which labels it in
    the session zone (Tokyo here); it comes back in the dose time's zone.
    """
    tz = 'US/Eastern'
    meds = _meds([('H1', '2024-03-10 10:00'), ('H1', '2024-11-03 01:30')]).with_columns(
        pl.col('admin_dttm').dt.replace_time_zone(tz, ambiguous='earliest'))
    vitals = _vitals([('H1', '2024-03-10 08:00', 72.0)]).with_columns(
        pl.col('recorded_dttm').dt.replace_time_zone(tz))
    to_kind = (lambda df: df) if kind == 'polars' else (lambda df: df.to_pandas())

    out = _attach(to_kind(meds), to_kind(vitals))
    assert out.schema['admin_dttm'].time_zone == tz
    assert out.schema['_weight_recorded_dttm'].time_zone == tz
    assert (out['admin_dttm'].dt.convert_time_zone('UTC').to_list()
            == meds['admin_dttm'].dt.convert_time_zone('UTC').to_list())
    assert out['_weight_recorded_dttm'][0] == vitals['recorded_dttm'][0]
    # 08:00 -> 10:00 on the US spring-forward day is still 2 wall-hours and 2 real ones
    assert out['_weight_age_hours'][0] == pytest.approx(2.0)

    rel = attach_weight(to_kind(meds), to_kind(vitals), return_format='duckdb')
    assert dict(zip(rel.columns, map(str, rel.types)))['admin_dttm'] == 'TIMESTAMP WITH TIME ZONE'


@pytest.mark.parametrize('fallbacks', [('asof',), ('same_hosp_earliest',)])
def test_staleness_uses_elapsed_time_across_dst(tokyo_default_tz, fallbacks):
    """US spring-forward: 10:00 EST to 10:00 EDT the next day is 24 wall-clock
    hours but 23 real ones. The cap must see 23."""
    tz = 'US/Eastern'
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        pl.col('admin_dttm').dt.replace_time_zone(tz))
    vitals = _vitals([('H1', '2024-03-09 10:00', 72.0)]).with_columns(
        pl.col('recorded_dttm').dt.replace_time_zone(tz))

    kept = _attach(meds, vitals, fallbacks=fallbacks, max_staleness_days=23.5 / 24)
    assert kept['weight_kg'][0] == 72.0
    assert kept['_weight_age_hours'][0] == pytest.approx(23.0)
    dropped = _attach(meds, vitals, fallbacks=fallbacks, max_staleness_days=22.5 / 24)
    assert dropped['weight_kg'][0] is None


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
    rel = attach_weight(meds, vitals, return_format='duckdb')
    types = dict(zip(rel.columns, map(str, rel.types)))
    assert types['admin_dttm'] == 'TIMESTAMP WITH TIME ZONE'
    assert types['_weight_recorded_dttm'] == 'TIMESTAMP WITH TIME ZONE'

    out = rel.pl()
    assert out['admin_dttm'][0] == datetime(2024, 3, 10, 15, 0, tzinfo=timezone.utc)
    assert out['_weight_recorded_dttm'][0] == datetime(2024, 3, 10, 12, 30, tzinfo=timezone.utc)
    assert out['_weight_age_hours'][0] == pytest.approx(2.5)


@pytest.mark.parametrize('make_meds, make_vitals', [
    # used to pass silently: DuckDB read the naive 08:00 in the session
    # TimeZone to pick the weight, so the pick depended on that setting
    (lambda m: _localize(m, 'admin_dttm'), lambda v: v),
    # used to fail with a raw DuckDB Binder Error (TIMESTAMP_NS vs TIMESTAMPTZ)
    (lambda m: _localize(m, 'admin_dttm'),
     lambda v: v.with_columns(pl.col('recorded_dttm').cast(pl.Datetime('ns')))),
    (lambda m: m, lambda v: _localize(v, 'recorded_dttm')),
    (lambda m: _localize(m, 'admin_dttm').lazy(), lambda v: v.lazy()),
], ids=['aware-med_naive-vitals', 'aware-med_naive-ns-vitals',
        'naive-med_aware-vitals', 'aware-med_naive-vitals-lazy'])
def test_mixed_tz_awareness_raises(make_meds, make_vitals):
    meds = make_meds(_meds([('H1', '2024-03-10 10:00')]))
    vitals = make_vitals(_vitals([('H1', '2024-03-10 08:00', 72.0)]))
    with pytest.raises(ValueError) as exc:
        attach_weight(meds, vitals)
    msg = str(exc.value)
    assert "med_df column 'admin_dttm'" in msg
    assert "vitals_df column 'recorded_dttm'" in msg
    assert 'is tz-aware' in msg and 'is naive' in msg


def test_mixed_tz_awareness_raises_for_relations(tokyo_default_tz):
    """Relations are checked from their schema. Two aware columns in different
    zones are compared as instants and pass."""
    meds = duckdb.sql("""
        SELECT 0 AS rn, 'H1' AS hospitalization_id
            , TIMESTAMPTZ '2024-03-10 15:00:00+00' AS admin_dttm
    """)
    naive_vitals = duckdb.sql("""
        SELECT 'H1' AS hospitalization_id
            , TIMESTAMP '2024-03-10 12:30:00' AS recorded_dttm
            , 'weight_kg' AS vital_category, 80.0 AS vital_value
    """)
    with pytest.raises(ValueError, match=r"\(TIMESTAMP WITH TIME ZONE\) is tz-aware"):
        attach_weight(meds, naive_vitals)

    meds_est = _localize(_meds([('H1', '2024-01-10 10:00')]), 'admin_dttm')  # 15:00 UTC
    vitals_utc = _localize(_vitals([('H1', '2024-01-10 13:00', 72.0)]), 'recorded_dttm', 'UTC')
    out = _attach(meds_est, vitals_utc)
    assert out['weight_kg'][0] == 72.0
    assert out['_weight_age_hours'][0] == pytest.approx(2.0)


@pytest.mark.parametrize('time_unit, pyarrow_backed, duckdb_type', [
    ('ns', False, 'TIMESTAMP_NS'),
    ('us', True, 'TIMESTAMP'),
], ids=['numpy-ns', 'pyarrow-us'])
def test_mixed_tz_awareness_raises_for_pandas_input(time_unit, pyarrow_backed, duckdb_type):
    """Boundary test: pandas input. A numpy-backed (pandas' default ns) or
    pyarrow-backed (pd.ArrowDtype) dose time is checked once it has crossed
    the Arrow boundary, like any other input: an aware one pairs with an
    aware recorded_dttm, and a naive one is caught, named by its DuckDB type."""
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        pl.col('admin_dttm').cast(pl.Datetime(time_unit)))
    vitals = _localize(_vitals([('H1', '2024-03-10 08:00', 72.0)]), 'recorded_dttm')
    aware = _localize(meds, 'admin_dttm').to_pandas(
        use_pyarrow_extension_array=pyarrow_backed)
    naive = meds.to_pandas(use_pyarrow_extension_array=pyarrow_backed)

    out = _attach(aware, vitals)
    assert out['weight_kg'][0] == 72.0
    assert out['_weight_age_hours'][0] == pytest.approx(2.0)
    with pytest.raises(ValueError,
                       match=rf"med_df column 'admin_dttm' \({duckdb_type}\) is naive"):
        attach_weight(naive, vitals)


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


@pytest.mark.parametrize('fallbacks', [
    {'asof', 'same_hosp_earliest'},
    frozenset({'asof', 'same_hosp_earliest'}),
], ids=['set', 'frozenset'])
def test_unordered_fallbacks_raise_type_error(scenario_data, fallbacks):
    """A set iterates in string-hash order, which changes with PYTHONHASHSEED,
    so the precedence (and the weight used) could change between runs."""
    meds, vitals, _ = scenario_data
    with pytest.raises(TypeError, match='ordered sequence'):
        attach_weight(meds, vitals, fallbacks=fallbacks)
    with pytest.raises(TypeError, match='ordered sequence'):
        convert_dose_units_by_med_category(
            meds.with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg')),
            vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'}, override=True,
            weight_fallbacks=fallbacks,
        )
    # a list is ordered and still works
    assert_frame_equal(
        _attach(meds, vitals, fallbacks=['asof', 'same_hosp_earliest']),
        _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest')),
    )


def test_hospitalization_df_missing_columns_raise(scenario_data):
    meds, vitals, hosp = scenario_data
    with pytest.raises(ValueError, match='admission_dttm'):
        attach_weight(
            meds, vitals, fallbacks=('prior_hosp_latest',),
            hospitalization_df=hosp.drop('admission_dttm'),
        )


def test_missing_input_columns_raise(scenario_data):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError, match='med_df'):
        attach_weight(meds.drop('admin_dttm'), vitals)
    with pytest.raises(ValueError, match='vitals_df'):
        attach_weight(meds, vitals.drop('recorded_dttm'))
    with pytest.raises(ValueError, match='already has'):
        attach_weight(meds.with_columns(weight_kg=pl.lit(70.0)), vitals)


@pytest.mark.parametrize('bad, reason', [
    (-1, 'must be >= 0'),
    (-np.inf, 'must be >= 0'),
    (-10**400, 'must be >= 0'),            # too big for a float, still negative
    (float('nan'), 'must not be NaN'),
    (np.float32('nan'), 'must not be NaN'),
    (Decimal('NaN'), 'must not be NaN'),
    ('seven', 'must be a real number'),
    ('7', 'must be a real number'),
    (True, 'not a bool'),
    (np.bool_(True), 'not a bool'),
], ids=['negative', '-inf', '-10**400', 'nan', 'np-nan', 'decimal-nan',
        'word', 'numeric-str', 'bool', 'np-bool'])
def test_invalid_staleness_raises(scenario_data, bad, reason):
    meds, vitals, _ = scenario_data
    with pytest.raises(ValueError, match=reason) as exc:
        attach_weight(meds, vitals, max_staleness_days=bad)
    assert 'max_staleness_days' in str(exc.value)


@pytest.mark.parametrize('days', [
    7, 7.0, np.int64(7), np.int32(7), np.uint16(7), np.float32(7), np.float64(7),
    Decimal('7'), Fraction(14, 2),
], ids=lambda d: type(d).__name__)
def test_staleness_accepts_any_real_number(days):
    """A cap read from a frame or a config (numpy scalar, Decimal) works like 7.

    numpy integers and Decimal used to be rejected as 'not a finite
    non-negative number'."""
    assert _coerce_max_staleness_days(days) == 7.0
    assert type(_coerce_max_staleness_days(days)) is float
    vitals = _vitals([('H_CUR', '2024-03-01 10:00', 70.0)])   # 10 days before the dose
    meds = _meds([('H_CUR', '2024-03-11 10:00')])
    out = _attach(meds, vitals, max_staleness_days=days)
    assert out['weight_kg'][0] is None                        # gated out at 7 days


@pytest.mark.parametrize('days, expected', [
    (float('inf'), None),
    (np.float64('inf'), None),
    (Decimal('Infinity'), None),
    (1e300, None),       # used to raise OverflowError
    (10**400, None),     # too big for a float
    (1e9, None),         # ~2.7 million years: past the 2**62 us limit
    (5e7, 5e7),          # ~137,000 years: still applied, and never binds
], ids=['inf', 'np-inf', 'decimal-inf', '1e300', '10**400', '1e9', '5e7'])
def test_unbindable_staleness_cap_means_no_cap(scenario_data, days, expected):
    """A cap no real pair of timestamps can exceed gives the uncapped result;
    one too long for BIGINT microseconds is returned as None (no cap)."""
    assert _coerce_max_staleness_days(days) == expected
    meds, vitals, hosp = scenario_data
    kw = dict(fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    assert_frame_equal(
        _attach(meds, vitals, **kw),
        _attach(meds, vitals, max_staleness_days=days, **kw),
    )


# ===========================================
# find_most_recent_weight: legacy contract
# ===========================================

def test_find_most_recent_weight_legacy_contract(scenario_data):
    meds, vitals, _ = scenario_data
    legacy_cols = meds.columns + ['weight_kg', '_weight_recorded_dttm', '_weight_source']

    plain = find_most_recent_weight(meds, vitals)
    assert plain.columns == legacy_cols
    by_id = _rows_by(plain.pl(), 'hospitalization_id')
    assert by_id['H_ASOF']['_weight_source'] == 'asof'
    assert by_id['H_AFTER']['_weight_source'] is None

    fb = find_most_recent_weight(meds, vitals, fallback_on_earliest=True)
    assert fb.columns == legacy_cols
    fb = fb.pl()
    by_id = _rows_by(fb, 'hospitalization_id')
    assert by_id['H_ASOF']['_weight_source'] == 'asof'
    assert by_id['H_AFTER']['_weight_source'] == 'earliest_fallback'
    assert by_id['H_AFTER']['weight_kg'] == 75.0
    assert by_id['H_NONE']['_weight_source'] is None
    # legacy ORDER BY hospitalization_id, admin_dttm, med_category
    assert fb['hospitalization_id'].to_list() == sorted(meds['hospitalization_id'].to_list())


# ===========================================
# convert_dose_units_by_med_category integration
# ===========================================

@pytest.fixture
def lagged_weight_case():
    """A propofol `mg` dose whose stay's only weight is charted after it."""
    med_df = pl.DataFrame({
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
    assert without['_convert_status'][0] == WEIGHT_MISSING_UNWEIGHTED

    with_fb, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units=prefs, override=True,
        show_intermediate=True, weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert with_fb['_convert_status'][0] == 'success'
    assert with_fb['_weight_source'][0] == 'same_hosp_earliest'
    # charted 30 minutes after the dose
    assert with_fb['_weight_age_hours'][0] == pytest.approx(-0.5)
    # 10 mg = 10,000 mcg over 72 kg
    assert with_fb['med_dose_converted'][0] == pytest.approx(10_000 / 72.0, rel=1e-9)
    assert with_fb['med_dose_unit_converted'][0] == 'mcg/kg'


def test_converter_prefilled_weight_wins_over_fallbacks(lagged_weight_case):
    med_df, vitals_df = lagged_weight_case
    out, _ = convert_dose_units_by_med_category(
        med_df.with_columns(weight_kg=pl.lit(50.0)), vitals_df=vitals_df,
        preferred_units={'propofol': 'mcg/kg'}, override=True,
        weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 50.0, rel=1e-9)


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


def test_pipeline_owned_columns_cover_every_written_column(lagged_weight_case):
    """PIPELINE_OWNED_COLUMNS must list every column the converter adds.

    Parking and the name checks rely on this set. show_intermediate and
    return_format='duckdb' keep every intermediate column, and the fallback chain runs the
    weight lookup.
    """
    med_df, vitals_df = lagged_weight_case
    rel, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True, show_intermediate=True, return_format='duckdb',
        weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    written = {c.lower() for c in rel.columns} - {c.lower() for c in med_df.columns}
    assert written - PIPELINE_OWNED_COLUMNS == set()


def test_custom_pair_columns_are_shared_or_suffixed(lagged_weight_case):
    """A pair other than med_dose writes no bare QA name.

    Every column it adds is a shared weight column, its converted pair, or a
    PAIR_QA_COLUMNS name under the pair's suffix, so a second pair converted
    on the same frame never meets the first pair's columns. show_intermediate
    keeps every intermediate column, and the fallback chain runs the weight
    lookup.
    """
    med_df, vitals_df = lagged_weight_case
    med_df = med_df.rename({'med_dose': 'rate', 'med_dose_unit': 'rate_unit'})
    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True, show_intermediate=True, dose_col='rate', unit_col='rate_unit',
        weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    written = set(out.columns) - set(med_df.columns)
    assert {'rate_converted', '_convert_status__rate', '_needs_wt__rate',
            'weight_kg', '_weight_source'} <= written
    allowed = (
        set(WEIGHT_OUTPUT_COLUMNS) | {'rate_converted', 'rate_unit_converted'}
        | {pair_column_name(c, 'rate') for c in PAIR_QA_COLUMNS}
    )
    assert written - allowed == set()


def test_converter_accepts_attach_weight_output(lagged_weight_case):
    """attach_weight's output converts as is: its weight is the one used, and
    its provenance comes back unchanged.

    Before, the default output dropped the caller's `_weight_source` and
    `_weight_recorded_dttm` as if they were the converter's own QA columns,
    and kept `_weight_age_hours`.
    """
    med_df, vitals_df = lagged_weight_case
    attached = attach_weight(
        med_df, vitals_df, fallbacks=('asof', 'same_hosp_earliest'))
    out, _ = convert_dose_units_by_med_category(
        attached, preferred_units={'propofol': 'mcg/kg'}, override=True)
    # 10 mg = 10,000 mcg over the lagged 72 kg weight
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 72.0, rel=1e-9)
    row = out.row(0, named=True)
    assert row['weight_kg'] == 72.0
    assert row['_weight_source'] == 'same_hosp_earliest'
    assert row['_weight_recorded_dttm'] == _ts('2024-01-01 09:30')
    # charted half an hour after the dose
    assert row['_weight_age_hours'] == pytest.approx(-0.5)


def test_converter_round_trips_renamed_columns_past_a_decoy_admin_dttm():
    """category/dose/unit/time columns all renamed, beside a real `admin_dttm`.

    Kills the surviving rename mutants: R2 (time_col not renamed: the lookup
    reads the decoy 09:00 and picks 60 kg), R3 (category_col not renamed: a
    binder error) and R4 (caller names not restored: `rate` comes back as
    `med_dose`).
    """
    med_df = pl.DataFrame({
        'rn': [0],
        'hospitalization_id': ['H1'],
        'drug': ['propofol'],
        'given_at': [_ts('2024-01-01 13:00')],
        # decoy: a real admin_dttm column, to be ignored and returned untouched
        'admin_dttm': [_ts('2024-01-01 09:00')],
        'rate': [10.0],
        'rate_unit': ['mg/hr'],
    })
    vitals_df = _vitals([
        ('H1', '2024-01-01 08:00', 60.0),
        ('H1', '2024-01-01 12:00', 90.0),
    ])
    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg/min'},
        override=True, category_col='drug', dose_col='rate', unit_col='rate_unit',
        time_col='given_at', converted_dose_col='rate_std',
        converted_unit_col='rate_unit_std',
    )
    # every caller column comes back under its own name with its own value
    assert_frame_equal(out.select(med_df.columns), med_df, check_dtypes=False)
    # the requested names, the shared weight, and the pair's own QA columns
    assert set(out.columns) - set(med_df.columns) == {
        'rate_std', 'rate_unit_std', 'weight_kg', '_weight_source', '_weight_age_hours',
        '_convert_status__rate', '_convert_status_details__rate',
    }
    # 10 mg/hr = 10,000 mcg / 60 min, over the weight as of 13:00 (90 kg)
    row = out.row(0, named=True)
    assert row['weight_kg'] == 90.0
    assert row['rate_std'] == pytest.approx(10_000 / 60 / 90.0, rel=1e-9)
    assert row['rate_unit_std'] == 'mcg/kg/min'
    assert row['_convert_status__rate'] == 'success'


def test_capitalised_weight_column_is_the_pre_attached_weight(lagged_weight_case):
    """`WEIGHT_KG` is the pre-attached weight, exactly as `weight_kg` is.

    DuckDB matches names case-insensitively, and so does resolve_output_names.
    Before, the converter's own `weight_kg` checks were case-sensitive: given
    vitals_df, it looked the weight up again into a stray `weight_kg_1` and
    refused the row as having no weight. attach_weight added a second weight
    column instead of refusing the frame.
    """
    med_df, vitals_df = lagged_weight_case
    med_df = med_df.with_columns(pl.Series('WEIGHT_KG', [50.0]))
    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True,
    )
    row = out.row(0, named=True)
    assert row['_convert_status'] == 'success'
    # 10 mg = 10,000 mcg over the caller's 50 kg
    assert row['med_dose_converted'] == pytest.approx(10_000 / 50.0, rel=1e-9)
    assert row['WEIGHT_KG'] == 50.0
    # the caller's spelling comes back once, and no other weight column
    assert list(out.columns).count('WEIGHT_KG') == 1
    assert set(out.columns) - set(med_df.columns) == {
        'med_dose_converted', 'med_dose_unit_converted',
        '_convert_status', '_convert_status_details', '_weight_source', '_weight_age_hours'}
    with pytest.raises(ValueError, match=r"already has column\(s\) \['WEIGHT_KG'\]"):
        attach_weight(med_df, vitals_df)


def test_renamed_input_columns_are_never_parked(lagged_weight_case):
    """category/dose/unit/time columns named like a column the converter writes.

    The run renames these four onto its internal names, so none is parked,
    read as weight provenance or refused as reserved; an output name of the
    call still clashes with them. Before, converting the `rate` pair with
    `category_col='_clean_unit'` parked the category instead of renaming it,
    and the call failed in DuckDB on the missing `med_category`;
    `time_col='_weight_source'` was refused as weight provenance without its
    weight_kg.
    """
    from clifpy.utils.unit_converter._columns import resolve_output_names

    med_df, _ = lagged_weight_case
    rate_df = med_df.rename({
        'med_category': '_clean_unit', 'med_dose': 'rate', 'med_dose_unit': 'rate_unit'})
    out, _ = convert_dose_units_by_med_category(
        rate_df, preferred_units={'propofol': 'mcg'}, override=True,
        category_col='_clean_unit', dose_col='rate', unit_col='rate_unit',
    )
    row = out.row(0, named=True)
    # 10 mg = 10,000 mcg; the category comes back under the caller's name
    assert row['rate_converted'] == pytest.approx(10_000.0)
    assert row['_clean_unit'] == 'propofol'
    assert row['_convert_status__rate'] == 'success'
    assert set(out.columns) - set(rate_df.columns) == {
        'rate_converted', 'rate_unit_converted', 'weight_kg', '_weight_source',
        '_weight_age_hours', '_convert_status__rate', '_convert_status_details__rate'}
    full, _ = convert_dose_units_by_med_category(
        rate_df, preferred_units={'propofol': 'mcg'}, override=True,
        category_col='_clean_unit', dose_col='rate', unit_col='rate_unit',
        show_intermediate=True,
    )
    assert full.row(0, named=True)['_clean_unit__rate'] == 'mg'

    # the default pair still writes a bare `_clean_unit`: into its counts
    # table (refused before any work) and, under show_intermediate=True,
    # into the rows too
    dose_df = med_df.rename({'med_category': '_clean_unit'})
    with pytest.raises(ValueError, match=r"counts table cannot show .*\['_clean_unit'\]"):
        convert_dose_units_by_med_category(
            dose_df, preferred_units={'propofol': 'mcg'}, override=True,
            category_col='_clean_unit')
    with pytest.raises(ValueError, match=r"already has column\(s\) \['_clean_unit'\]"):
        convert_dose_units_by_med_category(
            dose_df, preferred_units={'propofol': 'mcg'}, override=True,
            category_col='_clean_unit', show_intermediate=True)

    # a time column named like weight provenance, with no weight_kg beside it
    timed = med_df.rename({'admin_dttm': '_weight_source'})
    out, _ = convert_dose_units_by_med_category(
        timed, preferred_units={'propofol': 'mcg'}, override=True,
        time_col='_weight_source')
    row = out.row(0, named=True)
    assert row['_weight_source'] == _ts('2024-01-01 09:00')
    names = resolve_output_names(
        timed.columns, dose_col='med_dose', unit_col='med_dose_unit',
        time_col='_weight_source')
    assert names.park == {}
    # ...unless show_intermediate returns the weight lookup's own _weight_source
    with pytest.raises(ValueError, match=r"already has column\(s\) \['_weight_source'\]"):
        resolve_output_names(
            timed.columns, dose_col='med_dose', unit_col='med_dose_unit',
            time_col='_weight_source', show_intermediate=True)


# ===========================================
# Input kinds: polars (eager, lazy), pandas through Arrow, DuckDB relations
# ===========================================

AS_INPUT = {
    'polars_lazy': lambda df: df.lazy(),
    'pandas': lambda df: df.to_pandas(),
    'relation': lambda df: duckdb.from_arrow(df.to_arrow()),
}


@pytest.mark.parametrize('kind', sorted(AS_INPUT))
def test_attach_weight_accepts_every_input_kind(scenario_data, kind):
    """Every strategy gives the polars answer, with no LazyFrame PerformanceWarning."""
    meds, vitals, hosp = scenario_data
    want = _attach(meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    to_kind = AS_INPUT[kind]
    with warnings.catch_warnings():
        warnings.simplefilter('error', pl.exceptions.PerformanceWarning)
        got = _attach(
            to_kind(meds), to_kind(vitals),
            fallbacks=WEIGHT_FALLBACKS, hospitalization_df=to_kind(hosp))
    assert_frame_equal(got, want)


def test_attach_weight_accepts_relations_from_another_connection(scenario_data):
    """Relations from a caller-owned connection are copied through Arrow; the
    non-weight vitals row is filtered out on that connection first."""
    meds, vitals, hosp = scenario_data
    vitals = pl.concat([vitals, pl.DataFrame(
        [('H_ASOF', _ts('2024-03-10 09:30'), 'heart_rate', 999.0)],
        schema=vitals.schema, orient='row')])
    want = _attach(meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    con = duckdb.connect()
    got = _attach(
        con.from_arrow(meds.to_arrow()), con.from_arrow(vitals.to_arrow()),
        fallbacks=WEIGHT_FALLBACKS, hospitalization_df=con.from_arrow(hosp.to_arrow()))
    assert_frame_equal(got, want)


# ===========================================
# Weight-lookup gate
# ===========================================

@pytest.mark.parametrize('preferred', ['MCG/KG/MIN', 'mcg / kg / min', 'Mcg/Kg/Min'])
def test_weight_lookup_gate_reads_normalized_preferred_unit(preferred):
    """An upper-case or spaced weighted target still gets a weight.

    The gate tested the caller's raw spelling against the case-sensitive
    `/kg(/|$)` regex, so 'MCG/KG/MIN' looked unweighted and no weight was
    attached, while stage 2 (which reads the normalised unit) needed one and
    failed every row.
    """
    med = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        med_category=pl.lit('norepinephrine'),
        med_dose=pl.lit(7.0),
        med_dose_unit=pl.lit('mcg/min'),
    )
    vitals = _vitals([('H1', '2024-03-10 09:00', 70.0)])
    out, _ = convert_dose_units_by_med_category(
        med, vitals_df=vitals, preferred_units={'norepinephrine': preferred})
    row = out.row(0, named=True)
    assert row['_convert_status'] == 'success'
    assert row['weight_kg'] == 70.0
    assert row['med_dose_converted'] == pytest.approx(7.0 / 70.0)
    # an explicit target is echoed in the caller's spelling
    assert row['med_dose_unit_converted'] == preferred


def test_weighted_source_to_uppercase_weighted_target_needs_no_vitals():
    """'mcg/kg/min' -> 'MCG/KG/MIN' is the identity; no weight, no vitals.

    The raw-spelling gate saw a weighted source and an "unweighted" target and
    aborted the call for lack of vitals_df.
    """
    med = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        med_dose=pl.lit(50.0), med_dose_unit=pl.lit('mcg/kg/min'))
    out, _ = convert_dose_units_by_med_category(
        med, vitals_df=None, preferred_units={'propofol': 'MCG/KG/MIN'})
    row = out.row(0, named=True)
    assert row['_convert_status'] == 'success'
    assert row['med_dose_converted'] == 50.0
    assert row['med_dose_unit_converted'] == 'MCG/KG/MIN'
    assert row['weight_kg'] is None  # NULL weight, never looked up


@pytest.mark.parametrize('unit,preferred,status', [
    ('mg/kg/dose', 'mg/hr', 'original unit mg/kg/dose is not recognized'),
    ('mg/kg/hr', 'mg', 'cannot convert rate to amount'),
    ('mg/kg/hr', 'ml/hr', 'cannot convert mass to volume'),
])
def test_unconvertible_weighted_row_does_not_need_vitals(unit, preferred, status):
    """A weighted row stage 2 refuses anyway must not abort the call.

    `mg/kg/dose` is unrecognized but still carries '/kg', so the old gate
    asked for a weight and, with vitals_df=None, raised -- taking the
    convertible norepinephrine row down with it. Only rows stage 2 actually
    multiplies by weight may trigger the lookup.
    """
    med = _meds([('H1', '2024-03-10 10:00'), ('H1', '2024-03-10 11:00')]).with_columns(
        med_category=pl.Series(['norepinephrine', 'propofol']),
        med_dose=pl.Series([5.0, 2.0]),
        med_dose_unit=pl.Series(['mcg/min', unit]),
    )
    out, _ = convert_dose_units_by_med_category(
        med, vitals_df=None,
        preferred_units={'norepinephrine': 'mcg/min', 'propofol': preferred},
    )
    out = out.sort('rn')
    # norepinephrine converts; the refused propofol row passes its dose through
    assert out['_convert_status'].to_list() == ['success', status]
    assert out['med_dose_converted'].to_list() == [5.0, 2.0]


def test_converter_accepts_numpy_staleness(lagged_weight_case):
    med_df, vitals_df = lagged_weight_case
    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True, weight_fallbacks=('asof', 'same_hosp_earliest'),
        weight_max_staleness_days=np.int64(7),
    )
    assert out['_convert_status'][0] == 'success'
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 72.0, rel=1e-9)


def test_converter_mixed_tz_awareness_raises(lagged_weight_case):
    """The converter inherits the check through attach_weight, on both lookup paths."""
    med_df, vitals_df = lagged_weight_case
    med_df = _localize(med_df, 'admin_dttm')        # vitals_df stays naive
    for weight_kwargs in ({}, {'weight_fallbacks': ('asof', 'same_hosp_earliest')}):
        with pytest.raises(ValueError, match='is tz-aware'):
            convert_dose_units_by_med_category(
                med_df, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
                override=True, **weight_kwargs,
            )


@pytest.mark.parametrize('bad_weight', [0.0, -70.0, float('inf'), float('-inf'), float('nan')],
                         ids=['zero', 'negative', 'inf', '-inf', 'nan'])
@pytest.mark.parametrize('src_unit, preferred, status', [
    ('mg', 'mcg/kg', WEIGHT_MISSING_UNWEIGHTED),
    ('mg', 'mcg/lb', WEIGHT_MISSING_UNWEIGHTED),
    ('mcg/kg', 'mg', WEIGHT_MISSING_WEIGHTED),
    ('mcg/lb', 'mg', WEIGHT_MISSING_WEIGHTED),
], ids=['to-kg', 'to-lb', 'from-kg', 'from-lb'])
def test_prefilled_unusable_weight_reports_missing(bad_weight, src_unit, preferred, status):
    """A pre-filled weight_kg of zero, below zero, infinite or NaN used to give
    an infinite, zero, negative or NaN dose marked 'success'. It now counts as
    missing, and a conversion that never crosses the weight axis still
    ignores it. polars keeps a NaN weight as NaN all the way into DuckDB."""
    med_df = pl.DataFrame({
        'hospitalization_id': ['H1', 'H1'],
        'admin_dttm': [_ts('2024-03-10 10:00')] * 2,
        'med_category': ['propofol', 'dexmedetomidine'],
        'med_dose': [10.0, 0.6],
        'med_dose_unit': [src_unit, 'mcg/kg/hr'],
        'weight_kg': [bad_weight, bad_weight],
    })
    out, _ = convert_dose_units_by_med_category(
        med_df,
        preferred_units={'propofol': preferred, 'dexmedetomidine': 'mcg/kg/min'},
        override=True,
    )
    rows = _rows_by(out, 'med_category')
    assert rows['propofol']['_convert_status'] == status
    assert rows['propofol']['med_dose_converted'] == 10.0      # dose as given
    assert rows['propofol']['med_dose_unit_converted'] == src_unit
    # same weight qualifier on both sides: weight_kg is never read
    assert rows['dexmedetomidine']['_convert_status'] == 'success'
    assert rows['dexmedetomidine']['med_dose_converted'] == pytest.approx(0.01)


@pytest.mark.parametrize('weight_sql', ["'NaN'::DOUBLE", "'inf'::DOUBLE", '0.0', '-70'],
                         ids=['nan', 'inf', 'decimal-zero', 'int-negative'])
def test_relation_prefilled_unusable_weight_reports_missing(weight_sql):
    """DuckDB-native input, with INTEGER and DECIMAL weight columns too. A NaN
    weight passed `> 0` and gave a NaN dose marked 'success'."""
    med_rel = duckdb.sql(f"""
        SELECT 'H1' AS hospitalization_id
            , TIMESTAMP '2024-03-10 10:00:00' AS admin_dttm
            , 'propofol' AS med_category
            , 10.0::DOUBLE AS med_dose
            , 'mg' AS med_dose_unit
            , {weight_sql} AS weight_kg
    """)
    out, _ = convert_dose_units_by_med_category(
        med_rel, preferred_units={'propofol': 'mcg/kg'}, override=True,
    )
    assert out['_convert_status'][0] == WEIGHT_MISSING_UNWEIGHTED
    assert out['med_dose_converted'][0] == 10.0


# ===========================================
# Converter <-> attach_weight wiring: forwarding and id_name
# ===========================================

@pytest.fixture
def converter_log():
    """Collect (level, message) records of clifpy's converter logger at INFO+.

    Attached to the named logger rather than through caplog, for the reason
    `captured_warnings` in test_unit_converter_clif3.py gives: once clifpy has
    configured its own handlers, records stop reaching caplog's root handler.
    """
    import logging

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append((record.levelname, record.getMessage()))

    handler = _Capture()
    lg = logging.getLogger('clifpy.utils.unit_converter')
    previous_level = lg.level
    lg.addHandler(handler)
    lg.setLevel(logging.INFO)
    try:
        yield records
    finally:
        lg.removeHandler(handler)
        lg.setLevel(previous_level)


def _warnings(log) -> str:
    """The WARNING messages of a `converter_log`, one per line."""
    return '\n'.join(message for level, message in log if level == 'WARNING')


def _mg_doses(rows) -> pl.DataFrame:
    """`rows`: iterable of (hospitalization_id, admin_dttm). One 10 mg propofol
    dose each, so converting to mcg/kg needs a weight: 10,000 mcg / weight."""
    return _meds(rows).with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg'))


def _eager(result) -> pl.DataFrame:
    """A result in the 'polars' or 'duckdb' return_format, as a polars frame."""
    return result.pl() if isinstance(result, duckdb.DuckDBPyRelation) else result


MCG_PER_KG = {'propofol': 'mcg/kg'}


@pytest.mark.parametrize('return_format', ['polars', 'duckdb'])
def test_converter_forwards_weight_max_staleness_days(return_format):
    """Same input with and without the cap: the cap must change WHICH weight
    divides the dose, not just a status. The stay's only prior weight is
    10 d old; a fresher one is charted 4 h after the dose."""
    meds = _mg_doses([('H_CUR', '2024-03-11 10:00')])
    vitals = _vitals([
        ('H_CUR', '2024-03-01 10:00', 60.0),
        ('H_CUR', '2024-03-11 14:00', 75.0),
    ])

    def run(**extra):
        out, _ = convert_dose_units_by_med_category(
            meds, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
            show_intermediate=True, return_format=return_format,
            weight_fallbacks=('asof', 'same_hosp_earliest'), **extra,
        )
        return _eager(out).row(0, named=True)

    uncapped = run()
    assert uncapped['_weight_source'] == 'asof'
    assert uncapped['weight_kg'] == 60.0
    assert uncapped['med_dose_converted'] == pytest.approx(10_000 / 60.0, rel=1e-9)

    capped = run(weight_max_staleness_days=7)
    assert capped['_weight_source'] == 'same_hosp_earliest'
    assert capped['weight_kg'] == 75.0
    assert capped['_weight_recorded_dttm'] == _ts('2024-03-11 14:00')
    assert capped['_convert_status'] == 'success'
    assert capped['med_dose_converted'] == pytest.approx(10_000 / 75.0, rel=1e-9)


@pytest.mark.parametrize('return_format', ['polars', 'duckdb'])
def test_converter_forwards_hospitalization_df(return_format):
    """A prior stay's weight reaches the dose only through hospitalization_df,
    and the forwarded 7-day cap refuses the stale one row by row."""
    meds = _mg_doses([('H_RECENT', '2024-03-10 10:00'), ('H_STALE', '2024-03-10 10:00')])
    vitals = _vitals([
        ('H_P1_OLD', '2024-03-06 10:00', 80.0),   # P1's earlier stay, 4 d old
        ('H_P2_OLD', '2024-01-10 10:00', 90.0),   # P2's earlier stay, 60 d old
    ])
    hosp = _hosp([
        ('H_P1_OLD', 'P1', '2024-03-05'),
        ('H_RECENT', 'P1', '2024-03-09'),
        ('H_P2_OLD', 'P2', '2024-01-09'),
        ('H_STALE', 'P2', '2024-03-09'),
    ])
    out, _ = convert_dose_units_by_med_category(
        meds, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
        show_intermediate=True, return_format=return_format,
        weight_fallbacks=('asof', 'prior_hosp_latest'),
        hospitalization_df=hosp, weight_max_staleness_days=7,
    )
    rows = _rows_by(_eager(out), 'rn')
    assert rows[0]['_weight_source'] == 'prior_hosp_latest'
    assert rows[0]['weight_kg'] == 80.0
    assert rows[0]['med_dose_converted'] == pytest.approx(10_000 / 80.0, rel=1e-9)
    assert rows[1]['_weight_source'] is None
    assert rows[1]['_convert_status'] == WEIGHT_MISSING_UNWEIGHTED
    assert rows[1]['med_dose_converted'] == 10.0          # refused: dose kept
    assert rows[1]['med_dose_unit_converted'] == 'mg'


def test_converter_warns_that_hospitalization_df_needs_weight_fallbacks(converter_log):
    """Without weight_fallbacks the default as-of lookup runs and a passed
    hospitalization_df is unused -- say so."""
    meds = _mg_doses([('H_CUR', '2024-03-11 10:00')])
    vitals = _vitals([('H_CUR', '2024-03-11 09:00', 70.0)])
    out, _ = convert_dose_units_by_med_category(
        meds, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
        hospitalization_df=_hosp([('H_CUR', 'P1', '2024-03-10')]),
    )
    assert ('hospitalization_df is ignored unless weight_fallbacks is given'
            in _warnings(converter_log))
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 70.0, rel=1e-9)


@pytest.fixture
def encounter_block_case():
    """Two stays stitched into one encounter block. The only weight is charted
    in the sibling stay, so keying on hospitalization_id finds nothing and
    keying on encounter_block finds it: the two keys disagree on purpose."""
    meds = _mg_doses([('H_B', '2024-03-10 10:00')]).with_columns(encounter_block=pl.lit('E1'))
    vitals = _vitals([('H_A', '2024-03-10 08:00', 80.0)]).with_columns(encounter_block=pl.lit('E1'))
    return meds, vitals


@pytest.mark.parametrize('fallbacks', [('asof',), ('same_hosp_earliest',)])
def test_attach_weight_keys_on_custom_id_name(encounter_block_case, fallbacks):
    meds, vitals = encounter_block_case
    by_block = _attach(meds, vitals, id_name='encounter_block', fallbacks=fallbacks)
    assert by_block['weight_kg'][0] == 80.0
    assert by_block['_weight_source'][0] == fallbacks[0]
    assert _attach(meds, vitals, fallbacks=fallbacks)['weight_kg'][0] is None


@pytest.mark.parametrize('weight_fallbacks', [None, ('asof',)])
def test_converter_keys_weight_lookup_on_custom_id_name(encounter_block_case, weight_fallbacks):
    """Both lookup branches -- the legacy find_most_recent_weight path SOFA-2
    uses, and the attach_weight path -- must key on id_name."""
    meds, vitals = encounter_block_case
    out, _ = convert_dose_units_by_med_category(
        meds, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
        show_intermediate=True, id_name='encounter_block',
        weight_fallbacks=weight_fallbacks,
    )
    row = out.row(0, named=True)
    assert row['weight_kg'] == 80.0
    assert row['_weight_source'] == 'asof'
    assert row['med_dose_converted'] == pytest.approx(10_000 / 80.0, rel=1e-9)


# ===========================================
# Lookup boundaries: the dose instant and the exact cap
# ===========================================

def test_weight_charted_at_the_dose_instant_counts_as_asof():
    """`asof` means AT or before: a weight charted at the dose instant beats an
    older one at age 0, also under a 0-day cap, and a chain cannot hand that
    tie to its fallback."""
    vitals = _vitals([
        ('H_TWO', '2024-03-10 09:00', 70.0),
        ('H_TWO', '2024-03-10 10:00', 74.0),    # at the dose instant
        ('H_ONLY', '2024-03-10 10:00', 75.0),   # the stay's only weight
    ])
    meds = _meds([('H_TWO', '2024-03-10 10:00'), ('H_ONLY', '2024-03-10 10:00')])

    for kw in ({}, {'max_staleness_days': 0}):
        out = _attach(meds, vitals, **kw)
        assert out['weight_kg'].to_list() == [74.0, 75.0]
        assert out['_weight_source'].to_list() == ['asof', 'asof']
        assert out['_weight_age_hours'].to_list() == [0.0, 0.0]

    chained = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    assert chained['_weight_source'].to_list() == ['asof', 'asof']

    # Under a 0-day cap the earliest ELIGIBLE weight is the one at the instant.
    earliest = _attach(meds, vitals, fallbacks=('same_hosp_earliest',), max_staleness_days=0)
    assert earliest['weight_kg'].to_list() == [74.0, 75.0]


def test_converter_default_lookup_uses_weight_at_dose_instant():
    """The legacy path (no weight_fallbacks) is the most used one."""
    meds = _mg_doses([('H_TWO', '2024-03-10 10:00')])
    vitals = _vitals([
        ('H_TWO', '2024-03-10 09:00', 70.0),
        ('H_TWO', '2024-03-10 10:00', 74.0),
    ])
    out, _ = convert_dose_units_by_med_category(
        meds, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
    )
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 74.0, rel=1e-9)


@pytest.mark.parametrize('fallbacks, one_minute_past', [
    (('asof',), None),                  # asof gives up: a null weight
    (('same_hosp_earliest',), 70.0),    # moves on to the next eligible weight
])
def test_staleness_cap_is_inclusive_for_every_gated_strategy(fallbacks, one_minute_past):
    """A weight exactly `max_staleness_days` old is kept by both gated
    strategies, so neither gate can drift to a strict `<` on its own."""
    vitals = _vitals([
        ('H1', '2024-03-03 10:00', 60.0),   # exactly 7 d before the first dose
        ('H1', '2024-03-11 10:00', 70.0),   # after both doses
    ])
    meds = _meds([('H1', '2024-03-10 10:00'), ('H1', '2024-03-10 10:01')])
    out = _attach(meds, vitals, fallbacks=fallbacks, max_staleness_days=7)
    assert out['weight_kg'][0] == 60.0
    assert out['_weight_age_hours'][0] == pytest.approx(168.0)
    assert out['weight_kg'][1] == one_minute_past


@pytest.mark.parametrize('call', [
    'asof',
    'asof_then_capped_earliest',     # the capped forward lookup
    'full_chain_capped',
    'find_most_recent_weight',
    'converter',
])
def test_weight_lookup_on_arrow_input_plans_no_nested_loop_join(scenario_data, call):
    """DuckDB estimates an Arrow scan (polars or pandas input) at about one
    row, below `asof_loop_join_threshold`, so an ASOF join on it became a
    NESTED_LOOP_JOIN whose time grows with med rows times weight rows. Pins
    the plan of every lazy route to the lookup, not the time. The text
    format differs between DuckDB versions, and a wide plan truncates the
    operator's name ('NESTED_L...'), so the whole EXPLAIN output is searched
    for its prefix."""
    meds, vitals, hosp = scenario_data
    relations = {
        'asof': lambda: attach_weight(meds, vitals, return_format='duckdb'),
        'asof_then_capped_earliest': lambda: attach_weight(
            meds, vitals, fallbacks=('asof', 'same_hosp_earliest'),
            max_staleness_days=7, return_format='duckdb'),
        'full_chain_capped': lambda: attach_weight(
            meds, vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp,
            max_staleness_days=7, return_format='duckdb'),
        'find_most_recent_weight': lambda: find_most_recent_weight(
            meds, vitals, fallback_on_earliest=True),
        'converter': lambda: convert_dose_units_by_med_category(
            meds.with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg')),
            vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
            return_format='duckdb')[0],
    }
    plan = relations[call]().explain()
    assert 'NESTED_L' not in plan, plan


def test_id_columns_of_different_types_match_as_sql_equality():
    """A BIGINT med_df ID against text vitals IDs matches the way SQL `=`
    does, which casts the text to BIGINT (so '01' is stay 1), in both window
    lookups, and the ID column keeps its type. With no dose rows nothing is
    compared, so a text ID that cannot be cast does not raise."""
    vitals = _vitals([
        ('01', '2024-03-10 08:00', 70.0),   # before stay 1's dose -> asof
        ('2', '2024-03-10 12:00', 80.0),    # only after stay 2's dose
    ])
    meds = pl.DataFrame(
        {'rn': [0, 1], 'hospitalization_id': [1, 2], 'admin_dttm': [_ts('2024-03-10 10:00')] * 2},
        schema={'rn': pl.Int64, 'hospitalization_id': pl.Int64, 'admin_dttm': pl.Datetime('us')},
    )
    out = _attach(meds, vitals, fallbacks=('asof', 'same_hosp_earliest'), max_staleness_days=1)
    assert out['weight_kg'].to_list() == [70.0, 80.0]
    assert out['_weight_source'].to_list() == ['asof', 'same_hosp_earliest']
    assert out.schema['hospitalization_id'] == pl.Int64

    uncastable = _vitals([('abc', '2024-03-10 08:00', 70.0)])
    assert attach_weight(meds.clear(), uncastable).height == 0


# ===========================================
# Provenance: caller-attached weight columns pass through
# ===========================================

@pytest.mark.parametrize('show_intermediate', [False, True])
def test_attach_then_convert_keeps_values_instants_and_provenance(tokyo_default_tz, show_intermediate):
    """The guide's 'attach once, convert twice' recipe on tz-aware data:
    attach_weight's output, as the default polars frame or as a DuckDB
    relation, goes straight into the converter, and the caller's four weight
    columns come back unchanged in either mode."""
    tz = 'US/Eastern'
    meds = _mg_doses([('H1', '2024-03-12 10:00'), ('H2', '2024-03-12 10:00')]).with_columns(
        pl.col('admin_dttm').dt.replace_time_zone(tz))
    vitals = _vitals([
        ('H1', '2024-03-12 08:00', 80.0),   # before the dose -> asof
        ('H2', '2024-03-12 12:00', 50.0),   # only after it -> same_hosp_earliest
    ]).with_columns(pl.col('recorded_dttm').dt.replace_time_zone(tz))

    for return_format, kind in (('polars', pl.DataFrame), ('duckdb', duckdb.DuckDBPyRelation)):
        mac_w = attach_weight(
            meds, vitals, fallbacks=('asof', 'same_hosp_earliest'),
            return_format=return_format,
        )
        assert isinstance(mac_w, kind)
        for prefs, per_kg in (({'propofol': 'mcg/kg'}, 1_000.0), ({'propofol': 'mg/kg'}, 1.0)):
            out, _ = convert_dose_units_by_med_category(
                mac_w, preferred_units=prefs, show_intermediate=show_intermediate,
            )
            out = out.sort('rn')
            assert out['_convert_status'].to_list() == ['success', 'success']
            assert out['med_dose_converted'].to_list() == pytest.approx(
                [10 * per_kg / 80.0, 10 * per_kg / 50.0], rel=1e-9)
            # same instants (the zone label a relation renders in is Task 6's rule)
            assert (out['admin_dttm'].dt.convert_time_zone('UTC').to_list()
                    == meds['admin_dttm'].dt.convert_time_zone('UTC').to_list())
            assert out['weight_kg'].to_list() == [80.0, 50.0]
            assert out['_weight_source'].to_list() == ['asof', 'same_hosp_earliest']
            assert (out['_weight_recorded_dttm'].dt.convert_time_zone('UTC').to_list()
                    == vitals['recorded_dttm'].dt.convert_time_zone('UTC').to_list())
            assert out['_weight_age_hours'].to_list() == pytest.approx([2.0, -2.0])


# ===========================================
# Diagnostics: ignored arguments, per-strategy yield, cross-stay coverage
# ===========================================

def test_prefilled_weight_kg_warns_when_lookup_args_are_ignored(lagged_weight_case, converter_log):
    """A pre-filled weight_kg wins (documented), so lookup arguments passed
    with it do nothing -- a cap the caller asked for must not vanish silently."""
    med_df, vitals_df = lagged_weight_case
    prefilled = med_df.with_columns(weight_kg=pl.lit(50.0))
    kw = dict(vitals_df=vitals_df, preferred_units=MCG_PER_KG, override=True)

    convert_dose_units_by_med_category(prefilled, **kw)
    assert 'weight_kg column' not in _warnings(converter_log)    # nothing to ignore

    out, _ = convert_dose_units_by_med_category(
        prefilled, **kw,
        weight_fallbacks=('asof', 'prior_hosp_latest'),
        hospitalization_df=_hosp([('H_LAGGED', 'P1', '2023-12-31')]),
        weight_max_staleness_days=7,
    )
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 50.0, rel=1e-9)
    text = _warnings(converter_log)
    assert 'med_df already has a weight_kg column' in text
    for arg in ('weight_fallbacks', 'weight_max_staleness_days', 'hospitalization_df'):
        assert arg in text

    converter_log.clear()
    convert_dose_units_by_med_category(prefilled, **kw, fallback_on_earliest=True)
    assert 'fallback_on_earliest is ignored' in _warnings(converter_log)


VITALS_UNUSED = 'vitals_df is not used'
VOLUME_PAIR = dict(dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')


def _two_pair_doses() -> pl.DataFrame:
    """Norepinephrine rows carrying both CLIF 3.0 pairs. Converting the dose
    to mcg/min needs a weight on the two mcg/kg/min rows only; converting the
    volume rate to ml/hr never needs one."""
    return pl.DataFrame({
        'hospitalization_id': ['H1'] * 3,
        'admin_dttm': [_ts('2024-03-10 10:00')] * 3,
        'med_category': ['norepinephrine'] * 3,
        'med_dose': [0.05, 0.08, 4.0],
        'med_dose_unit': ['mcg/kg/min', 'mcg/kg/min', 'mcg/min'],
        'volume_infusion_rate': [7.5, 0.2, 15.0],
        'volume_infusion_rate_unit': ['mL/hr', 'mL/min', 'mL/hr'],
    })


def _volume_then_dose(vitals_df, *, return_format='polars', show_intermediate=False):
    """The volume pass (with `show_intermediate`), then the dose pass on its
    result, both given `vitals_df`. Returns (volume pass output, dose pass
    output as polars)."""
    volume, _ = convert_dose_units_by_med_category(
        _two_pair_doses(), vitals_df=vitals_df,
        preferred_units={'norepinephrine': 'ml/hr'}, **VOLUME_PAIR,
        show_intermediate=show_intermediate,
    )
    dose, _ = convert_dose_units_by_med_category(
        volume, vitals_df=vitals_df, preferred_units={'norepinephrine': 'mcg/min'},
        return_format=return_format,
    )
    return volume, _eager(dose)


@pytest.mark.parametrize('return_format', ['polars', 'duckdb'])
def test_prefilled_weight_kg_warns_when_vitals_df_cannot_fill_it(converter_log, return_format):
    """A pass that needs no weight still returns weight_kg, all NULL. The next
    pass reads it as a pre-attached weight and skips the lookup, so vitals_df
    went unused without a word. The rule is unchanged (the statuses still say
    'weight_kg is missing'); one WARNING now gives the count of rows that need
    a weight, also for a lazy 'duckdb' result."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    volume, dose = _volume_then_dose(vitals, return_format=return_format)

    assert volume['weight_kg'].to_list() == [None] * 3
    assert dose['_convert_status'].to_list() == [WEIGHT_MISSING_WEIGHTED] * 2 + ['success']
    assert dose['med_dose_converted'].to_list() == [0.05, 0.08, 4.0]   # doses kept
    warned = [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
    assert len(warned) == 1
    # the mcg/min row needs no weight, so it is not counted
    assert "2 row(s) that need a weight have none and will report 'weight_kg is missing'" in warned[0]
    for remedy in ('Drop weight_kg and its _weight_* provenance columns first', 'crosses the weight axis first',
                   'if both pairs cross it', 'attach_weight'):
        assert remedy in warned[0]


def test_complete_prefilled_weight_kg_with_vitals_df_does_not_warn(converter_log):
    """Every row that needs a weight has one: vitals_df is not needed, so the
    orchestrator's routine vitals_df next to pre-attached weights stays quiet."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    out, _ = convert_dose_units_by_med_category(
        _two_pair_doses().with_columns(weight_kg=pl.lit(50.0)), vitals_df=vitals,
        preferred_units={'norepinephrine': 'mcg/min'},
    )
    assert out['_convert_status'].to_list() == ['success'] * 3
    assert out['med_dose_converted'].to_list() == pytest.approx([2.5, 4.0, 4.0], rel=1e-9)
    assert VITALS_UNUSED not in _warnings(converter_log)


def test_prefilled_weight_kg_without_vitals_df_does_not_warn(converter_log):
    """The same chain with vitals_df=None: the caller gave no weights to use,
    so there is nothing to warn about; the statuses already say it."""
    _, dose = _volume_then_dose(None)
    assert dose['_convert_status'].to_list() == [WEIGHT_MISSING_WEIGHTED] * 2 + ['success']
    assert VITALS_UNUSED not in _warnings(converter_log)


@pytest.mark.parametrize('weights', [(0.0, float('nan')), (-1.0, float('inf'))],
                         ids=['zero-nan', 'negative-inf'])
def test_prefilled_weight_kg_warning_counts_every_unusable_weight(converter_log, weights):
    """Zero, below zero, NaN and infinite weights are as unusable as a NULL one
    (stage 2's rule), so the count includes them: a NULL-only count gives 0."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    out, _ = convert_dose_units_by_med_category(
        _two_pair_doses().with_columns(weight_kg=pl.Series([*weights, 70.0])),
        vitals_df=vitals, preferred_units={'norepinephrine': 'mcg/min'},
    )
    assert out['_convert_status'].to_list() == [WEIGHT_MISSING_WEIGHTED] * 2 + ['success']
    warned = [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
    assert len(warned) == 1
    assert '2 row(s) that need a weight have none' in warned[0]


def test_show_intermediate_first_pass_still_warns(converter_log):
    """A first pass with show_intermediate=True returns `_weight_source`,
    `_weight_age_hours` and `_weight_recorded_dttm` too. Where it looked
    nothing up its `_weight_source` reads 'not_needed', so its NULL weight_kg
    is still recognized as an earlier pass's placeholder and the next pass
    warns."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    volume, dose = _volume_then_dose(vitals, show_intermediate=True)

    assert {'_weight_source', '_weight_age_hours', '_weight_recorded_dttm'} <= set(volume.columns)
    assert volume['_weight_source'].to_list() == ['not_needed'] * 3
    assert dose['_convert_status'].to_list() == [WEIGHT_MISSING_WEIGHTED] * 2 + ['success']
    warned = [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
    assert len(warned) == 1
    assert '2 row(s) that need a weight have none' in warned[0]


@pytest.mark.parametrize('upper', [False, True], ids=['as_returned', 'upper_case'])
def test_attach_weight_gaps_with_vitals_df_do_not_warn(converter_log, upper):
    """attach_weight's gaps are that chain's verdict (here a 1-day cap refuses
    a 9-day-old weight) and its provenance columns already record them, so the
    'vitals_df is not used' WARNING, whose cause and remedies concern an
    earlier conversion pass, stays quiet, whatever the columns' letter case.
    The statuses still say 'weight_kg is missing'."""
    vitals = _vitals([('H1', '2024-03-01 10:00', 80.0)])   # 9 d before the doses
    attached = attach_weight(
        _two_pair_doses(), vitals, fallbacks=('asof',), max_staleness_days=1)
    if upper:
        attached = attached.rename(
            {c: c.upper() for c in attached.columns if c.startswith('_weight_')})
    assert attached['weight_kg'].to_list() == [None] * 3
    out, _ = convert_dose_units_by_med_category(
        attached, vitals_df=vitals, preferred_units={'norepinephrine': 'mcg/min'},
    )
    assert out['_convert_status'].to_list() == [WEIGHT_MISSING_WEIGHTED] * 2 + ['success']
    assert VITALS_UNUSED not in _warnings(converter_log)


def test_converter_logs_rows_supplied_per_strategy(scenario_data, converter_log):
    """Each strategy's yield is logged in chain order, zeros included, so a
    cross-stay strategy that recovered nothing is visible. Legacy path too."""
    meds, vitals, hosp = scenario_data
    doses = meds.with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg'))
    kw = dict(vitals_df=vitals, preferred_units=MCG_PER_KG, override=True)

    convert_dose_units_by_med_category(
        doses, **kw, weight_fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp,
    )
    convert_dose_units_by_med_category(doses, **kw, fallback_on_earliest=True)
    logged = [m for level, m in converter_log
              if level == 'INFO' and m.startswith('Weight sources')]
    assert logged == [
        'Weight sources for 5 row(s) needing weight: asof=1, same_hosp_earliest=1, '
        'prior_hosp_latest=1, later_hosp_earliest=1, none=1',
        'Weight sources for 5 row(s) needing weight: asof=1, earliest_fallback=1, none=3',
    ]


def test_cross_stay_warns_when_inputs_are_cohort_filtered(scenario_data, converter_log):
    """prior_hosp_latest / later_hosp_earliest read each patient's OTHER stays.
    A vitals_df or hospitalization_df filtered to med_df's stays -- clifpy's
    usual loading pattern -- leaves them nothing to find: warn, don't go quiet."""
    meds, vitals, hosp = scenario_data
    cohort = meds['hospitalization_id'].to_list()
    kw = dict(fallbacks=('asof', 'prior_hosp_latest', 'later_hosp_earliest'))

    _attach(meds, vitals, hospitalization_df=hosp, **kw)
    assert 'outside med_df' not in _warnings(converter_log)   # full inputs: quiet

    cohort_vitals = vitals.filter(pl.col('hospitalization_id').is_in(cohort))
    _attach(meds, cohort_vitals, hospitalization_df=hosp, **kw)
    assert ("vitals_df has no weight_kg on any of the 2 stay(s) of med_df's "
            "patients outside med_df" in _warnings(converter_log))

    converter_log.clear()
    cohort_hosp = hosp.filter(pl.col('hospitalization_id').is_in(cohort))
    _attach(meds, vitals, hospitalization_df=cohort_hosp, **kw)
    assert ("hospitalization_df lists no stay of med_df's patients outside med_df"
            in _warnings(converter_log))

    # the converter's own lookup reaches the same check
    converter_log.clear()
    convert_dose_units_by_med_category(
        meds.with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg')),
        vitals_df=cohort_vitals, preferred_units=MCG_PER_KG, override=True,
        weight_fallbacks=kw['fallbacks'], hospitalization_df=hosp,
    )
    assert 'vitals_df has no weight_kg' in _warnings(converter_log)


def test_cross_stay_warning_allows_for_stays_inside_med_df(converter_log):
    """A cohort-filtered hospitalization_df still lets a readmission inside the
    cohort borrow from its sibling stay, so the warning must not claim the
    strategies 'cannot supply' a weight -- only that they reach stays inside
    med_df."""
    vitals = _vitals([('H1', '2024-01-01 08:00', 70.0)])
    hosp = _hosp([('H1', 'P1', '2024-01-01'), ('H2', 'P1', '2024-03-01')])
    meds = _meds([('H1', '2024-01-01 09:00'), ('H2', '2024-03-01 09:00')])

    out = _attach(
        meds, vitals, hospitalization_df=hosp,
        fallbacks=('asof', 'prior_hosp_latest'),
    )
    assert out['_weight_source'].to_list() == ['asof', 'prior_hosp_latest']
    assert out['weight_kg'].to_list() == [70.0, 70.0]
    warned = [m for level, m in converter_log if level == 'WARNING']
    assert len(warned) == 1
    assert 'outside med_df' in warned[0]
    assert 'cannot supply' not in warned[0]


# ===========================================
# Review Focus 5: input order, row count, duplicates and ties
# ===========================================

@pytest.mark.parametrize('as_relation', [False, True])
def test_attach_weight_returns_rows_in_input_order(as_relation):
    """attach_weight alone keeps input order. Its window lookups and hash
    joins emit rows grouped by stay and time, so without an explicit ORDER BY
    the result came back reshuffled. Two doses share a stay and instant, and
    one weight is a same-instant tie."""
    from polars.testing import assert_frame_equal

    vitals = _vitals([
        ('H1', '2024-03-10 09:00', 70.0),
        ('H1', '2024-03-10 09:00', 74.0),   # same instant -> averaged to 72
        ('H2', '2024-03-10 08:00', 60.0),
        ('H3', '2024-03-10 12:00', 50.0),
    ])
    meds = _meds([
        ('H3', '2024-03-10 13:00'),
        ('H1', '2024-03-10 11:00'),
        ('H2', '2024-03-10 10:00'),
        ('H1', '2024-03-10 10:00'),
        ('H1', '2024-03-10 10:00'),   # same stay and instant as the row above
        ('H3', '2024-03-10 09:00'),
        ('H2', '2024-03-10 07:00'),
        ('H9', '2024-03-10 10:00'),   # no weight anywhere
    ])
    source = duckdb.sql("FROM meds") if as_relation else meds
    for kw in (
        {},
        {'fallbacks': ('asof', 'same_hosp_earliest')},
        {'fallbacks': ('asof', 'same_hosp_earliest'), 'max_staleness_days': 1},
    ):
        out = attach_weight(source, vitals, **kw)
        assert out['rn'].to_list() == list(range(meds.height)), kw
        assert_frame_equal(out.select(meds.columns), meds, check_dtypes=False)
    # the last (capped) chain: weights stay on their own rows; H9 has none
    assert out['weight_kg'].to_list() == [50.0, 72.0, 60.0, 72.0, 72.0, 50.0, 60.0, None]


def test_attach_weight_relation_skips_the_input_order_sort():
    """Only the eager formats pay for input order. A 'duckdb' relation has no
    row order of its own (as for the converter), so its query carries neither
    the row_number() ordinal nor the sort that blocks pushdown."""
    vitals = _vitals([
        ('H1', '2024-03-10 09:00', 70.0),
        ('H2', '2024-03-10 08:00', 60.0),
    ])
    meds = _meds([
        ('H2', '2024-03-10 10:00'),
        ('H1', '2024-03-10 11:00'),
        ('H1', '2024-03-10 10:00'),
    ])
    rel = attach_weight(meds, vitals, return_format='duckdb')
    sql = rel.sql_query().lower()
    assert 'row_number' not in sql
    assert '_attach_weight_ord_' not in sql
    assert rel.fetchall()   # the relation still runs
    out = attach_weight(meds, vitals)
    assert out['rn'].to_list() == list(range(meds.height))


def test_duplicate_mar_rows_with_tied_weights_keep_count_and_order():
    """Identical MAR rows, a weight charted twice at one instant, and rows
    needing no weight interleaved with rows that do. attach_weight neither fans
    out nor reorders; the converter returns one row per input row, in input
    order (the needs-weight split must not reorder)."""
    vitals = _vitals([
        ('H1', '2024-03-10 09:00', 70.0),
        ('H1', '2024-03-10 09:00', 74.0),   # same instant -> averaged to 72
    ])
    med_df = pl.DataFrame({
        'hospitalization_id': ['H1'] * 5,
        'admin_dttm': [_ts('2024-03-10 10:00')] * 5,
        'med_category': ['propofol', 'propofol', 'fentanyl', 'propofol', 'fentanyl'],
        'med_dose': [10.0, 10.0, 50.0, 10.0, 50.0],
        'med_dose_unit': ['mg', 'mg', 'mcg/hr', 'mg', 'mcg/hr'],
    })

    attached = attach_weight(med_df, vitals, fallbacks=('asof', 'same_hosp_earliest'))
    assert attached.height == 5
    assert attached['med_category'].to_list() == med_df['med_category'].to_list()
    assert attached['weight_kg'].to_list() == [72.0] * 5

    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals, override=True,
        preferred_units={'propofol': 'mcg/kg', 'fentanyl': 'mcg/min'},
        weight_fallbacks=('asof', 'same_hosp_earliest'),
    )
    assert out.height == 5
    assert out['med_category'].to_list() == med_df['med_category'].to_list()
    assert out['med_dose_converted'].to_list() == pytest.approx(
        [10_000 / 72.0, 10_000 / 72.0, 50 / 60.0, 10_000 / 72.0, 50 / 60.0], rel=1e-9)


def test_null_time_gets_no_gated_weight_under_cap():
    """A NULL dose time cannot be checked against a cap (audit M81).

    attach_weight documents it: without a cap the row still takes the stay's
    earliest weight; with one, every gated strategy skips it. The row count
    is unchanged either way.
    """
    vitals = _vitals([('H1', '2024-03-10 08:00', 70.0)])
    # rn 0 has no dose time. It is nulled with a polars expression, so the
    # test does not depend on what `_ts` does with None.
    meds = _meds([('H1', '2024-03-10 10:00'), ('H1', '2024-03-10 10:00')]).with_columns(
        admin_dttm=pl.when(pl.col('rn') != 0).then(pl.col('admin_dttm')),
    )
    chain = ('asof', 'same_hosp_earliest')

    uncapped = _attach(meds, vitals, fallbacks=chain)
    capped = _attach(meds, vitals, fallbacks=chain, max_staleness_days=7)

    assert uncapped.height == capped.height == meds.height
    untimed = uncapped.row(0, named=True)
    untimed_capped = capped.row(0, named=True)
    assert untimed['weight_kg'] == 70.0
    assert untimed['_weight_source'] == 'same_hosp_earliest'
    assert untimed_capped['weight_kg'] is None
    assert untimed_capped['_weight_source'] is None
    # the timed row is unaffected by the cap
    timed_capped = capped.row(1, named=True)
    assert timed_capped['weight_kg'] == 70.0
    assert timed_capped['_weight_source'] == 'asof'


def test_null_time_gets_no_window_weight_at_any_input_size():
    """A NULL dose time has no 'at or before' and no cap window, so neither
    window lookup gives it a weight, whatever plan the input gets. The ASOF
    join these replaced kept that only while DuckDB planned it as a nested
    loop (Arrow input, or under 64 rows): its ASOF operator sorted the NULL
    time last and handed the dose its stay's latest weight. The default
    converter copies its input into a temp table, so it took that path."""
    timed = [('H0', f'2024-03-{day} {hour:02d}:00') for day in (10, 11, 12) for hour in range(24)]
    doses = _mg_doses(timed + [('H1', '2024-03-10 10:00')] * 5).with_columns(
        admin_dttm=pl.when(pl.col('hospitalization_id') == 'H0').then(pl.col('admin_dttm')),
    )
    vitals = _vitals([('H0', '2024-03-09 23:00', 70.0), ('H1', '2024-03-10 08:00', 60.0)])
    untimed = pl.col('admin_dttm').is_null()

    duckdb.execute("CREATE OR REPLACE TEMP TABLE _null_time_doses AS FROM doses")
    try:
        for chain, cap in ((('asof',), None), (('same_hosp_earliest',), 7),
                           (('asof', 'same_hosp_earliest'), 7)):
            out = attach_weight(duckdb.table('_null_time_doses'), vitals,
                                fallbacks=chain, max_staleness_days=cap)
            assert out.filter(untimed)['weight_kg'].to_list() == [None] * 5, chain
            assert out.filter(~untimed)['weight_kg'].to_list() == [70.0] * len(timed), chain
    finally:
        duckdb.execute("DROP TABLE IF EXISTS _null_time_doses")

    out, _ = convert_dose_units_by_med_category(
        doses, vitals_df=vitals, preferred_units=MCG_PER_KG, override=True,
    )
    assert out.filter(untimed)['_convert_status'].to_list() == [WEIGHT_MISSING_UNWEIGHTED] * 5
    assert out.filter(~untimed)['med_dose_converted'].to_list() == pytest.approx(
        [10_000 / 70.0] * len(timed), rel=1e-9)


def test_weight_without_an_id_reaches_no_undated_or_unidentified_dose():
    """Doses with no time or no ID have no stay to sort into, so they share
    one window partition, and a weight row with no ID must stay out of it.
    A vitals row with hospitalization_id=None, charted before every dose,
    gives none of them a weight under `asof` or under the capped chain.
    Uncapped `same_hosp_earliest` is a hash join on the ID, so it still
    serves the undated dose of a known stay, as documented."""
    vitals = _vitals([
        (None, '2024-03-10 09:00', 55.0),      # no stay, before every dose
        ('H1', '2024-03-13 10:00', 70.0),      # H1's only weight, after the doses
    ])
    # rn 0: H1, dated; rn 1: no ID, dated; rn 2: H1, undated; rn 3: neither
    meds = _meds([('H1', '2024-03-10 10:00'), (None, '2024-03-10 10:00')] * 2).with_columns(
        admin_dttm=pl.when(pl.col('rn') < 2).then(pl.col('admin_dttm')),
    )
    cases = [
        ({'fallbacks': ('asof',)}, [None] * 4, [None] * 4),
        ({'fallbacks': ('asof', 'same_hosp_earliest'), 'max_staleness_days': 7},
         [70.0, None, None, None], ['same_hosp_earliest', None, None, None]),
        ({'fallbacks': ('asof', 'same_hosp_earliest')},
         [70.0, None, 70.0, None], ['same_hosp_earliest', None, 'same_hosp_earliest', None]),
    ]
    for kw, weights, sources in cases:
        out = _attach(meds, vitals, **kw)
        assert out['weight_kg'].to_list() == weights, kw
        assert out['_weight_source'].to_list() == sources, kw


# --- Untyped and text time columns ------------------------------------------
# An Arrow null-typed column (pd.DataFrame(columns=...), pl.DataFrame({c: []}),
# a pandas column holding only None) reaches DuckDB as an untyped INTEGER.

_MED_COLS = ['rn', 'hospitalization_id', 'admin_dttm', 'med_category']
_VITAL_COLS = ['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']


def _untyped(kind: str, cols):
    """An empty frame whose columns carry no type, as polars or pandas."""
    import pandas as pd
    if kind == 'polars':
        return pl.DataFrame({c: [] for c in cols})
    return pd.DataFrame(columns=list(cols))


def _naive_vitals() -> pl.DataFrame:
    return _vitals([('H1', '2024-03-10 08:00', 70.0)])


def _aware_vitals() -> pl.DataFrame:
    return _localize(_naive_vitals(), 'recorded_dttm')


@pytest.mark.parametrize('kind', ['polars', 'pandas'])
@pytest.mark.parametrize('aware', [False, True], ids=['naive_vitals', 'aware_vitals'])
def test_attach_weight_accepts_untyped_empty_med_df(kind, aware):
    vitals = _aware_vitals() if aware else _naive_vitals()
    out = attach_weight(_untyped(kind, _MED_COLS), vitals)
    assert out.height == 0
    assert set(WEIGHT_OUTPUT_COLUMNS) <= set(out.columns)
    assert out.columns[:len(_MED_COLS)] == _MED_COLS


@pytest.mark.parametrize('kind', ['polars', 'pandas'])
def test_attach_weight_gives_null_weight_for_untyped_empty_vitals(kind):
    out = attach_weight(_meds([('H1', '2024-03-10 10:00')]), _untyped(kind, _VITAL_COLS))
    assert out.height == 1
    assert out['weight_kg'][0] is None
    assert out['_weight_source'][0] is None


def test_attach_weight_gives_null_weight_for_pandas_none_recorded_dttm():
    import pandas as pd
    vitals = pd.DataFrame({
        'hospitalization_id': ['H1'], 'recorded_dttm': [None],
        'vital_category': ['weight_kg'], 'vital_value': [70.0],
    })
    out = attach_weight(_meds([('H1', '2024-03-10 10:00')]), vitals)
    assert out['weight_kg'][0] is None


def test_attach_weight_accepts_untyped_empty_hospitalization_admission():
    hosp = pl.DataFrame({'hospitalization_id': [], 'patient_id': [], 'admission_dttm': []})
    out = attach_weight(
        _meds([('H1', '2024-03-10 10:00')]), _naive_vitals(),
        fallbacks=('asof', 'prior_hosp_latest'), hospitalization_df=hosp,
    )
    assert out['weight_kg'][0] == 70.0


@pytest.mark.parametrize('kind', ['polars', 'pandas'])
@pytest.mark.parametrize('aware', [False, True], ids=['naive_meds', 'aware_meds'])
def test_converter_reports_missing_weight_for_untyped_empty_vitals(kind, aware):
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        med_dose=pl.lit(10.0), med_dose_unit=pl.lit('mg'),
    )
    if aware:
        meds = _localize(meds, 'admin_dttm')
    out, _ = convert_dose_units_by_med_category(
        meds, vitals_df=_untyped(kind, _VITAL_COLS),
        preferred_units={'propofol': 'mcg/kg'}, override=True,
    )
    assert out['_convert_status'][0] == WEIGHT_MISSING_UNWEIGHTED


@pytest.mark.parametrize('kind', ['polars', 'pandas'])
def test_find_most_recent_weight_accepts_untyped_empty_med_df(kind):
    out = find_most_recent_weight(_untyped(kind, _MED_COLS), _naive_vitals())
    assert out.pl().height == 0


def test_text_time_column_raises_a_clear_value_error():
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        admin_dttm=pl.col('admin_dttm').dt.to_string('%Y-%m-%d %H:%M:%S'),
    )
    with pytest.raises(ValueError, match=r"med_df.*'admin_dttm'.*VARCHAR.*[Pp]arse"):
        attach_weight(meds, _naive_vitals())
    vitals = _naive_vitals().with_columns(
        recorded_dttm=pl.col('recorded_dttm').dt.to_string('%Y-%m-%d %H:%M:%S'),
    )
    with pytest.raises(ValueError, match=r"vitals_df.*'recorded_dttm'.*VARCHAR.*[Pp]arse"):
        attach_weight(_meds([('H1', '2024-03-10 10:00')]), vitals)


def test_integer_time_column_with_values_raises_a_clear_value_error():
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        admin_dttm=pl.lit(5, dtype=pl.Int64),
    )
    with pytest.raises(ValueError, match=r"med_df.*'admin_dttm'.*BIGINT.*[Pp]arse"):
        attach_weight(meds, _naive_vitals())


def test_duckdb_output_pins_the_session_zone_to_time_col(tokyo_default_tz):
    """The pinned session zone is time_col's, not admin_dttm's or the first column's."""
    meds = _meds([('H1', '2024-03-10 10:00')]).with_columns(
        pl.col('admin_dttm').dt.replace_time_zone('US/Eastern'),
        other_dttm=pl.col('admin_dttm').dt.replace_time_zone('Europe/Paris'),
    )
    vitals = _vitals([('H1', '2024-03-10 08:00', 72.0)]).with_columns(
        pl.col('recorded_dttm').dt.replace_time_zone('Europe/Paris'))
    attach_weight(meds, vitals, time_col='other_dttm', return_format='duckdb')
    assert duckdb.sql("SELECT current_setting('TimeZone')").fetchone()[0] == 'Europe/Paris'


@pytest.mark.parametrize('bad', [{'a': [1]}, None, [1, 2], 5], ids=['dict', 'none', 'list', 'int'])
@pytest.mark.parametrize('entry_point', [
    'standardize_dose_to_base_units', 'convert_dose_units_by_med_category',
    'standardize_med_dose_units', 'attach_weight', 'find_most_recent_weight',
])
def test_non_table_input_raises_type_error(entry_point, bad):
    """The documented TypeError, not an AttributeError from `.columns`."""
    from clifpy.utils import unit_converter as uc
    good_vitals = _vitals([('H1', '2024-03-10 08:00', 72.0)])
    func = getattr(uc, entry_point)
    if entry_point == 'convert_dose_units_by_med_category':
        call = lambda: func(bad, preferred_units={'propofol': 'mcg/kg/min'})
    elif entry_point == 'standardize_med_dose_units':
        call = lambda: func(bad, {'propofol': 'mcg/kg/min'})
    elif entry_point == 'standardize_dose_to_base_units':
        call = lambda: func(bad)
    else:
        call = lambda: func(bad, good_vitals)
    with pytest.raises(TypeError, match='med_df'):
        call()
    if entry_point == 'attach_weight':
        with pytest.raises(TypeError, match='vitals_df'):
            func(_meds([('H1', '2024-03-10 10:00')]), bad)


def test_converter_duckdb_output_pins_the_session_zone_to_time_col(tokyo_default_tz):
    meds = pl.DataFrame({
        'hospitalization_id': ['H1'], 'med_category': ['propofol'],
        'med_dose': [10.0], 'med_dose_unit': ['mcg/kg/min'], 'weight_kg': [70.0],
        'admin_dttm': [_ts('2024-03-10 10:00')],
    }).with_columns(
        pl.col('admin_dttm').dt.replace_time_zone('US/Eastern'),
        other_dttm=pl.col('admin_dttm').dt.replace_time_zone('Europe/Paris'),
    )
    convert_dose_units_by_med_category(
        meds, preferred_units={'propofol': 'mg/kg/hr'}, time_col='other_dttm',
        return_format='duckdb')
    assert duckdb.sql("SELECT current_setting('TimeZone')").fetchone()[0] == 'Europe/Paris'


# ===========================================
# _weight_source in the lean output and the counts table (2026-11-19)
# ===========================================

def _dosed(meds, unit='mg', extra=None):
    """`meds` with a propofol dose of 10 `unit` per row (plus `extra` rows)."""
    out = meds.with_columns(med_dose=pl.lit(10.0), med_dose_unit=pl.lit(unit))
    if extra is not None:
        out = pl.concat([out, extra], how='diagonal_relaxed')
    return out


def test_lean_output_names_each_weight_strategy(scenario_data):
    """The full chain resolves each stay to its own strategy, the lean output
    says which, and a stay with no weight anywhere reads NULL."""
    meds, vitals, hosp = scenario_data
    out, counts = convert_dose_units_by_med_category(
        _dosed(meds), vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'},
        override=True, weight_fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    out = out.sort('rn')
    assert '_weight_recorded_dttm' not in out.columns
    assert out['_weight_source'].to_list() == [
        'asof', 'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest', None]
    assert out['_convert_status'][4] == WEIGHT_MISSING_UNWEIGHTED
    # the counts table splits the one (category, unit) group by strategy
    assert '_weight_source' in counts.columns
    assert sorted(counts['_weight_source'].to_list(), key=str) == sorted(
        ['asof', 'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest', None],
        key=str)
    assert counts['count'].to_list() == [1] * 5


def test_lean_output_names_the_legacy_earliest_fallback(scenario_data):
    meds, vitals, _ = scenario_data
    out, counts = convert_dose_units_by_med_category(
        _dosed(meds), vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'},
        override=True, fallback_on_earliest=True)
    by_id = _rows_by(out, 'hospitalization_id')
    assert by_id['H_ASOF']['_weight_source'] == 'asof'
    assert by_id['H_AFTER']['_weight_source'] == 'earliest_fallback'
    assert 'earliest_fallback' in counts['_weight_source'].to_list()


def test_weight_source_is_not_needed_where_no_weight_is_needed(scenario_data):
    """A row whose conversion never crosses the weight axis reads
    'not_needed', in the same call as rows that do."""
    meds, vitals, _ = scenario_data
    plain = pl.DataFrame({
        'rn': [99], 'hospitalization_id': ['H_ASOF'], 'admin_dttm': [_ts('2024-03-10 10:00')],
        'med_category': ['fentanyl'], 'med_dose': [50.0], 'med_dose_unit': ['mcg/hr']})
    out, counts = convert_dose_units_by_med_category(
        _dosed(meds, extra=plain), vitals_df=vitals,
        preferred_units={'propofol': 'mcg/kg', 'fentanyl': 'mcg/min'}, override=True)
    by_rn = _rows_by(out, 'rn')
    assert by_rn[99]['_weight_source'] == 'not_needed'
    assert by_rn[99]['_convert_status'] == 'success'
    assert by_rn[0]['_weight_source'] == 'asof'


def test_attach_weight_provenance_is_kept_and_counted(scenario_data):
    """attach_weight's own `_weight_source` comes back unchanged (never a
    second column, never 'provided'), and the counts table groups by it."""
    meds, vitals, hosp = scenario_data
    attached = attach_weight(
        _dosed(meds), vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    out, counts = convert_dose_units_by_med_category(
        attached, preferred_units={'propofol': 'mcg/kg'}, override=True)
    out = out.sort('rn')
    assert out.columns.count('_weight_source') == 1
    assert out['_weight_source'].to_list() == [
        'asof', 'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest', None]
    assert 'provided' not in counts['_weight_source'].to_list()
    assert counts.height == 5


def test_chained_pass_keeps_the_first_pass_labels(scenario_data):
    """A second pass over the first pass's output reads its weight_kg as
    pre-filled, and its `_weight_source` as the caller's: kept, not
    relabelled 'provided'."""
    meds, vitals, hosp = scenario_data
    first, _ = convert_dose_units_by_med_category(
        _dosed(meds).with_columns(rate=pl.lit(1.0), rate_unit=pl.lit('mg/hr')),
        vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'}, override=True,
        weight_fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    second, counts = convert_dose_units_by_med_category(
        first, preferred_units={'propofol': 'mcg/kg/hr'}, override=True,
        dose_col='rate', unit_col='rate_unit')
    second = second.sort('rn')
    assert second['_weight_source'].to_list() == [
        'asof', 'same_hosp_earliest', 'prior_hosp_latest', 'later_hosp_earliest', None]
    assert second['_convert_status__rate'].to_list()[:4] == ['success'] * 4
    assert 'provided' not in counts['_weight_source'].to_list()


def test_prefilled_weight_warning_advice_works_as_written(converter_log):
    """Review finding: every pass now returns `_weight_source`, so the
    warning's 'drop weight_kg' advice must name it too, and following the
    advice -- dropping both, then converting again or attaching weights --
    must work."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    volume, _ = _volume_then_dose(vitals)
    warned = [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
    assert len(warned) == 1
    assert 'Drop weight_kg and its _weight_* provenance columns first' in warned[0]
    fresh = volume.drop(cs.starts_with('_weight_'), 'weight_kg')
    dose, _ = convert_dose_units_by_med_category(
        fresh, vitals_df=vitals, preferred_units={'norepinephrine': 'mcg/min'})
    assert dose['_convert_status'].to_list() == ['success'] * 3
    attached = attach_weight(fresh, vitals)
    assert attached['_weight_source'].to_list() == ['asof'] * 3


def test_ignored_lookup_arguments_advice_works_as_written(lagged_weight_case, converter_log):
    """'med_df already has a weight_kg column ... ignored': the advice to drop
    it must also name its provenance columns, which an attach_weight or
    converter output carries, or the next call refuses them as stale."""
    med_df, vitals_df = lagged_weight_case
    attached = attach_weight(med_df, vitals_df, fallbacks=('asof', 'same_hosp_earliest'))
    convert_dose_units_by_med_category(
        attached, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True, weight_fallbacks=('asof', 'same_hosp_earliest'))
    warned = [m for level, m in converter_log if level == 'WARNING' and 'is used as-is' in m]
    assert len(warned) == 1
    assert 'Drop weight_kg and its _weight_* provenance columns' in warned[0]
    fresh = attached.drop('weight_kg', '_weight_source', '_weight_recorded_dttm', '_weight_age_hours')
    out, _ = convert_dose_units_by_med_category(
        fresh, vitals_df=vitals_df, preferred_units={'propofol': 'mcg/kg'},
        override=True, weight_fallbacks=('asof', 'same_hosp_earliest'))
    assert out['_weight_source'].to_list() == ['same_hosp_earliest']


# ===========================================
# _weight_source labels and _weight_age_hours in the lean output
# (D-261119-weight-labels)
# ===========================================

def test_lean_output_carries_the_weight_age_of_each_strategy(scenario_data):
    """The converter returns attach_weight's `_weight_age_hours` in the lean
    output: the same ages a direct attach_weight call gives."""
    meds, vitals, hosp = scenario_data
    kw = dict(fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    out, _ = convert_dose_units_by_med_category(
        _dosed(meds), vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'},
        override=True, weight_fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    out = out.sort('rn')
    expected = _attach(_dosed(meds), vitals, **kw)['_weight_age_hours'].to_list()
    assert out['_weight_age_hours'].to_list() == pytest.approx(expected, nan_ok=True)
    assert out['_weight_age_hours'][0] == pytest.approx(1.0)       # asof, 09:00 -> 10:00
    assert out['_weight_age_hours'][1] == pytest.approx(-1.0)      # charted after the dose
    assert out['_weight_age_hours'][4] is None                     # none anywhere
    assert '_weight_recorded_dttm' not in out.columns


@pytest.mark.parametrize('fallback_on_earliest, h_after_age', [(False, None), (True, -1.0)])
def test_weight_age_on_the_legacy_lookup_path(scenario_data, fallback_on_earliest, h_after_age):
    """Without weight_fallbacks the converter's own as-of lookup (and its
    legacy earliest fallback) also reports the age."""
    meds, vitals, _ = scenario_data
    out, _ = convert_dose_units_by_med_category(
        _dosed(meds), vitals_df=vitals, preferred_units={'propofol': 'mcg/kg'},
        override=True, fallback_on_earliest=fallback_on_earliest)
    by_id = _rows_by(out, 'hospitalization_id')
    assert by_id['H_ASOF']['_weight_age_hours'] == pytest.approx(1.0)
    assert by_id['H_AFTER']['_weight_age_hours'] == (
        None if h_after_age is None else pytest.approx(h_after_age))


def test_rows_needing_no_weight_read_not_needed(scenario_data):
    """`_weight_source = 'not_needed'` on every row whose conversion needs no
    weight; NULL only where a weight was needed and none was found."""
    meds, vitals, _ = scenario_data
    plain = pl.DataFrame({
        'rn': [99], 'hospitalization_id': ['H_NONE'], 'admin_dttm': [_ts('2024-03-10 10:00')],
        'med_category': ['fentanyl'], 'med_dose': [50.0], 'med_dose_unit': ['mcg/hr']})
    targets = {'propofol': 'mcg/kg', 'fentanyl': 'mcg/min'}
    # a lookup ran for the propofol rows
    out, counts = convert_dose_units_by_med_category(
        _dosed(meds, extra=plain), vitals_df=vitals, preferred_units=targets, override=True)
    by_rn = _rows_by(out, 'rn')
    assert (by_rn[99]['_weight_source'], by_rn[99]['_weight_age_hours']) == ('not_needed', None)
    assert by_rn[99]['weight_kg'] is None
    assert by_rn[0]['_weight_source'] == 'asof'
    assert _rows_by(out, 'rn')[4]['_weight_source'] is None          # H_NONE propofol: needed, none
    assert 'not_needed' in counts['_weight_source'].to_list()
    # no row needs a weight: no lookup at all
    none_needed, _ = convert_dose_units_by_med_category(
        plain, vitals_df=vitals, preferred_units={'fentanyl': 'mcg/min'})
    assert none_needed['_weight_source'].to_list() == ['not_needed']
    # rows need a weight but no vitals_df (override): needed -> NULL, others -> not_needed
    no_vitals, _ = convert_dose_units_by_med_category(
        _dosed(meds, extra=plain), preferred_units=targets, override=True)
    by_rn = _rows_by(no_vitals, 'rn')
    assert by_rn[99]['_weight_source'] == 'not_needed'
    assert by_rn[0]['_weight_source'] is None
    assert by_rn[0]['_convert_status'] == WEIGHT_MISSING_UNWEIGHTED


@pytest.mark.parametrize('weight, expected', [
    (50.0, 'prefilled'), (None, None), (0.0, None), (float('nan'), None)])
def test_prefilled_weight_reads_prefilled(lagged_weight_case, weight, expected):
    """A caller's usable weight_kg with no provenance of its own reads
    'prefilled' (the label was 'provided'); its age is unknown (NULL)."""
    med_df, _ = lagged_weight_case
    out, counts = convert_dose_units_by_med_category(
        med_df.with_columns(weight_kg=pl.lit(weight, dtype=pl.Float64)),
        preferred_units={'propofol': 'mcg/kg'}, override=True)
    assert out['_weight_source'].to_list() == [expected]
    assert out['_weight_age_hours'].to_list() == [None]
    assert counts['_weight_source'].to_list() == [expected]


def test_attach_weight_age_is_kept(scenario_data):
    meds, vitals, hosp = scenario_data
    attached = attach_weight(
        _dosed(meds), vitals, fallbacks=WEIGHT_FALLBACKS, hospitalization_df=hosp)
    out, _ = convert_dose_units_by_med_category(
        attached, preferred_units={'propofol': 'mcg/kg'}, override=True)
    out = out.sort('rn')
    assert out.columns.count('_weight_age_hours') == 1
    assert out['_weight_age_hours'].to_list() == pytest.approx(
        attached.sort('rn')['_weight_age_hours'].to_list(), nan_ok=True)


def test_second_pass_warning_keys_on_the_weight_source(converter_log):
    """An earlier pass's 'not_needed' placeholders warn, even though that
    output now carries `_weight_age_hours`; a lookup that found nothing
    (NULL) does not."""
    vitals = _vitals([('H1', '2024-03-10 09:00', 80.0)])
    volume, dose = _volume_then_dose(vitals)
    assert '_weight_age_hours' in volume.columns
    assert volume['_weight_source'].to_list() == ['not_needed'] * 3
    assert [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
    converter_log.clear()
    # same frame, but its weight was looked up and not found: no warning
    looked_up = volume.with_columns(pl.lit(None, dtype=pl.String).alias('_weight_source'))
    convert_dose_units_by_med_category(
        looked_up, vitals_df=vitals, preferred_units={'norepinephrine': 'mcg/min'})
    assert not [m for level, m in converter_log if level == 'WARNING' and VITALS_UNUSED in m]
