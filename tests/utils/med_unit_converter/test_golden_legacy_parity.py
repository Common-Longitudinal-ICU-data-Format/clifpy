"""Golden parity: the converter package against the pre-refactor monolith.

``golden_legacy_parity.csv`` holds what the single-file converter at 5cd5fcd
returned for 72 legacy target units (plus a category with no target, which
falls back to the base unit) x 86 raw-spelling slots x weight present/missing:
12,556 rows. ``_make_golden_legacy_parity.py`` built it once from
``git show 5cd5fcd:clifpy/utils/unit_converter.py``. This test runs the same
inputs through the current package and requires the same dose (exact float
equality), unit and status on every row. Rows are matched on ``grid_row``, so
output row order does not matter. The fixture is read and compared in polars.

Two changes since 5cd5fcd are intended. They are declared here instead of being
regenerated into the fixture:

- ``Nanogram/kg/min`` was unrecognized by the monolith. The CLIF 3.0 grammar
  reads ``nanogram`` as ``ng``, so those 146 rows now convert.

- With no preferred unit, the converted unit is the base unit, which the
  package spells with the canonical ``units`` token (``units/min``) where the
  monolith wrote ``u/min`` (40 rows). Doses and statuses are unchanged.

A failure means legacy behaviour changed. Fix the converter, or declare the
change here with its reason; never regenerate the fixture to make this pass.
"""
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from clifpy.utils.unit_converter import convert_dose_units_by_med_category

FIXTURE = Path(__file__).with_name('golden_legacy_parity.csv')
NA_TOKEN = '<NA>'
FALLBACK_CATEGORY = 'base_fallback'
INPUT_COLUMNS = ['grid_row', 'med_category', 'med_dose_unit', 'weight_kg', 'med_dose']
OUTPUT_COLUMNS = ['med_dose_converted', 'med_dose_unit_converted', '_convert_status']
TEXT_COLUMNS = ['med_category', 'preferred_unit', 'med_dose_unit',
                'med_dose_unit_converted', '_convert_status']

N_TARGETS = 73          # 72 legacy units + the base fallback
N_RAW_SLOTS = 86        # 72 legacy units + 14 variants ...
N_RAW_DISTINCT = 85     # ... 'mcg/lb/hr' is in both, at two doses
N_WEIGHTS = 2           # 70.0 kg, missing

# Declared change 1: a spelling the monolith rejected now converts.
NEWLY_RECOGNIZED_RAW = 'Nanogram/kg/min'
LEGACY_REJECTION = 'original unit nanogram/kg/min is not recognized'
# Declared change 2: base-unit spelling on the no-target fallback path.
FALLBACK_SPELLING = {
    'u': 'units',
    'u/min': 'units/min',
    'u/kg': 'units/kg',
    'u/kg/min': 'units/kg/min',
}


def _load_golden() -> pl.DataFrame:
    """Read the fixture: '' stays '', '<NA>' becomes missing, floats round-trip exactly."""
    return pl.read_csv(
        FIXTURE,
        null_values=[NA_TOKEN],
        missing_utf8_is_empty_string=True,
        schema_overrides={col: pl.String for col in TEXT_COLUMNS},
        infer_schema_length=None,
    )


def _convert_grid(golden: pl.DataFrame) -> pl.DataFrame:
    """Run the fixture's inputs through the current package: ``grid_row`` plus the outputs."""
    med = golden.select(INPUT_COLUMNS).with_columns(
        hospitalization_id=pl.lit('H0'), admin_dttm=pl.lit(datetime(2024, 1, 1)),
    )
    targets = (
        golden.drop_nulls('preferred_unit')
        .unique('med_category', keep='first', maintain_order=True)
    )
    out, _ = convert_dose_units_by_med_category(
        med,
        preferred_units=dict(zip(targets['med_category'].to_list(),
                                 targets['preferred_unit'].to_list())),
        override=True,
    )
    return out.select(['grid_row', *OUTPUT_COLUMNS])


@pytest.mark.unit_conversion
def test_golden_fixture_covers_the_legacy_grid():
    golden = _load_golden()
    assert golden['grid_row'].is_unique().all()
    assert golden.height == N_TARGETS * N_RAW_SLOTS * N_WEIGHTS == 12_556
    assert golden['med_category'].n_unique() == N_TARGETS
    assert golden['preferred_unit'].drop_nulls().n_unique() == N_TARGETS - 1
    assert golden.filter(pl.col('med_category') == FALLBACK_CATEGORY)['preferred_unit'].is_null().all()
    # n_unique counts a missing value as a value, like pandas' nunique(dropna=False)
    assert golden['med_dose_unit'].n_unique() == N_RAW_DISTINCT
    assert golden['weight_kg'].n_unique() == N_WEIGHTS
    per_cell = golden.group_by(['med_category', 'weight_kg']).len()
    assert per_cell.height == N_TARGETS * N_WEIGHTS
    assert (per_cell['len'] == N_RAW_SLOTS).all()


@pytest.mark.unit_conversion
def test_legacy_units_bit_identical():
    expected = _load_golden()
    actual = _convert_grid(expected)
    assert actual['grid_row'].is_unique().all(), 'conversion duplicated rows'
    assert set(actual['grid_row'].to_list()) == set(expected['grid_row'].to_list()), (
        'conversion lost or invented rows')
    both = expected.join(actual, on='grid_row', how='left', suffix='_now')

    # Declared change 1: the monolith rejected Nanogram/kg/min; the package converts it.
    new = pl.col('med_dose_unit').eq_missing(NEWLY_RECOGNIZED_RAW)
    newly = both.filter(new)
    assert newly.height == N_TARGETS * N_WEIGHTS
    assert (newly['_convert_status'] == LEGACY_REJECTION).all()
    assert not newly['_convert_status_now'].str.starts_with('original unit').any()

    # Declared change 2: the no-target fallback spells the unit family 'units'.
    # Declared change 3: a failed row's unit (its cleaned source unit) spells
    # the unit family out too (`u/hr` -> `units/hr`, `mu` -> `milli-units`),
    # as every unit leaving clifpy now does; a token is rewritten only when a
    # `/` or the end follows it. Doses and statuses are compared unchanged.
    spelled = pl.col('med_dose_unit_converted')
    for token, name in (('mnu', 'million-units'), ('mu', 'milli-units'), ('u', 'units')):
        spelled = spelled.str.replace(rf'^{token}(/|$)', f'{name}$1')
    want_unit = (
        pl.when(pl.col('med_category') == FALLBACK_CATEGORY)
        .then(pl.col('med_dose_unit_converted').replace(FALLBACK_SPELLING))
        .when(pl.col('_convert_status') != 'success')
        .then(spelled)
        .otherwise(pl.col('med_dose_unit_converted'))
    )
    # eq_missing: two missing values are equal, and no comparison is ever null
    ok = (
        pl.col('med_dose_converted_now').eq_missing(pl.col('med_dose_converted'))
        & pl.col('med_dose_unit_converted_now').eq_missing(want_unit)
        & pl.col('_convert_status_now').eq_missing(pl.col('_convert_status'))
    )
    legacy = both.filter(~new)
    bad = legacy.filter(~ok)
    assert bad.is_empty(), (
        f'{bad.height} of {legacy.height} legacy rows differ from 5cd5fcd; first 10:\n'
        + str(bad.select(['preferred_unit', 'med_dose_unit', 'weight_kg', *OUTPUT_COLUMNS,
                          *(f'{c}_now' for c in OUTPUT_COLUMNS)]).head(10))
    )
