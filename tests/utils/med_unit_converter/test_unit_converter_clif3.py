"""Tests for the CLIF 3.0 unit-converter expansion.

Covers the work that widened the grammar to the mCIDE target vocabulary, added
the `countable` class and placeholder handling, made column names parameters,
and added schema-driven standardization.

The two regression tests worth reading first are
:func:`test_preferred_unit_normalized_milli_units` and
:func:`test_million_units_not_confused_with_milliunits`. Both guard *silent*
failures -- cases where the converter reported success while returning a dose
off by 1000x or 1e9 -- which is why they assert on values rather than status.

Style follows tests/utils/med_unit_converter/test_unit_converter.py: CSV
fixtures co-located in this directory, module-level constants over magic
numbers.
"""

import ast
import inspect
import re
import typing
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd  # boundary tests only: pandas input and the deprecated pandas output
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from clifpy.utils.unit_converter import (
    ALL_ACCEPTABLE_UNITS,
    DEFAULT_COUNTABLE_UNITS,
    SUBCLASS_SPEC,
    attach_weight,
    convert_dose_units_by_med_category,
    load_dose_unit_targets,
    register_target_reader,
    standardize_dose_to_base_units,
    standardize_med_dose_units,
    validate_column_name,
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb,
)
import clifpy.utils.unit_converter as unit_converter_pkg
from clifpy.utils.unit_converter._columns import (
    PAIR_QA_COLUMNS,
    PIPELINE_OWNED_COLUMNS,
    WEIGHT_OUTPUT_COLUMNS,
    OutputNames,
    pair_column_name,
    quote_identifier,
    resolve_output_names,
)
from clifpy.utils._duckdb_helpers import _drop_temp_table, _register_temp_table
from clifpy.utils.io import new_duckdb_con
from clifpy.utils.unit_converter._convert import (
    INPUT_ORDINAL_COL,
    _capture_input,
    _register_input,
)
from clifpy.utils.unit_converter._inputs import REPLACEMENT_SCAN_NAMES

from clifpy.utils.unit_converter import _targets

FIXTURE_DIR = Path(__file__).parent

# 1 kg = 2.20462 lb; mirrors clifpy.utils.unit_converter.KG_PER_LB
WEIGHT_KG = 70.0
TS = datetime(2024, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def captured_warnings():
    """Collect WARNING records from clifpy's converter logger.

    Not `caplog`: clifpy configures its own handlers, and once another test has
    initialised logging the records no longer propagate to the root logger that
    caplog attaches to -- so caplog passes in isolation and fails in a full
    run. Attaching directly to the named logger is order-independent.
    """
    import logging

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

    handler = _Capture()
    lg = logging.getLogger('clifpy.utils.unit_converter')
    previous_level = lg.level
    lg.addHandler(handler)
    lg.setLevel(logging.WARNING)
    try:
        yield records
    finally:
        lg.removeHandler(handler)
        lg.setLevel(previous_level)


def _frame(rows) -> pl.DataFrame:
    """Build a minimal med frame from (category, dose, unit) triples."""
    return pl.DataFrame(
        {
            'hospitalization_id': [f'H{i}' for i in range(len(rows))],
            'med_category': [r[0] for r in rows],
            'med_dose': [r[1] for r in rows],
            'med_dose_unit': [r[2] for r in rows],
            'weight_kg': [WEIGHT_KG] * len(rows),
            'admin_dttm': [TS] * len(rows),
        },
        schema={
            'hospitalization_id': pl.String, 'med_category': pl.String,
            'med_dose': pl.Float64, 'med_dose_unit': pl.String,
            'weight_kg': pl.Float64, 'admin_dttm': pl.Datetime('us', 'UTC'),
        },
    )


def _by_category(out: pl.DataFrame) -> dict:
    """{med_category: output row as a dict}; categories are unique in these tests."""
    return {row['med_category']: row for row in out.iter_rows(named=True)}


def _convert(rows, preferred_units, **kwargs):
    """Convert `rows` (see `_frame`); return ({med_category: output row}, counts)."""
    kwargs.setdefault('override', True)
    out, counts = convert_dose_units_by_med_category(
        _frame(rows), preferred_units=preferred_units, **kwargs)
    return _by_category(out), counts


def _classes(counts):
    """{med_category: _unit_class} from a counts table, which carries the
    class under every query shape (the lean row output does not)."""
    return dict(zip(counts['med_category'].to_list(), counts['_unit_class'].to_list()))


def _normalize(unit):
    """Push a unit string through the converter's own cleaning chain."""
    df = pl.DataFrame({'u': [unit]}, schema={'u': pl.String})
    rel = _clean_dose_unit_formats_duckdb(df, col='u', out_col='c')
    rel = _clean_dose_unit_names_duckdb(rel, col='c')
    return rel.project('c').fetchone()[0]


def _normalize_many(units):
    """`_normalize` for many strings in one query: {raw: cleaned}."""
    df = pl.DataFrame({'u': list(units)}, schema={'u': pl.String})
    rel = _clean_dose_unit_formats_duckdb(df, col='u', out_col='c')
    rel = _clean_dose_unit_names_duckdb(rel, col='c')
    return dict(rel.project('u, c').fetchall())


# ===========================================
# Silent-failure regressions
# ===========================================

@pytest.mark.unit_conversion
def test_preferred_unit_normalized_milli_units():
    """mCIDE's `milli-units/min` must behave identically to `mu/min`.

    The caller's preferred unit used to be matched against the factor regexes
    raw. `milli-units/min` starts "mi", so it failed `MU_REGEX = ^(mu)`, the
    multiplier fell through to 1, and the dose came out 1000x wrong while
    `_convert_status` still said 'success'.
    """
    out, _ = _convert(
        [('spelled', 1.0, 'units/min'), ('abbrev', 1.0, 'units/min')],
        {'spelled': 'milli-units/min', 'abbrev': 'mu/min'},
    )
    assert out['spelled']['med_dose_converted'] == pytest.approx(1000.0)
    assert out['abbrev']['med_dose_converted'] == pytest.approx(1000.0)
    assert (out['spelled']['med_dose_converted']
            == out['abbrev']['med_dose_converted'])
    assert out['spelled']['_convert_status'] == 'success'


@pytest.mark.unit_conversion
def test_million_units_not_confused_with_milliunits():
    """`million units` and `milli-units` must stay 1e9 apart.

    Clinically `MU` is million units and `mU` is milliunits, differing only by
    case -- and case is gone by the time the converter sees the string. Mapping
    million-units onto the existing `mu` token would be a 1e9 error on
    oxytocin, penicillin and heparin doses.
    """
    out, _ = _convert(
        [('mega', 1.0, 'million units'), ('milli', 1.0, 'milli-units')],
        {'mega': 'u', 'milli': 'u'},
    )
    assert out['mega']['med_dose_converted'] == pytest.approx(1e6)
    assert out['milli']['med_dose_converted'] == pytest.approx(1e-3)
    ratio = (out['mega']['med_dose_converted']
             / out['milli']['med_dose_converted'])
    assert ratio == pytest.approx(1e9)


@pytest.mark.unit_conversion
def test_bare_mu_is_never_read_as_million_units():
    """A bare `mu` means milli-units, never million units."""
    assert _normalize('MU') == 'mu'
    assert _normalize('mu') == 'mu'
    assert _normalize('million units') == 'mnu'
    assert _normalize('MMU') == 'mnu'


# Peer contract with the SOFA-1 rewrite (session pyclif-1b): byte-identical.
AMBIGUOUS_MU = "cannot convert milli-units to million-units (ambiguous 'MU')"


@pytest.mark.unit_conversion
def test_ambiguous_mu_status_is_exported_verbatim():
    from clifpy.utils.unit_converter import AMBIGUOUS_MU_STATUS
    assert AMBIGUOUS_MU_STATUS == AMBIGUOUS_MU


@pytest.mark.unit_conversion
@pytest.mark.parametrize('src,tgt,clean_src', [
    ('mU/min', 'million-units/min', 'milli-units/min'),
    ('MU', 'million-units', 'milli-units'),                # penicillin G charted '5 MU'
    ('milli-units/kg/hr', 'MMU/kg/hr', 'milli-units/kg/hr'),
    ('million units', 'milli-units', 'million-units'),      # the reverse direction
    ('MMU/hr', 'mU/hr', 'million-units/hr'),
])
def test_milli_to_million_units_is_refused(src, tgt, clean_src):
    """milli-units <-> million-units is refused, never computed.

    D-260930-mu-ambiguity. A bare `MU` is read as milli-units, while
    clinically it usually means million units. The two families are 1e9
    apart, and the conversion used to report 'success': '5 MU' of penicillin
    G targeted at million-units came out as 5e-9. The row now passes through
    untouched, like every other refused conversion.
    """
    out, counts = _convert([('drug', 5.0, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == AMBIGUOUS_MU
    assert out['drug']['med_dose_converted'] == 5.0
    assert out['drug']['med_dose_unit_converted'] == clean_src
    assert AMBIGUOUS_MU in counts['_convert_status'].to_list()


@pytest.mark.unit_conversion
@pytest.mark.parametrize('src,tgt,clean_src', [
    ('mU/kg/min', 'million-units/min', 'milli-units/kg/min'),
    ('million units/kg/hr', 'mU/hr', 'million-units/kg/hr'),
])
def test_ambiguous_mu_row_does_not_need_vitals(src, tgt, clean_src):
    """A refused milli <-> million row never triggers the weight lookup.

    Its units cross the weight axis, but stage 2 refuses the row before any
    weight is read. `_weight_lookup_filter` must therefore leave it out, so a
    call with neither `weight_kg` nor `vitals_df` returns the refusal instead
    of raising 'weight_kg is missing ... vitals_df=None'.
    """
    med_df = _frame([('drug', 5.0, src)]).drop('weight_kg')
    out, _ = convert_dose_units_by_med_category(
        med_df, preferred_units={'drug': tgt}, override=False,   # must NOT raise
    )
    assert out.height == 1
    row = out.row(0, named=True)    # polars by default (D-261022-return-format)
    assert row['_convert_status'] == AMBIGUOUS_MU
    assert row['med_dose_converted'] == 5.0
    assert row['med_dose_unit_converted'] == clean_src


@pytest.mark.unit_conversion
@pytest.mark.parametrize('dose,src,tgt,expected', [
    (2.0, 'mU/min', 'milli-units/min', 2.0),      # pitocin, identity
    (2000.0, 'mU/min', 'units/min', 2.0),
    (2.0, 'MU/min', 'units/min', 0.002),          # a bare MU reads as milli-units
    (3.0, 'million units', 'units', 3e6),
    (3e6, 'units', 'million-units', 3.0),
])
def test_unit_family_conversions_beside_mu_mnu_still_succeed(dose, src, tgt, expected):
    """The refusal is narrow: units <-> milli-units and units <-> million-units convert."""
    out, _ = _convert([('drug', dose, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == 'success'
    assert out['drug']['med_dose_converted'] == pytest.approx(expected)


# ===========================================
# clifpy#153
# ===========================================

@pytest.mark.unit_conversion
def test_issue_153_partial_preferred_units():
    """An unconvertible drug outside `preferred_units` must not abort the call.

    Requesting vasopressor units on a table that also held sodium bicarbonate
    charted in mEq/min used to raise, naming a unit the caller never asked for.
    """
    med_df = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('sodium bicarbonate', 50.0, 'not-a-unit'),
    ])
    out, _ = convert_dose_units_by_med_category(
        med_df,
        preferred_units={'norepinephrine': 'mcg/kg/min'},
        override=False,          # must NOT raise
    )
    out = _by_category(out)
    assert out['norepinephrine']['_convert_status'] == 'success'
    assert out['norepinephrine']['med_dose_converted'] == pytest.approx(5.0 / WEIGHT_KG)
    # the untouched drug passes through and is reported per row
    assert out['sodium bicarbonate']['med_dose_converted'] == 50.0
    assert 'not recognized' in out['sodium bicarbonate']['_convert_status']


@pytest.mark.unit_conversion
def test_bad_caller_unit_still_raises_and_names_the_category():
    """A genuinely bad unit the caller DID supply must still raise."""
    with pytest.raises(ValueError) as exc:
        convert_dose_units_by_med_category(
            _frame([('norepinephrine', 5.0, 'mcg/min')]),
            preferred_units={'norepinephrine': 'iu/hr'},
            override=False,
        )
    assert 'norepinephrine' in str(exc.value)


# ===========================================
# New unit families
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('dose,src,tgt,expected', [
    (60.0, 'mEq/hr', 'meq/min', 1.0),
    (1.0, 'meq/min', 'meq/hr', 60.0),
    (5.0, 'mmol', 'mmol', 5.0),
    (20.0, 'ppm', 'ppm', 20.0),
    (2.0, 'cells', 'cells', 2.0),
    (1440.0, 'mcg/kg/day', 'mcg/kg/min', 1.0),
    (1.0, 'mcg/kg/min', 'mcg/kg/day', 1440.0),
    (5.0, 'nanogram/kg/min', 'ng/kg/min', 5.0),
    (1000.0, 'microliter/min', 'ml/min', 1.0),
    (1.0, 'units', 'million-units', 1e-6),
])
def test_new_family_conversions(dose, src, tgt, expected):
    out, _ = _convert([('drug', dose, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == 'success'
    assert out['drug']['med_dose_converted'] == pytest.approx(expected)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw,expected', [
    # mass: 'milli-?' leaves "mgram(s)", which used to stay unrecognised
    ('milligram', 'mg'),
    ('Milligrams', 'mg'),
    ('milligrams/kg/hour', 'mg/kg/hr'),
    # volume: ASCII `ul` for microlitre, as `ug` is for microgram
    ('uL', 'mcl'),
    ('UL/KG/MIN', 'mcl/kg/min'),
    # equivalents and moles
    ('milliequivalents', 'meq'),
    ('milli-equivalent/kg/hour', 'meq/kg/hr'),
    ('millimoles', 'mmol'),
    ('millimole/kg/day', 'mmol/kg/day'),
    # time: plurals and the 24-hour spellings (audit mutant G4)
    ('mg/hours', 'mg/hr'),
    ('mg/hrs', 'mg/hr'),
    ('mcg/minutes', 'mcg/min'),
    ('mcg/mins', 'mcg/min'),
    ('mg/days', 'mg/day'),
    ('mg/kg/24hr', 'mg/kg/day'),
    ('mg/kg/24hrs', 'mg/kg/day'),
    ('mcg/24hour', 'mcg/day'),
    ('mcg / 24 hours', 'mcg/day'),
    ('mg/24h', 'mg/day'),
    # near misses stay unrecognised: no rule takes an optional leading m
    ('moles', 'moles'),
    ('equivalents', 'equivalents'),
])
def test_spelled_out_unit_variants(raw, expected):
    """Spelled-out forms normalize to the internal token (audit M28, M81, M91)."""
    assert _normalize(raw) == expected


@pytest.mark.unit_conversion
@pytest.mark.parametrize('dose,src,tgt,expected', [
    (1440.0, 'mcg/kg/24hr', 'mcg/kg/min', 1.0),
    (1440.0, 'mcg / 24 hours', 'mcg/min', 1.0),
    (60.0, 'milligrams/hours', 'mg/min', 1.0),
    (2.0, 'milligram', 'mcg', 2000.0),
    (1000.0, 'uL/minutes', 'ml/min', 1.0),
    (1440.0, 'milliequivalents/days', 'meq/min', 1.0),
    (5.0, 'millimoles', 'mmol', 5.0),
])
def test_spelled_out_variants_convert(dose, src, tgt, expected):
    out, _ = _convert([('drug', dose, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == 'success'
    assert out['drug']['med_dose_converted'] == pytest.approx(expected)


@pytest.mark.unit_conversion
def test_cleaning_leaves_canonical_units_unchanged():
    """Over-match guard for UNIT_NAMING_VARIANTS.

    Every unit the converter accepts, and every default countable token, is
    already canonical, so cleaning must return it unchanged. A rule that
    over-matches -- an unanchored `/d` for days would turn `/day` into
    `/dayay` -- fails here; the near-miss rows of
    test_spelled_out_unit_variants catch rules too loose on other input
    (`^m?moles?`). It is also why cleaning the countable vocabulary cannot
    change the defaults.
    """
    vocab = sorted(ALL_ACCEPTABLE_UNITS | set(DEFAULT_COUNTABLE_UNITS))
    changed = {u: c for u, c in _normalize_many(vocab).items() if u != c}
    assert not changed, f'cleaning rewrote canonical units: {changed}'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('src,tgt,message', [
    ('mEq', 'mg', 'cannot convert equivalent to mass'),
    ('mmol', 'mg', 'cannot convert substance to mass'),
    ('ppm', 'mcg', 'cannot convert gas_fraction to mass'),
    ('cells', 'mg', 'cannot convert cell_count to mass'),
])
def test_cross_family_conversion_is_refused(src, tgt, message):
    """mEq -> mg needs ion valence and molar mass the table does not carry.

    The existing subclass guard blocks this with no family-specific code; the
    dose must pass through untouched rather than being silently scaled.
    """
    out, _ = _convert([('drug', 10.0, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == message
    assert out['drug']['med_dose_converted'] == 10.0


@pytest.mark.unit_conversion
def test_standalone_units_have_no_weight_or_time_forms():
    """`ppm/kg/min` and `cells/kg` are meaningless and must not be accepted."""
    for bad in ('ppm/kg/min', 'ppm/hr', 'ppm/kg', 'cells/kg/min', 'cells/hr'):
        assert bad not in ALL_ACCEPTABLE_UNITS
    for good in ('ppm', 'cells'):
        assert good in ALL_ACCEPTABLE_UNITS


@pytest.mark.unit_conversion
def test_mcide_targets_all_acceptable():
    """Every CLIF 3.0 mCIDE target unit must normalize to an accepted unit.

    This is the acceptance test for the whole expansion. The external schema is
    a better adversary than fixtures written against our own implementation --
    it is what surfaced the `milli-units/min` bug in the first place.
    """
    spec = pl.read_csv(FIXTURE_DIR / 'mcide_target_units.csv')
    assert len(spec) == 25, (
        'fixture should list 25 (table, target unit) rows: 23 distinct units, '
        'with mg and ml in both tables'
    )
    # Row counts, not distinct categories: CLIF 3.0 keys on
    # (med_category, med_group), so epoprostenol and terbutaline each occupy
    # two rows with different targets. 77 rows cover 75 distinct categories.
    assert spec.filter(pl.col('table') == 'continuous')['n_rows'].sum() == 77
    assert spec.filter(pl.col('table') == 'intermittent')['n_rows'].sum() == 271

    rejected = [u for u in spec['target_unit'].to_list() if _normalize(u) not in ALL_ACCEPTABLE_UNITS]
    assert not rejected, f'mCIDE targets not accepted: {rejected}'


# ===========================================
# Grammar invariants
# ===========================================

@pytest.mark.unit_conversion
def test_subclass_spec_regexes_longest_first():
    """Longer tokens must win over shorter ones that prefix them.

    Regex alternation is leftmost-match, so `^(mg|mcg)` matches only `mg`
    against "mcg". Guards a future token added to SUBCLASS_SPEC from silently
    breaking precedence.
    """
    from clifpy.utils.unit_converter import SUBCLASS_REGEX

    for name, spec in SUBCLASS_SPEC.items():
        rx = SUBCLASS_REGEX[name]
        for token in spec['tokens']:
            matched = duckdb.sql(
                f"SELECT regexp_extract('{token}', '{rx}', 1) AS m"
            ).fetchone()[0]
            assert matched == token, (
                f'{name}: {token!r} matched as {matched!r}; alternation is not '
                f'longest-first'
            )


@pytest.mark.unit_conversion
def test_canonical_output_spelling():
    """Explicit requests echo the caller; clifpy's own choice uses mCIDE."""
    out, _ = _convert(
        [('short', 1.0, 'units/min'),
         ('long', 1.0, 'units/min'),
         ('none', 1.0, 'units/min')],
        {'short': 'u/min', 'long': 'units/min'},
    )
    assert out['short']['med_dose_unit_converted'] == 'u/min'
    assert out['long']['med_dose_unit_converted'] == 'units/min'
    # no preferred unit -> clifpy picks, so it spells it the mCIDE way
    assert out['none']['med_dose_unit_converted'] == 'units/min'


# ===========================================
# countable class and placeholders
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('show_intermediate', [False, True])
@pytest.mark.parametrize('raw', ['tablet', 'Tablet.', 'drop', 'puff', 'dose', 'each'])
def test_countable_class_passthrough(raw, show_intermediate):
    """Countable forms are understood, reported as such, and never scaled.

    Runs under both stage-1 query shapes: the show_intermediate one once had
    its own copy of the classification CASE (audit M90, mutant B3).
    """
    out, counts = _convert([('drug', 3.0, raw)], None, show_intermediate=show_intermediate)
    assert _classes(counts)['drug'] == 'countable'
    assert out['drug']['med_dose_converted'] == 3.0
    assert 'countable dosage form' in out['drug']['_convert_status']


@pytest.mark.unit_conversion
def test_mg_kg_dose_is_not_countable():
    """`mg/kg/dose` is a prescribing rate, not a dosage form.

    Guards the choice of exact set membership over a regex: a regex on 'dose'
    would swallow this.
    """
    out, counts = _convert([('drug', 3.0, 'mg/kg/dose')], None)
    assert _classes(counts)['drug'] == 'unrecognized'
    assert 'not recognized' in out['drug']['_convert_status']


@pytest.mark.unit_conversion
def test_countable_units_override():
    """Callers can extend the countable vocabulary without editing clifpy."""
    _, default = _convert([('drug', 1.0, 'troche')], None)
    assert _classes(default)['drug'] == 'unrecognized'

    _, extended = _convert([('drug', 1.0, 'troche')], None,
                           countable_units=set(DEFAULT_COUNTABLE_UNITS) | {'troche'})
    assert _classes(extended)['drug'] == 'countable'


@pytest.mark.unit_conversion
def test_countable_units_replace_the_defaults():
    """A passed collection replaces DEFAULT_COUNTABLE_UNITS; union to extend."""
    _, counts = _convert([('tab', 1.0, 'tablet'), ('tro', 1.0, 'troche')], None,
                         countable_units={'troche'})
    assert _classes(counts) == {'tab': 'unrecognized', 'tro': 'countable'}


@pytest.mark.unit_conversion
def test_countable_units_are_cleaned_like_the_data():
    """Caller tokens get the same cleaning as `_clean_unit` (audit M26).

    Sites pass their vocabulary as charted -- capitalised, with a trailing
    period or an inner space -- and it must still match the cleaned data.
    """
    _, counts = _convert(
        [('a', 1.0, 'troche'), ('b', 1.0, 'Lozenge.'), ('c', 1.0, 'nebule pack')],
        None,
        countable_units={'Troche', 'LOZENGE.', 'Nebule Pack'},
    )
    assert _classes(counts) == {
        'a': 'countable', 'b': 'countable', 'c': 'countable'}


@pytest.mark.unit_conversion
def test_countable_units_with_a_quote_are_literals():
    """A token holding an apostrophe is matched as text (audit M26)."""
    _, counts = _convert(
        [('own', 1.0, "patient's own supply")],
        None,
        countable_units={"Patient's own supply"},
    )
    assert _classes(counts)['own'] == 'countable'


@pytest.mark.unit_conversion
def test_countable_units_cannot_inject_sql():
    """A crafted token must not rewrite the stage-1 IN predicate (audit M26)."""
    _, counts = _convert(
        [('junk', 1.0, 'zzbag')],
        None,
        countable_units={"x') OR TRUE OR ('"},
    )
    assert _classes(counts)['junk'] == 'unrecognized'


@pytest.mark.unit_conversion
def test_countable_units_rejects_a_bare_string():
    """'troche' alone would be iterated as six one-letter tokens."""
    with pytest.raises(TypeError, match='collection'):
        _convert([('drug', 1.0, 'troche')], None, countable_units='troche')


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', [
    'nan', 'None', '*Unspecified', 'unknown', '', 'null', 'NULL', 'unspecified',
])
def test_placeholder_units_report_as_missing(raw):
    """Placeholders mean "no unit recorded", not "unit we cannot parse"."""
    out, _ = _convert([('drug', 1.0, raw)], None)
    assert out['drug']['_convert_status'] == 'original unit is missing'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', ['asord', 'zzbag', 'XX'])
def test_site_junk_codes_stay_unrecognized(raw):
    """Junk codes are a real data-quality signal; folding them into "missing"
    would hide it."""
    out, _ = _convert([('drug', 1.0, raw)], None)
    assert 'not recognized' in out['drug']['_convert_status']


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw,expected', [
    ('g (central catheter)', 'g'),
    ('mg (central catheter)', 'mg'),
    ('ml given', 'ml'),
    ('mg central line', 'mg'),     # audit M81: only 'given' was tested
    ('ML Central Line', 'ml'),
    ('Tablet.', 'tablet'),
])
def test_trailing_noise_is_stripped(raw, expected):
    assert _normalize(raw) == expected


# ===========================================
# Column-name parameters
# ===========================================

@pytest.mark.unit_conversion
def test_custom_column_names_and_collision():
    """Converting volume_infusion_rate must not disturb med_dose.

    A medication_admin_continuous row carries both, so the collision is the
    normal case rather than an edge case.
    """
    df = _frame([('sodium_chloride', 100.0, 'ml/hr')])
    df = df.with_columns(pl.Series('volume_infusion_rate', [2.0]))
    df = df.with_columns(pl.Series('volume_infusion_rate_unit', ['l/hr']))

    out, _ = convert_dose_units_by_med_category(
        df,
        preferred_units={'sodium_chloride': 'ml/hr'},
        override=True,
        dose_col='volume_infusion_rate',
        unit_col='volume_infusion_rate_unit',
        converted_dose_col='volume_infusion_rate_converted',
        converted_unit_col='volume_infusion_rate_unit_converted',
    )
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(2000.0)
    assert out['volume_infusion_rate_unit_converted'][0] == 'ml/hr'
    # the parked columns came back intact
    assert out['med_dose'][0] == 100.0
    assert out['med_dose_unit'][0] == 'ml/hr'
    # ...and so did the source pair, under its own names (kills mutant R4)
    assert out['volume_infusion_rate'][0] == 2.0
    assert out['volume_infusion_rate_unit'][0] == 'l/hr'
    assert not any(c.startswith('_saved__') for c in out.columns)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('bad', [
    'x"; DROP TABLE y --', 'a b', '1col', 'col;', "o'brien", '*', 'a-b',
])
def test_column_name_validation_rejects_injection(bad):
    with pytest.raises(ValueError):
        validate_column_name(bad, 'dose_col')


@pytest.mark.unit_conversion
def test_missing_column_raises_with_available_listed():
    with pytest.raises(ValueError) as exc:
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units=None, override=True, dose_col='no_such_column',
        )
    msg = str(exc.value)
    assert 'not found' in msg
    # names the caller's column, not the internal one stage 1 trips over later
    assert 'no_such_column' in msg


# ===========================================
# Column names: parking, per-pair output names, collisions
# ===========================================

@pytest.mark.unit_conversion
def test_resolve_output_names_contract(monkeypatch):
    """What the converter settles before it runs: names, parking, in-place, clashes.

    Every comparison is case-insensitive, because DuckDB's are.
    """
    base = ['hospitalization_id', 'med_category', 'med_dose', 'med_dose_unit',
            'volume_infusion_rate', 'volume_infusion_rate_unit']
    dose = {'dose_col': 'med_dose', 'unit_col': 'med_dose_unit'}
    volume = {'dose_col': 'volume_infusion_rate',
              'unit_col': 'volume_infusion_rate_unit'}

    # the default pair: the derived names are today's, and nothing is renamed
    assert resolve_output_names(base, **dose) == OutputNames(
        converted_dose_col='med_dose_converted',
        converted_unit_col='med_dose_unit_converted',
        output_map={}, park={}, in_place=False)
    # any other pair: '<source>_converted', and '__<dose_col>' on every QA column
    names = resolve_output_names(base, **volume)
    assert names.converted_dose_col == 'volume_infusion_rate_converted'
    assert names.converted_unit_col == 'volume_infusion_rate_unit_converted'
    assert names.output_map == {
        'med_dose_converted': 'volume_infusion_rate_converted',
        'med_dose_unit_converted': 'volume_infusion_rate_unit_converted',
        **{c: f'{c}__volume_infusion_rate' for c in PAIR_QA_COLUMNS},
    }
    assert pair_column_name('_convert_status', 'MED_DOSE') == '_convert_status'
    assert (pair_column_name('_convert_status', 'volume_infusion_rate')
            == '_convert_status__volume_infusion_rate')
    # an earlier pass's columns are parked, in any letter case; weight_kg is
    # the pre-attached weight, and its provenance is parked with the rest
    earlier = ['med_dose_converted', 'med_dose_unit_converted', '_Clean_Unit',
               '_unit_class', '_convert_status', *WEIGHT_OUTPUT_COLUMNS]
    assert resolve_output_names(base + earlier, **volume).park == {
        c: f'_saved__{c}' for c in earlier if c != 'weight_kg'}
    # an intermediate is this call's output only under show_intermediate
    assert resolve_output_names(base + ['_base_dose'], **dose).park == {
        '_base_dose': '_saved___base_dose'}
    with pytest.raises(ValueError, match=r"already has column\(s\) \['_base_dose'\]"):
        resolve_output_names(base + ['_base_dose'], **dose, show_intermediate=True)
    # an output name equal to its own source column is an in-place overwrite
    names = resolve_output_names(base, **dose, converted_dose_col='MED_DOSE')
    assert names.in_place
    assert names.output_map == {'med_dose_converted': 'MED_DOSE'}
    assert resolve_output_names(
        base, **volume, converted_unit_col='volume_infusion_rate_unit').in_place

    with pytest.raises(ValueError) as exc:
        resolve_output_names(
            base + ['_Convert_Status', '_saved__med_dose', '_weight_source'],
            **dose, converted_dose_col='_base_dose', converted_unit_col='_BASE_DOSE')
    msg = str(exc.value)
    # every clash in one message: an output name the input already has, the
    # two outputs sharing a name, outputs named like a converter column, the
    # parking prefix, and weight provenance without its weight_kg
    assert msg.startswith('Column-name collision: ')
    assert "already has column(s) ['_Convert_Status']" in msg
    assert "converted_unit_col='_BASE_DOSE' name the same column" in msg
    assert "converted_dose_col='_base_dose' is a column the converter writes" in msg
    assert "['_saved__med_dose'] start with '_saved__'" in msg
    assert "['_weight_source'] but no weight_kg" in msg

    # a pipeline column outside the per-pair and weight sets never reaches an
    # output, so an input column of that name is refused, not parked
    monkeypatch.setattr(
        'clifpy.utils.unit_converter._columns.PIPELINE_OWNED_COLUMNS',
        PIPELINE_OWNED_COLUMNS | {'_hidden_probe'})
    with pytest.raises(ValueError, match=r"\['_Hidden_Probe'\] use a name"):
        resolve_output_names(base + ['_Hidden_Probe'], **dose)
    assert all(c == c.lower() for c in PIPELINE_OWNED_COLUMNS)


def _mac_frame():
    """One MAC row carrying a dose and a volume rate, as CLIF 3.0 rows do.

    7 mcg/min norepinephrine at 70 kg, infusing 0.5 l/hr.
    """
    df = _frame([('norepinephrine', 7.0, 'mcg/min')])
    df = df.with_columns(pl.Series('volume_infusion_rate', [0.5]))
    df = df.with_columns(pl.Series('volume_infusion_rate_unit', ['l/hr']))
    return df


DOSE_PASS = {'preferred_units': {'norepinephrine': 'mcg/kg/min'}}
VOLUME_PASS = {
    'preferred_units': {'norepinephrine': 'ml/hr'},
    'dose_col': 'volume_infusion_rate',
    'unit_col': 'volume_infusion_rate_unit',
}
# What the two passes add to _mac_frame, in either order: each pair's
# converted and QA columns, the volume pair's under its suffix.
BOTH_PASSES_ADD = {
    'med_dose_converted', 'med_dose_unit_converted',
    '_convert_status', '_convert_status_details', '_weight_source', '_weight_age_hours',
    'volume_infusion_rate_converted', 'volume_infusion_rate_unit_converted',
    '_convert_status__volume_infusion_rate',
    '_convert_status_details__volume_infusion_rate',
}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('first, second', [
    (DOSE_PASS, VOLUME_PASS),
    (VOLUME_PASS, DOSE_PASS),
], ids=['dose_then_volume', 'volume_then_dose'])
def test_chained_passes_keep_both_results(first, second):
    """The dose and the volume rate of one frame, converted one after the other.

    Before, the second pass read the first pass's columns as its own. Dose
    then volume reported 'cannot convert mass to volume' and returned the dose
    pass's 0.1 mcg/kg/min under `med_dose_converted_1`; volume then dose
    reported 'cannot convert volume to mass'. Both statuses landed in
    `_convert_status_1`, beside the first pass's 'success'.
    """
    once, _ = convert_dose_units_by_med_category(_mac_frame(), override=True, **first)
    out, _ = convert_dose_units_by_med_category(once, override=True, **second)
    row = out.row(0, named=True)
    # 7 mcg/min over the 70 kg weight_kg both passes read
    assert row['med_dose_converted'] == pytest.approx(7.0 / WEIGHT_KG)
    assert row['med_dose_unit_converted'] == 'mcg/kg/min'
    assert row['_convert_status'] == 'success'
    assert row['_convert_status_details'] == 'success: converted to the target unit'
    # 0.5 l/hr = 500 ml/hr
    assert row['volume_infusion_rate_converted'] == pytest.approx(500.0)
    assert row['volume_infusion_rate_unit_converted'] == 'ml/hr'
    assert row['_convert_status__volume_infusion_rate'] == 'success'
    assert row['_convert_status_details__volume_infusion_rate'] == (
        'success: converted to the target unit')
    # the source columns and the shared weight are untouched
    assert row['med_dose'] == 7.0
    assert row['volume_infusion_rate'] == 0.5
    assert row['weight_kg'] == WEIGHT_KG
    assert set(out.columns) - set(_mac_frame().columns) == BOTH_PASSES_ADD
    assert len(out.columns) == len(set(out.columns))


@pytest.mark.unit_conversion
@pytest.mark.parametrize('pass_kwargs, taken', [
    (DOSE_PASS, ['_convert_status', '_convert_status_details',
                 'med_dose_converted', 'med_dose_unit_converted']),
    (VOLUME_PASS, ['_convert_status__volume_infusion_rate',
                   '_convert_status_details__volume_infusion_rate',
                   'volume_infusion_rate_converted',
                   'volume_infusion_rate_unit_converted']),
], ids=['dose_twice', 'volume_twice'])
def test_same_pair_converted_twice_raises(pass_kwargs, taken):
    """A second pass over the same pair would overwrite the first one's result.

    Before, it suffixed its own columns (`_convert_status_1`,
    `med_dose_converted_1`) and the visible ones kept the first pass's values.
    """
    once, _ = convert_dose_units_by_med_category(
        _mac_frame(), override=True, **pass_kwargs)
    with pytest.raises(ValueError) as exc:
        convert_dose_units_by_med_category(once, override=True, **pass_kwargs)
    msg = str(exc.value)
    assert f"already has column(s) {taken}" in msg
    # the message says how to proceed, and that way works
    assert 'drop or rename these columns' in msg
    again, _ = convert_dose_units_by_med_category(
        once.drop(taken), override=True, **pass_kwargs)
    assert set(again.columns) == set(once.columns)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kwargs, named', [
    ({'converted_dose_col': 'out', 'converted_unit_col': 'out'}, "'out'"),
    ({'converted_dose_col': 'out', 'converted_unit_col': 'OUT'}, "'OUT'"),
    ({'converted_dose_col': 'admin_dttm'}, "'admin_dttm'"),
    ({'converted_dose_col': 'Hospitalization_ID'}, "'hospitalization_id'"),
    ({'converted_unit_col': 'med_dose'}, "'med_dose'"),
    ({'converted_dose_col': '_convert_status'}, "'_convert_status'"),
    ({'converted_dose_col': 'med_dose_unit_converted',
      'converted_unit_col': 'med_dose_converted'}, "'med_dose_unit_converted'"),
], ids=['same_name', 'same_name_any_case', 'other_input', 'other_input_any_case',
        'crossed_pair', 'pipeline_owned', 'swapped_defaults'])
def test_output_name_collisions_raise(kwargs, named):
    """An output name may reuse only its own source column (in-place overwrite).

    Before, DuckDB suffixed the duplicate (`admin_dttm_1`, `out_1`), so the
    requested name held the wrong values.
    """
    with pytest.raises(ValueError) as exc:
        _convert([('propofol', 10.0, 'mg/hr')], {'propofol': 'mcg/min'}, **kwargs)
    assert named in str(exc.value)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', ['polars', 'duckdb'])
def test_in_place_overwrite_of_the_dose_column(return_format):
    """converted_dose_col == dose_col swaps the raw dose for the converted one.

    Before, the raw 10.0 stayed under `med_dose` and the converted dose went to
    `med_dose_1`: the defect the sofa2 CV pivot would inherit.
    """
    out, _ = convert_dose_units_by_med_category(
        _frame([('propofol', 10.0, 'mg/hr')]),
        preferred_units={'propofol': 'mcg/min'}, override=True,
        converted_dose_col='med_dose', return_format=return_format)
    if return_format == 'duckdb':
        out = out.pl()
    # 10 mg/hr = 10,000 mcg / 60 min
    assert out['med_dose'][0] == pytest.approx(10_000 / 60)
    assert out['med_dose_unit_converted'][0] == 'mcg/min'
    # only the dose was overwritten; the source unit is untouched
    assert out['med_dose_unit'][0] == 'mg/hr'
    assert list(out.columns).count('med_dose') == 1
    assert 'med_dose_converted' not in out.columns
    assert not [c for c in out.columns if c.endswith('_1')]


@pytest.mark.unit_conversion
def test_in_place_overwrite_of_a_custom_source_pair():
    """Overwriting volume_infusion_rate in place leaves the parked med_dose pair alone."""
    out, _ = convert_dose_units_by_med_category(
        _mac_frame(), preferred_units={'norepinephrine': 'ml/hr'}, override=True,
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit',
        converted_dose_col='volume_infusion_rate',
        converted_unit_col='volume_infusion_rate_unit')
    row = out.row(0, named=True)
    # 0.5 l/hr = 500 ml/hr, written over the source pair
    assert row['volume_infusion_rate'] == pytest.approx(500.0)
    assert row['volume_infusion_rate_unit'] == 'ml/hr'
    assert row['med_dose'] == 7.0
    assert row['med_dose_unit'] == 'mcg/min'
    # nothing else is added but the pair's own QA columns
    assert set(out.columns) - set(_mac_frame().columns) == {
        '_convert_status__volume_infusion_rate',
        '_convert_status_details__volume_infusion_rate', '_weight_source',
        '_weight_age_hours'}


@pytest.mark.unit_conversion
def test_case_variant_of_an_internal_name_is_parked():
    """DuckDB matches names case-insensitively, so `MED_DOSE` clashes with `med_dose`.

    Before, `MED_DOSE` was not parked: stage 1 read it as the dose being
    converted, and 2 l/hr came out as 100,000 ml/hr (MED_DOSE's 100 x 1000).
    """
    df = _frame([('sodium_chloride', 100.0, 'ml/hr')]).rename({'med_dose': 'MED_DOSE'})
    df = df.with_columns(pl.Series('volume_infusion_rate', [2.0]))
    df = df.with_columns(pl.Series('volume_infusion_rate_unit', ['l/hr']))
    out, _ = convert_dose_units_by_med_category(
        df, preferred_units={'sodium_chloride': 'ml/hr'}, override=True,
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit',
        converted_dose_col='volume_infusion_rate_converted',
        converted_unit_col='volume_infusion_rate_unit_converted')
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(2000.0)
    assert out['volume_infusion_rate'][0] == 2.0
    assert out['MED_DOSE'][0] == 100.0


@pytest.mark.unit_conversion
def test_column_with_an_embedded_quote_round_trips():
    """Caller columns reach the rename projections unvalidated, so quoting must escape.

    Before, `note "a"` broke the projection with a DuckDB ParserException.
    """
    df = _frame([('sodium_chloride', 100.0, 'ml/hr')])
    df = df.with_columns(pl.Series('note "a"', ['keep me']))
    df = df.with_columns(pl.Series('volume_infusion_rate', [2.0]))
    df = df.with_columns(pl.Series('volume_infusion_rate_unit', ['l/hr']))
    out, _ = convert_dose_units_by_med_category(
        df, preferred_units={'sodium_chloride': 'ml/hr'}, override=True,
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit',
        converted_dose_col='volume_infusion_rate_converted',
        converted_unit_col='volume_infusion_rate_unit_converted')
    assert out['note "a"'][0] == 'keep me'
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(2000.0)
    assert quote_identifier('note "a"') == '"note ""a"""'


# ===========================================
# Schema-driven standardization
# ===========================================

@pytest.mark.unit_conversion
def test_load_dose_unit_targets_csv(tmp_path):
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\npitocin,milli-units/min\n')
    assert load_dose_unit_targets(p) == {
        'norepinephrine': 'mcg/kg/min', 'pitocin': 'milli-units/min'}


@pytest.mark.unit_conversion
def test_load_dose_unit_targets_drops_blank_and_na(tmp_path):
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\na,mg\nb,NA\nc,\nd,mcg\n')
    assert load_dose_unit_targets(p) == {'a': 'mg', 'd': 'mcg'}


@pytest.mark.unit_conversion
def test_multiple_targets_per_category_keep_first_and_warn(tmp_path, captured_warnings):
    """CLIF 3.0 keys targets on (med_category, med_group), clifpy on category.

    epoprostenol is ng/kg/min as an IV infusion and mcg/kg/min inhaled -- a
    factor of 1000 -- and terbutaline is mg inhaled and mcg/kg/min otherwise.
    Both schema rows are correct; the flat `preferred_units` mapping is what
    cannot hold them.

    Until composite keys are supported, the collapse must be deterministic for
    a given file -- the first row in file order wins, so a reordered file
    flips it (see test_conflicting_duplicates_follow_file_order) -- and must
    be announced, pointing at med_group so the reader knows how to resolve it.
    See docs/user-guide/med-unit-conversion.md.
    """
    p = tmp_path / 'targets.csv'
    p.write_text(
        'med_category,med_dose_unit,med_group\n'
        'epoprostenol,ng/kg/min,pulmonary_vasodilators_iv\n'
        'epoprostenol,mcg/kg/min,pulmonary_vasodilators_inhaled\n'
        'terbutaline,mg,inhaled\n'
        'terbutaline,mcg/kg/min,others\n'
    )
    targets = load_dose_unit_targets(p)

    assert targets == {'epoprostenol': 'ng/kg/min', 'terbutaline': 'mg'}
    text = '\n'.join(captured_warnings)
    # the warning must name both candidate units and point at med_group,
    # not merely report a duplicate
    assert 'med_group' in text
    assert 'epoprostenol' in text and 'ng/kg/min' in text and 'mcg/kg/min' in text
    assert 'terbutaline' in text


@pytest.mark.unit_conversion
def test_overriding_a_route_specific_target(tmp_path):
    """The documented workaround: override the category for your cohort's route."""
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\n'
                 'epoprostenol,ng/kg/min\nepoprostenol,mcg/kg/min\n')
    targets = load_dose_unit_targets(p)
    targets['epoprostenol'] = 'mcg/kg/min'          # inhaled cohort

    out, _ = standardize_med_dose_units(
        _frame([('epoprostenol', 1000.0, 'ng/kg/min')]), targets)
    # 1000 ng/kg/min == 1 mcg/kg/min
    assert out['med_dose_converted'][0] == pytest.approx(1.0)
    assert out['med_dose_unit_converted'][0] == 'mcg/kg/min'


@pytest.mark.unit_conversion
def test_register_target_reader(tmp_path, monkeypatch):
    """A new schema format is a small function plus one registration.

    The placeholder `setitem` makes monkeypatch delete '.myfmt' again after
    the test, so the registration no longer leaks into the rest of the
    session, while the call itself still goes through the public function
    and its suffix normalisation.
    """
    monkeypatch.setitem(_targets._TARGET_READERS, '.myfmt', None)
    p = tmp_path / 'targets.myfmt'
    p.write_text('norepinephrine=mcg/kg/min\n')

    def _reader(source):
        rows = [ln.split('=', 1) for ln in Path(source).read_text().splitlines() if ln.strip()]
        return pl.DataFrame(rows, schema=['med_category', 'med_dose_unit'], orient='row')

    register_target_reader('MYFMT', _reader)          # no dot, upper case
    assert _targets._TARGET_READERS['.myfmt'] is _reader
    assert load_dose_unit_targets(p) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_target_reader_may_return_pandas(tmp_path):
    """A reader written for the old contract still works: its pandas frame is
    read through the input boundary (D-261022-pandas-input)."""
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\nheparin,NA\n')

    def _reader(source):
        return pd.read_csv(source, dtype=str, keep_default_na=False)

    assert load_dose_unit_targets(p, reader=_reader) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_unknown_schema_format_raises(tmp_path):
    with pytest.raises(ValueError) as exc:
        load_dose_unit_targets(tmp_path / 'targets.unknownext')
    assert 'No dose-unit target reader' in str(exc.value)


@pytest.mark.unit_conversion
def test_load_dose_unit_targets_names_missing_schema_columns():
    """A schema without the requested unit column fails loudly and names it."""
    def _reader(source):
        return pl.DataFrame({'med_category': ['norepinephrine'],
                             'volume_infusion_rate_units': ['ml/hr']})

    with pytest.raises(ValueError, match=r"targets file is missing column\(s\) \['med_dose_unit'\]"):
        load_dose_unit_targets('targets.csv', reader=_reader)


@pytest.mark.unit_conversion
def test_standardize_med_dose_units_end_to_end(tmp_path, captured_warnings):
    """Targets file in, standardized table out -- including categories it omits.

    A category the targets omit is kept as charted (dose copied, unit spelling
    cleaned), `_convert_status_details` says so on every row, and one warning
    names the category.
    """
    schema = tmp_path / 'targets.csv'
    schema.write_text('med_category,med_dose_unit\n'
                      'norepinephrine,mcg/kg/min\npitocin,milli-units/min\n')
    med = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('pitocin', 1.0, 'units/min'),
        ('not_in_schema', 7.0, 'mg'),
    ])
    out, counts = standardize_med_dose_units(med, schema)
    out = _by_category(out)

    assert out['norepinephrine']['med_dose_converted'] == pytest.approx(5.0 / WEIGHT_KG)
    assert out['pitocin']['med_dose_converted'] == pytest.approx(1000.0)
    assert out['pitocin']['med_dose_unit_converted'] == 'milli-units/min'
    # a category the targets omit is kept as charted: 'success', the dose
    # copied, and labelled as unlisted
    assert out['not_in_schema']['_convert_status'] == 'success'
    assert out['not_in_schema']['med_dose_unit_converted'] == 'mg'
    assert out['not_in_schema']['med_dose_converted'] == 7.0
    details = {category: row['_convert_status_details'] for category, row in out.items()}
    assert details == {
        'norepinephrine': 'success: converted to the target unit',
        'pitocin': 'success: converted to the target unit',
        'not_in_schema': 'success: no target listed; unit cleaned only'}
    assert dict(zip(counts['med_category'].to_list(),
                    counts['_convert_status_details'].to_list())) == details
    assert any("'not_in_schema' (1 row)" in m for m in captured_warnings)
    # unlisted='base' restores the converter's base-unit fallback (mg -> mcg)
    base, _ = standardize_med_dose_units(med, schema, unlisted='base')
    base = _by_category(base)['not_in_schema']
    assert (base['med_dose_converted'], base['med_dose_unit_converted']) == (
        pytest.approx(7000.0), 'mcg')
    assert base['_convert_status_details'] == (
        'success: no target listed; converted to the base unit')


@pytest.mark.unit_conversion
def test_standardize_accepts_a_dict_schema():
    med = _frame([('norepinephrine', 5.0, 'mcg/min')])
    out, _ = standardize_med_dose_units(med, {'norepinephrine': 'mcg/kg/min'})
    assert out['med_dose_converted'][0] == pytest.approx(5.0 / WEIGHT_KG)


@pytest.mark.unit_conversion
def test_standardize_rejects_an_empty_schema(tmp_path):
    schema = tmp_path / 'targets.csv'
    schema.write_text('med_category,med_dose_unit\n')
    with pytest.raises(ValueError) as exc:
        standardize_med_dose_units(_frame([('a', 1.0, 'mg')]), schema)
    assert 'no usable targets' in str(exc.value)


# ===========================================
# Return formats (D-261022-return-format)
# ===========================================

def _to_polars(result):
    """A result in any return format, as a polars DataFrame."""
    if isinstance(result, duckdb.DuckDBPyRelation):
        return result.pl()
    if isinstance(result, pl.LazyFrame):
        return result.collect()
    if isinstance(result, pd.DataFrame):
        return pl.from_pandas(result)
    return result


def _call(entry_point):
    """One call per public function on the same one-row input; kwargs pass through.

    Each call returns a tuple: (converted, counts), or (attached,) for attach_weight.
    """
    med = _frame([('drug', 1.0, 'mg')])
    vitals = pl.DataFrame({
        'hospitalization_id': ['H0'], 'recorded_dttm': [TS],
        'vital_category': ['weight_kg'], 'vital_value': [WEIGHT_KG],
    })
    return {
        'convert': lambda **kw: convert_dose_units_by_med_category(
            med, preferred_units={'drug': 'mcg'}, override=True, **kw),
        'standardize': lambda **kw: standardize_med_dose_units(
            med, {'drug': 'mcg'}, **kw),
        'base_units': lambda **kw: standardize_dose_to_base_units(med, **kw),
        'attach_weight': lambda **kw: (
            attach_weight(med.drop('weight_kg'), vitals, **kw),),
    }[entry_point]


ENTRY_POINTS = ['convert', 'standardize', 'base_units', 'attach_weight']
FRAME_TYPES = {
    'polars': pl.DataFrame,
    'polars_lazy': pl.LazyFrame,
    'duckdb': duckdb.DuckDBPyRelation,
}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
def test_default_output_is_polars(entry_point):
    """load_data returns polars by default, and so does every converter function."""
    results = _call(entry_point)()
    assert all(isinstance(r, pl.DataFrame) for r in results)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', sorted(FRAME_TYPES))
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
def test_return_format_types_and_values(entry_point, return_format, los_angeles_default_tz):
    """Each format is its own type and holds the default's values.

    The session zone is one no input uses, so a timestamp DuckDB labels in the
    session zone (attach_weight's _weight_recorded_dttm) shows up as a dtype
    mismatch in every run order, not only when the test runs alone.
    """
    call = _call(entry_point)
    want = call()
    got = call(return_format=return_format)
    assert all(isinstance(r, FRAME_TYPES[return_format]) for r in got)
    for g, w in zip(got, want):
        assert_frame_equal(_to_polars(g), w)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
def test_pandas_output_is_deprecated_but_served(entry_point):
    """return_format='pandas' works for one more release, with a DeprecationWarning."""
    call = _call(entry_point)
    want = call()
    with pytest.warns(DeprecationWarning, match="return_format='pandas' is deprecated"):
        got = call(return_format='pandas')
    for g, w in zip(got, want):
        assert isinstance(g, pd.DataFrame)
        assert_frame_equal(pl.from_pandas(g), w)


@pytest.mark.unit_conversion
def test_return_rel_is_a_deprecated_alias_for_duckdb():
    med = _frame([('drug', 1.0, 'mg')])
    kwargs = dict(preferred_units={'drug': 'mcg'}, override=True)
    with pytest.warns(DeprecationWarning, match='return_rel=True is deprecated'):
        out, counts = convert_dose_units_by_med_category(med, return_rel=True, **kwargs)
    assert isinstance(out, duckdb.DuckDBPyRelation)
    assert isinstance(counts, duckdb.DuckDBPyRelation)
    # next to an agreeing return_format it still warns; the new spelling alone is silent
    with pytest.warns(DeprecationWarning, match='return_rel=True is deprecated'):
        convert_dose_units_by_med_category(
            med, return_rel=True, return_format='duckdb', **kwargs)
    with warnings.catch_warnings():
        warnings.simplefilter('error', DeprecationWarning)
        convert_dose_units_by_med_category(med, return_format='duckdb', **kwargs)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', ['polars_lazy', 'pandas'])
def test_return_rel_with_a_conflicting_format_raises(return_format):
    with pytest.raises(ValueError, match='return_rel=True conflicts'):
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]), preferred_units={'drug': 'mcg'},
            override=True, return_rel=True, return_format=return_format)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
def test_unknown_return_format_raises(entry_point):
    with pytest.raises(ValueError, match='Unknown return_format'):
        _call(entry_point)(return_format='arrow')


@pytest.mark.unit_conversion
def test_deprecation_warnings_point_at_the_caller():
    """standardize_med_dose_units forwards to the converter; the warning must
    still name the caller's line, not clifpy's."""
    med = _frame([('drug', 1.0, 'mg')])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        standardize_med_dose_units(med, {'drug': 'mcg'}, return_format='pandas')
        standardize_med_dose_units(med, {'drug': 'mcg'}, return_rel=True)
    ours = [w for w in caught
            if issubclass(w.category, DeprecationWarning) and 'return_' in str(w.message)]
    assert len(ours) == 2
    assert {w.filename for w in ours} == {__file__}


# ===========================================
# Input materialization: isolation and row order
# ===========================================

def _med_unit_objects():
    """Names of every `_med_unit_*` table or view on the default connection."""
    return {
        name for (name,) in duckdb.sql("""
            FROM duckdb_tables() SELECT name: table_name
            UNION ALL
            FROM duckdb_views() SELECT view_name WHERE NOT internal
        """).fetchall()
        if name.startswith('_med_unit_')
    }


def _converted_pairs(rel):
    """Sorted (med_category, med_dose_converted) rows of a converted relation."""
    return sorted(rel.project('med_category, med_dose_converted').fetchall())


def _columns_of(result):
    """Column names of a result in any return format."""
    if isinstance(result, pl.LazyFrame):
        return result.collect_schema().names()
    return list(result.columns)


@pytest.mark.unit_conversion
def test_register_input_gives_each_call_its_own_table():
    df = _frame([('a', 1.0, 'mg'), ('b', 2.0, 'mg'), ('c', 3.0, 'mg')])
    first = _register_input(_capture_input(df).rel)
    second = _register_input(_capture_input(df).rel)
    try:
        assert first != second
        assert first.startswith('_med_unit_input_')
        assert second.startswith('_med_unit_input_')
        rows = (duckdb.table(first)
                .order(INPUT_ORDINAL_COL)
                .project(f'med_category, {INPUT_ORDINAL_COL}')
                .fetchall())
        assert rows == [('a', 0), ('b', 1), ('c', 2)]
    finally:
        _drop_temp_table(first)
        _drop_temp_table(second)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('where', ['input_column', 'output_name'])
def test_input_ordinal_column_name_is_reserved(where):
    """`_input_row_ord` is refused as an input column and as an output name.

    The converter adds a hidden column of that name and drops it before
    returning. resolve_output_names refuses both uses: the name is in
    PIPELINE_OWNED_COLUMNS but in neither PAIR_QA_COLUMNS nor
    WEIGHT_OUTPUT_COLUMNS, so it is hidden.
    """
    df = _frame([('a', 1.0, 'mg')])
    kwargs = {}
    if where == 'input_column':
        df = df.with_columns(pl.lit(7).alias(INPUT_ORDINAL_COL))
    else:
        kwargs['converted_dose_col'] = INPUT_ORDINAL_COL
    with pytest.raises(ValueError, match=INPUT_ORDINAL_COL):
        convert_dose_units_by_med_category(
            df, preferred_units=None, override=True, **kwargs)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', ['polars', 'polars_lazy', 'duckdb', 'pandas'])
@pytest.mark.parametrize('show_intermediate', [False, True])
def test_ordinal_never_reaches_the_output(return_format, show_intermediate):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)   # 'pandas' is deprecated
        out, counts = convert_dose_units_by_med_category(
            _frame([('a', 1.0, 'mg')]), preferred_units={'a': 'mcg'}, override=True,
            return_format=return_format, show_intermediate=show_intermediate)
    assert INPUT_ORDINAL_COL not in _columns_of(out)
    assert INPUT_ORDINAL_COL not in _columns_of(counts)


@pytest.mark.unit_conversion
def test_two_duckdb_results_held_at_once():
    """A later call must not overwrite what an earlier relation reads."""
    first, first_counts = convert_dose_units_by_med_category(
        _frame([('first', 1.0, 'mg')]),
        preferred_units={'first': 'mcg'}, override=True, return_format='duckdb')
    second, _ = convert_dose_units_by_med_category(
        _frame([('second', 2.0, 'mg')]),
        preferred_units={'second': 'mcg'}, override=True, return_format='duckdb')

    assert _converted_pairs(first) == [('first', 1000.0)]
    assert _converted_pairs(second) == [('second', 2000.0)]
    assert first_counts.project('med_category').fetchall() == [('first',)]


@pytest.mark.unit_conversion
def test_eager_call_keeps_an_outstanding_relation_readable():
    """An eager call's cleanup must not drop what a held relation reads."""
    held, _ = convert_dose_units_by_med_category(
        _frame([('held', 1.0, 'mg')]),
        preferred_units={'held': 'mcg'}, override=True, return_format='duckdb')
    convert_dose_units_by_med_category(
        _frame([('eager', 2.0, 'mg')]),
        preferred_units={'eager': 'mcg'}, override=True)
    assert _converted_pairs(held) == [('held', 1000.0)]


@pytest.mark.unit_conversion
def test_cleanup_spares_tables_registered_by_other_code():
    """`finally` drops this call's tables only, never the shared registry."""
    duckdb.execute("CREATE OR REPLACE TEMP TABLE _other_module_table AS SELECT 1 AS x")
    _register_temp_table('_other_module_table')
    try:
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units={'drug': 'mcg'}, override=True)
        assert duckdb.table('_other_module_table').fetchall() == [(1,)]
    finally:
        _drop_temp_table('_other_module_table')


@pytest.mark.unit_conversion
def test_no_med_unit_tables_outlive_either_format():
    """'duckdb' never creates a table; an eager call drops its own."""
    duckdb.execute("DROP TABLE IF EXISTS _med_unit_input")   # the old fixed name
    before = _med_unit_objects()
    held, _ = convert_dose_units_by_med_category(
        _frame([('drug', 1.0, 'mg')]),
        preferred_units={'drug': 'mcg'}, override=True, return_format='duckdb')
    assert _med_unit_objects() == before
    convert_dose_units_by_med_category(
        _frame([('drug', 1.0, 'mg')]),
        preferred_units={'drug': 'mcg'}, override=True)
    assert _med_unit_objects() == before
    assert _converted_pairs(held) == [('drug', 1000.0)]


def _order_case():
    """Four rows; the weight lookup used to pull the two drip rows ahead of the boluses."""
    med = pl.DataFrame({
        'hospitalization_id': ['H3', 'H2', 'H1', 'H0'],
        'med_category': ['bolus', 'drip', 'bolus', 'drip'],
        'med_dose': [1.0, 2.0, 3.0, 4.0],
        'med_dose_unit': ['mg', 'mcg/kg/min', 'mg', 'mcg/kg/min'],
        'admin_dttm': [TS] * 4,
    })
    vitals = pl.DataFrame({
        'hospitalization_id': ['H2', 'H0'],
        'recorded_dttm': [TS - timedelta(hours=1)] * 2,
        'vital_category': ['weight_kg', 'weight_kg'],
        'vital_value': [50.0, 80.0],
    })
    return med, vitals


ORDER_PREFS = {'bolus': 'mcg', 'drip': 'mcg/min'}
# mg -> mcg is x1000; mcg/kg/min -> mcg/min multiplies by the weight
ORDER_CONVERTED = [1000.0, 100.0, 3000.0, 320.0]


@pytest.mark.unit_conversion
def test_output_keeps_input_order():
    """Rows come back in input order (the weight lookup used to reorder them)."""
    med, vitals = _order_case()
    out, _ = convert_dose_units_by_med_category(
        med, vitals_df=vitals, preferred_units=ORDER_PREFS, override=True)
    assert out['med_dose'].to_list() == [1.0, 2.0, 3.0, 4.0]
    assert out['med_dose_converted'].to_list() == pytest.approx(ORDER_CONVERTED)


def _order_input(kind, med):
    """`med` as a polars frame, a pandas frame, or a relation on either kind of connection."""
    if kind == 'polars':
        return med
    if kind == 'pandas':
        return med.to_pandas()
    if kind == 'default_relation':
        return duckdb.from_arrow(med.to_arrow())
    return new_duckdb_con().from_arrow(med.to_arrow())


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', ['converter', 'standardize'])
@pytest.mark.parametrize('return_format', ['polars', 'pandas'])
@pytest.mark.parametrize('input_kind', ['polars', 'pandas', 'default_relation', 'new_con_relation'])
def test_output_keeps_input_order_for_every_input_kind(input_kind, return_format, entry_point):
    """Input order holds whatever the input kind; the default-connection relation is
    ordered by a row_number() ordinal, the other kinds by their Arrow position."""
    med, vitals = _order_case()
    med_in = _order_input(input_kind, med)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)   # 'pandas' is deprecated
        if entry_point == 'converter':
            out, _ = convert_dose_units_by_med_category(
                med_in, vitals_df=vitals, preferred_units=ORDER_PREFS,
                override=True, return_format=return_format)
        else:
            out, _ = standardize_med_dose_units(
                med_in, ORDER_PREFS, vitals_df=vitals, override=True,
                return_format=return_format)
    out = _to_polars(out)
    assert out['med_dose'].to_list() == [1.0, 2.0, 3.0, 4.0]
    assert out['med_dose_converted'].to_list() == pytest.approx(ORDER_CONVERTED)


@pytest.mark.unit_conversion
def test_deprecated_pandas_output_keeps_input_order_and_index():
    """pandas in, pandas out (deprecated): input order, under the caller's index."""
    med, vitals = _order_case()
    med = med.to_pandas().set_index(pd.Index(['r40', 'r30', 'r20', 'r10'], name='mar_row'))
    with pytest.warns(DeprecationWarning, match="return_format='pandas'"):
        out, counts = convert_dose_units_by_med_category(
            med, vitals_df=vitals.to_pandas(), preferred_units=ORDER_PREFS,
            override=True, return_format='pandas')
    assert isinstance(out, pd.DataFrame) and isinstance(counts, pd.DataFrame)
    assert out.index.equals(med.index)
    assert out['med_dose'].tolist() == [1.0, 2.0, 3.0, 4.0]
    assert out['med_dose_converted'].tolist() == pytest.approx(ORDER_CONVERTED)


@pytest.mark.unit_conversion
def test_output_order_survives_parallel_execution():
    """Large inputs used to come back shuffled even without a weight lookup."""
    n = 60_000
    med = pl.DataFrame({
        'hospitalization_id': [f'H{i}' for i in range(n)],
        'med_category': [('a', 'b', 'c')[i % 3] for i in range(n)],
        'med_dose': [float(i) for i in range(n)],
        'med_dose_unit': ['mg'] * n,
        'weight_kg': [WEIGHT_KG] * n,
        'admin_dttm': [TS] * n,
    })
    out, _ = convert_dose_units_by_med_category(
        med, preferred_units={'a': 'mcg', 'b': 'mg'}, override=True)
    assert out['med_dose'].to_list() == med['med_dose'].to_list()


# ===========================================
# Input kinds: polars (eager, lazy), pandas through Arrow, relations
# ===========================================

PREFS = {'bolus': 'mcg', 'drip': 'mcg/min'}


@pytest.fixture
def los_angeles_default_tz():
    """Pin the default connection to a zone no input uses; restore it after."""
    previous = duckdb.sql("SELECT current_setting('TimeZone')").fetchone()[0]
    duckdb.execute("SET TimeZone = 'America/Los_Angeles'")
    try:
        yield
    finally:
        duckdb.execute(f"SET TimeZone = '{previous}'")


def _weighted_med_and_vitals():
    """Two drugs, one needing a weight lookup; timestamps in US/Central."""
    med = pl.DataFrame({
        'hospitalization_id': ['H1', 'H0', 'H1'],
        'med_category': ['bolus', 'drip', 'drip'],
        'med_dose': [1.0, 2.0, 3.0],
        'med_dose_unit': ['mg', 'mcg/kg/min', 'mcg/kg/min'],
        'admin_dttm': [TS] * 3,
    }).with_columns(pl.col('admin_dttm').dt.convert_time_zone('US/Central'))
    vitals = pl.DataFrame({
        'hospitalization_id': ['H0', 'H1'],
        'recorded_dttm': [TS - timedelta(hours=1)] * 2,
        'vital_category': ['weight_kg', 'weight_kg'],
        'vital_value': [50.0, 80.0],
    }).with_columns(pl.col('recorded_dttm').dt.convert_time_zone('US/Central'))
    return med, vitals


AS_INPUT = {
    'polars_lazy': lambda df: df.lazy(),
    'pandas': lambda df: df.to_pandas(),
}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kind', sorted(AS_INPUT))
@pytest.mark.parametrize('entry_point', ['convert', 'standardize'])
def test_every_input_kind_converts_alike(entry_point, kind, los_angeles_default_tz):
    """A LazyFrame, and pandas through the Arrow boundary, convert like polars:
    same values, order and zones (US/Central) under a default connection
    pinned elsewhere, and no PerformanceWarning from a LazyFrame."""
    med, vitals = _weighted_med_and_vitals()
    run = {
        'convert': lambda m, v: convert_dose_units_by_med_category(
            m, vitals_df=v, preferred_units=PREFS, override=True),
        'standardize': lambda m, v: standardize_med_dose_units(
            m, targets=PREFS, vitals_df=v),
    }[entry_point]
    want, want_counts = run(med, vitals)
    to_kind = AS_INPUT[kind]
    with warnings.catch_warnings():
        warnings.simplefilter('error', pl.exceptions.PerformanceWarning)
        got, got_counts = run(to_kind(med), to_kind(vitals))
    assert got.schema['admin_dttm'].time_zone == 'US/Central'
    assert_frame_equal(got, want)
    assert_frame_equal(got_counts, want_counts)


@pytest.mark.unit_conversion
def test_show_intermediate_labels_the_weight_time_in_the_dose_zone(los_angeles_default_tz):
    """_weight_recorded_dttm comes from vitals through DuckDB, which labels it
    in the session zone; it is labelled like admin_dttm instead, so one result
    holds one zone."""
    med, vitals = _weighted_med_and_vitals()
    out, _ = convert_dose_units_by_med_category(
        med, vitals_df=vitals, preferred_units=PREFS, override=True,
        show_intermediate=True)
    assert out.schema['admin_dttm'].time_zone == 'US/Central'
    assert out.schema['_weight_recorded_dttm'].time_zone == 'US/Central'


@pytest.mark.unit_conversion
def test_pandas_mixed_type_object_columns_are_read_as_text(captured_warnings):
    """Raw EHR exports mix str and int in one object column, which Arrow refuses.

    The boundary reads those columns with DuckDB's own pandas scanner, as text,
    as clifpy did before the Arrow boundary; the result equals the same frame
    with those columns cast to str, and a warning names them.
    """
    clean = _frame([('drug', 1.0, 'mg'), ('drug', 2.0, '5'), ('drug', 3.0, None)]).with_columns(
        pl.Series('hospitalization_id', ['H0', '1', 'H2']))
    messy = clean.to_pandas()
    messy['hospitalization_id'] = ['H0', 1, 'H2']
    messy['med_dose_unit'] = ['mg', 5, None]

    want, want_counts = convert_dose_units_by_med_category(
        clean, preferred_units={'drug': 'mcg'}, override=True)
    got, got_counts = convert_dose_units_by_med_category(
        messy, preferred_units={'drug': 'mcg'}, override=True)
    assert_frame_equal(got, want)

    def by_unit(frame):
        # the counts rows tie on (med_category, count), so their order is free
        return frame.sort('med_dose_unit', nulls_last=True)

    assert_frame_equal(by_unit(got_counts), by_unit(want_counts))
    assert any('hospitalization_id' in m and 'med_dose_unit' in m for m in captured_warnings)


@pytest.mark.unit_conversion
def test_relations_in_duckdb_out_like_sofa2_cv():
    """sofa2/_cv.py's call shape: default-connection relations in (the med one
    deduplicated with QUALIFY), return_format='duckdb', then CREATE TEMP TABLE
    ... AS SELECT * FROM the returned relation, read by its variable name."""
    med, vitals = _weighted_med_and_vitals()
    want, _ = convert_dose_units_by_med_category(
        med, vitals_df=vitals, preferred_units=PREFS, override=True)
    doubled = pl.concat([med, med]).to_arrow()   # every MAR row twice; read by name below
    weights = vitals.to_arrow()                  # read by name below
    duckdb.execute("CREATE OR REPLACE TEMP TABLE _cv_like_raw AS FROM doubled")
    try:
        deduped = duckdb.sql("""
            FROM _cv_like_raw t
            SELECT t.*
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY t.hospitalization_id, t.med_category, t.med_dose
                ORDER BY t.admin_dttm
            ) = 1
        """)
        cohort_weights = duckdb.sql(
            "FROM weights SELECT * WHERE vital_category = 'weight_kg'")
        pressor_events_rel, _ = convert_dose_units_by_med_category(
            med_df=deduped, vitals_df=cohort_weights, preferred_units=PREFS,
            override=True, return_format='duckdb')
        duckdb.execute(
            "CREATE OR REPLACE TEMP TABLE _cv_like AS SELECT * FROM pressor_events_rel")
        got = duckdb.table('_cv_like')
        assert INPUT_ORDINAL_COL not in got.columns
        assert got.columns == want.columns

        def in_utc(frame):
            return (frame.with_columns(pl.col('admin_dttm').dt.convert_time_zone('UTC'))
                    .sort('hospitalization_id', 'med_category'))

        assert_frame_equal(in_utc(got.pl()), in_utc(want))
    finally:
        duckdb.execute("DROP TABLE IF EXISTS _cv_like")
        duckdb.execute("DROP TABLE IF EXISTS _cv_like_raw")


# ===========================================
# Input materialization: catalog shadowing
# ===========================================

_DECOY_SQL = (
    "SELECT 'H9' AS hospitalization_id, 'decoy' AS med_category, 9.0 AS med_dose,"
    " 'mg' AS med_dose_unit, 70.0 AS weight_kg,"
    " TIMESTAMPTZ '2024-01-01 00:00:00+00' AS admin_dttm"
)


# Catalog-object kinds a decoy can take; each needs its own teardown statement.
_SHADOW_KINDS = ('table', 'temp_table', 'view', 'temp_view', 'registered')

# A vital-signs decoy (the `vitals_df` shape) charting a weight no real input has.
_DECOY_VITALS_SQL = (
    "SELECT 'H0' AS hospitalization_id,"
    " TIMESTAMP '2023-12-31 00:00:00' AS recorded_dttm,"
    " 'weight_kg' AS vital_category, 999.0 AS vital_value"
)


@pytest.fixture
def shadow_table():
    """Create decoy catalog objects (the input's schema) on the default connection.

    `make(name, schema='main', kind='table', sql=None)`; `kind` is one of
    `_SHADOW_KINDS`. Teardown is kind-aware (`DROP VIEW` for views,
    `duckdb.unregister` for a registered frame, `DROP TABLE` for tables), runs
    on failure too, and drops every schema other than `main` it created.
    """
    made, schemas = [], []

    def make(name, schema='main', kind='table', sql=None):
        assert kind in _SHADOW_KINDS, kind
        sql = sql or _DECOY_SQL
        if schema != 'main':
            duckdb.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            schemas.append(schema)
        if kind == 'registered':
            made.append((kind, name))
            duckdb.register(name, duckdb.sql(sql).pl())
            return
        create = {'table': 'TABLE', 'temp_table': 'TEMP TABLE',
                  'view': 'VIEW', 'temp_view': 'TEMP VIEW'}[kind]
        qualified = name if kind.startswith('temp') else f"{schema}.{name}"
        made.append((kind, qualified))
        duckdb.execute(f"CREATE {create} {qualified} AS {sql}")

    yield make
    for kind, qualified in made:
        if kind == 'registered':
            duckdb.unregister(qualified)
        elif kind.endswith('view'):
            duckdb.execute(f"DROP VIEW IF EXISTS {qualified}")
        else:
            duckdb.execute(f"DROP TABLE IF EXISTS {qualified}")
    for schema in schemas:
        duckdb.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.mark.unit_conversion
@pytest.mark.parametrize('name', sorted(REPLACEMENT_SCAN_NAMES))
def test_shadowing_table_on_default_connection_is_refused(name, shadow_table):
    """DuckDB reads a catalog table before a Python variable of the same name;
    a decoy used to be converted in place of the caller's rows as 'success'."""
    shadow_table(name)
    with pytest.raises(ValueError, match=name):
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units={'drug': 'mcg'}, override=True)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
def test_every_entry_point_checks_for_shadowing_tables(entry_point, shadow_table):
    shadow_table('med_df')
    with pytest.raises(ValueError, match='med_df'):
        _call(entry_point)()


@pytest.mark.unit_conversion
def test_shadow_check_is_case_insensitive(shadow_table):
    shadow_table('MED_DF')
    with pytest.raises(ValueError, match='MED_DF'):
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units={'drug': 'mcg'}, override=True)


@pytest.mark.unit_conversion
def test_shadow_check_ignores_schemas_outside_the_search_path(shadow_table):
    """`uc_elsewhere.med_df` is not what an unqualified `med_df` resolves to."""
    shadow_table('med_df', schema='uc_elsewhere')
    out, _ = convert_dose_units_by_med_category(
        _frame([('drug', 1.0, 'mg')]),
        preferred_units={'drug': 'mcg'}, override=True)
    assert out['med_category'].to_list() == ['drug']
    assert out['med_dose_converted'].to_list() == [1000.0]


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kind', _SHADOW_KINDS)
def test_shadow_check_refuses_every_catalog_object_kind(kind, shadow_table):
    """A view, a TEMP table or view (the temp database) and a registered frame
    shadow a Python variable just as a main-schema table does."""
    shadow_table('med_df', kind=kind)
    with pytest.raises(ValueError, match='med_df'):
        convert_dose_units_by_med_category(
            _frame([('drug', 1.0, 'mg')]),
            preferred_units={'drug': 'mcg'}, override=True)


def _weighted_case():
    """Med rows with no weight column, and the vitals that would supply one."""
    med_df = pl.DataFrame({
        'rn': [0],
        'hospitalization_id': ['H0'],
        'admin_dttm': [datetime(2024, 1, 1, 9, 0)],
        'med_category': ['propofol'],
        'med_dose': [10.0],
        'med_dose_unit': ['mg'],
    })
    vitals_df = pl.DataFrame(
        {'hospitalization_id': ['H0'], 'recorded_dttm': [datetime(2024, 1, 1, 8, 0)],
         'vital_category': ['weight_kg'], 'vital_value': [72.0]},
        schema={'hospitalization_id': pl.String, 'recorded_dttm': pl.Datetime('us'),
                'vital_category': pl.String, 'vital_value': pl.Float64})
    return med_df, vitals_df


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kind', _SHADOW_KINDS)
def test_vitals_df_shadow_is_refused_not_silently_read(kind, shadow_table):
    """The silent path: a catalog `vitals_df` holding a different weight would
    be read in place of the caller's vitals, and the dose scaled by 999 kg."""
    shadow_table('vitals_df', kind=kind, sql=_DECOY_VITALS_SQL)
    med_df, vitals_df = _weighted_case()
    with pytest.raises(ValueError, match='vitals_df'):
        convert_dose_units_by_med_category(
            med_df, vitals_df=vitals_df,
            preferred_units={'propofol': 'mcg/kg'}, override=True)


@pytest.mark.unit_conversion
def test_vitals_df_case_without_a_shadow_uses_the_callers_weight():
    """Control for the test above: with no decoy the same call converts at 72 kg."""
    med_df, vitals_df = _weighted_case()
    out, _ = convert_dose_units_by_med_category(
        med_df, vitals_df=vitals_df,
        preferred_units={'propofol': 'mcg/kg'}, override=True)
    assert out['med_dose_converted'][0] == pytest.approx(10_000 / 72.0)


@pytest.mark.unit_conversion
def test_converter_cleanup_leaves_other_input_tables_alone(shadow_table):
    """The converter's `finally` drops only the temp table it created: a
    same-prefix table another call (or a user) holds must survive."""
    shadow_table('_med_unit_input_other', kind='temp_table')
    out, _ = convert_dose_units_by_med_category(
        _frame([('drug', 1.0, 'mg')]),
        preferred_units={'drug': 'mcg'}, override=True)
    assert out['med_dose_converted'].to_list() == [1000.0]
    assert duckdb.sql(
        "SELECT count(*) FROM _med_unit_input_other").fetchone() == (1,)
    leaked = duckdb.sql("""
        SELECT table_name FROM duckdb_tables()
        WHERE temporary AND table_name LIKE '\\_med\\_unit\\_input\\_%' ESCAPE '\\'
          AND table_name <> '_med_unit_input_other'
    """).fetchall()
    assert leaked == []


def _package_sources():
    """(file name, source text) of every module in the converter package."""
    pkg = Path(convert_dose_units_by_med_category.__code__.co_filename).parent
    return [(p.name, p.read_text()) for p in sorted(pkg.glob('*.py'))]


@pytest.mark.unit_conversion
def test_replacement_scan_names_cover_every_sql_source():
    """Every Python variable the package's SQL reads by name is guarded.

    A FROM / JOIN target that is neither a CTE nor a table function is read
    through DuckDB's replacement scan. Doctest and comment lines are skipped.
    """
    referenced, ctes = set(), set()
    for _, text in _package_sources():
        code = '\n'.join(
            line for line in text.splitlines()
            if not line.lstrip().startswith(('>>>', '...', '#'))
        )
        referenced |= set(re.findall(
            r'\b(?:FROM|JOIN)\s+([A-Za-z_]\w*)\b(?!\s*\()', code))
        ctes |= set(re.findall(r'\b([A-Za-z_]\w*)\s+AS\s*\(', code))
    unguarded = sorted(
        name for name in referenced - ctes
        if name.lower() not in REPLACEMENT_SCAN_NAMES)
    assert not unguarded, (
        f"SQL reads {unguarded} by name; add them to REPLACEMENT_SCAN_NAMES "
        f"in clifpy/utils/unit_converter/_inputs.py")


# ===========================================
# Relations from a caller-owned connection
# ===========================================

@pytest.fixture
def captured_info():
    """Collect INFO-and-above messages from clifpy's converter logger."""
    import logging

    records = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record.getMessage())

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


@pytest.mark.unit_conversion
def test_relations_from_a_caller_owned_connection(los_angeles_default_tz, captured_info):
    """io's `new_duckdb_con()` pattern: these used to fail with
    InvalidInputException; now they are copied through Arrow (logged) and keep
    that connection's zone."""
    med, vitals = _weighted_med_and_vitals()
    want, _ = convert_dose_units_by_med_category(
        med, vitals_df=vitals, preferred_units=PREFS, override=True)
    con = new_duckdb_con(site_tz='US/Central')
    got, _ = convert_dose_units_by_med_category(
        con.from_arrow(med.to_arrow()), vitals_df=con.from_arrow(vitals.to_arrow()),
        preferred_units=PREFS, override=True)
    assert_frame_equal(got, want)
    assert any('another DuckDB connection' in m for m in captured_info)


@pytest.mark.unit_conversion
def test_default_connection_relation_renders_in_session_zone(los_angeles_default_tz):
    """A relation carries no zone: the output takes its connection's zone;
    instants are unchanged."""
    med, vitals = _weighted_med_and_vitals()
    out, _ = convert_dose_units_by_med_category(
        duckdb.from_arrow(med.to_arrow()), vitals_df=vitals,
        preferred_units=PREFS, override=True)
    assert out.schema['admin_dttm'].time_zone == 'America/Los_Angeles'
    assert (out['admin_dttm'].dt.convert_time_zone('UTC').to_list()
            == med['admin_dttm'].dt.convert_time_zone('UTC').to_list())


# ===========================================
# Empty and all-NULL-unit input
# ===========================================

AS_KIND = {
    'polars': lambda df: df,
    'pandas': lambda df: df.to_pandas(),
    'relation': lambda df: duckdb.from_arrow(df.to_arrow()),
}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kind', sorted(AS_KIND))
def test_empty_input_returns_the_full_output_schema(kind, captured_warnings):
    """Zero rows: no exception, the columns a one-row input gets, and a warning."""
    prefs = {'norepinephrine': 'mcg/kg/min'}
    one_row = _frame([('norepinephrine', 5.0, 'mcg/min')])
    want, _ = convert_dose_units_by_med_category(one_row, preferred_units=prefs)

    out, counts = convert_dose_units_by_med_category(
        AS_KIND[kind](one_row.clear()), preferred_units=prefs)
    assert out.columns == want.columns
    assert out.height == 0
    assert counts.height == 0
    assert any('no rows' in m for m in captured_warnings)


# Each input kind's own spelling of "every unit is NULL".
ALL_NULL_UNITS = {
    'polars': lambda df: df.with_columns(pl.lit(None).alias('med_dose_unit')),
    'pandas': lambda df: df.with_columns(pl.lit(None).alias('med_dose_unit')).to_pandas(),
    'relation': lambda df: duckdb.from_arrow(df.to_arrow()).project(
        '* REPLACE (NULL AS med_dose_unit)'),
}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('kind', sorted(ALL_NULL_UNITS))
def test_all_null_units_report_missing_with_the_full_schema(kind):
    with_units = _frame([('norepinephrine', 5.0, 'mcg/min'), ('propofol', 2.0, 'mg')])
    want, _ = convert_dose_units_by_med_category(
        with_units, preferred_units=None, override=True)

    out, _ = convert_dose_units_by_med_category(
        ALL_NULL_UNITS[kind](with_units), preferred_units=None, override=True)
    assert out.columns == want.columns
    assert out['_convert_status'].to_list() == ['original unit is missing'] * 2


# ===========================================
# Preferred-target resolution
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw', ['', ' ', 'unknown', 'nan'])
def test_blank_explicit_target_is_not_recognized(raw):
    """An explicit blank or placeholder target is reported, never replaced.

    All four strings normalise to NULL. The fallback join used to COALESCE
    that NULL to `_base_unit`, so propofol came out as 166.67 mcg/min labelled
    with the caller's blank string and `_convert_status == 'success'`. Base
    units are the fallback only when the caller gave no target at all.
    """
    out, _ = _convert([('propofol', 10.0, 'mg/hr')], {'propofol': raw})
    row = out['propofol']  # _convert returns {med_category: row dict}
    assert row['_convert_status'] == f'user-preferred unit {raw} is not recognized'
    # failed rows pass the source dose and cleaned unit through untouched
    assert row['med_dose_converted'] == 10.0
    assert row['med_dose_unit_converted'] == 'mg/hr'
    # without override the same request still aborts, naming the category
    with pytest.raises(ValueError, match='propofol'):
        _convert([('propofol', 10.0, 'mg/hr')], {'propofol': raw}, override=False)


@pytest.mark.unit_conversion
def test_standardize_dict_schema_drops_na_targets():
    """A dict schema follows the file loader's rule: blank/NA means no target.

    `load_dose_unit_targets` drops schema rows whose unit is blank or an NA
    token, so those categories are unlisted (kept as charted). The dict branch passed
    the tokens through as explicit units instead: 'NA' failed as an
    unrecognized target and '' silently converted under a blank label.
    """
    med = _frame([
        ('propofol', 10.0, 'mg/hr'),
        ('heparin', 5.0, 'units/hr'),
        ('norepinephrine', 5.0, 'mcg/min'),
    ])
    out, _ = standardize_med_dose_units(
        med,
        targets={'propofol': 'NA', 'heparin': '', 'norepinephrine': 'mcg/kg/min'},
    )
    rows = _by_category(out)  # {med_category: row dict}
    # dropped -> unlisted: dose kept, unit in mCIDE spelling
    for category, dose, unit in [('propofol', 10.0, 'mg/hr'), ('heparin', 5.0, 'units/hr')]:
        assert rows[category]['_convert_status_details'] == (
            'success: no target listed; unit cleaned only')
        assert rows[category]['med_dose_unit_converted'] == unit
        assert rows[category]['med_dose_converted'] == dose
    # the real target is untouched
    assert rows['norepinephrine']['med_dose_converted'] == pytest.approx(5.0 / WEIGHT_KG)


@pytest.mark.unit_conversion
def test_standardize_dict_schema_of_only_na_targets_raises():
    """A dict with nothing usable fails like an empty schema file does."""
    with pytest.raises(ValueError, match='no usable targets'):
        standardize_med_dose_units(
            _frame([('propofol', 10.0, 'mg/hr')]),
            targets={'propofol': 'NA', 'heparin': None},
        )


# ===========================================
# No vitals: raise by default, warn under override
# ===========================================

def test_no_vitals_raises_by_default_when_a_row_needs_weight():
    """vitals_df=None and a weight-crossing target: override=False (the
    converter's default) raises; a call crossing no weight axis does not."""
    med = _frame([('norepinephrine', 0.1, 'mcg/kg/min')]).drop('weight_kg')
    with pytest.raises(ValueError, match='vitals_df=None'):
        convert_dose_units_by_med_category(
            med, preferred_units={'norepinephrine': 'mcg/min'},
        )
    out, _ = convert_dose_units_by_med_category(
        med, preferred_units={'norepinephrine': 'mcg/kg/hr'},
    )
    assert out['med_dose_converted'][0] == pytest.approx(6.0)


def test_no_vitals_warns_and_fails_only_the_weight_crossing_rows(captured_warnings):
    """standardize_med_dose_units defaults to override=True, so a forgotten
    vitals_df is a warning plus a per-row status: rows crossing the weight
    axis keep their dose and fail, the others still convert."""
    med = _frame([
        ('norepinephrine', 0.1, 'mcg/kg/min'),   # weighted -> unweighted
        ('vasopressin', 2.4, 'units/hr'),        # unweighted -> weighted
        ('propofol', 60.0, 'mg/hr'),             # 60 mg/hr = 1000 mcg/min
    ]).drop('weight_kg')
    out, _ = standardize_med_dose_units(
        med,
        targets={'norepinephrine': 'mcg/min', 'vasopressin': 'units/kg/hr', 'propofol': 'mcg/min'},
    )
    rows = _by_category(out)
    assert 'vitals_df=None' in '\n'.join(captured_warnings)
    assert rows['norepinephrine']['_convert_status'] == (
        'cannot convert weighted to unweighted: weight_kg is missing')
    assert rows['vasopressin']['_convert_status'] == (
        'cannot convert unweighted to weighted: weight_kg is missing')
    assert rows['norepinephrine']['med_dose_converted'] == 0.1
    assert rows['vasopressin']['med_dose_converted'] == 2.4
    assert rows['propofol']['_convert_status'] == 'success'
    assert rows['propofol']['med_dose_converted'] == pytest.approx(1000.0)


# ===========================================
# Weight-transition factors
# ===========================================

# 1 kg = 2.20462 lb (clifpy names the constant KG_PER_LB). A literal, so a
# changed constant is caught here rather than mirrored.
LB_PER_KG = 2.20462


@pytest.mark.unit_conversion
@pytest.mark.parametrize('src,tgt,expected', [
    ('mcg/kg/min', 'mcg/kg/min', 10.0),
    ('mcg/kg/min', 'mcg/lb/min', 10.0 / LB_PER_KG),
    ('mcg/kg/min', 'mcg/min', 10.0 * WEIGHT_KG),
    ('mcg/lb/min', 'mcg/kg/min', 10.0 * LB_PER_KG),
    ('mcg/lb/min', 'mcg/lb/min', 10.0),
    ('mcg/lb/min', 'mcg/min', 10.0 * LB_PER_KG * WEIGHT_KG),
    ('mcg/min', 'mcg/kg/min', 10.0 / WEIGHT_KG),
    ('mcg/min', 'mcg/lb/min', 10.0 / (WEIGHT_KG * LB_PER_KG)),   # mutant P2
    ('mcg/min', 'mcg/min', 10.0),
])
def test_weight_transition_factor_values(src, tgt, expected):
    """Every {/kg, /lb, none} x {/kg, /lb, none} factor, by value (audit M88).

    test_needs_wt_full_matrix checks only the `_needs_wt` flag, so
    `weight_kg / KG_PER_LB` in place of `weight_kg * KG_PER_LB` in the
    unweighted -> per-lb factor (a 4.86x error) passed the suite.
    """
    out, _ = _convert([('drug', 10.0, src)], {'drug': tgt})
    assert out['drug']['_convert_status'] == 'success'
    assert out['drug']['med_dose_converted'] == pytest.approx(expected, rel=1e-9)


# ===========================================
# Public surface: signatures, counts table, exports
# ===========================================

PACKAGE_DIR = Path(unit_converter_pkg.__file__).parent
CONVERT_MODULE_PATH = PACKAGE_DIR / '_convert.py'
API_REFERENCE_PAGE = Path(__file__).resolve().parents[3] / 'docs' / 'api' / 'utilities.md'

LEGACY_POSITIONAL = (
    'med_df', 'vitals_df', 'preferred_units', 'show_intermediate',
    'override', 'return_rel', 'id_name', 'fallback_on_earliest',
)
NEW_KEYWORD_ONLY = (
    'countable_units', 'category_col', 'dose_col', 'unit_col', 'time_col',
    'converted_dose_col', 'converted_unit_col',
    'weight_fallbacks', 'hospitalization_df', 'weight_max_staleness_days',
    'unlisted', 'allow_weight_variant', 'return_format',
)


@pytest.mark.unit_conversion
def test_converter_new_parameters_are_keyword_only():
    """Only the eight pre-3.0 parameters may be passed by position.

    Every parameter added since, `return_format` included, is keyword-only, so
    a later insertion can never shift a caller's positional argument onto the
    wrong parameter.
    """
    params = inspect.signature(convert_dose_units_by_med_category).parameters
    positional = tuple(
        name for name, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD)
    keyword_only = {
        name for name, p in params.items() if p.kind is p.KEYWORD_ONLY}
    assert positional == LEGACY_POSITIONAL
    assert set(NEW_KEYWORD_ONLY) <= keyword_only
    assert set(params) == set(LEGACY_POSITIONAL) | keyword_only


@pytest.mark.unit_conversion
def test_converter_rejects_a_ninth_positional_argument():
    med = _frame([('heparin', 1.0, 'tablet')])
    with pytest.raises(TypeError, match='positional arguments but 9 were given'):
        convert_dose_units_by_med_category(
            med, None, None, False, True, False, 'hospitalization_id', False,
            {'tablet'},
        )


@pytest.mark.unit_conversion
def test_stage1_countable_units_is_keyword_only():
    params = inspect.signature(standardize_dose_to_base_units).parameters
    assert params['countable_units'].kind is inspect.Parameter.KEYWORD_ONLY
    assert params['return_format'].kind is inspect.Parameter.KEYWORD_ONLY
    assert [
        name for name, p in params.items() if p.kind is p.POSITIONAL_OR_KEYWORD
    ] == ['med_df', 'vitals_df', 'show_intermediate', 'id_name']


@pytest.mark.unit_conversion
def test_every_overload_stub_lists_every_parameter():
    """A parameter missing from the stubs is a type error at every call that
    passes it, although the call runs fine. The stubs key on `return_format`;
    `return_rel`, its deprecated alias, is a plain bool in each."""
    get_overloads = getattr(typing, 'get_overloads', None)
    if get_overloads is None:
        pytest.skip('typing.get_overloads needs Python 3.11+')
    impl = [
        (name, p.kind)
        for name, p in inspect.signature(
            convert_dose_units_by_med_category).parameters.items()
    ]
    stubs = get_overloads(convert_dose_units_by_med_category)

    def hints(param):
        # Annotations are strings once the module defers them; normalise both forms.
        return sorted(
            str(inspect.signature(stub).parameters[param].annotation)
            .replace('typing.', '')
            for stub in stubs
        )

    assert hints('return_format') == [
        "Literal['duckdb']", "Literal['pandas']",
        "Literal['polars']", "Literal['polars_lazy']",
    ]
    assert hints('return_rel') == ['bool'] * 4
    for stub in stubs:
        assert [
            (name, p.kind)
            for name, p in inspect.signature(stub).parameters.items()
        ] == impl


@pytest.mark.unit_conversion
def test_overload_stubs_sit_directly_above_the_implementation():
    """Type checkers treat stubs followed by other code as having no
    implementation (mypy: `no-overload-impl`)."""
    tree = ast.parse(CONVERT_MODULE_PATH.read_text())
    positions = [
        i for i, node in enumerate(tree.body)
        if isinstance(node, ast.FunctionDef)
        and node.name == 'convert_dose_units_by_med_category'
    ]
    assert len(positions) == 5
    assert positions == list(range(positions[0], positions[0] + 5))


def _annotation_nodes(tree):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            args = node.args
            for arg in [*args.posonlyargs, *args.args, *args.kwonlyargs,
                        args.vararg, args.kwarg]:
                if arg is not None and arg.annotation is not None:
                    yield arg.annotation
            if node.returns is not None:
                yield node.returns
        elif isinstance(node, ast.AnnAssign):
            yield node.annotation


def _defers_pep604_hints(source):
    """False when `source` has an `X | Y` hint but no
    `from __future__ import annotations`."""
    tree = ast.parse(source)
    uses_pep604 = any(
        isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr)
        for annotation in _annotation_nodes(tree)
        for node in ast.walk(annotation)
    )
    deferred = any(
        isinstance(node, ast.ImportFrom) and node.module == '__future__'
        and any(alias.name == 'annotations' for alias in node.names)
        for node in tree.body
    )
    return deferred or not uses_pep604


@pytest.mark.unit_conversion
def test_pep604_hints_are_deferred_for_python_39():
    """`X | Y` in a hint raises TypeError at import on Python 3.9, the
    package floor, unless the module defers annotations."""
    offenders = [
        path.name
        for path in sorted(PACKAGE_DIR.glob('*.py'), key=lambda p: p.name)
        if not _defers_pep604_hints(path.read_text())
    ]
    assert offenders == []


COUNTS_ROWS = (
    [('norepinephrine', 5.0, 'mcg/min')] * 3
    + [('norepinephrine', 0.1, 'mcg/kg/min')]
    + [('propofol', 20.0, 'mg/hr')] * 2
    + [('zosyn', 1.0, 'bogus')]
)
COUNTS_PREFERRED = {'norepinephrine': 'mcg/kg/min', 'propofol': 'mcg/kg/min'}
# (category, unit, _preferred_unit, converted unit, _convert_status_details,
#  _weight_source, count, then the QA detail: _clean_unit, _base_unit,
#  _unit_class, _convert_status), in the documented sort order: category
# ascending, then count descending. `_frame` pre-fills weight_kg, so every row
# reads 'prefilled'.
EXPECTED_COUNTS = [
    ('norepinephrine', 'mcg/min', 'mcg/kg/min', 'mcg/kg/min',
     'success: converted to the target unit', 'prefilled', 3,
     'mcg/min', 'mcg/min', 'rate', 'success'),
    ('norepinephrine', 'mcg/kg/min', 'mcg/kg/min', 'mcg/kg/min',
     'success: already the target unit', 'prefilled', 1,
     'mcg/kg/min', 'mcg/kg/min', 'rate', 'success'),
    ('propofol', 'mg/hr', 'mcg/kg/min', 'mcg/kg/min',
     'success: converted to the target unit', 'prefilled', 2,
     'mg/hr', 'mcg/min', 'rate', 'success'),
    ('zosyn', 'bogus', 'bogus', 'bogus',
     'original unit bogus is not recognized', 'prefilled', 1,
     'bogus', 'bogus', 'unrecognized', 'original unit bogus is not recognized'),
]


def _review_order(category, unit, converted_unit, pair=''):
    """The counts columns in review order, under the given names."""
    return [category, unit, f'_preferred_unit{pair}', converted_unit,
            f'_convert_status_details{pair}', '_weight_source', 'count',
            f'_clean_unit{pair}', f'_base_unit{pair}', f'_unit_class{pair}',
            f'_convert_status{pair}']


def _renamed_counts_frame():
    return _frame(COUNTS_ROWS).rename({
        'med_category': 'drug', 'med_dose': 'rate',
        'med_dose_unit': 'rate_unit', 'admin_dttm': 'given_at',
    })


@pytest.mark.unit_conversion
def test_counts_table_values():
    """Every group value and every count, in the documented order."""
    _, counts = _convert(COUNTS_ROWS, COUNTS_PREFERRED)
    assert counts.columns == _review_order(
        'med_category', 'med_dose_unit', 'med_dose_unit_converted')
    assert counts.rows() == EXPECTED_COUNTS


@pytest.mark.unit_conversion
def test_counts_table_counts_rows_whose_unit_is_missing():
    """A group whose unit is missing still counts every row. The cleaned unit
    is NULL there, so counting that column instead of the rows reads 0."""
    _, counts = _convert(
        [('drug', 1.0, None), ('drug', 2.0, None), ('drug', 3.0, 'mg')],
        {'drug': 'mg'})
    missing = counts.filter(pl.col('_convert_status') == 'original unit is missing')
    assert missing['count'].to_list() == [2]
    assert counts.filter(pl.col('_convert_status') == 'success')['count'].to_list() == [1]


@pytest.mark.unit_conversion
def test_counts_table_uses_caller_column_names():
    """Under custom names the counts table uses the converted frame's names:
    the caller's columns, and the pair's `__rate` QA columns. So it joins
    straight back onto the converted frame."""
    out, counts = convert_dose_units_by_med_category(
        _renamed_counts_frame(),
        preferred_units=COUNTS_PREFERRED,
        override=True,
        category_col='drug',
        dose_col='rate',
        unit_col='rate_unit',
        time_col='given_at',
        converted_dose_col='rate_out',
        converted_unit_col='rate_unit_out',
    )
    assert counts.columns == _review_order('drug', 'rate_unit', 'rate_unit_out', '__rate')
    assert counts.rows() == EXPECTED_COUNTS
    # The lean rows carry no `_clean_unit` / `_unit_class`; those QA columns
    # are fixed by (category, unit), so the join on what the rows do carry
    # is still many-to-one.
    keys = ['drug', 'rate_unit', 'rate_unit_out', '_convert_status__rate',
            '_convert_status_details__rate', '_weight_source']
    joined = out.join(
        counts.select(*keys, 'count'), on=keys, how='left', validate='m:1')
    assert joined.height == out.height
    assert sorted(joined['count'].to_list()) == [1, 1, 2, 2, 3, 3, 3]


@pytest.mark.unit_conversion
def test_counts_table_under_in_place_overwrite():
    """Overwriting the source pair leaves one name for two unit columns, so
    the counts table shows the converted unit as `<unit_col>_converted`. The
    QA columns keep the pair's `__rate` names."""
    _, counts = convert_dose_units_by_med_category(
        _renamed_counts_frame(),
        preferred_units=COUNTS_PREFERRED,
        override=True,
        category_col='drug',
        dose_col='rate',
        unit_col='rate_unit',
        time_col='given_at',
        converted_dose_col='rate',
        converted_unit_col='rate_unit',
    )
    assert counts.columns == _review_order(
        'drug', 'rate_unit', 'rate_unit_converted', '__rate')
    assert counts.rows() == EXPECTED_COUNTS


@pytest.mark.unit_conversion
def test_counts_table_sorts_by_category_when_unit_col_is_named_med_category():
    """A shown name can equal another column's internal name. The sort must
    still follow the category, not whichever column is now called
    `med_category`."""
    med = pl.DataFrame({
        'hospitalization_id': ['H0', 'H1', 'H2'],
        'drug': ['aaa', 'bbb', 'bbb'],
        'med_dose': [1.0, 2.0, 2.0],
        'med_category': ['mg', 'mcg', 'mcg'],
        'weight_kg': [WEIGHT_KG] * 3,
        'admin_dttm': [TS] * 3,
    })
    _, counts = convert_dose_units_by_med_category(
        med, preferred_units=None, override=True,
        category_col='drug', unit_col='med_category',
    )
    assert counts['drug'].to_list() == ['aaa', 'bbb']
    assert counts['med_category'].to_list() == ['mg', 'mcg']
    assert counts['count'].to_list() == [1, 2]


@pytest.mark.unit_conversion
def test_counts_output_names_in_place_overwrite_is_case_insensitive():
    from clifpy.utils.unit_converter._counts import counts_output_names

    assert counts_output_names(
        category_col='drug', dose_col='rate', unit_col='rate_unit',
        converted_unit_col='Rate_Unit',
    ) == {
        'med_category': 'drug',
        'med_dose_unit': 'rate_unit',
        '_clean_unit': '_clean_unit__rate',
        '_base_unit': '_base_unit__rate',
        '_unit_class': '_unit_class__rate',
        '_preferred_unit': '_preferred_unit__rate',
        '_convert_status': '_convert_status__rate',
        '_convert_status_details': '_convert_status_details__rate',
        'med_dose_unit_converted': 'rate_unit_converted',
        '_weight_source': '_weight_source',
    }
    # the default pair, in any letter case: every name is shown as it is
    assert counts_output_names(
        category_col='med_category', dose_col='MED_DOSE', unit_col='med_dose_unit',
        converted_unit_col='med_dose_unit_converted',
    ) == {
        name: name for name in (
            'med_category', 'med_dose_unit', '_clean_unit', '_base_unit',
            '_unit_class', '_preferred_unit', '_convert_status',
            '_convert_status_details', 'med_dose_unit_converted', '_weight_source')
    }


@pytest.mark.unit_conversion
def test_counts_table_name_clash_raises():
    med = _frame([('heparin', 1.0, 'tablet')]).rename({'med_category': 'count'})
    with pytest.raises(ValueError, match="'count'"):
        convert_dose_units_by_med_category(
            med, preferred_units=None, override=True, category_col='count')

    # a QA column's shown name counts too: `_base_unit__rate` is the rate
    # pair's own
    from clifpy.utils.unit_converter._counts import counts_output_names

    with pytest.raises(ValueError, match="'_base_unit__rate'"):
        counts_output_names(
            category_col='_base_unit__rate', dose_col='rate', unit_col='rate_unit',
            converted_unit_col='rate_unit_converted')


@pytest.mark.unit_conversion
def test_standardize_counts_table_uses_caller_column_names():
    med = _frame([('norepinephrine', 5.0, 'mcg/min')]).rename(
        {'med_category': 'drug', 'med_dose_unit': 'rate_unit'})
    _, counts = standardize_med_dose_units(
        med, {'norepinephrine': 'mcg/kg/min'},
        category_col='drug', unit_col='rate_unit',
    )
    # the converted unit defaults to '<unit_col>_converted'; the dose pair is
    # still med_dose, so the QA columns keep their bare names
    assert {'drug', 'rate_unit', 'rate_unit_converted', '_clean_unit',
            '_convert_status', 'count'} <= set(counts.columns)
    assert not ({'med_category', 'med_dose_unit', 'med_dose_unit_converted'}
                & set(counts.columns))


PUBLIC_SURFACE = frozenset({
    'convert_dose_units_by_med_category', 'standardize_med_dose_units',
    'standardize_dose_to_base_units',
    'attach_weight', 'WEIGHT_FALLBACKS', 'find_most_recent_weight',
    'CONVERT_STATUS_DETAILS', 'DETAILS_CONVERTED_TO_TARGET', 'DETAILS_ALREADY_TARGET',
    'DETAILS_CONVERTED_TO_VARIANT', 'DETAILS_ALREADY_VARIANT',
    'DETAILS_UNLISTED_CLEANED', 'DETAILS_UNLISTED_TO_BASE',
    'DETAILS_UNLISTED_ALREADY_BASE',
    'load_dose_unit_targets', 'register_target_reader',
    'available_target_formats', 'TargetReader',
    'validate_column_name',
    'AMBIGUOUS_MU_STATUS',
    'DEFAULT_COUNTABLE_UNITS', 'MISSING_UNIT_PLACEHOLDERS',
    'CANONICAL_UNIT_SPELLING', 'UNIT_NAMING_VARIANTS', 'REGEX_TO_FACTOR_MAPPER',
    'SUBCLASS_SPEC', 'SUBCLASS_REGEX', 'STANDALONE_SUBCLASSES',
    'TOKEN_TO_BASE_FACTOR', 'TIME_TO_BASE_FACTOR', 'KG_PER_LB',
    'ACCEPTABLE_BASE_AMOUNT_UNITS', 'STANDALONE_AMOUNT_UNITS',
    'ACCEPTABLE_WEIGHT_UNITS', 'ACCEPTABLE_TIME_UNITS',
    'ACCEPTABLE_AMOUNT_UNITS', 'ACCEPTABLE_RATE_UNITS', 'ALL_ACCEPTABLE_UNITS',
    'MASS_REGEX', 'VOLUME_REGEX', 'UNIT_REGEX', 'HR_REGEX', 'WEIGHT_REGEX',
})


@pytest.mark.unit_conversion
def test_all_lists_the_full_public_surface():
    names = unit_converter_pkg.__all__
    assert len(names) == len(set(names))
    assert sorted(PUBLIC_SURFACE - set(names)) == []
    assert [
        name for name in names
        if name.startswith('_') or not hasattr(unit_converter_pkg, name)
    ] == []


@pytest.mark.unit_conversion
def test_package_docstring_names_every_public_name():
    doc = unit_converter_pkg.__doc__
    assert [
        name for name in unit_converter_pkg.__all__
        if not re.search(rf'\b{re.escape(name)}\b', doc)
    ] == []


@pytest.mark.unit_conversion
def test_api_reference_documents_every_public_name():
    """Each public name has a mkdocstrings entry. An entry may point at the
    defining submodule (`...unit_converter._preferred.<name>`)."""
    documented = {
        dotted.rpartition('.')[2]
        for dotted in re.findall(
            r'^::: (clifpy\.utils\.unit_converter\.[\w.]+)\s*$',
            API_REFERENCE_PAGE.read_text(), re.M)
    }
    assert sorted(set(unit_converter_pkg.__all__) - documented) == []


# ===========================================
# Task 12: schema targets -- readers, provenance, shape guard
# ===========================================

@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw,expected', [
    ('milli_units/min', 'mu/min'),
    ('Milli_Units', 'mu'),
    ('million_units/hr', 'mnu/hr'),
])
def test_underscore_unit_family_spellings(raw, expected):
    """`milli_units` is pitocin's unit in clifpy's own 3.0 continuous schema
    YAML and in upstream mCIDE before 2026-09-09. It must parse as
    milli-units, not as the unrecognised `m_u`."""
    assert _normalize(raw) == expected


@pytest.mark.unit_conversion
def test_milli_units_target_converts():
    out, _ = _convert([('pitocin', 1.0, 'units/min')], {'pitocin': 'milli_units/min'})
    assert out['pitocin']['_convert_status'] == 'success'
    assert out['pitocin']['med_dose_converted'] == pytest.approx(1000.0)
    # the caller's spelling is echoed, as for every explicit target
    assert out['pitocin']['med_dose_unit_converted'] == 'milli_units/min'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('url', [
    'https://example.org/targets.csv?token=ABC',
    'https://example.org/clif_3.0/targets.CSV#L1',
])
def test_url_query_and_fragment_do_not_hide_the_format(monkeypatch, url):
    """A private raw link carries `?token=...`; a browser link may carry `#L1`.
    The upper-case `.CSV` also kills mutant T3 (suffix matched case-sensitively)."""
    seen = []

    def _fake_csv_reader(source):
        seen.append(source)
        return pl.DataFrame({'med_category': ['norepinephrine'],
                             'med_dose_unit': ['mcg/kg/min']})

    monkeypatch.setitem(_targets._TARGET_READERS, '.csv', _fake_csv_reader)
    assert load_dose_unit_targets(url) == {'norepinephrine': 'mcg/kg/min'}
    assert seen == [url]


@pytest.mark.unit_conversion
def test_gzipped_csv_schema(tmp_path):
    import gzip

    p = tmp_path / 'targets.csv.gz'
    p.write_bytes(gzip.compress(b'med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\n'))
    assert load_dose_unit_targets(p) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('as_path', [True, False], ids=['Path', 'str'])
def test_plain_parquet_schema_keeps_the_native_read(tmp_path, as_path):
    p = tmp_path / 'targets.parquet'
    pl.DataFrame({'med_category': ['norepinephrine'], 'med_dose_unit': ['mcg/kg/min']}).write_parquet(p)
    assert load_dose_unit_targets(p if as_path else str(p)) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_plain_parquet_does_not_go_through_read_bytes(tmp_path, monkeypatch):
    p = tmp_path / 'targets.parquet'
    pl.DataFrame({'med_category': ['norepinephrine'], 'med_dose_unit': ['mcg/kg/min']}).write_parquet(p)
    def boom(source):
        raise AssertionError('plain Parquet must be read natively')
    monkeypatch.setattr(_targets, '_read_bytes', boom)
    assert load_dose_unit_targets(p) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_gzipped_parquet_schema(tmp_path):
    import gzip
    import io

    buf = io.BytesIO()
    pl.DataFrame({'med_category': ['norepinephrine'], 'med_dose_unit': ['mcg/kg/min']}).write_parquet(buf)
    p = tmp_path / 'targets.parquet.gz'
    p.write_bytes(gzip.compress(buf.getvalue()))
    assert load_dose_unit_targets(p) == {'norepinephrine': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_compressed_csv_schema_from_a_url(monkeypatch):
    """The query string hides the `.gz` from any check on the end of the
    string, so the CSV reader fetches the URL itself and decompresses by the
    suffix of the URL's path."""
    import gzip
    import io
    import urllib.request

    requested = []

    def _fake_urlopen(url, *args, **kwargs):
        requested.append(url)
        return io.BytesIO(gzip.compress(b'med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\n'))

    monkeypatch.setattr(urllib.request, 'urlopen', _fake_urlopen)
    url = 'https://example.org/targets.csv.gz?token=ABC'
    assert load_dose_unit_targets(url) == {'norepinephrine': 'mcg/kg/min'}
    assert requested == [url]


@pytest.mark.unit_conversion
def test_compressed_json_schema_from_a_url(monkeypatch):
    """The text readers fetch URLs themselves: the query string must not reach
    the format check, and the payload is decompressed by its suffix."""
    import gzip
    import io
    import urllib.request

    requested = []

    def _fake_urlopen(url, *args, **kwargs):
        requested.append(url)
        return io.BytesIO(gzip.compress(b'{"norepinephrine": "mcg/kg/min"}'))

    monkeypatch.setattr(urllib.request, 'urlopen', _fake_urlopen)
    url = 'https://example.org/targets.json.gz?token=ABC'
    assert load_dose_unit_targets(url) == {'norepinephrine': 'mcg/kg/min'}
    assert requested == [url]


@pytest.mark.unit_conversion
@pytest.mark.parametrize('suffix,reader,error', [
    ('', pl.read_csv, ValueError),
    ('.', pl.read_csv, ValueError),
    ('.csv.gz', pl.read_csv, ValueError),
    (None, pl.read_csv, TypeError),
    ('.toml', 'not a reader', TypeError),
])
def test_register_target_reader_validates_arguments(monkeypatch, suffix, reader, error):
    # A private copy of the registry, so even a wrongly accepted
    # registration cannot leak out of this test.
    monkeypatch.setattr(_targets, '_TARGET_READERS', dict(_targets._TARGET_READERS))
    with pytest.raises(error):
        register_target_reader(suffix, reader)


_TWO_TARGETS = {'norepinephrine': 'mcg/kg/min', 'pitocin': 'milli-units/min'}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('name,text', [
    ('targets.yaml', 'norepinephrine: mcg/kg/min\npitocin: milli-units/min\n'),
    ('targets.yml',
     '- {med_category: norepinephrine, med_dose_unit: mcg/kg/min}\n'
     '- {med_category: pitocin, med_dose_unit: milli-units/min}\n'),
    ('targets.json', '{"norepinephrine": "mcg/kg/min", "pitocin": "milli-units/min"}'),
    ('targets.json',
     '[{"med_category": "norepinephrine", "med_dose_unit": "mcg/kg/min"},'
     ' {"med_category": "pitocin", "med_dose_unit": "milli-units/min"}]'),
    ('targets.json',
     '{"targets": {"norepinephrine": "mcg/kg/min", "pitocin": "milli-units/min"}}'),
])
def test_yaml_and_json_schema_readers(tmp_path, name, text):
    """List-of-records and category -> unit mapping forms, bare or wrapped.

    The mapping cases kill mutant T4 (mapping keys read as units).
    """
    p = tmp_path / name
    p.write_text(text)
    assert load_dose_unit_targets(p) == _TWO_TARGETS


@pytest.mark.unit_conversion
def test_parquet_schema_reader(tmp_path):
    p = tmp_path / 'targets.parquet'
    pl.DataFrame({
        'med_category': ['norepinephrine', 'pitocin', 'heparin'],
        'med_dose_unit': ['mcg/kg/min', 'milli-units/min', None],
    }).write_parquet(p)
    assert load_dose_unit_targets(p) == _TWO_TARGETS


@pytest.mark.unit_conversion
@pytest.mark.parametrize('name,text', [
    ('targets.yaml', 'sodium_chloride: ml/hr\nheparin: NA\n'),
    ('targets.json', '{"sodium_chloride": "ml/hr", "heparin": "NA"}'),
])
def test_mapping_form_honours_caller_column_names(tmp_path, name, text):
    """A mapping file names no columns, so it serves whichever pair the
    caller asks for -- here the volume targets."""
    p = tmp_path / name
    p.write_text(text)
    targets = load_dose_unit_targets(
        p, category_col='category', unit_col='volume_infusion_rate_units')
    assert targets == {'sodium_chloride': 'ml/hr'}


@pytest.mark.unit_conversion
def test_mapping_form_list_value_follows_the_conflict_rule(tmp_path, captured_warnings):
    """`epoprostenol: [ng/kg/min, mcg/kg/min]` is the route split written as
    a list: like duplicate CSV rows, the first wins and the warning names it."""
    p = tmp_path / 'targets.yaml'
    p.write_text('epoprostenol: [ng/kg/min, mcg/kg/min]\nnorepinephrine: mcg/kg/min\n')
    assert load_dose_unit_targets(p) == {
        'epoprostenol': 'ng/kg/min', 'norepinephrine': 'mcg/kg/min'}
    assert any('epoprostenol' in m and 'using ng/kg/min' in m for m in captured_warnings)


@pytest.mark.unit_conversion
def test_mapping_form_targets_key_skips_metadata(tmp_path):
    p = tmp_path / 'targets.yaml'
    p.write_text('version: "3.0"\nsource: mCIDE\ntargets:\n'
                 '  norepinephrine: mcg/kg/min\n  pitocin: milli-units/min\n')
    assert load_dose_unit_targets(p) == _TWO_TARGETS


@pytest.mark.unit_conversion
def test_mapping_form_rejects_a_nested_mapping(tmp_path):
    """Read literally, `{norepinephrine: {unit: ...}}` would invent a
    category named `unit`; it must raise instead."""
    p = tmp_path / 'targets.json'
    p.write_text('{"norepinephrine": {"unit": "mcg/kg/min"}}')
    with pytest.raises(ValueError, match='norepinephrine'):
        load_dose_unit_targets(p)


@pytest.mark.unit_conversion
def test_reader_must_return_a_dataframe(tmp_path):
    """An explicit reader= overrides the extension, and what it returns is
    checked: a dict is refused, not iterated as rows. Kills mutant T5."""
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nnorepinephrine,mcg/kg/min\n')
    with pytest.raises(ValueError, match='must return a polars or pandas DataFrame'):
        load_dose_unit_targets(p, reader=lambda source: {'norepinephrine': 'mcg/kg/min'})


@pytest.mark.unit_conversion
@pytest.mark.parametrize('token', [
    'NA', 'na', 'N/A', 'nan', 'NaN', 'None', 'null', 'NULL', 'pending', 'Pending', '',
])
def test_na_target_tokens_are_dropped(tmp_path, token):
    """Every NA spelling drops its row. 'pending' is what every
    volume_infusion_rate_units cell held in upstream mCIDE from 2026-07-28
    to 2026-07-30. Kills mutant X3 (NA tokens shrunk)."""
    p = tmp_path / 'targets.csv'
    p.write_text(f'med_category,med_dose_unit\nkept,mg\ndropped,{token}\n')
    assert load_dose_unit_targets(p) == {'kept': 'mg'}


@pytest.mark.unit_conversion
def test_schema_categories_are_stripped_and_lowercased(tmp_path):
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\n Norepinephrine ,mcg/kg/min\nPROPOFOL,mcg/kg/min\n')
    assert load_dose_unit_targets(p) == {
        'norepinephrine': 'mcg/kg/min', 'propofol': 'mcg/kg/min'}


@pytest.mark.unit_conversion
def test_spelling_only_duplicates_are_not_conflicts(tmp_path, captured_warnings):
    """`units/hr` and `U/hr` are one unit, and `Heparin` is `heparin`: one
    target, kept in the first row's spelling, and no route warning."""
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nheparin,units/hr\nHeparin,U/hr\n')
    assert load_dose_unit_targets(p) == {'heparin': 'units/hr'}
    assert not any('med_group' in m for m in captured_warnings)


@pytest.mark.unit_conversion
def test_conflicting_duplicates_follow_file_order(tmp_path, captured_warnings):
    """The mirror image of test_multiple_targets_per_category_keep_first_and_warn:
    with the rows swapped the other unit wins. First-in-file-order is
    deterministic for one file, not across reorderings of it."""
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\n'
                 'epoprostenol,mcg/kg/min\nepoprostenol,ng/kg/min\n')
    assert load_dose_unit_targets(p) == {'epoprostenol': 'mcg/kg/min'}
    assert any('using mcg/kg/min' in m for m in captured_warnings)


@pytest.mark.unit_conversion
def test_load_dose_unit_targets_refuses_a_shadowing_table(tmp_path, shadow_table):
    """Comparing differently spelled duplicates runs the cleaning chain, which
    reads its input by name; a default-connection table named `relation`
    must stop it with Task 6's error instead of being read in its place."""
    shadow_table('relation')
    p = tmp_path / 'targets.csv'
    p.write_text('med_category,med_dose_unit\nheparin,units/hr\nheparin,U/hr\n')
    with pytest.raises(ValueError, match='relation'):
        load_dose_unit_targets(p)


@pytest.mark.unit_conversion
def test_standardize_details_in_every_return_format():
    """`_convert_status_details` is computed in stage 2, so every
    return_format carries it, in the rows and in the counts. (Task 6's
    test_return_format_types_and_values then checks that the formats hold the
    same values.)"""
    med = _frame([('norepinephrine', 5.0, 'mcg/min'), ('other', 1.0, 'mg')])
    expected = {
        'norepinephrine': 'success: converted to the target unit',
        'other': 'success: no target listed; unit cleaned only'}
    for return_format in ('polars', 'polars_lazy', 'duckdb', 'pandas'):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', DeprecationWarning)   # 'pandas' is deprecated
            out, counts = standardize_med_dose_units(
                med, {'norepinephrine': 'mcg/kg/min'}, return_format=return_format)
        out, counts = _to_polars(out), _to_polars(counts)
        assert dict(zip(out['med_category'].to_list(),
                        out['_convert_status_details'].to_list())) == expected, return_format
        assert dict(zip(counts['med_category'].to_list(),
                        counts['_convert_status_details'].to_list())) == expected, return_format


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', ['polars', 'polars_lazy', 'duckdb', 'pandas'])
def test_category_drift_is_flagged_not_silently_fallen_back(captured_warnings, return_format):
    """Review focus 4: 'Norepinephrine ' and 'NOREPINEPHRINE' are not the
    listed 'norepinephrine'. Their rows are kept as charted, each says so,
    and one warning names every drifted spelling."""
    med = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('Norepinephrine ', 5.0, 'mcg/min'),
        ('NOREPINEPHRINE', 5.0, 'mcg/min'),
        ('NOREPINEPHRINE', 6.0, 'mcg/min'),
    ])
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)   # 'pandas' is deprecated
        out, _ = standardize_med_dose_units(
            med, {'norepinephrine': 'mcg/kg/min'}, return_format=return_format)
    out = _to_polars(out)
    ids = out['hospitalization_id'].to_list()
    unlisted = 'success: no target listed; unit cleaned only'
    assert dict(zip(ids, out['_convert_status_details'].to_list())) == {
        'H0': 'success: converted to the target unit',
        'H1': unlisted, 'H2': unlisted, 'H3': unlisted}
    assert dict(zip(ids, out['med_dose_unit_converted'].to_list())) == {
        'H0': 'mcg/kg/min', 'H1': 'mcg/min', 'H2': 'mcg/min', 'H3': 'mcg/min'}
    missing = [m for m in captured_warnings if 'no listed target' in m]
    assert len(missing) == 1
    assert "'Norepinephrine ' (1 row)" in missing[0]
    assert "'NOREPINEPHRINE' (2 rows)" in missing[0]
    assert "'norepinephrine'" not in missing[0]


@pytest.mark.unit_conversion
def test_absent_category_warning_always_names_spelling_drift(captured_warnings):
    """A rare misspelling must not fall off the truncated list: drift of a
    listed category is named first and never cut, the others follow."""
    rows = [('norepinephrine', 5.0, 'mcg/min'), ('NOREPINEPHRINE ', 5.0, 'mcg/min')]
    for i in range(10):
        rows += [(f'other_drug_{i}', 1.0, 'mcg/min')] * 3
    out, _ = standardize_med_dose_units(_frame(rows), {'norepinephrine': 'mcg/kg/min'})
    missing = [m for m in captured_warnings if 'no listed target' in m]
    assert len(missing) == 1
    msg = missing[0]
    assert "likely spelling drift of a listed category: 'NOREPINEPHRINE ' (1 row)" in msg
    assert msg.index('NOREPINEPHRINE ') < msg.index('other_drug_')
    assert 'others: ' in msg
    assert msg.count('other_drug_') == _targets._MAX_NAMES_IN_WARNING
    assert '(+2 more)' in msg


@pytest.mark.unit_conversion
def test_convert_status_details_is_pipeline_owned():
    """`_convert_status_details` is a per-pair column. A copy left by an
    earlier pass over the same pair is refused before any work, by the
    converter's own collision check (no standardize-only guard). Another
    pair's copy is set aside for the run and comes back unchanged."""
    assert '_convert_status_details' in PAIR_QA_COLUMNS
    assert '_convert_status_details' in PIPELINE_OWNED_COLUMNS
    med = _frame([('norepinephrine', 5.0, 'mcg/min')]).with_columns(
        volume_infusion_rate=pl.lit(0.5),
        volume_infusion_rate_unit=pl.lit('l/hr'),
    )
    # the same pair again, in any letter case: refused
    for stale in ('_convert_status_details', '_Convert_Status_Details'):
        for call, key in ((convert_dose_units_by_med_category, 'preferred_units'),
                          (standardize_med_dose_units, 'targets')):
            with pytest.raises(ValueError, match=rf"Column-name collision: .*\['{stale}'\]"):
                call(med.with_columns(pl.lit('stale').alias(stale)),
                     **{key: {'norepinephrine': 'mcg/kg/min'}})
    # another pair: the earlier label passes through, and the pair gets its own
    out, counts = standardize_med_dose_units(
        med.with_columns(pl.lit('stale').alias('_convert_status_details')),
        {'norepinephrine': 'ml/hr'},
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    assert out['_convert_status_details'].to_list() == ['stale']
    assert out['_convert_status_details__volume_infusion_rate'].to_list() == [
        'success: converted to the target unit']
    assert '_convert_status_details__volume_infusion_rate' in counts.columns
    # 0.5 l/hr = 500 ml/hr
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(500.0)


@pytest.mark.unit_conversion
def test_pipeline_owned_columns_cover_every_column_standardize_writes():
    """Task 5's drift guard, extended to standardize_med_dose_units: every
    column it adds to the rows, `_convert_status_details` included, is
    pipeline-owned, so feeding its output back in is parked or refused,
    never read as this call's own."""
    med = _frame([('norepinephrine', 5.0, 'mcg/min'), ('other', 1.0, 'mg')])
    rel, _ = standardize_med_dose_units(
        med, {'norepinephrine': 'mcg/kg/min'}, show_intermediate=True,
        return_format='duckdb')
    written = {c.lower() for c in rel.columns} - {c.lower() for c in med.columns}
    assert '_convert_status_details' in written
    assert written - PIPELINE_OWNED_COLUMNS == set()


@pytest.mark.unit_conversion
def test_target_warnings_on_empty_counts(captured_warnings):
    """Review focus 3 for the warning step: zero rows log nothing, for a
    polars frame and for a relation, even when the empty category column
    arrives untyped (polars Null, DuckDB INTEGER)."""
    targets = {'norepinephrine': 'mcg/kg/min'}
    _targets._warn_about_targets(
        pl.DataFrame(schema={
            'med_category': pl.Null, 'med_dose_unit': pl.String,
            '_unit_class': pl.String, '_convert_status': pl.String, 'count': pl.Int64,
        }),
        targets, category_col='med_category')
    _targets._warn_about_targets(
        duckdb.sql("SELECT NULL::INTEGER AS med_category, NULL::VARCHAR AS _unit_class,"
                   " NULL::VARCHAR AS _convert_status, 0::BIGINT AS count WHERE false"),
        targets, category_col='med_category')
    assert captured_warnings == []


@pytest.mark.unit_conversion
def test_amount_only_schema_warns_per_category(captured_warnings):
    """A pre-2026-09-09 mCIDE copy held amount-only targets (norepinephrine
    -> 'mcg'). Every rate row then fails 'cannot convert rate to amount', a
    per-row status nobody reads, so the call must say so -- for the category
    that mostly fails, not for one with a single odd row."""
    med = _frame([
        ('norepinephrine', 0.1, 'mcg/kg/min'),
        ('norepinephrine', 0.2, 'mcg/kg/min'),
        ('propofol', 50.0, 'mcg/kg/min'),
        ('propofol', 40.0, 'mcg/kg/min'),
        ('propofol', 30.0, 'mcg/kg/min'),
        ('propofol', 20.0, 'mg'),          # one bolus-like row: 1 of 4 fails
    ])
    out, _ = standardize_med_dose_units(
        med, {'norepinephrine': 'mcg', 'propofol': 'mcg/kg/min'})
    norepinephrine = out.filter(pl.col('med_category') == 'norepinephrine')
    assert set(norepinephrine['_convert_status'].to_list()) == {'cannot convert rate to amount'}
    shape = [m for m in captured_warnings if 'fails on unit class' in m]
    assert len(shape) == 1
    assert "'norepinephrine'" in shape[0] and "'mcg'" in shape[0]
    assert '2 of 2' in shape[0] and '2026-09-09' in shape[0]


@pytest.mark.unit_conversion
def test_standardize_default_tolerates_schema_categories_absent_from_data(captured_warnings):
    """override=True is the default because a schema lists categories the
    data lacks; override=False makes exactly that raise. Kills mutant C5."""
    med = _frame([('norepinephrine', 5.0, 'mcg/min')])
    schema = {'norepinephrine': 'mcg/kg/min', 'angiotensin': 'ng/kg/min'}
    out, _ = standardize_med_dose_units(med, schema)
    assert out.height == 1
    assert any('angiotensin' in m for m in captured_warnings)
    with pytest.raises(ValueError, match='angiotensin'):
        standardize_med_dose_units(med, schema, override=False)


@pytest.mark.unit_conversion
def test_standardize_uses_its_targets_column_parameters(tmp_path):
    """The data's and the targets file's column names are separate parameters.

    The schema's `med_dose_unit` (mg/hr) and `volume_infusion_rate_units`
    (ml/hr) disagree, and its category column is not the data's, so reading
    the wrong column or dropping a name fails loudly. Kills mutants C6
    (schema read with the data's unit_col) and X7 (category_col not
    forwarded).
    """
    schema = tmp_path / 'targets.csv'
    schema.write_text('category,med_dose_unit,volume_infusion_rate_units\n'
                      'sodium_chloride,mg/hr,ml/hr\n')
    med = _frame([('sodium_chloride', 100.0, 'mg/hr')]).rename(
        {'med_category': 'drug'}
    ).with_columns(
        volume_infusion_rate=pl.lit(2.0),
        volume_infusion_rate_unit=pl.lit('l/hr'),
    )

    out, counts = standardize_med_dose_units(
        med, schema,
        category_col='drug',
        unit_col='volume_infusion_rate_unit',
        targets_category_col='category',
        targets_unit_col='volume_infusion_rate_units',
        dose_col='volume_infusion_rate',
    )
    # the converted pair and the details column are named after the pair
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(2000.0)
    assert out['volume_infusion_rate_unit_converted'][0] == 'ml/hr'
    listed = 'success: converted to the target unit'
    assert out['_convert_status_details__volume_infusion_rate'][0] == listed
    assert '_convert_status_details' not in out.columns
    # the dose pair is untouched
    assert out['med_dose'][0] == 100.0
    assert out['med_dose_unit'][0] == 'mg/hr'
    assert set(counts['_convert_status_details__volume_infusion_rate'].to_list()) == {listed}


# ===========================================
# Row order must not depend on DuckDB session settings
# ===========================================

@pytest.fixture
def session_setting():
    """Set a DuckDB setting on the default connection; RESET every one touched."""
    touched = []

    def _set(name, value):
        touched.append(name)
        duckdb.sql(f"SET {name} = {value}")

    yield _set
    for name in touched:
        duckdb.sql(f"RESET {name}")


@pytest.mark.unit_conversion
def test_pandas_mixed_types_keep_their_rows_without_insertion_order(session_setting):
    """Mixed-type pandas columns are scanned by DuckDB; the scan must keep row order."""
    n = 300_000
    ids = [i if i % 2 else f'H{i}' for i in range(n)]
    units = ['mg' if i % 3 != 2 else 1 for i in range(n)]
    med = pd.DataFrame({
        'hospitalization_id': pd.Series(ids, dtype=object),
        'med_category': ['drug'] * n,
        'med_dose': [float(i) for i in range(n)],  # the dose encodes the row
        'med_dose_unit': pd.Series(units, dtype=object),
        'admin_dttm': [TS] * n,
    })
    session_setting('preserve_insertion_order', 'false')
    from clifpy.utils.unit_converter._inputs import pandas_to_arrow
    got = pandas_to_arrow(med)
    expected = med.assign(
        hospitalization_id=med['hospitalization_id'].astype(str),
        med_dose_unit=med['med_dose_unit'].astype(str),
    )
    assert got.column('med_dose').to_pylist() == med['med_dose'].tolist()
    assert got.column('hospitalization_id').to_pylist() == expected['hospitalization_id'].tolist()
    assert got.column('med_dose_unit').to_pylist() == expected['med_dose_unit'].tolist()


@pytest.mark.unit_conversion
def test_pandas_mixed_types_scan_leaves_default_connection_settings_alone(session_setting):
    """The private scan never changes a setting on the caller's default connection."""
    from clifpy.utils.unit_converter._inputs import pandas_to_arrow
    session_setting('preserve_insertion_order', 'false')
    pandas_to_arrow(pd.DataFrame({'u': pd.Series(['mg', 1], dtype=object)}))
    value = duckdb.sql(
        "SELECT value FROM duckdb_settings() WHERE name = 'preserve_insertion_order'"
    ).fetchone()[0]
    assert str(value).lower() == 'false'


@pytest.mark.unit_conversion
@pytest.mark.parametrize('as_pandas', [False, True])
def test_output_order_ignores_default_order(session_setting, as_pandas):
    """default_order='DESC' must not reverse eager outputs or mis-pin the pandas index."""
    med, vitals = _order_case()
    if as_pandas:
        med = med.to_pandas().set_index(pd.Index(['r40', 'r30', 'r20', 'r10'], name='mar_row'))
        vitals = vitals.to_pandas()
        fmt = 'pandas'
    else:
        fmt = 'polars'
    session_setting('default_order', "'DESC'")
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        out, _ = convert_dose_units_by_med_category(
            med, vitals_df=vitals, preferred_units=ORDER_PREFS,
            override=True, return_format=fmt)
        attached = attach_weight(med, vitals, return_format=fmt)
    for frame in (out, attached):
        assert list(frame['med_dose']) == [1.0, 2.0, 3.0, 4.0]
    if as_pandas:
        assert out.index.equals(med.index)
        assert attached.index.equals(med.index)
    assert list(out['med_dose_converted']) == pytest.approx(ORDER_CONVERTED)


def _indexed_pandas_cases():
    """(name, pandas frame under a string index) pairs: full, then filtered."""
    med, vitals = _order_case()
    full = med.to_pandas().set_index(pd.Index(['r40', 'r30', 'r20', 'r10'], name='mar_row'))
    return full, vitals.to_pandas()


@pytest.mark.unit_conversion
@pytest.mark.parametrize('filtered', [False, True])
def test_attach_weight_pandas_output_keeps_the_callers_index(filtered):
    """attach_weight under return_format='pandas' keeps input order and the
    caller's index, so assigning a column back by index lines up."""
    med, vitals = _indexed_pandas_cases()
    if filtered:
        med = med[med['med_dose'] > 1.0]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        out = attach_weight(med, vitals, return_format='pandas')
    assert out.index.equals(med.index)
    assert out['med_dose'].tolist() == med['med_dose'].tolist()
    med = med.assign(weight_kg_attached=out['weight_kg'])
    assert med['weight_kg_attached'].equals(out['weight_kg'])


@pytest.mark.unit_conversion
@pytest.mark.parametrize('filtered', [False, True])
def test_base_units_pandas_output_keeps_the_callers_index(filtered):
    """Stage 1 under return_format='pandas' keeps input order and the
    caller's index, with no private ordinal in the output."""
    med, _ = _indexed_pandas_cases()
    if filtered:
        med = med[med['med_dose'] > 1.0]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        out, _counts = standardize_dose_to_base_units(med, return_format='pandas')
    assert out.index.equals(med.index)
    assert out['med_dose'].tolist() == med['med_dose'].tolist()
    assert not any(c.startswith('_stage1_ord_') for c in out.columns)
    med = med.assign(base=out['_base_dose'])
    assert med['base'].equals(out['_base_dose'])


@pytest.mark.unit_conversion
def test_base_units_keep_input_order_for_multi_chunk_polars():
    """A polars frame of several chunks (concat) keeps its row order through
    stage 1, and the output carries no extra private column."""
    parts = [
        pl.DataFrame({
            'med_dose': [float(i * 30_000 + j) for j in range(30_000)][::-1],
            'med_dose_unit': ['mg/hr'] * 30_000,
        })
        for i in (3, 1, 2, 0)
    ]
    med = pl.concat(parts, rechunk=False)
    assert med.n_chunks() > 1
    out, _counts = standardize_dose_to_base_units(med)
    assert out['med_dose'].to_list() == med['med_dose'].to_list()
    assert set(out.columns) - set(med.columns) == {
        '_clean_unit', '_unit_class', '_base_dose', '_base_unit'}


# ===========================================
# What a 'duckdb' result reads
# ===========================================

def _detach_case():
    """A pandas med frame and a pandas vitals frame with numeric buffers to edit."""
    med, vitals = _order_case()
    med = med.to_pandas()
    vitals = vitals.to_pandas()
    return med, vitals


def _detach_entry_points():
    """{id: callable(med, vitals) -> a 'duckdb' main result} for the four entry points."""
    return {
        'converter': lambda med, vitals: convert_dose_units_by_med_category(
            med, vitals_df=vitals, preferred_units=ORDER_PREFS, override=True,
            return_format='duckdb')[0],
        'standardize_med_dose_units': lambda med, vitals: standardize_med_dose_units(
            med, ORDER_PREFS, vitals_df=vitals, return_format='duckdb')[0],
        'standardize_dose_to_base_units': lambda med, vitals: standardize_dose_to_base_units(
            med, vitals, return_format='duckdb')[0],
        'attach_weight': lambda med, vitals: attach_weight(
            med, vitals, return_format='duckdb'),
    }


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry_point', list(_detach_entry_points()))
def test_duckdb_result_is_detached_from_pandas_input(entry_point):
    """A later in-place edit of the caller's pandas frame must not reach a held relation."""
    med, vitals = _detach_case()
    rel = _detach_entry_points()[entry_point](med, vitals)
    before = rel.pl().sort('hospitalization_id')

    med['med_dose'].values[:] = 999.0
    if entry_point == 'attach_weight':
        vitals['vital_value'].values[:] = 1.0
    assert med['med_dose'].tolist() == [999.0] * 4  # the edit really reached the frame

    after = rel.pl().sort('hospitalization_id')
    assert_frame_equal(after, before)


@pytest.mark.unit_conversion
@pytest.mark.parametrize('level', [None, 'INFO'], ids=['default_level', 'info_level'])
def test_duckdb_relation_input_is_read_live_at_any_log_level(level):
    """A relation on the default connection is read in place, whatever the log level."""
    import logging

    med, vitals = _order_case()
    drip = med.filter(pl.col('med_category') == 'drip')  # noqa: F841 (read by name)
    table = '_t17_4_live_med'
    duckdb.sql(f"CREATE OR REPLACE TEMP TABLE {table} AS FROM drip")
    lg = logging.getLogger('clifpy.utils.unit_converter')
    previous_level = lg.level
    try:
        if level is not None:
            lg.setLevel(level)
        rel, _ = convert_dose_units_by_med_category(
            duckdb.table(table), vitals_df=vitals, preferred_units={'drip': 'mcg/min'},
            override=True, return_format='duckdb')
        duckdb.sql(f"UPDATE {table} SET med_dose = med_dose * 10")
        converted = sorted(rel.project('med_dose, med_dose_converted').fetchall())
    finally:
        lg.setLevel(previous_level)
        duckdb.sql(f"DROP TABLE IF EXISTS {table}")
    # mcg/kg/min x weight: doses 2 and 4 become 20 and 40, weights 50 and 80
    assert [dose for dose, _ in converted] == [20.0, 40.0]
    assert [conv for _, conv in converted] == pytest.approx([1000.0, 3200.0])


# Task 17.5: duration columns through the eager formats
# ===========================================

def _with_duration(med: pl.DataFrame, kind: str):
    """`med` plus a 90-minute `since_admit` column, as a polars, pandas or relation input."""
    if kind == 'polars':
        return med.with_columns(since_admit=pl.duration(minutes=90))
    if kind == 'pandas':
        out = med.to_pandas()
        out['since_admit'] = pd.to_timedelta(90, unit='min')
        return out
    return duckdb.from_arrow(med.to_arrow()).project("*, INTERVAL 90 MINUTE AS since_admit")


@pytest.mark.unit_conversion
@pytest.mark.parametrize('return_format', ['polars', 'polars_lazy', 'pandas'])
@pytest.mark.parametrize('entry_point', ENTRY_POINTS)
@pytest.mark.parametrize('kind', ['polars', 'pandas', 'relation'])
def test_duration_columns_survive_every_eager_format(kind, entry_point, return_format):
    """A caller's duration column (time since admission) comes back as a polars Duration."""
    med = _with_duration(_frame([('drug', 1.0, 'mg')]), kind)
    vitals = pl.DataFrame({
        'hospitalization_id': ['H0'], 'recorded_dttm': [TS],
        'vital_category': ['weight_kg'], 'vital_value': [WEIGHT_KG],
    })
    if entry_point == 'attach_weight':
        med = (med.project("* EXCLUDE (weight_kg)") if kind == 'relation'
               else med.drop(columns=['weight_kg']) if kind == 'pandas'
               else med.drop('weight_kg'))
        call = lambda: (attach_weight(med, vitals, return_format=return_format),)  # noqa: E731
    elif entry_point == 'convert':
        call = lambda: convert_dose_units_by_med_category(  # noqa: E731
            med, preferred_units={'drug': 'mcg'}, override=True, return_format=return_format)
    elif entry_point == 'standardize':
        call = lambda: standardize_med_dose_units(  # noqa: E731
            med, {'drug': 'mcg'}, return_format=return_format)
    else:
        call = lambda: standardize_dose_to_base_units(  # noqa: E731
            med, return_format=return_format)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', DeprecationWarning)
        result = call()[0]
    frame = _to_polars(result)
    assert frame.schema['since_admit'] == pl.Duration('us')
    assert frame['since_admit'].to_list() == [timedelta(minutes=90)]


# --- Stage 1 refuses columns it writes itself --------------------------------

@pytest.mark.unit_conversion
def test_stage1_refuses_stale_columns_after_in_place_overwrite():
    """6 mg/hr overwritten in place to 100 mcg/min keeps its stale `_clean_unit`.

    Before, stage 1 bound to the stale column and returned `_base_dose` 1666.67
    (scaled as mg/hr) with no error.
    """
    converted, _ = convert_dose_units_by_med_category(
        _frame([('norepinephrine', 6.0, 'mg/hr')]),
        preferred_units={'norepinephrine': 'mcg/min'},
        converted_dose_col='med_dose', converted_unit_col='med_dose_unit',
        override=True, show_intermediate=True)
    assert '_clean_unit' in converted.columns
    with pytest.raises(ValueError, match=r'^Column-name collision: ') as exc:
        standardize_dose_to_base_units(converted)
    assert '_clean_unit' in str(exc.value)
    assert 'drop' in str(exc.value).lower()


@pytest.mark.unit_conversion
@pytest.mark.parametrize('stale', [
    '_CLEAN_UNIT', '_Unit_Class', '_base_dose', '_BASE_UNIT',
    '_amount_multiplier', '_time_multiplier', '_weight_multiplier',
])
@pytest.mark.parametrize('show_intermediate', [False, True])
def test_stage1_refuses_every_column_it_writes(stale, show_intermediate):
    frame = _frame([('propofol', 10.0, 'mg/hr')]).with_columns(
        pl.lit('x').alias(stale))
    with pytest.raises(ValueError, match=r'^Column-name collision: ') as exc:
        standardize_dose_to_base_units(
            frame, show_intermediate=show_intermediate)
    assert stale in str(exc.value)


@pytest.mark.unit_conversion
def test_stage1_written_columns_are_pair_qa_columns():
    from clifpy.utils.unit_converter._base import STAGE1_COLUMNS
    assert STAGE1_COLUMNS <= PAIR_QA_COLUMNS


@pytest.mark.unit_conversion
def test_converter_refuses_a_stale_clean_unit_only_when_it_would_write_one():
    """On the default med_dose pair the lean output writes no `_clean_unit`,
    so a stale one is parked and returned; show_intermediate=True writes one,
    so there it is refused."""
    frame = _frame([('norepinephrine', 6.0, 'mg/hr')]).with_columns(
        pl.lit('stale').alias('_clean_unit'))
    out, _ = convert_dose_units_by_med_category(
        frame, preferred_units={'norepinephrine': 'mcg/min'}, override=True)
    assert out['_clean_unit'].to_list() == ['stale']
    assert out['med_dose_converted'].to_list() == [pytest.approx(100.0)]
    with pytest.raises(ValueError, match=r'^Column-name collision: '):
        convert_dose_units_by_med_category(
            frame, preferred_units={'norepinephrine': 'mcg/min'}, override=True,
            show_intermediate=True)


@pytest.mark.unit_conversion
def test_converter_still_parks_a_stale_clean_unit():
    """For a pair other than med_dose, the converter parks a stale `_clean_unit` before stage 1.

    The default pair parks it too unless show_intermediate=True (see the test
    above). Either way stage 1 never sees the column, so its path is unaffected.

    A second column pair is converted while the first pass's QA columns are
    still on the frame: they are parked, then returned unchanged.
    """
    frame = _frame([('norepinephrine', 6.0, 'mg/hr')]).with_columns(
        pl.col('med_dose').alias('rate'), pl.col('med_dose_unit').alias('rate_unit'),
        pl.lit('stale').alias('_clean_unit'))
    out, _ = convert_dose_units_by_med_category(
        frame, preferred_units={'norepinephrine': 'mcg/min'}, override=True,
        dose_col='rate', unit_col='rate_unit')
    assert out['rate_converted'].to_list() == [pytest.approx(100.0)]
    assert out['_clean_unit'].to_list() == ['stale']


# ===========================================
# Weight-lookup gate: NULL safety
# ===========================================

def _gate_case():
    """One row that triggers the weight lookup plus two rows with bad targets.

    `_frame` pre-fills weight_kg, which skips the lookup split entirely, so the
    column is dropped. The two mcg/kg/min rows are weighted sources whose
    explicit targets are blank / unknown: that is where the lookup predicate
    would be NULL without its COALESCE.
    """
    med = _frame([
        ('norepinephrine', 5.0, 'mcg/min'),
        ('propofol', 6.0, 'mcg/kg/min'),
        ('fentanyl', 7.0, 'mcg/kg/min'),
    ]).drop('weight_kg')
    vitals = pl.DataFrame({
        'hospitalization_id': ['H0', 'H1', 'H2'],
        'recorded_dttm': [TS - timedelta(hours=1)] * 3,
        'vital_category': ['weight_kg'] * 3,
        'vital_value': [50.0] * 3,
    })
    return med, vitals


def _assert_gate_rows_survive(out, counts):
    assert out.height == 3
    assert out['med_category'].to_list() == ['norepinephrine', 'propofol', 'fentanyl']
    assert out['med_dose'].to_list() == [5.0, 6.0, 7.0]
    status = out['_convert_status'].to_list()
    assert status[0] == 'success'
    assert status[1] == 'user-preferred unit  is not recognized'
    assert status[2] == 'user-preferred unit bananas/min is not recognized'
    assert counts['count'].sum() == 3


@pytest.mark.unit_conversion
def test_weight_lookup_split_keeps_rows_with_unrecognized_targets():
    """A NULL gate predicate would drop rows from both halves of the split.

    The split runs `WHERE p` and `WHERE NOT (p)`; a row whose predicate is NULL
    is in neither. Rows in must equal rows out, in input order.
    """
    med, vitals = _gate_case()
    out, counts = convert_dose_units_by_med_category(
        med, vitals_df=vitals, override=True,
        preferred_units={
            'norepinephrine': 'mcg/kg/min', 'propofol': '', 'fentanyl': 'bananas/min'})
    _assert_gate_rows_survive(out, counts)


@pytest.mark.unit_conversion
def test_standardize_weight_lookup_split_keeps_rows_with_unrecognized_targets():
    """The same through standardize_med_dose_units (default arguments).

    A blank schema unit is dropped as "no target" there, so the bad row uses a
    non-blank garbage unit.
    """
    med, vitals = _gate_case()
    out, counts = standardize_med_dose_units(
        med, vitals_df=vitals,
        targets={
            'norepinephrine': 'mcg/kg/min', 'propofol': 'bananas/min',
            'fentanyl': 'bananas/min'})
    assert out.height == 3
    assert out['med_category'].to_list() == ['norepinephrine', 'propofol', 'fentanyl']
    assert out['_convert_status'].to_list() == [
        'success',
        'user-preferred unit bananas/min is not recognized',
        'user-preferred unit bananas/min is not recognized',
    ]
    assert counts['count'].sum() == 3


@pytest.mark.unit_conversion
def test_weight_lookup_filter_is_never_null():
    """The gate predicate is NULL-safe over NULL, blank and unknown inputs."""
    from clifpy.utils.unit_converter._preferred import _weight_lookup_filter

    classes = [None, '', 'rate', 'amount', 'unrecognized']
    units = [None, '', 'mcg/min', 'mcg/kg/min', 'mcg', 'mcg/lb/hr', 'ml/hr',
             'units/kg/hr', 'mu/min', 'bananas/min']
    grid = pl.DataFrame(
        [(c, b, p, b) for c in classes for b in units for p in units],
        schema={'_unit_class': pl.String, '_base_unit': pl.String,
                '_preferred_unit_clean': pl.String, '_clean_unit': pl.String},
        orient='row')
    con = duckdb.connect()
    try:
        con.register('grid', grid.to_arrow())
        n_null, n_all = con.execute(
            f"SELECT count(*) FILTER (WHERE ({_weight_lookup_filter()}) IS NULL), "
            "count(*) FROM grid").fetchone()
    finally:
        con.close()
    assert n_all == len(classes) * len(units) ** 2
    assert n_null == 0


# ===========================================
# _convert_status_details, `targets`, `unlisted` (2026-11-19)
# ===========================================

@pytest.mark.unit_conversion
def test_convert_status_details_levels_from_the_converter():
    """One detail per outcome; `_convert_status` keeps its released strings."""
    out, counts = convert_dose_units_by_med_category(
        _frame([
            ('propofol', 10.0, 'mg/hr'),        # listed, factor applied
            ('fentanyl', 50.0, 'mcg/hr'),       # listed, already the target
            ('insulin', 2.0, 'Units/Hour'),     # listed, factor-1 respelling
            ('heparin', 3.0, 'mg'),             # unlisted: base unit is mcg
            ('vasopressin', 0.04, 'u/min'),     # unlisted, already the base unit
            ('drug_x', 1.0, 'mg/kg/dose'),      # listed, unrecognized source
        ]),
        preferred_units={'propofol': 'mcg/min', 'fentanyl': 'mcg/hr',
                         'insulin': 'u/hr', 'drug_x': 'mg'},
    )
    rows = _by_category(out)
    details = {c: r['_convert_status_details'] for c, r in rows.items()}
    assert details == {
        'propofol': 'success: converted to the target unit',
        'fentanyl': 'success: already the target unit',
        'insulin': 'success: already the target unit',
        'heparin': 'success: no target listed; converted to the base unit',
        'vasopressin': 'success: no target listed; already the base unit',
        'drug_x': 'original unit mg/kg/dose is not recognized',
    }
    assert {c: r['_convert_status'] for c, r in rows.items()} == {
        c: ('success' if d.startswith('success') else d) for c, d in details.items()}
    assert '_convert_status_details' in counts.columns


@pytest.mark.unit_conversion
@pytest.mark.parametrize('dose', [0.0, 0.1, 1e-12, 123456.789])
def test_already_levels_copy_the_dose_bit_exact(dose):
    """An 'already' detail is exactly a copied dose, zero included."""
    out, _ = convert_dose_units_by_med_category(
        _frame([('fentanyl', dose, 'MCG/HR'), ('vasopressin', dose, 'u/min')]),
        preferred_units={'fentanyl': 'mcg/hr'},
    )
    for row in out.iter_rows(named=True):
        assert 'already' in row['_convert_status_details']
        assert row['med_dose_converted'] == row['med_dose']


@pytest.mark.unit_conversion
def test_convert_status_details_is_named_per_pair():
    df = _frame([('sodium_chloride', 1.0, 'ml/hr')]).with_columns(
        volume_infusion_rate=pl.lit(0.5), volume_infusion_rate_unit=pl.lit('l/hr'))
    out, counts = convert_dose_units_by_med_category(
        df, preferred_units={'sodium_chloride': 'ml/hr'},
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    assert out['_convert_status_details__volume_infusion_rate'].to_list() == [
        'success: converted to the target unit']
    assert '_convert_status_details' not in out.columns
    assert '_convert_status_details__volume_infusion_rate' in counts.columns


@pytest.mark.unit_conversion
def test_standardize_keeps_unlisted_categories_as_charted():
    """No written-out target, no conversion: the dose stays, the unit is cleaned."""
    med = _frame([
        ('fentanyl', 50.0, 'mcg/hr'),
        ('insulin', 2.0, 'Units/Hour'),
        ('heparin', 3.0, 'mcg/lb/min'),
        ('alteplase', 4.0, None),
        ('aspirin', 1.0, 'tablet'),
        ('zosyn', 1.0, 'bogus'),
    ])
    out, _ = standardize_med_dose_units(med, {'fentanyl': 'mcg/hr'})
    rows = _by_category(out)
    assert (rows['insulin']['med_dose_converted'], rows['insulin']['med_dose_unit_converted']) == (2.0, 'units/hr')
    assert rows['insulin']['_convert_status_details'] == 'success: no target listed; unit cleaned only'
    assert (rows['heparin']['med_dose_converted'], rows['heparin']['med_dose_unit_converted']) == (3.0, 'mcg/lb/min')
    assert rows['alteplase']['_convert_status'] == 'original unit is missing'
    assert rows['aspirin']['_convert_status'] == (
        'original unit tablet is a countable dosage form; not convertible')
    assert rows['zosyn']['_convert_status'] == 'original unit bogus is not recognized'
    assert rows['fentanyl']['_convert_status_details'] == 'success: already the target unit'
    base, _ = standardize_med_dose_units(med, {'fentanyl': 'mcg/hr'}, unlisted='base')
    assert _by_category(base)['insulin']['med_dose_unit_converted'] == 'units/min'


@pytest.mark.unit_conversion
def test_converter_unlisted_defaults_to_base_and_keep_opts_in():
    rows = [('fentanyl', 50.0, 'mcg/hr'), ('heparin', 3.0, 'mg')]
    base, _ = _convert(rows, {'fentanyl': 'mcg/hr'})
    keep, _ = _convert(rows, {'fentanyl': 'mcg/hr'}, unlisted='keep')
    assert (base['heparin']['med_dose_converted'], base['heparin']['med_dose_unit_converted']) == (3000.0, 'mcg')
    assert (keep['heparin']['med_dose_converted'], keep['heparin']['med_dose_unit_converted']) == (3.0, 'mg')
    with pytest.raises(ValueError, match='unlisted'):
        _convert(rows, {'fentanyl': 'mcg/hr'}, unlisted='drop')


@pytest.mark.unit_conversion
def test_standardize_takes_targets_and_auto_picks_the_volume_column(tmp_path):
    p = tmp_path / 'mcide.csv'
    p.write_text('med_category,med_dose_unit,volume_infusion_rate_units\n'
                 'sodium_chloride,mg/hr,ml/hr\n')
    med = _frame([('sodium_chloride', 100.0, 'mg/hr')]).with_columns(
        volume_infusion_rate=pl.lit(2.0), volume_infusion_rate_unit=pl.lit('l/hr'))
    out, _ = standardize_med_dose_units(
        med, targets=p, dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    assert out['volume_infusion_rate_converted'][0] == pytest.approx(2000.0)
    assert out['volume_infusion_rate_unit_converted'][0] == 'ml/hr'
    dose, _ = standardize_med_dose_units(med, p)
    assert dose['med_dose_unit_converted'][0] == 'mg/hr'
    with pytest.raises(TypeError, match='targets'):
        standardize_med_dose_units(med, target_schema=p)
    # the old file-column names are gone too (forwarded, then refused)
    for old in ('schema_unit_col', 'schema_category_col'):
        with pytest.raises(TypeError, match=old):
            standardize_med_dose_units(med, p, **{old: 'med_dose_unit'})


@pytest.mark.unit_conversion
@pytest.mark.parametrize('path', ['targets.csv', Path('targets.yaml')])
def test_preferred_units_path_raises_type_error(path):
    with pytest.raises(TypeError, match='standardize_med_dose_units'):
        _convert([('fentanyl', 50.0, 'mcg/hr')], path)


@pytest.mark.unit_conversion
def test_standardize_warnings_do_not_say_schema(captured_warnings):
    standardize_med_dose_units(
        _frame([('fentanyl', 50.0, 'mcg/hr'), ('Fentanyl ', 5.0, 'mcg/hr')]),
        {'fentanyl': 'mcg/hr'})
    assert any("'Fentanyl '" in m for m in captured_warnings)
    assert not any('schema' in m.lower() for m in captured_warnings)


# ===========================================
# Lean default output (2026-11-19)
# ===========================================

LEAN_ADDS = {'med_dose_converted', 'med_dose_unit_converted',
             '_convert_status', '_convert_status_details', '_weight_source',
             '_weight_age_hours'}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry', ['convert', 'standardize'])
def test_default_output_is_lean(entry):
    """`_clean_unit` and `_unit_class` are QA columns: only
    show_intermediate=True and the counts table carry them."""
    med = _frame([('propofol', 10.0, 'mg/hr'), ('heparin', 1.0, 'tablet')])
    call = (convert_dose_units_by_med_category if entry == 'convert'
            else standardize_med_dose_units)
    # the converter's second positional parameter is vitals_df
    targets = {('preferred_units' if entry == 'convert' else 'targets'): {'propofol': 'mcg/min'}}
    out, counts = call(med, **targets)
    assert set(out.columns) - set(med.columns) == LEAN_ADDS
    assert _by_category(out)['propofol']['_convert_status_details'] == (
        'success: converted to the target unit')
    assert {'_clean_unit', '_unit_class'} <= set(counts.columns)
    full, _ = call(med, show_intermediate=True, **targets)
    assert {'_clean_unit', '_unit_class', '_base_unit'} <= set(full.columns)


@pytest.mark.unit_conversion
def test_lean_output_for_another_pair():
    med = _frame([('sodium_chloride', 1.0, 'ml/hr')]).with_columns(
        volume_infusion_rate=pl.lit(0.5), volume_infusion_rate_unit=pl.lit('l/hr'))
    out, counts = convert_dose_units_by_med_category(
        med, preferred_units={'sodium_chloride': 'ml/hr'},
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    assert out['_convert_status_details__volume_infusion_rate'].to_list() == [
        'success: converted to the target unit']
    assert set(out.columns) - set(med.columns) == {
        'volume_infusion_rate_converted', 'volume_infusion_rate_unit_converted',
        '_convert_status__volume_infusion_rate',
        '_convert_status_details__volume_infusion_rate', '_weight_source',
        '_weight_age_hours'}
    assert '_clean_unit__volume_infusion_rate' in counts.columns
    # the weight is shared by every pair, so its provenance is shown bare
    assert '_weight_source' in counts.columns


@pytest.mark.unit_conversion
def test_earlier_clean_unit_is_parked_and_returned():
    """A `_clean_unit` from an earlier show_intermediate pass (or the
    caller's own) is not this call's output any more: it is set aside for
    the run and comes back unchanged, and the run still cleans its units."""
    med = _frame([('propofol', 10.0, 'MG/HR')]).with_columns(
        _clean_unit=pl.lit('stale'), _unit_class=pl.lit('stale too'))
    out, counts = convert_dose_units_by_med_category(
        med, preferred_units={'propofol': 'mcg/hr'})
    row = out.row(0, named=True)
    assert (row['_clean_unit'], row['_unit_class']) == ('stale', 'stale too')
    assert row['med_dose_converted'] == pytest.approx(10_000.0)
    assert counts['_clean_unit'].to_list() == ['mg/hr']


# ===========================================
# allow_weight_variant (2026-11-19)
# ===========================================

LB_PER_KG = 2.20462   # 1 mg/lb/hr = 2.20462 mg/kg/hr


def _unweighed(rows):
    """`_frame(rows)` without weight_kg: any lookup would need vitals."""
    return _frame(rows).drop('weight_kg')


def _vitals_frame(weight=WEIGHT_KG, n=8):
    """One weight per stay H0..H{n-1}, charted before TS."""
    return pl.DataFrame({
        'hospitalization_id': [f'H{i}' for i in range(n)],
        'recorded_dttm': [TS - timedelta(hours=1)] * n,
        'vital_category': ['weight_kg'] * n,
        'vital_value': [weight] * n,
    }, schema={'hospitalization_id': pl.String, 'recorded_dttm': pl.Datetime('us', 'UTC'),
               'vital_category': pl.String, 'vital_value': pl.Float64})


VARIANT_ROWS = [
    ('a', 50.0, 'mcg/hr'),       # already the target
    ('b', 3.0, 'mcg/kg/hr'),     # already its weight variant
    ('c', 2.0, 'mg/kg/hr'),      # converted to the weight variant
    ('d', 1.0, 'mg/lb/hr'),      # /lb folds into /kg
    ('e', 1.0, 'mg/min'),        # converted to the target itself
]
VARIANT_TARGETS = dict.fromkeys('abcde', 'mcg/hr')


@pytest.mark.unit_conversion
def test_weight_variant_accepts_the_target_or_its_weight_variant():
    """Target mcg/hr: mcg/hr and mcg/kg/hr stay as charted, mg/kg/hr becomes
    mcg/kg/hr (not mcg/hr), and no weight is read: no vitals_df, no
    weight_kg, and `_weight_source = 'not_needed'` throughout."""
    out, counts = standardize_med_dose_units(
        _unweighed(VARIANT_ROWS), VARIANT_TARGETS, allow_weight_variant=True)
    rows = _by_category(out)
    got = {c: (r['med_dose_converted'], r['med_dose_unit_converted'],
               r['_convert_status_details']) for c, r in rows.items()}
    assert got == {
        'a': (50.0, 'mcg/hr', 'success: already the target unit'),
        'b': (3.0, 'mcg/kg/hr', 'success: already the weight variant of the target'),
        'c': (pytest.approx(2000.0), 'mcg/kg/hr',
              'success: converted to the weight variant of the target'),
        'd': (pytest.approx(1000.0 * LB_PER_KG, rel=1e-5), 'mcg/kg/hr',
              'success: converted to the weight variant of the target'),
        'e': (pytest.approx(60_000.0), 'mcg/hr', 'success: converted to the target unit'),
    }
    assert {r['_convert_status'] for r in rows.values()} == {'success'}
    assert (out['_weight_source'] == 'not_needed').all()
    assert '_preferred_is_weight_variant' not in out.columns
    assert set(counts['_convert_status_details']) >= {
        'success: already the weight variant of the target',
        'success: converted to the weight variant of the target'}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('target, source, dose, expected', [
    # a weighted target accepts the unweighted variant
    ('mcg/kg/min', 'mcg/min', 5.0, (5.0, 'mcg/min', 'already the weight variant')),
    ('mcg/kg/min', 'mg/hr', 6.0, (100.0, 'mcg/min', 'converted to the weight variant')),
    # units: the label is the variant in mCIDE spelling
    ('units/hr', 'units/kg/hr', 2.0, (2.0, 'units/kg/hr', 'already the weight variant')),
    ('units/hr', 'u/kg/min', 1.0, (60.0, 'units/kg/hr', 'converted to the weight variant')),
    # weighted amounts
    ('mg', 'mcg/kg', 500.0, (0.5, 'mg/kg', 'converted to the weight variant')),
    # same weight basis as the target: plain conversion
    ('mcg/kg/min', 'mcg/kg/hr', 60.0, (1.0, 'mcg/kg/min', 'converted to the target unit')),
])
def test_weight_variant_cases(target, source, dose, expected):
    out, _ = standardize_med_dose_units(
        _unweighed([('x', dose, source)]), {'x': target}, allow_weight_variant=True)
    row = out.row(0, named=True)
    assert row['med_dose_converted'] == pytest.approx(expected[0])
    assert row['med_dose_unit_converted'] == expected[1]
    assert row['_convert_status_details'] == f'success: {expected[2]}' + (
        ' of the target' if 'variant' in expected[2] else '')


@pytest.mark.unit_conversion
def test_weight_variant_on_the_volume_pair():
    med = _unweighed([('sodium_chloride', 1.0, 'mg/hr')]).with_columns(
        volume_infusion_rate=pl.lit(2.0), volume_infusion_rate_unit=pl.lit('ml/kg/hr'))
    out, _ = standardize_med_dose_units(
        med, {'sodium_chloride': 'ml/hr'}, allow_weight_variant=True,
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    row = out.row(0, named=True)
    assert row['volume_infusion_rate_converted'] == 2.0
    assert row['volume_infusion_rate_unit_converted'] == 'ml/kg/hr'
    assert row['_convert_status_details__volume_infusion_rate'] == (
        'success: already the weight variant of the target')


EDGE_ROWS = [
    ('gas', 20.0, 'ppm'),             # standalone target, same unit
    ('gas2', 1.0, 'mcg/kg/min'),      # standalone target, rate source
    ('cells', 2.0, 'cells'),          # standalone count
    ('pill', 1.0, 'tablet'),          # countable
    ('junk', 1.0, 'bogus'),           # unrecognized
    ('bolus', 1.0, 'mg'),             # amount source, rate target
    ('pit', 1.0, 'mu/min'),           # milli-units -> million-units: refused
    ('neb', 1.0, 'mg/kg/hr'),         # mass source, volume target
]
EDGE_TARGETS = {'gas': 'ppm', 'gas2': 'ppm', 'cells': 'cells', 'pill': 'mg',
                'junk': 'mg/hr', 'bolus': 'mcg/hr', 'pit': 'million-units/min',
                'neb': 'ml/hr'}


@pytest.mark.unit_conversion
def test_weight_variant_leaves_other_outcomes_alone():
    """Standalone, countable, unrecognized, rate <-> amount, the MU refusal
    and the subclass checks read the same with the flag on as off."""
    off, _ = standardize_med_dose_units(_frame(EDGE_ROWS), EDGE_TARGETS)
    on, _ = standardize_med_dose_units(
        _frame(EDGE_ROWS), EDGE_TARGETS, allow_weight_variant=True)
    cols = ['med_category', 'med_dose_converted', 'med_dose_unit_converted',
            '_convert_status', '_convert_status_details']
    assert_frame_equal(on.select(cols), off.select(cols))
    assert set(off['_convert_status']) > {'success'}


@pytest.mark.unit_conversion
def test_weight_variant_off_by_default_crosses_the_weight_axis():
    """The default (exact target) is today's conversion: mcg/kg/hr against
    mcg/hr multiplies by the weight, and without one the row fails."""
    out, _ = standardize_med_dose_units(_frame(VARIANT_ROWS), VARIANT_TARGETS)
    assert_frame_equal(
        out, standardize_med_dose_units(
            _frame(VARIANT_ROWS), VARIANT_TARGETS, allow_weight_variant=False)[0])
    rows = _by_category(out)
    assert rows['b']['med_dose_converted'] == pytest.approx(3.0 * WEIGHT_KG)
    assert rows['b']['med_dose_unit_converted'] == 'mcg/hr'
    assert rows['d']['med_dose_converted'] == pytest.approx(1000.0 * LB_PER_KG * WEIGHT_KG, rel=1e-5)
    unweighed, _ = standardize_med_dose_units(_unweighed(VARIANT_ROWS), VARIANT_TARGETS)
    assert _by_category(unweighed)['b']['_convert_status'] == (
        'cannot convert weighted to unweighted: weight_kg is missing')


@pytest.mark.unit_conversion
@pytest.mark.parametrize('weights', ['lookup', 'prefilled'])
def test_exact_mode_crosses_the_weight_axis_both_ways(weights):
    """standardize with the exact target, in both directions, with the weight
    looked up from vitals and pre-filled."""
    rows = [('fentanyl', 2.0, 'mcg/kg/hr'), ('norepinephrine', 7.0, 'mcg/min')]
    targets = {'fentanyl': 'mcg/hr', 'norepinephrine': 'mcg/kg/min'}
    if weights == 'lookup':
        out, _ = standardize_med_dose_units(
            _unweighed(rows), targets, vitals_df=_vitals_frame())
        expected_source = 'asof'
    else:
        out, _ = standardize_med_dose_units(_frame(rows), targets)
        expected_source = 'prefilled'
    got = _by_category(out)
    assert got['fentanyl']['med_dose_converted'] == pytest.approx(2.0 * WEIGHT_KG)
    assert got['fentanyl']['med_dose_unit_converted'] == 'mcg/hr'
    assert got['norepinephrine']['med_dose_converted'] == pytest.approx(7.0 / WEIGHT_KG)
    assert got['norepinephrine']['med_dose_unit_converted'] == 'mcg/kg/min'
    assert {r['_weight_source'] for r in got.values()} == {expected_source}
    assert {r['_convert_status_details'] for r in got.values()} == {
        'success: converted to the target unit'}


@pytest.mark.unit_conversion
def test_volume_pair_crosses_the_weight_axis_via_a_lookup():
    """Loose end 8: the volume pair's ml/kg/hr <-> ml/hr reads weights from
    vitals_df like the dose pair does."""
    med = _unweighed([('sodium_chloride', 1.0, 'mg/hr'), ('lactated_ringers', 1.0, 'mg/hr')]
                     ).with_columns(
        volume_infusion_rate=pl.Series([2.0, 140.0]),
        volume_infusion_rate_unit=pl.Series(['ml/kg/hr', 'ml/hr']))
    out, _ = standardize_med_dose_units(
        med, {'sodium_chloride': 'ml/hr', 'lactated_ringers': 'ml/kg/hr'},
        vitals_df=_vitals_frame(),
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    got = _by_category(out)
    assert got['sodium_chloride']['volume_infusion_rate_converted'] == pytest.approx(2.0 * WEIGHT_KG)
    assert got['lactated_ringers']['volume_infusion_rate_converted'] == pytest.approx(140.0 / WEIGHT_KG)
    assert {r['_weight_source'] for r in got.values()} == {'asof'}


@pytest.mark.unit_conversion
def test_allow_weight_variant_is_keyword_only_everywhere():
    for fn in (convert_dose_units_by_med_category, standardize_med_dose_units):
        p = inspect.signature(fn).parameters['allow_weight_variant']
        assert p.kind is p.KEYWORD_ONLY and p.default is False
    out, _ = convert_dose_units_by_med_category(
        _unweighed(VARIANT_ROWS), preferred_units=VARIANT_TARGETS,
        allow_weight_variant=True)
    assert _by_category(out)['b']['med_dose_unit_converted'] == 'mcg/kg/hr'


@pytest.mark.unit_conversion
def test_weight_variant_leaves_a_per_lb_target_alone():
    """Review finding: `/lb` and `/kg` are both weighted, so a per-lb target
    crosses no weight axis and needs no variant. A row already in it stays
    'already the target unit'; a per-kg row converts to it exactly as with
    the flag off (no weight needed either way)."""
    rows = [('a', 10.0, 'mcg/lb/hr'), ('b', 22.0462, 'mcg/kg/hr')]
    on, _ = standardize_med_dose_units(
        _unweighed(rows), {'a': 'mcg/lb/hr', 'b': 'mcg/lb/hr'}, allow_weight_variant=True)
    off, _ = standardize_med_dose_units(_unweighed(rows), {'a': 'mcg/lb/hr', 'b': 'mcg/lb/hr'})
    cols = ['med_category', 'med_dose_converted', 'med_dose_unit_converted',
            '_convert_status_details']
    assert_frame_equal(on.select(cols), off.select(cols))
    got = _by_category(on)
    assert (got['a']['med_dose_converted'], got['a']['med_dose_unit_converted']) == (10.0, 'mcg/lb/hr')
    assert got['a']['_convert_status_details'] == 'success: already the target unit'
    assert got['b']['med_dose_converted'] == pytest.approx(10.0, rel=1e-5)


@pytest.mark.unit_conversion
def test_weight_variant_costs_no_more_than_exact_mode():
    """Review finding: the flag's per-row rewrite (IN lists of every
    acceptable unit, regexes) roughly doubled the time of a call with
    pre-filled weights. It is now resolved per target, so the flag may not
    make a call markedly slower. Timed on the warm path, best of three."""
    import time
    n = 200_000
    units = ['mcg/hr', 'mcg/kg/hr', 'mg/kg/hr', 'mg/min', 'units/hr']
    med = pl.DataFrame({
        'hospitalization_id': [f'H{i % 500}' for i in range(n)],
        'med_category': [f'c{i % 5}' for i in range(n)],
        'med_dose': [1.0] * n,
        'med_dose_unit': [units[i % 5] for i in range(n)],
        'weight_kg': [WEIGHT_KG] * n,
        'admin_dttm': [TS] * n,
    }, schema_overrides={'admin_dttm': pl.Datetime('us', 'UTC')})
    targets = {'c0': 'mcg/hr', 'c1': 'mcg/hr', 'c2': 'mcg/hr', 'c3': 'mcg/kg/min', 'c4': 'units/kg/hr'}

    def best(**kw):
        times = []
        for _ in range(3):
            t0 = time.perf_counter()
            convert_dose_units_by_med_category(med, preferred_units=targets, **kw)
            times.append(time.perf_counter() - t0)
        return min(times)

    best()   # warm up
    off, on = best(), best(allow_weight_variant=True)
    assert on < 1.3 * off + 0.05, f'flag on {on:.3f}s vs off {off:.3f}s'


# ===========================================
# Countable targets: no conversion needed when the unit already matches
# ===========================================

COUNTABLE_ROWS = [
    ('a', 2.0, 'Dose'),        # same countable form as the target, other spelling
    ('b', 1.0, 'tablet'),      # same form
    ('c', 5.0, 'mg'),          # a measured amount against a countable target
    ('d', 1.0, 'puff'),        # another countable form
]
COUNTABLE_TARGETS = {'a': 'dose', 'b': 'tablet', 'c': 'dose', 'd': 'tablet'}


@pytest.mark.unit_conversion
@pytest.mark.parametrize('override', [True, False])
def test_countable_target_already_matching_is_success(override):
    """A target that is the row's own countable form needs no conversion:
    'success', the dose copied, the caller's spelling of the target. Other
    rows against a countable target still fail, now saying why."""
    out, counts = convert_dose_units_by_med_category(
        _unweighed(COUNTABLE_ROWS), preferred_units=COUNTABLE_TARGETS, override=override)
    got = {c: (r['med_dose_converted'], r['med_dose_unit_converted'], r['_convert_status'],
               r['_convert_status_details']) for c, r in _by_category(out).items()}
    assert got == {
        'a': (2.0, 'dose', 'success', 'success: already the target unit'),
        'b': (1.0, 'tablet', 'success', 'success: already the target unit'),
        'c': (5.0, 'mg', 'cannot convert amount to countable', 'cannot convert amount to countable'),
        'd': (1.0, 'puff', 'original unit puff is a countable dosage form; not convertible',
              'original unit puff is a countable dosage form; not convertible'),
    }
    assert set(out['_weight_source']) == {'not_needed'}
    assert _classes(counts) == {'a': 'countable', 'b': 'countable', 'c': 'amount', 'd': 'countable'}


@pytest.mark.unit_conversion
def test_custom_countable_target_matches_like_the_default_ones():
    rows = [('x', 3.0, 'Troche')]
    out, _ = convert_dose_units_by_med_category(
        _unweighed(rows), preferred_units={'x': 'troche'}, override=False,
        countable_units=set(DEFAULT_COUNTABLE_UNITS) | {'troche'})
    assert out.row(0, named=True)['_convert_status'] == 'success'
    # without it, 'troche' is no known unit: refused up front in strict mode
    with pytest.raises(ValueError, match='Cannot accommodate'):
        convert_dose_units_by_med_category(
            _unweighed(rows), preferred_units={'x': 'troche'}, override=False)


@pytest.mark.unit_conversion
def test_unlisted_countable_rows_report_as_before():
    """D-261119-unlisted ('Report as today' for bad units): a countable row
    whose category has no target is not turned into a success by unlisted='keep'."""
    out, _ = standardize_med_dose_units(_unweighed([('a', 2.0, 'dose'), ('z', 1.0, 'mg')]),
                                        {'z': 'mg'})
    assert _by_category(out)['a']['_convert_status'] == (
        'original unit dose is a countable dosage form; not convertible')


# ===========================================
# Counts column order, spelled-out units, picograms (2026-11-19)
# ===========================================

REVIEW_ORDER = ['med_category', 'med_dose_unit', '_preferred_unit', 'med_dose_unit_converted',
                '_convert_status_details', '_weight_source', 'count',
                '_clean_unit', '_base_unit', '_unit_class', '_convert_status']


@pytest.mark.unit_conversion
@pytest.mark.parametrize('entry', ['convert', 'standardize'])
def test_counts_columns_lead_with_the_review_columns(entry):
    med = _frame([('propofol', 10.0, 'mg/hr'), ('heparin', 1.0, 'tablet')])
    if entry == 'convert':
        _, counts = convert_dose_units_by_med_category(med, preferred_units={'propofol': 'mcg/min'})
    else:
        _, counts = standardize_med_dose_units(med, {'propofol': 'mcg/min'})
    assert counts.columns == REVIEW_ORDER


@pytest.mark.unit_conversion
def test_counts_columns_lead_with_the_review_columns_for_another_pair():
    med = _frame([('sodium_chloride', 1.0, 'ml/hr')]).with_columns(
        volume_infusion_rate=pl.lit(0.5), volume_infusion_rate_unit=pl.lit('l/hr'))
    _, counts = convert_dose_units_by_med_category(
        med, preferred_units={'sodium_chloride': 'ml/hr'},
        dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    suffixed = {'_preferred_unit', '_convert_status_details', '_clean_unit', '_base_unit',
                '_unit_class', '_convert_status'}
    assert counts.columns == [
        {'med_dose_unit': 'volume_infusion_rate_unit',
         'med_dose_unit_converted': 'volume_infusion_rate_unit_converted'}.get(c, c)
        + ('__volume_infusion_rate' if c in suffixed else '')
        for c in REVIEW_ORDER]


SHORT_TOKEN = r'^(u|mu|mnu)(/|$)'


@pytest.mark.unit_conversion
def test_units_are_spelled_out_wherever_they_leave_clifpy():
    """No `u` / `mu` / `mnu` reaches a user: cleaned, base, unlisted-target
    and failed-row units are spelled out, in the rows and the counts. A
    target the caller wrote is still echoed as written."""
    rows = [('a', 2.0, 'Units/Hour'), ('b', 1.0, 'mU/min'), ('c', 1.0, 'million units'),
            ('d', 3.0, 'u/hr'), ('e', 1.0, 'units/dose'), ('f', 4.0, 'u/hr')]
    targets = {'a': 'u/hr', 'd': 'mg/hr'}   # f is unlisted
    for unlisted in ('base', 'keep'):
        out, counts = convert_dose_units_by_med_category(
            _frame(rows), preferred_units=targets, show_intermediate=True, unlisted=unlisted)
        got = _by_category(out)
        assert got['a']['med_dose_unit_converted'] == 'u/hr'          # the caller's spelling
        assert got['a']['_clean_unit'] == 'units/hr'
        assert got['a']['_base_unit'] == 'units/min'
        assert got['b']['_clean_unit'] == 'milli-units/min'
        assert got['c']['_clean_unit'] == 'million-units'
        assert got['d']['_convert_status'] == 'cannot convert unit to mass'
        assert got['d']['med_dose_unit_converted'] == 'units/hr'      # failed row: cleaned source
        assert got['e']['_convert_status'] == 'original unit units/dose is not recognized'
        assert got['f']['_preferred_unit'] == ('units/hr' if unlisted == 'keep' else 'units/min')
        for frame in (out.drop('med_dose_unit'), counts.drop('med_dose_unit')):
            for col, dtype in frame.schema.items():
                if dtype == pl.String:
                    values = frame.filter(pl.col('med_category') != 'a')[col] if col in (
                        'med_dose_unit_converted', '_preferred_unit') else frame[col]
                    assert not values.str.contains(SHORT_TOKEN).any(), (unlisted, col)


@pytest.mark.unit_conversion
def test_base_unit_function_spells_units_out():
    out, counts = standardize_dose_to_base_units(_frame([('a', 2.0, 'Units/Hour'), ('b', 1.0, 'unk')]))
    got = _by_category(out)
    assert (got['a']['_clean_unit'], got['a']['_base_unit']) == ('units/hr', 'units/min')
    assert (got['b']['_clean_unit'], got['b']['_base_unit']) == ('unk', 'unk')   # not 'unitsnk'
    assert sorted(counts['_base_unit'].to_list()) == ['units/min', 'unk']


@pytest.mark.unit_conversion
@pytest.mark.parametrize('raw, dose, target, expected', [
    ('pg/kg/min', 1000.0, 'ng/kg/min', 1.0),
    ('PG/KG/MIN', 500.0, 'mcg/kg/min', 0.0005),
    ('picograms/hr', 60.0, 'pg/min', 1.0),
    ('picogram', 2000.0, 'ng', 2.0),
    ('mcg/hr', 1.0, 'pg/hr', 1_000_000.0),
])
def test_picograms_convert(raw, dose, target, expected):
    out, _ = convert_dose_units_by_med_category(
        _frame([('x', dose, raw)]), preferred_units={'x': target}, override=False)
    row = out.row(0, named=True)
    assert row['_convert_status'] == 'success'
    assert row['med_dose_converted'] == pytest.approx(expected)
    assert row['med_dose_unit_converted'] == target


@pytest.mark.unit_conversion
def test_targets_file_messages_do_not_say_schema(tmp_path, captured_warnings):
    """The duplicate-targets WARNING and the targets-file errors name a
    'targets file', not a 'schema' (the word users found confusing)."""
    dup = tmp_path / 'dup.csv'
    dup.write_text('med_category,med_dose_unit\nterbutaline,mg\nterbutaline,mcg/kg/min\n')
    load_dose_unit_targets(dup)
    assert any('more than one target unit' in m for m in captured_warnings)
    assert not any('schema' in m.lower() for m in captured_warnings)
    bad_cols = tmp_path / 'cols.csv'
    bad_cols.write_text('category,unit\nx,mg\n')
    with pytest.raises(ValueError) as exc:
        load_dose_unit_targets(bad_cols)
    assert 'schema' not in str(exc.value).lower()
    nested = tmp_path / 'nested.yaml'
    nested.write_text('targets:\n  x:\n    unit: mg\n')
    with pytest.raises(ValueError) as exc:
        load_dose_unit_targets(nested)
    assert 'schema' not in str(exc.value).lower()


@pytest.mark.unit_conversion
def test_unrecognized_target_warns_once_under_override(captured_warnings):
    """The converter checks the explicit targets up front; stage 2 used to
    repeat the same 'Cannot accommodate' WARNING. A direct stage-2 call
    still checks on its own."""
    convert_dose_units_by_med_category(
        _frame([('x', 1.0, 'mg/hr')]), preferred_units={'x': 'bogus/hr'}, override=True)
    assert len([m for m in captured_warnings if 'Cannot accommodate' in m]) == 1
