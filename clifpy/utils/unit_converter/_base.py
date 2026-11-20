"""Stage 1: raw dose units -> canonical base units.

Normalises the amount axis (mass->mcg, volume->ml, unit->u) and the time axis
(->/min), and collapses `/lb` into `/kg` by a constant factor. Stage 1 never
references patient weight.
"""

from __future__ import annotations

import uuid

import duckdb
import polars as pl
from typing import Tuple, Collection, FrozenSet, Optional

from clifpy.utils.logging_config import get_logger

from ._grammar import (
    _canonical_spelling_expr,
    KG_PER_LB, AMOUNT_FACTOR_PATTERNS, TIME_FACTOR_PATTERNS,
    SUBCLASS_REGEX, MASS_REGEX, VOLUME_REGEX, UNIT_REGEX,
    LB_REGEX, WEIGHT_REGEX, RATE_UNITS_STR, AMOUNT_UNITS_STR,
    DEFAULT_COUNTABLE_UNITS, _convert_set_to_str_for_sql,
)
from ._sql import (
    _concat_builders_by_patterns,
    _pattern_to_factor_builder_for_base,
    _base_unit_case_ladder,
)
from ._clean import _clean_dose_unit_formats_duckdb, _clean_dose_unit_names_duckdb
from ._counts import _create_unit_conversion_counts_table
from ._inputs import (
    FrameLike,
    check_no_shadowing_tables,
    column_names,
    pandas_index,
    read_input,
)
from ._outputs import ReturnFormat, emit, resolve_return_format

logger = get_logger('utils.unit_converter')


def _normalize_countable_units(
    countable_units: Optional[Collection[str]] = None,
) -> FrozenSet[str]:
    """Clean a countable dosage-form vocabulary exactly as `_clean_unit` is cleaned.

    Stage 1 compares this set with `_clean_unit`, which has already been
    lowercased, stripped of whitespace and a trailing period, and
    spelling-normalised. The caller's tokens get the same two passes, so
    `{'Troche', 'Lozenge.'}` matches data charted `troche` or `LOZENGE`.

    Parameters
    ----------
    countable_units : collection of str, optional
        None selects `DEFAULT_COUNTABLE_UNITS`. A collection REPLACES the
        defaults rather than extending them; pass
        `set(DEFAULT_COUNTABLE_UNITS) | {'troche'}` to extend.

    Returns
    -------
    frozenset of str
        Cleaned tokens. Tokens that clean to NULL (blank, or a missing-unit
        placeholder such as 'unknown') are dropped. The result is still
        unescaped; `_convert_set_to_str_for_sql` quotes it for SQL.

    Raises
    ------
    TypeError
        If `countable_units` is a single string, which would otherwise be
        iterated as one-letter tokens.

    Examples
    --------
    >>> sorted(_normalize_countable_units({'Troche', ' Lozenge. '}))
    ['lozenge', 'troche']
    """
    if countable_units is None:
        countable_units = DEFAULT_COUNTABLE_UNITS
    elif isinstance(countable_units, str):
        raise TypeError(
            "countable_units must be a collection of unit strings, not a single "
            f"string; pass {{{countable_units!r}}} rather than {countable_units!r}"
        )
    raw = sorted({str(token) for token in countable_units if token is not None})
    if not raw:
        return frozenset()
    # The tokens reach DuckDB as data in a polars frame, never as SQL text, and
    # come back through fetchall(): no pandas (D-261022-pandas-free-backend).
    tokens = pl.DataFrame({'_countable_raw': raw}, schema={'_countable_raw': pl.String})
    rel = _clean_dose_unit_formats_duckdb(
        tokens, col='_countable_raw', out_col='_countable_clean',
    )
    rel = _clean_dose_unit_names_duckdb(rel, col='_countable_clean')
    return frozenset(
        token for (token,) in rel.project('_countable_clean').fetchall()
        if token is not None
    )


def _convert_clean_units_to_base_units(
    med_df: duckdb.DuckDBPyRelation,
    show_intermediate: bool = False,
    countable_units: Collection[str] | None = None
) -> duckdb.DuckDBPyRelation:
    """Convert clean dose units to base units (weight-preserving).

    Stage-1 of the conversion pipeline. Normalizes the **amount** axis
    (mass→mcg, volume→ml, unit→u) and the **time** axis (/hr→/min) but
    preserves the weight qualifier (`/kg`, `/lb`, or none) verbatim from
    `_clean_unit`. Stage 1 never references `weight_kg` — patient weight is
    consumed only in stage 2 (preferred-unit conversion), and only when source
    and target differ in *presence* of a weight qualifier.

    Parameters
    ----------
    med_df : duckdb.DuckDBPyRelation
        Medication data with required columns:

        - `_clean_unit`: cleaned unit strings (after format + name cleaning)
        - `med_dose`: original dose values

        `weight_kg` is **not** required by this stage; it is ignored if present.

    show_intermediate : bool, default False
        If True, expose intermediate `_amount_multiplier` and `_time_multiplier`
        columns for QA. If False, inline the multiplier expressions.

    countable_units : collection of str, optional
        Countable dosage-form vocabulary, cleaned by
        `_normalize_countable_units`. None uses `DEFAULT_COUNTABLE_UNITS`; a
        collection replaces it.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Input columns plus:

        - `_unit_class`: 'rate', 'amount', 'countable', or 'unrecognized'
        - `_base_dose`: dose in base units (no weight scaling applied)
        - `_base_unit`: base unit, preserving weight qualifier
            * mass rate `mcg/kg/hr` → `mcg/kg/min`
            * volume rate `l/hr` → `ml/min`
            * unit rate `mu/min` → `u/min`
            * mass amount `mg` → `mcg`
            * unrecognized → original `_clean_unit`

    Notes
    -----
    Stage 1 is bit-exact: every multiplier is a constant (no `weight_kg`),
    so float drift is bounded by the amount/time factors only. Identity
    conversions (e.g. `mcg/kg/min` → `mcg/kg/min`) preserve the input dose
    exactly via factor 1×1.
    """

    # Countable dosage forms pass through untouched, exactly like unrecognized
    # units; only the reported class and status differ. The vocabulary is
    # cleaned exactly as `_clean_unit` was, and quote-escaped, before it is
    # inlined into SQL.
    countable_units_str = _convert_set_to_str_for_sql(
        _normalize_countable_units(countable_units)
    )
    # Classes whose dose must not be scaled.
    passthrough = "('unrecognized', 'countable')"
    # ONE classification CASE shared by both query shapes below. The
    # show_intermediate branch used to carry its own copy, which could drift
    # from the default one unnoticed (audit M90, mutant B3).
    unit_class_expr = (
        f"CASE "
        f"WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate' "
        f"WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount' "
        f"WHEN _clean_unit IN ('{countable_units_str}') THEN 'countable' "
        f"ELSE 'unrecognized' END"
    )

    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=AMOUNT_FACTOR_PATTERNS,
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=TIME_FACTOR_PATTERNS,
        else_case='1'
        )

    # Stage 1 weight handling: collapse `/lb` into the canonical `/kg` axis
    # using a CONSTANT factor (no patient weight). The base unit's weight
    # qualifier is in {'/kg', ''} only — never `/lb`. This keeps the base set
    # as small as possible, in line with the goal of `_base_unit` being a
    # canonical pivot.
    #
    # - `weight_const_expr`: 1 for `/kg` or unweighted, KG_PER_LB (2.20462)
    #   for `/lb`. NOT a function of patient weight.
    # - `base_weight_qual_expr`: '/kg' if source had `/kg` OR `/lb`, else ''.
    #   This is INTENTIONALLY different from `_weight_qual_clause()`, which
    #   preserves the actual `/kg` vs `/lb` axis for stage 2's transition
    #   factor. Here in stage 1 we collapse both into `/kg` because the
    #   canonical `_base_unit` never carries `/lb`. The two expressions are
    #   not interchangeable.
    weight_const_expr = (
        f"CASE WHEN regexp_matches(_clean_unit, '{LB_REGEX}') THEN {KG_PER_LB} "
        f"ELSE 1 END"
    )
    base_weight_qual_expr = (
        f"CASE WHEN regexp_matches(_clean_unit, '{WEIGHT_REGEX}') THEN '/kg' "
        f"ELSE '' END"
    )
    # One branch pair per subclass, derived from SUBCLASS_SPEC. Standalone
    # subclasses (ppm, cells) get a single amount branch with no qualifier.
    base_unit_ladder = _base_unit_case_ladder(base_weight_qual_expr)

    if show_intermediate:
        q = f"""
        SELECT *
            , _unit_class: {unit_class_expr}
            , _amount_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({amount_clause}) END
            , _time_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({time_clause}) END
            , _weight_multiplier: CASE
                WHEN _unit_class IN {passthrough} THEN 1 ELSE ({weight_const_expr}) END
            -- amount + time + (constant) lb→kg scaling. No patient weight.
            , _base_dose: CASE
                WHEN _unit_class IN {passthrough} THEN med_dose
                ELSE med_dose * _amount_multiplier * _time_multiplier * _weight_multiplier
                END
            -- base unit collapses /lb into /kg; unweighted stays unweighted
            , _base_unit: CASE
                WHEN _unit_class IN {passthrough} THEN _clean_unit
                {base_unit_ladder}
                END
        FROM med_df
        """
    else:
        amount_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({amount_clause}) END"
        time_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({time_clause}) END"
        weight_expr = f"CASE WHEN _unit_class IN {passthrough} THEN 1 ELSE ({weight_const_expr}) END"

        q = f"""
        WITH classified AS (
            SELECT *
                , _unit_class: {unit_class_expr}
            FROM med_df
        )
        SELECT *
            , _base_dose: CASE
                WHEN _unit_class IN {passthrough} THEN med_dose
                ELSE med_dose * ({amount_expr}) * ({time_expr}) * ({weight_expr})
                END
            , _base_unit: CASE
                WHEN _unit_class IN {passthrough} THEN _clean_unit
                {base_unit_ladder}
                END
        FROM classified
        """
    return duckdb.sql(q)


# The columns stage 1 writes (the multipliers only under show_intermediate).
# An input column of one of these names would be read in place of the value
# this call computes, so the stage refuses it. Each is a PAIR_QA_COLUMNS name
# (test_stage1_written_columns_are_pair_qa_columns enforces that).
STAGE1_COLUMNS: frozenset[str] = frozenset({
    '_clean_unit', '_unit_class', '_base_dose', '_base_unit',
    '_amount_multiplier', '_time_multiplier', '_weight_multiplier',
})


def standardize_dose_to_base_units(
    med_df: FrameLike,
    vitals_df: FrameLike | None = None,
    show_intermediate: bool = False,
    id_name: str = 'hospitalization_id',
    *,
    countable_units: Collection[str] | None = None,
    return_format: ReturnFormat = 'polars',
) -> Tuple[FrameLike, FrameLike]:
    """Standardize medication dose units to a base set of standard units.

    Main public API function that performs complete dose unit standardization
    pipeline: format cleaning, name cleaning, and unit conversion.
    Returns both base data and a summary table of conversions.

    Parameters
    ----------
    med_df : pl.DataFrame, pl.LazyFrame, duckdb.DuckDBPyRelation or pd.DataFrame
        Medication data with required columns:

        - med_dose_unit: Original dose unit strings
        - med_dose: Dose values

        Additional columns are preserved in output. Stage 1 never uses
        patient weight, so `weight_kg` is not required. A pandas DataFrame
        is handed to DuckDB through Arrow at the boundary; no pandas code
        runs on it.

    vitals_df : object, optional
        Unused. Kept for backward compatibility: stage 1 folds `/lb` into
        `/kg` with a constant and never looks up a patient weight.

    show_intermediate : bool, default False
        If True, expose intermediate columns (_amount_multiplier, _time_multiplier,
        _weight_multiplier) for QA purposes. If False (default), inline multiplier
        expressions to avoid materializing intermediate columns.

    id_name : str, default 'hospitalization_id'
        Unused here. Accepted so
        :func:`~clifpy.utils.unit_converter.convert_dose_units_by_med_category`
        can forward its arguments unchanged.

    countable_units : collection of str, optional
        Unit strings to classify as countable dosage forms (`_unit_class =
        'countable'`; the dose passes through unconverted). None (default)
        uses `DEFAULT_COUNTABLE_UNITS`. A collection REPLACES the defaults
        rather than extending them, so pass
        `set(DEFAULT_COUNTABLE_UNITS) | {'troche'}` to extend. Tokens are
        cleaned exactly like the unit column (lowercased, whitespace and a
        trailing period removed, spelling variants rewritten), so pass them as
        the data spells them. A single string raises TypeError.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, default 'polars'
        Output format, as for :func:`clifpy.utils.io.load_data`. ``'pandas'``
        is deprecated (DeprecationWarning) and retires in the next release.

    Returns
    -------
    Tuple[pl.DataFrame, pl.DataFrame]
        `(base, counts)`, both in the format `return_format` names: polars
        DataFrames by default, LazyFrames for 'polars_lazy', lazy relations
        on DuckDB's default connection for 'duckdb', and pandas DataFrames
        for the deprecated 'pandas'.

        - [0] the medication rows with additional columns:

            * _clean_unit: Cleaned unit string
            * _unit_class: 'rate', 'amount', 'countable', or 'unrecognized'
            * _base_dose: base dose value
            * _base_unit: base unit, keeping the weight qualifier (`/lb`
              folded into `/kg`)

            If show_intermediate=True, also includes:

            * _amount_multiplier, _time_multiplier, _weight_multiplier: Conversion factors

        - [1] summary counts per (med_dose_unit, _clean_unit, _base_unit,
          _unit_class)

    Raises
    ------
    ValueError
        If required columns are missing from med_df.
    ValueError
        `Column-name collision`: med_df already has a column that stage 1
        writes, typically from an earlier conversion. Drop it first, or start
        from the original table.

    Examples
    --------
    >>> import polars as pl
    >>> med_df = pl.DataFrame({
    ...     'med_dose': [6, 100, 500],
    ...     'med_dose_unit': ['MCG/KG/HR', 'mL / hr', 'mg'],
    ... })
    >>> base, counts = standardize_dose_to_base_units(med_df)
    >>> '_base_unit' in base.columns
    True
    >>> 'count' in counts.columns
    True

    Notes
    -----
    Standard (base) units for conversion, one amount base per
    `SUBCLASS_SPEC` family (mcg, ml, u, meq, mmol, cells, ppm):

    - Rate units (with optional weight qualifier): the family base per
      minute, e.g. mcg/min, mcg/kg/min, ml/min, u/kg/min, meq/min -- `/lb`
      collapsed into `/kg` via the constant `KG_PER_LB`.
    - Amount units (with optional weight qualifier): the family base, e.g.
      mcg, mcg/kg, ml, u, meq -- `/lb` likewise collapsed into `/kg`.
    - cells and ppm are standalone: they have no weight or time form.

    The function automatically handles:

    - Weight-based dosing (/kg, /lb) using the constant `KG_PER_LB` to collapse
      `/lb` into `/kg` in stage 1 (no patient weight needed). `weight_kg` is
      consumed only in stage 2 (preferred-unit conversion) when source and
      target differ in *presence* of a weight qualifier.
    - Time conversions (per hour and per day to per minute)
    - Volume conversions (L and mcL to mL)
    - Mass conversions (mg, ng, g to mcg)
    - Unit conversions (milli-units and million units to units)

    Unrecognized units are flagged but preserved in the output.
    """
    return _standardize_to_base(
        med_df, vitals_df, show_intermediate, id_name,
        countable_units=countable_units, return_format=return_format, spell_out=True,
    )


def _standardize_to_base(
    med_df: FrameLike,
    vitals_df: FrameLike | None = None,
    show_intermediate: bool = False,
    id_name: str = 'hospitalization_id',
    *,
    countable_units: Collection[str] | None = None,
    return_format: ReturnFormat = 'polars',
    spell_out: bool = True,
) -> Tuple[FrameLike, FrameLike]:
    """The body of :func:`standardize_dose_to_base_units`.

    `spell_out=False` keeps the internal `u` / `mu` / `mnu` tokens in
    `_clean_unit` and `_base_unit`: the converter's stage 2 matches on them
    and spells them out itself where they leave it.
    """
    fmt = resolve_return_format(return_format)
    logger.info("Standardizing dose units to base...")
    # The SQL below reads `relation` and `med_df` by name; see _inputs.py.
    check_no_shadowing_tables()

    # NOTE: under the weight-aware redesign, base conversion no longer needs
    # `weight_kg`. The `vitals_df` parameter is retained for API compatibility
    # but is unused here. Patient weight is consumed only in stage 2
    # (preferred-unit conversion), and only for rows where source and target
    # differ in weight-qualifier presence. See `convert_dose_units_by_med_category`.
    _ = vitals_df  # explicitly mark as unused

    # check if the required columns are present (weight_kg no longer required)
    required_columns = {'med_dose_unit', 'med_dose'}
    missing_columns = required_columns - set(column_names(med_df, 'med_df'))
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")

    # A column stage 1 writes, already on the input (typically the output of an
    # earlier pass), would be bound in place of the new one: DuckDB suffixes
    # the new column `_clean_unit_1` and the base dose is scaled with the stale
    # factor, with no error. DuckDB compares names case-insensitively, so do we.
    # The converter parks or refuses these columns before stage 1 runs.
    stale = sorted(c for c in column_names(med_df, 'med_df') if c.lower() in STAGE1_COLUMNS)
    if stale:
        raise ValueError(
            f"Column-name collision: med_df already has column(s) {stale} that "
            f"stage 1 writes, most likely from an earlier conversion; drop them "
            f"first (or start from the original table)"
        )

    # Read the input onto DuckDB's default connection (pandas only through
    # Arrow), keeping its zones for the eager formats.
    idx = pandas_index(med_df)
    med_df, input_tz_map = read_input(med_df, 'med_df', detach=fmt == 'duckdb')

    # The eager formats come back in input order. Stage 1 has no ordinal of
    # its own, and a multi-chunk polars frame would otherwise come back
    # shuffled, so number the rows here and let `emit` sort on it. The random
    # suffix keeps the name clear of every caller column. A 'duckdb' relation
    # has no row order, so it skips the window.
    ord_col = None
    if fmt != 'duckdb':
        ord_col = f"_stage1_ord_{uuid.uuid4().hex[:8]}"
        med_df = med_df.project(f"*, row_number() OVER () AS {ord_col}")

    # Clean dose units using DuckDB to avoid pandas materialization
    logger.debug("Cleaning unit formats...")
    med_df_cleaned = _clean_dose_unit_formats_duckdb(med_df)
    logger.debug("Cleaning unit names...")
    med_df_cleaned = _clean_dose_unit_names_duckdb(med_df_cleaned)
    logger.debug("Converting to base units...")
    med_df_base = _convert_clean_units_to_base_units(
        med_df_cleaned,
        show_intermediate=show_intermediate,
        countable_units=countable_units,
    )
    if spell_out:
        # What users see spells the unit family out (`units/hr`); the
        # converter keeps the internal `u` / `mu` / `mnu` tokens it matches on.
        med_df_base = med_df_base.project(
            "* REPLACE ("
            f"{_canonical_spelling_expr('_clean_unit')} AS _clean_unit, "
            f"{_canonical_spelling_expr('_base_unit')} AS _base_unit)"
        )
    convert_counts_df = _create_unit_conversion_counts_table(
        med_df_base,
        group_by=['med_dose_unit', '_clean_unit', '_base_unit', '_unit_class']
        )

    logger.info("Standardization complete")
    base = emit(
        med_df_base, fmt, tz_map=input_tz_map,
        order_by=ord_col, drop=(ord_col,) if ord_col else (), pandas_index=idx,
    )
    return base, emit(convert_counts_df, fmt)
    
