"""Stage 2: canonical base units -> the caller's preferred units.

This is where patient weight is consumed (only for the XOR cases) and where
`_convert_status` records why a row could not be converted.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa

from clifpy.utils.logging_config import get_logger

from ._grammar import (
    KG_PER_LB, ALL_ACCEPTABLE_UNITS, AMOUNT_FACTOR_PATTERNS,
    TIME_FACTOR_PATTERNS, MASS_REGEX, VOLUME_REGEX, UNIT_REGEX,
    SUBCLASS_REGEX, RATE_UNITS_STR, AMOUNT_UNITS_STR, MU_REGEX, MNU_REGEX,
    _weight_qual_clause, _canonical_spelling_expr, _convert_set_to_str_for_sql,
)
from ._sql import (
    _concat_builders_by_patterns,
    _pattern_to_factor_builder_for_preferred,
    _subclass_case_ladder,
)

logger = get_logger('utils.unit_converter')

# D-260930-mu-ambiguity. Clinically `MU` is million units and `mU` is
# milli-units; the case that separates them is gone before parsing, so a bare
# `MU` is read as milli-units (see UNIT_NAMING_VARIANTS). A request that moves
# a dose between the two families is a factor of 1e9 and almost always that
# misreading on one side, so stage 2 refuses it in BOTH directions with this
# status rather than computing it. `mu` <-> `u` and `u` <-> `mnu` convert.
# NOTE: byte-identical by contract with the SOFA-1 rewrite (session
# pyclif-1b); it is the only `_convert_status` value added for CLIF 3.0.
AMBIGUOUS_MU_STATUS = "cannot convert milli-units to million-units (ambiguous 'MU')"

# `_convert_status_details` splits `_convert_status == 'success'` by how the
# row reached its output unit; a failed row repeats its `_convert_status`.
# `_convert_status` itself stays byte-identical (peer contract, `== 'success'`
# filters such as wide_dataset's). "already" means stage 2 copied `med_dose`
# unchanged: the cleaned source unit equals the target, the same predicate as
# the identity branch below, so the label and the dose can never disagree.
DETAILS_CONVERTED_TO_TARGET = 'success: converted to the target unit'
DETAILS_ALREADY_TARGET = 'success: already the target unit'
DETAILS_CONVERTED_TO_VARIANT = 'success: converted to the weight variant of the target'
DETAILS_ALREADY_VARIANT = 'success: already the weight variant of the target'
DETAILS_UNLISTED_CLEANED = 'success: no target listed; unit cleaned only'
DETAILS_UNLISTED_TO_BASE = 'success: no target listed; converted to the base unit'
DETAILS_UNLISTED_ALREADY_BASE = 'success: no target listed; already the base unit'

# Every success detail, in the order the guide lists them.
CONVERT_STATUS_DETAILS = (
    DETAILS_CONVERTED_TO_TARGET,
    DETAILS_ALREADY_TARGET,
    DETAILS_CONVERTED_TO_VARIANT,
    DETAILS_ALREADY_VARIANT,
    DETAILS_UNLISTED_CLEANED,
    DETAILS_UNLISTED_TO_BASE,
    DETAILS_UNLISTED_ALREADY_BASE,
)

# What happens to a row whose category has no listed target.
UNLISTED_MODES = ('base', 'keep')


def _ambiguous_mu_predicate(
    source_col: str = '_clean_unit',
    preferred_col: str = '_preferred_unit_clean',
) -> str:
    """SQL boolean, never NULL: one side is milli-units, the other million-units.

    Both columns hold cleaned units, so the `^mu` / `^mnu` token regexes are
    exact on every recognised unit. The source side is `_clean_unit`, not
    `_base_unit`: stage 1 collapses both families to `u`. Shared by the
    `statused` CTE, which sets AMBIGUOUS_MU_STATUS, and `_weight_lookup_filter`,
    which keeps those rows out of the weight lookup.

    Parameters
    ----------
    source_col, preferred_col : str
        SQL expressions for the cleaned source and preferred units.

    Returns
    -------
    str
        Boolean SQL expression; a NULL unit counts as no match.
    """
    src = f"coalesce({source_col}, '')"
    pref = f"coalesce({preferred_col}, '')"
    return (
        f"((regexp_matches({src}, '{MU_REGEX}') "
        f"AND regexp_matches({pref}, '{MNU_REGEX}')) "
        f"OR (regexp_matches({src}, '{MNU_REGEX}') "
        f"AND regexp_matches({pref}, '{MU_REGEX}')))"
    )


def _convert_base_units_to_preferred_units(
    med_df: duckdb.DuckDBPyRelation,
    override: bool = False,
    show_intermediate: bool = False,
    unlisted: str = 'base',
    countable_units: tuple = (),
    targets_validated: bool = False,
) -> duckdb.DuckDBPyRelation:
    """Convert base standardized units to user-preferred units.

    Performs the second stage of unit conversion, transforming from the
    stage-1 base units (`_base_unit`, which keep the weight qualifier, e.g.
    mcg/kg/min, ml/min, u/min) to medication-specific preferred units while
    maintaining unit class consistency.

    Parameters
    ----------
    med_df : duckdb.DuckDBPyRelation
        Stage-1 output with required columns:

        - _base_dose: Dose values in standardized units
        - _base_unit: Standardized unit strings (may be NULL)
        - _preferred_unit: Target unit strings for each medication
        - weight_kg: Patient weights (optional, used for weight-based conversions;
          a weight that is zero or below, NaN or infinite counts as missing)

    override : bool, default False
        Only for explicit targets clifpy cannot parse (not in
        ALL_ACCEPTABLE_UNITS or the countable tokens): True warns and
        continues, False raises ValueError. Cross-class (rate to amount) and
        cross-subclass (mass to volume) rows never raise either way; each
        reports its reason in `_convert_status`.

    show_intermediate : bool, default False
        If True, expose intermediate columns (_amount_multiplier_preferred,
        _time_multiplier_preferred, _weight_multiplier_preferred) for QA purposes.
        If False (default), inline multiplier expressions to avoid materializing
        intermediate columns.

    unlisted : {'base', 'keep'}, default 'base'
        How the preferred-unit join treated rows with no listed target, so
        `_convert_status_details` can name it: 'base' rows were sent to their
        base unit, 'keep' rows to their own cleaned unit (dose copied).

    countable_units : tuple of str, default ()
        The countable tokens stage 1 classified with, already cleaned
        (`_base._normalize_countable_units`). A target among them is a known
        unit of class 'countable': a row already in that form succeeds with
        its dose copied, any other row fails on unit class.

    targets_validated : bool, default False
        True when the caller already checked the explicit targets (the
        converter does, up front), so the unrecognized-target WARNING or
        ValueError is not issued twice. A direct call checks here.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Lazy relation: the input columns plus:

        - _unit_class: Classification of base unit ('rate', 'amount',
          'countable', 'unrecognized'); carried over from stage 1 when present
        - _unit_subclass: Subclassification, a `SUBCLASS_SPEC` key ('mass',
          'volume', 'unit', 'equivalent', 'substance', 'cell_count',
          'gas_fraction') or 'unrecognized'
        - _unit_class_preferred: Classification of preferred unit
        - _unit_subclass_preferred: Subclassification of preferred unit
        - _convert_status: Success or failure reason message
        - med_dose_converted: Final converted dose value
        - med_dose_unit_converted: Final unit string after conversion
        - _convert_status_details: 'success' split by how the row reached its
          unit (`CONVERT_STATUS_DETAILS`), or the failure reason

        If show_intermediate=True, also includes:

        - _amount_multiplier_preferred: Conversion factor for amount units
        - _time_multiplier_preferred: Conversion factor for time units
        - _weight_multiplier_preferred: Conversion factor for weight-based units

    Raises
    ------
    ValueError
        If required columns are missing from med_df or if preferred units are not
        in ALL_ACCEPTABLE_UNITS (when override=False).

    Notes
    -----
    Conversion rules enforced:

    - Conversions only allowed within same unit class (rate→rate, amount→amount)
    - Cannot convert between incompatible subclasses (e.g., mass→volume)
    - Cannot convert between milli-units and million-units, in either
      direction: such rows get `AMBIGUOUS_MU_STATUS` (needs `_clean_unit`)
    - When conversion fails, falls back to base units and dose values
    - Missing units (NULL) are handled with 'original unit is missing' status

    The function uses DuckDB SQL for efficient processing and applies regex
    pattern matching to classify units and calculate conversion factors.

    See Also
    --------
    _convert_clean_units_to_base_units : First-stage conversion
    convert_dose_units_by_med_category : Public API for complete conversion pipeline
    """
    # ---- required column check ----
    required_columns = {'_base_dose', '_preferred_unit'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")

    # `_preferred_unit_clean` is the normalised form every regex below matches
    # against; the orchestrator supplies it. When this function is called
    # directly with an already-canonical `_preferred_unit`, alias the two so
    # the rest of the query needs no conditional.
    if '_preferred_unit_clean' not in med_df.columns:
        med_df = duckdb.sql("""
            SELECT *, _preferred_unit AS _preferred_unit_clean
            FROM med_df
        """)

    # ---- preferred-unit acceptability validation via ANTI JOIN (no .to_df()) ----
    # Per docs/duckdb_perf_guide.md §7e and §1: ANTI JOIN keeps everything lazy
    # and only materializes the violations (typically empty) via .fetchall().
    #
    # NOTE: when the orchestrator supplies `_preferred_is_explicit`, only rows
    # the caller actually requested are validated. Rows where the column is
    # false carry a unit COALESCE'd from `_base_unit` -- clifpy's own derived
    # value, not user input -- and an unconvertible one there is reported per
    # row via `_convert_status` rather than aborting the call (clifpy#153).
    # Called directly without that column, every row is validated as before.
    explicit_filter = (
        "AND _preferred_is_explicit"
        if '_preferred_is_explicit' in med_df.columns
        else ""
    )
    # Countable forms are valid targets too (see `countable_units`).
    acceptable_units_relation = pa.table(
        {'unit': pa.array(sorted(ALL_ACCEPTABLE_UNITS | set(countable_units)), pa.string())}
    )
    bad_units_rows = [] if targets_validated else duckdb.sql(f"""
        SELECT DISTINCT _preferred_unit
        FROM med_df
        ANTI JOIN acceptable_units_relation ON _preferred_unit_clean = unit
        WHERE _preferred_unit_clean IS NOT NULL
        {explicit_filter}
    """).fetchall()
    if bad_units_rows:
        bad_set = {row[0] for row in bad_units_rows}
        error_msg = (
            f"Cannot accommodate the conversion to the following preferred units: "
            f"{bad_set}. Consult the function documentation for a list of acceptable units."
        )
        if override:
            logger.warning(error_msg)
        else:
            raise ValueError(error_msg)

    # Unit rendered on success. When the caller named a preferred unit, echo
    # their spelling verbatim (`u/min` in -> `u/min` out). When clifpy picked
    # the unit itself -- the fallback to `_base_unit` for categories with no
    # entry in `preferred_units` -- render it in the mCIDE vocabulary, which
    # spells the unit family out (`units`, `milli-units`, `million-units`).
    success_unit_expr = (
        f"CASE WHEN _preferred_is_explicit THEN _preferred_unit "
        f"ELSE {_canonical_spelling_expr('_preferred_unit')} END"
        if '_preferred_is_explicit' in med_df.columns
        else "_preferred_unit"
    )

    # Subclass ladders, derived from SUBCLASS_SPEC.
    subclass_ladder_base = _subclass_case_ladder('_base_unit')
    subclass_ladder_pref = _subclass_case_ladder('_preferred_unit_clean')

    # ---- multiplier clauses ----
    # Amount and time use the inverse-pattern builder (factor: canonical -> preferred).
    # Weight is now handled separately by the 9-case transition factor below,
    # NOT by the inverse-pattern builder, since the new base unit preserves
    # the weight qualifier.
    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=AMOUNT_FACTOR_PATTERNS,
        else_case='1'
    )
    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=TIME_FACTOR_PATTERNS,
        else_case='1'
    )

    # Weight-transition factor based on (base_wt, pref_wt). Only kg<->none and
    # lb<->none cases reference `weight_kg`. kg<->lb is the constant `KG_PER_LB`.
    weight_factor_clause = f"""
        CASE
            WHEN _base_wt = _pref_wt THEN 1
            WHEN _base_wt = '/kg' AND _pref_wt = '/lb' THEN 1.0/{KG_PER_LB}
            WHEN _base_wt = '/lb' AND _pref_wt = '/kg' THEN {KG_PER_LB}
            WHEN _base_wt = ''    AND _pref_wt = '/kg' THEN 1.0/weight_kg
            WHEN _base_wt = ''    AND _pref_wt = '/lb' THEN 1.0/(weight_kg * {KG_PER_LB})
            WHEN _base_wt = '/kg' AND _pref_wt = ''    THEN weight_kg
            WHEN _base_wt = '/lb' AND _pref_wt = ''    THEN weight_kg * {KG_PER_LB}
            ELSE 1
            END
    """

    # Schema-aware: only emit columns that aren't already there
    cols = set(med_df.columns)
    has_unit_class = '_unit_class' in cols
    has_clean_unit = '_clean_unit' in cols
    has_med_dose = 'med_dose' in cols
    has_weight_kg = 'weight_kg' in cols

    # Fallback values when conversion cannot proceed
    dose_fallback = "med_dose" if has_med_dose else "_base_dose"
    unit_fallback = "_clean_unit" if has_clean_unit else "_base_unit"
    # Identity short-circuit: only meaningful when both _clean_unit and med_dose
    # are available. Returns med_dose bit-exact (no multiplication).
    identity_dose_branch = (
        f"WHEN _convert_status = 'success' AND _clean_unit = _preferred_unit_clean THEN {dose_fallback}\n            "
        if has_clean_unit and has_med_dose else ""
    )

    # `_convert_status_details`. The "already" test is the identity branch's
    # own predicate, so an "already" label always means a copied dose.
    if unlisted not in UNLISTED_MODES:
        raise ValueError(f"unlisted must be one of {UNLISTED_MODES}; got {unlisted!r}")
    identity_test = "_clean_unit = _preferred_unit_clean" if has_clean_unit else "FALSE"
    explicit_test = "_preferred_is_explicit" if '_preferred_is_explicit' in cols else "TRUE"
    variant_test = (
        "_preferred_is_weight_variant" if '_preferred_is_weight_variant' in cols else "FALSE"
    )

    def _already_or_converted(already: str, converted: str) -> str:
        return f"CASE WHEN {identity_test} THEN '{already}' ELSE '{converted}' END"

    unlisted_detail = (
        f"'{DETAILS_UNLISTED_CLEANED}'" if unlisted == 'keep'
        else _already_or_converted(DETAILS_UNLISTED_ALREADY_BASE, DETAILS_UNLISTED_TO_BASE)
    )
    details_expr = (
        f"CASE WHEN _convert_status != 'success' THEN _convert_status "
        f"WHEN NOT {explicit_test} THEN {unlisted_detail} "
        f"WHEN {variant_test} THEN "
        f"{_already_or_converted(DETAILS_ALREADY_VARIANT, DETAILS_CONVERTED_TO_VARIANT)} "
        f"ELSE {_already_or_converted(DETAILS_ALREADY_TARGET, DETAILS_CONVERTED_TO_TARGET)} END"
    )

    # The unit a status message names, in the spelling users see (`units/dose`,
    # not the internal `u/dose`).
    base_unit_shown = _canonical_spelling_expr('_base_unit')

    # A countable target (`tablet`, `dose`, ...) is a known unit of its own class.
    countable_preferred = (
        f"WHEN _preferred_unit_clean IN ('{_convert_set_to_str_for_sql(set(countable_units))}') "
        "THEN 'countable'"
        if countable_units else ""
    )

    # If weight_kg isn't in the schema, treat it as NULL throughout.
    weight_kg_expr = "weight_kg" if has_weight_kg else "CAST(NULL AS DOUBLE)"
    weight_unusable = _weight_unusable_predicate(weight_kg_expr)

    classify_extra = (
        f""", _unit_class: CASE
                WHEN _base_unit IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _base_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                ELSE 'unrecognized' END"""
        if not has_unit_class else ""
    )

    # milli-units <-> million-units refusal (AMBIGUOUS_MU_STATUS). It sits
    # after the recognised/class/subclass checks and before the weight arms,
    # so it fires only on two recognised `unit`-subclass units and never needs
    # a weight. `_base_unit` cannot tell the families apart, so the source
    # side is read from `_clean_unit`; on a direct call without that column
    # the branch is omitted.
    mu_status_sql = AMBIGUOUS_MU_STATUS.replace("'", "''")
    ambiguous_mu_branch = (
        f"WHEN {_ambiguous_mu_predicate()} THEN '{mu_status_sql}'"
        if has_clean_unit else ""
    )

    # CTE chain: classified -> statused -> final select. Each step adds named
    # columns the next can reference, eliminating the verbose nested-CASE
    # duplication of the previous implementation.
    q = f"""
    WITH classified AS (
        SELECT l.*
            {classify_extra}
            , _unit_subclass: CASE
                {subclass_ladder_base}
                ELSE 'unrecognized' END
            , _unit_class_preferred: CASE
                WHEN _preferred_unit_clean IN ('{RATE_UNITS_STR}') THEN 'rate'
                WHEN _preferred_unit_clean IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                {countable_preferred}
                ELSE 'unrecognized' END
            , _unit_subclass_preferred: CASE
                {subclass_ladder_pref}
                ELSE 'unrecognized' END
            , _base_wt: {_weight_qual_clause('_base_unit')}
            , _pref_wt: {_weight_qual_clause('_preferred_unit_clean')}
        FROM med_df l
    )
    , statused AS (
        SELECT *
            -- _needs_wt = 1 iff exactly one side has a weight qualifier (XOR)
            , _needs_wt: CASE
                WHEN (_base_wt != '' AND _pref_wt = '')
                  OR (_base_wt = '' AND _pref_wt != '') THEN 1
                ELSE 0 END
            , _convert_status: CASE
                WHEN _base_unit IS NULL
                    THEN 'original unit is missing'
                -- Countable dosage forms are understood but inherently
                -- unconvertible: "1 tablet" carries no dose without the
                -- product strength. Distinguishing them from genuinely
                -- unparseable strings is the point of the class.
                -- ...unless the caller's target is that very form: then there
                -- is nothing to convert, and the dose is copied.
                WHEN _unit_class = 'countable' AND {explicit_test} AND {identity_test}
                    THEN 'success'
                WHEN _unit_class = 'countable'
                    THEN 'original unit ' || {base_unit_shown} || ' is a countable dosage form; not convertible'
                WHEN _unit_class = 'unrecognized' OR _unit_subclass = 'unrecognized'
                    THEN 'original unit ' || {base_unit_shown} || ' is not recognized'
                WHEN _unit_class_preferred = 'unrecognized'
                  OR (_unit_class_preferred != 'countable' AND _unit_subclass_preferred = 'unrecognized')
                    THEN 'user-preferred unit ' || _preferred_unit || ' is not recognized'
                WHEN _unit_class != _unit_class_preferred
                    THEN 'cannot convert ' || _unit_class || ' to ' || _unit_class_preferred
                WHEN _unit_subclass != _unit_subclass_preferred
                    THEN 'cannot convert ' || _unit_subclass || ' to ' || _unit_subclass_preferred
                -- milli-units <-> million-units: refused before any weight is read
                {ambiguous_mu_branch}
                -- two-message split: weight required but missing
                -- (or unusable: zero or below, NaN, infinite)
                WHEN _base_wt != '' AND _pref_wt = '' AND {weight_unusable}
                    THEN 'cannot convert weighted to unweighted: weight_kg is missing'
                WHEN _base_wt = '' AND _pref_wt != '' AND {weight_unusable}
                    THEN 'cannot convert unweighted to weighted: weight_kg is missing'
                ELSE 'success'
                END
        FROM classified
    )"""

    if show_intermediate:
        q += f"""
    SELECT *
        , _amount_multiplier_preferred: {amount_clause}
        , _time_multiplier_preferred: {time_clause}
        , _weight_multiplier_preferred: {weight_factor_clause}
        , med_dose_converted: CASE
            {identity_dose_branch}WHEN _convert_status = 'success' THEN _base_dose * _amount_multiplier_preferred * _time_multiplier_preferred * _weight_multiplier_preferred
            ELSE {dose_fallback}
            END
        , med_dose_unit_converted: CASE
            WHEN _convert_status = 'success' THEN {success_unit_expr}
            ELSE {unit_fallback}
            END
        , _convert_status_details: {details_expr}
    FROM statused
    """
    else:
        q += f"""
    SELECT *
        , med_dose_converted: CASE
            {identity_dose_branch}WHEN _convert_status = 'success' THEN _base_dose * ({amount_clause}) * ({time_clause}) * ({weight_factor_clause})
            ELSE {dose_fallback}
            END
        , med_dose_unit_converted: CASE
            WHEN _convert_status = 'success' THEN {success_unit_expr}
            ELSE {unit_fallback}
            END
        , _convert_status_details: {details_expr}
    FROM statused
    """
    return duckdb.sql(q)




def _weight_unusable_predicate(weight_kg_expr: str = 'weight_kg') -> str:
    """SQL predicate true when stage 2 cannot use `weight_kg_expr` as a weight.

    A weight that is zero or below, NaN or infinite is as unusable as a NULL
    one: the weight factor would make the dose infinite, negative or NaN and
    still report 'success'. This covers a caller's pre-filled weight_kg,
    which never passes through attach_weight's filter. isfinite() is needed
    on top of `> 0` because DuckDB sorts NaN above every number.

    Stage 2 reports `weight_kg is missing` on the rows that need a weight
    (:func:`_weight_lookup_filter`) where this holds; the converter counts
    the same rows to warn that a pre-filled weight_kg leaves vitals_df
    unused.
    """
    return (
        f"({weight_kg_expr} IS NULL"
        f" OR NOT (CAST({weight_kg_expr} AS DOUBLE) > 0"
        f" AND isfinite(CAST({weight_kg_expr} AS DOUBLE))))"
    )


def _weight_lookup_filter() -> str:
    """SQL predicate selecting the rows stage 2 will multiply by `weight_kg`.

    The orchestrator sends only these rows through the weight lookup. A row
    qualifies when stage 2 can convert it at all -- source and preferred unit
    recognized, same class, same subclass, and not a milli-units <->
    million-units pair -- AND exactly one side carries a weight qualifier.
    Rows stage 2 refuses for another reason (`mg/kg/dose`, rate -> amount,
    mass -> volume, milli-units <-> million-units with AMBIGUOUS_MU_STATUS)
    never consume a weight, so they must not trigger the lookup, nor abort a
    call made without `vitals_df`.

    Units are read in normalised form (`_base_unit`, `_preferred_unit_clean`),
    exactly as stage 2 reads them: the caller's raw `_preferred_unit`
    (`'MCG/KG/MIN'`) does not match the case-sensitive weight regexes.

    NULL-safe by construction (COALESCE to FALSE): the orchestrator splits the
    table with `WHERE p` and `WHERE NOT (p)`, and a NULL would drop the row
    from both halves.

    Returns
    -------
    str
        Boolean SQL expression over `_unit_class`, `_clean_unit`,
        `_base_unit` and `_preferred_unit_clean`.
    """
    pref_class = (
        f"CASE WHEN _preferred_unit_clean IN ('{RATE_UNITS_STR}') THEN 'rate' "
        f"WHEN _preferred_unit_clean IN ('{AMOUNT_UNITS_STR}') THEN 'amount' END"
    )
    base_subclass = f"CASE {_subclass_case_ladder('_base_unit')} END"
    pref_subclass = f"CASE {_subclass_case_ladder('_preferred_unit_clean')} END"
    base_wt = _weight_qual_clause('_base_unit')
    pref_wt = _weight_qual_clause('_preferred_unit_clean')
    return (
        "COALESCE("
        "_unit_class IN ('rate', 'amount') "
        f"AND _unit_class = ({pref_class}) "
        f"AND ({base_subclass}) = ({pref_subclass}) "
        f"AND NOT {_ambiguous_mu_predicate()} "
        f"AND ((({base_wt}) != '') != (({pref_wt}) != '')), "
        "FALSE)"
    )
