"""Unit grammar: the vocabulary of dose units clifpy can parse and emit.

`SUBCLASS_SPEC` is the single source of truth for the amount axis; the regexes,
factor tables and acceptable-unit sets in this module are all derived from it.
Adding a unit family is a one-line change here.
"""

from typing import Set


# NOTE: 1 kg = 2.20462 lb. Centralised so SQL builders, Python fallbacks,
# and test fixtures can all reference the same literal (avoids float drift).
KG_PER_LB = 2.20462

# Temp-table lifecycle: boundary 2a per docs/duckdb_perf_guide.md. The input
# is referenced by many downstream SQL queries (cleaning, validation, base
# conversion, preferred-unit join, weight join), so _convert._capture_input
# reads it into DuckDB once (pandas through Arrow) and, for the eager return
# formats, _convert._register_input copies it into a temp table named per call
# (`_med_unit_input_<8 hex>`); the converter's `finally` drops exactly the
# tables that call created (clifpy/utils/_duckdb_helpers._drop_temp_table).
# return_format='duckdb' creates no table: its relations read the captured
# input itself.


UNIT_NAMING_VARIANTS = {
    # NOTE: applied as nested regexp_replace in INSERTION ORDER, so position
    # here is semantic. Each block's ordering constraint is called out below.

    # time -- every rule is `/`-prefixed and `$`-anchored, and the `/24h...`
    # spellings start with a digit, so no rule can match another rule's input
    # and their relative order is free (`/h(rs?|ours?)?$` cannot match
    # `/24hr`). Plurals and the 24-hour spellings are accepted.
    '/day': '/(days?|24h|24hrs?|24hours?)$',
    '/hr': '/h(rs?|ours?)?$',
    '/min': '/m(ins?|inutes?)?$',
    # unit -- NOTE: plaural always go first to avoid having result like "us" or "gs"
    #
    # CRITICAL: million-units MUST precede both 'u' and 'milli-?'. Otherwise
    # `u(nits|nit)?` rewrites "millionunits" -> "millionu", then `milli-?`
    # rewrites that -> "monu", and the dose silently fails to convert.
    # NOTE: strictly, only the order against 'milli-?' is load-bearing:
    # `(million|mm)-?u` still matches the "millionu" that 'u' leaves behind
    # (audit mutant G3 is equivalent). Keeping 'mnu' ahead of 'u' is harmless.
    # Deliberately NOT accepted here: a bare `mu` meaning million units. In
    # clinical use `MU` is million units and `mU` is milliunits -- an ISMP
    # error-prone abbreviation separated only by case, and case is already
    # gone by this point. Only the unambiguous spellings are recognised.
    # A bare `MU` therefore reads as milli-units, and stage 2 refuses every
    # milli-units <-> million-units request with AMBIGUOUS_MU_STATUS
    # (_preferred.py) instead of returning a dose 1e9 off.
    'mnu': '(million|mm)[-_]?u(nits|nit)?',
    'u': 'u(nits|nit)?',
    # milli -- '-' or '_' (clifpy's 3.0 YAML spells pitocin `milli_units`)
    'm': 'milli[-_]?',
    # spelled-out milli- forms. They match what 'milli-?' leaves behind
    # ("milligrams" -> "mgrams"), so they MUST follow 'm'. Each is
    # `^`-anchored and requires the leading m: a bare "moles" (mol, 1000x
    # mmol) or "equivalents" must stay unrecognised, never become mmol/meq.
    'mg': '^mgrams?',
    'meq': '^mequivalents?',
    'mmol': '^mmoles?',
    # volume -- 'mcl' precedes 'l', which would otherwise rewrite
    # "microliter" -> "microl" and lose the micro- prefix. `ul` is the ASCII
    # spelling of µl, as `ug` is of µg; the 'u' rule above leaves it intact.
    'mcl': '^(microliters?|microlitres?|µl|μl|ul)',
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass -- 'ng' and 'pg' precede 'g'; all are anchored so they cannot overlap
    'ng': '^nanograms?',
    'pg': '^picograms?',
    'mcg': '^(u|µ|μ)g|^micrograms?',
    'g': '^g(rams|ram)?',
    # dose
    # 'dose': '^doses?',
}

# Canonical output spelling, applied only when rendering the converted unit.
#
# CLIF 3.0 mCIDE spells the unit family out (`units/hr`, `milli-units/min`)
# while abbreviating mass and volume (`mcg`, `ml`), so we match it exactly
# rather than imposing internal consistency. Input still accepts both
# spellings, so existing callers passing `u/min` are unaffected.
CANONICAL_UNIT_SPELLING = {
    'u': 'units',
    'mu': 'milli-units',
    'mnu': 'million-units',
}

# Strings that mean "no unit was recorded". Routed to NULL so they report as
# 'original unit is missing' rather than masquerading as unrecognised units.
#
# NOTE: 'nan'/'none' show up as literal strings only when a frame has been
# round-tripped through a text format -- a CSV export of a pandas frame, say.
# In a live DataFrame they are already NULL. Normalising them is defensive, not
# a fix for a converter bug.
#
# Deliberately NOT included: site-local junk codes such as 'asord', 'zzbag',
# 'xx'. Those genuinely ARE unrecognised, and collapsing them to NULL would
# hide a real data-quality signal.
MISSING_UNIT_PLACEHOLDERS = frozenset({
    'nan', 'none', 'null', 'unspecified', '*unspecified', 'unknown',
})

# Countable dosage forms. A tablet or a drop carries no dose information
# without the product strength, so these are never convertible -- across the
# 11-site consortium inventory the median value for every one of them is 1.
# Classifying them separately keeps `_convert_status` honest: "countable dosage
# form" is actionable, "not recognized" is not.
#
# Chosen by measurement, not guesswork. The countable pool is 53 distinct
# tokens but extremely top-heavy -- `tablet` alone is 51% of the volume and the
# top six reach 96%, so a short list covers nearly all of it. Callers with
# local vocabulary pass `countable_units=` rather than editing this set. The
# user guide lists every token, and
# tests/utils/med_unit_converter/test_docs_drift.py fails when that list and
# this set disagree.
#
# NOTE: matched by EXACT set membership, never by regex. `mg/kg/dose` is a
# prescribing rate, not a dosage form, and a regex on 'dose' would capture it.
DEFAULT_COUNTABLE_UNITS = frozenset({
    'tablet', 'tab', 'capsule', 'cap',
    'dose', 'drop', 'puff', 'patch', 'spray',
    'application', 'applicator', 'applicatorful', 'appl',
    'each', 'packet', 'enema', 'bag', 'film',
    'vial', 'syringe', 'suppository', 'bottle', 'half-tab',
})

AMOUNT_ENDER = "($|/*)"

# ===========================================================================
# Unit grammar
# ===========================================================================
# A dose unit is parsed on three independent axes:
#
#     amount  x  weight (/kg, /lb, none)  x  time (/min, /hr, /day)
#
# `SUBCLASS_SPEC` is the single source of truth for the amount axis. Every
# regex, acceptable-unit set and SQL CASE ladder below is DERIVED from it, so
# adding a unit family is a one-line change here rather than eight
# hand-synchronised edits scattered through the SQL builders.
#
# `base` is the canonical token each family collapses to in stage 1. Families
# never convert across subclass boundaries: mEq -> mg would need ion valence
# and molar mass, which the medication tables do not carry, so the subclass
# guard in `_convert_base_units_to_preferred_units` refuses it.
SUBCLASS_SPEC = {
    'mass':         {'tokens': ('mcg', 'mg', 'ng', 'pg', 'g'), 'base': 'mcg'},
    'volume':       {'tokens': ('ml', 'l', 'mcl'),        'base': 'ml'},
    'unit':         {'tokens': ('u', 'mu', 'mnu'),        'base': 'u'},
    'equivalent':   {'tokens': ('meq',),                  'base': 'meq'},
    'substance':    {'tokens': ('mmol',),                 'base': 'mmol'},
    'cell_count':   {'tokens': ('cells',),                'base': 'cells'},
    'gas_fraction': {'tokens': ('ppm',),                  'base': 'ppm'},
}

# Subclasses kept OUT of the weight x time cartesian product: a gas fraction
# or a cell count has no meaningful `/kg` or `/min` form. They convert only to
# themselves. CLIF 3.0 mCIDE targets bare `ppm` (nitric_oxide) and bare `cells`
# (CAR-T products), matching this.
#
# NOTE: CAR-T doses are sometimes charted as `cells/kg`. To support that,
# remove 'cell_count' from this set -- the weighted forms are then generated
# automatically and nothing else needs to change.
STANDALONE_SUBCLASSES = frozenset({'gas_fraction', 'cell_count'})

# Multiplicative factor from each token to its subclass `base`. Values are SQL
# expression strings, not numbers, because they are inlined into generated SQL.
# Identity tokens (the bases themselves) are omitted and fall through to 1;
# `meq`, `mmol`, `cells` and `ppm` are their own base and appear nowhere here.
TOKEN_TO_BASE_FACTOR = {
    # volume -> ml
    'l': '1000',
    'mcl': '1/1000',      # microlitres
    # unit -> u
    'mu': '1/1000',       # milli-units
    'mnu': '1000000',     # million units
    # mass -> mcg
    'mg': '1000',
    'ng': '1/1000',
    'pg': '1/1000000',    # picograms
    'g': '1000000',
}

# Time axis. `/min` is the canonical base, so it is omitted (factor 1).
# `/day` is required by mCIDE (tacrolimus is targeted at `mcg/kg/day`).
TIME_TO_BASE_FACTOR = {
    '/hr': '1/60',
    '/day': '1/1440',
}

ACCEPTABLE_WEIGHT_UNITS = ('/kg', '/lb', '')
ACCEPTABLE_TIME_UNITS = tuple(['/min'] + sorted(TIME_TO_BASE_FACTOR))


def _tokens_alternation(tokens) -> str:
    """Build a regex alternation with the longest tokens first.

    Regex alternation is leftmost-match, so `^(mg|mcg)` would match only `mg`
    against the string "mcg". Sorting by descending length guarantees that a
    longer token always wins over a shorter one that prefixes it.

    Parameters
    ----------
    tokens : Iterable[str]
        Unit tokens belonging to one subclass.

    Returns
    -------
    str
        Pipe-separated alternation, longest token first.

    Examples
    --------
    >>> _tokens_alternation(('u', 'mu'))
    'mu|u'
    >>> _tokens_alternation(('mcg', 'mg', 'ng', 'g'))
    'mcg|mg|ng|g'
    """
    return '|'.join(sorted(tokens, key=lambda t: (-len(t), t)))


def _token_regex(token: str) -> str:
    """Build the anchored amount regex for a single unit token."""
    return f"^({token}){AMOUNT_ENDER}"


# Per-subclass amount regexes, derived from SUBCLASS_SPEC.
SUBCLASS_REGEX = {
    name: f"^({_tokens_alternation(spec['tokens'])}){AMOUNT_ENDER}"
    for name, spec in SUBCLASS_SPEC.items()
}

# Back-compat aliases: published in docs/api/utilities.md and imported by tests.
MASS_REGEX = SUBCLASS_REGEX['mass']
VOLUME_REGEX = SUBCLASS_REGEX['volume']
UNIT_REGEX = SUBCLASS_REGEX['unit']

# time
HR_REGEX = f"/hr$"

# mass
MU_REGEX = _token_regex('mu')
MNU_REGEX = _token_regex('mnu')
MG_REGEX = _token_regex('mg')
NG_REGEX = _token_regex('ng')
G_REGEX = _token_regex('g')

# volume
L_REGEX = _token_regex('l')

# weight
# NOTE: trailing alternation `(/|$)` matches both rates (where the weight
# qualifier is followed by a time unit slash, e.g. `/kg/min`) and weighted
# amounts (where the qualifier ends the string, e.g. `mcg/kg`). Without the
# `$` branch, weighted amounts would be silently misclassified as unweighted.
LB_REGEX = f"/lb(/|$)"
KG_REGEX = f"/kg(/|$)"
WEIGHT_REGEX = f"/(lb|kg)(/|$)"

# Ordered pattern lists consumed by the amount/time multiplier builders.
# Longest token first, mirroring _tokens_alternation's rationale.
AMOUNT_FACTOR_PATTERNS = [
    _token_regex(t)
    for t in sorted(TOKEN_TO_BASE_FACTOR, key=lambda t: (-len(t), t))
]
TIME_FACTOR_PATTERNS = [
    f"{t}$" for t in sorted(TIME_TO_BASE_FACTOR, key=lambda t: (-len(t), t))
]

REGEX_TO_FACTOR_MAPPER = {
    # time -> /min
    **{f"{t}$": f for t, f in TIME_TO_BASE_FACTOR.items()},

    # amount -> subclass base (volume -> ml, unit -> u, mass -> mcg)
    **{_token_regex(t): f for t, f in TOKEN_TO_BASE_FACTOR.items()},

    # weight (consumed only in stage 2 / preferred conversion under the
    # weight-aware redesign — kept here for reference; stage 1 ignores them)
    KG_REGEX: 'weight_kg',
    LB_REGEX: f'weight_kg * {KG_PER_LB}',
}


def _weight_qual_clause(col: str) -> str:
    """Build a SQL CASE expression that extracts the weight qualifier of a unit column.

    Returns one of '/kg', '/lb', or '' (empty string for unweighted / unrecognized).

    Used to drive the 9-case weight-transition factor in stage 2 and the
    `_needs_wt` planning column. NULL inputs return ''.

    NOTE: this helper preserves the **actual** axis (`/kg` vs `/lb`) so stage 2
    can apply the correct transition factor. Stage 1 deliberately uses a
    different expression (`base_weight_qual_expr` in
    `_convert_clean_units_to_base_units`) that collapses `/lb` into `/kg`,
    because the canonical `_base_unit` never carries `/lb`. The two expressions
    are not interchangeable.
    """
    return (
        f"CASE "
        f"WHEN {col} IS NULL THEN '' "
        f"WHEN regexp_matches({col}, '{KG_REGEX}') THEN '/kg' "
        f"WHEN regexp_matches({col}, '{LB_REGEX}') THEN '/lb' "
        f"ELSE '' END"
    )

# Base amount tokens (no weight qualifier). The amount-axis "vocabulary"
# shared by both `_acceptable_amount_units` and `_acceptable_rate_units`.
ACCEPTABLE_BASE_AMOUNT_UNITS = {
    token
    for name, spec in SUBCLASS_SPEC.items()
    if name not in STANDALONE_SUBCLASSES
    for token in spec['tokens']
}

# Standalone amount units: accepted bare, but never combined with a weight or
# time qualifier (see STANDALONE_SUBCLASSES).
STANDALONE_AMOUNT_UNITS = {
    token
    for name, spec in SUBCLASS_SPEC.items()
    if name in STANDALONE_SUBCLASSES
    for token in spec['tokens']
}

def _acceptable_amount_units() -> Set[str]:
    """Generate all acceptable amount unit combinations (with optional weight qualifier).

    Mirrors `_acceptable_rate_units` but without a time axis. Weight qualifiers
    are `/kg`, `/lb`, or none — same axis as rate units.

    Returns
    -------
    Set[str]
        Set of all valid amount unit combinations.

    Examples
    --------
    >>> amount_units = _acceptable_amount_units()
    >>> 'mcg' in amount_units
    True
    >>> 'mcg/kg' in amount_units
    True
    >>> 'mcg/lb' in amount_units
    True
    >>> 'mcg/hr' in amount_units
    False

    Notes
    -----
    Amount units are combinations of:

    - Amount tokens: `ACCEPTABLE_BASE_AMOUNT_UNITS` (every `SUBCLASS_SPEC`
      token outside `STANDALONE_SUBCLASSES`)
    - Weight qualifiers: `ACCEPTABLE_WEIGHT_UNITS` (/kg, /lb, or none)

    plus the bare `STANDALONE_AMOUNT_UNITS` (cells, ppm).

    See Also
    --------
    _acceptable_rate_units : Same, plus a time axis.
    """
    combined = {
        a + b
        for a in ACCEPTABLE_BASE_AMOUNT_UNITS
        for b in ACCEPTABLE_WEIGHT_UNITS
    }
    return combined | set(STANDALONE_AMOUNT_UNITS)

ACCEPTABLE_AMOUNT_UNITS = _acceptable_amount_units()

def _acceptable_rate_units() -> Set[str]:
    """Generate all acceptable rate unit combinations.

    Creates a cartesian product of amount units, weight qualifiers, and time units
    to generate all valid rate unit patterns that the converter can handle.

    Returns
    -------
    Set[str]
        Set of all valid rate unit combinations.

    Examples
    --------
    >>> rate_units = _acceptable_rate_units()
    >>> 'mcg/kg/hr' in rate_units
    True
    >>> 'ml/min' in rate_units
    True
    >>> 'tablespoon/hr' in rate_units
    False

    Notes
    -----
    Rate units are combinations of:

    - Amount tokens: `ACCEPTABLE_BASE_AMOUNT_UNITS` (the standalone cells and
      ppm have no rate form)
    - Weight qualifiers: `ACCEPTABLE_WEIGHT_UNITS` (/kg, /lb, or none)
    - Time units: `ACCEPTABLE_TIME_UNITS` (/min plus every key of
      `TIME_TO_BASE_FACTOR`: /day, /hr)

    See Also
    --------
    _acceptable_amount_units : Same, minus the time axis.
    """
    # find the cartesian product of the three sets
    return {
        a + b + c
        for a in ACCEPTABLE_BASE_AMOUNT_UNITS
        for b in ACCEPTABLE_WEIGHT_UNITS
        for c in ACCEPTABLE_TIME_UNITS
    }

ACCEPTABLE_RATE_UNITS = _acceptable_rate_units()

ALL_ACCEPTABLE_UNITS = ACCEPTABLE_RATE_UNITS | ACCEPTABLE_AMOUNT_UNITS

def _convert_set_to_str_for_sql(s: Set[str]) -> str:
    """Convert a set of strings to SQL IN clause format.

    Transforms a Python set into a comma-separated string suitable for use
    in SQL IN clauses within DuckDB queries.

    Parameters
    ----------
    s : Set[str]
        Set of strings to be formatted for SQL.

    Returns
    -------
    str
        Comma-separated string with items separated by "','".
        Does not include outer quotes - those are added in SQL query.

    Examples
    --------
    >>> units = {'ml/hr', 'mcg/min', 'u/hr'}
    >>> _convert_set_to_str_for_sql(units)
    "ml/hr','mcg/min','u/hr"

    Usage in SQL queries:

    >>> # f"WHERE unit IN ('{_convert_set_to_str_for_sql(units)}')"

    A quote inside a value is escaped:

    >>> _convert_set_to_str_for_sql(["patient's own"])
    "patient''s own"

    Notes
    -----
    This is a helper function for building DuckDB SQL queries that need to check
    if values are in a set of acceptable units.

    Single quotes inside a value are doubled, the SQL escape inside a
    single-quoted literal, so a caller-supplied value (`countable_units`) can
    never end the literal early or inject SQL.
    """
    return "','".join(str(v).replace("'", "''") for v in s)

RATE_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_RATE_UNITS)
AMOUNT_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_AMOUNT_UNITS)


def _canonical_spelling_expr(col: str) -> str:
    """Build a SQL expression rewriting a unit's leading token to mCIDE spelling.

    Applied wherever a unit leaves clifpy: `_clean_unit`, `_base_unit`, the
    labels clifpy chooses itself (the fallback to `_base_unit`, an unlisted
    row's own unit), a failed row's unit and the unit a status message names.
    The internal tokens stay `u` / `mu` / `mnu`, which the grammar matches on.
    When the caller names a preferred unit, their spelling is echoed back
    verbatim instead, so `u/min` in gives `u/min` out. Each token is rewritten
    only when a `/` or the end follows it, so an unrecognized string such as
    `unk` is left alone.

    Tokens are rewritten longest-first (`mnu` before `mu` before `u`), and each
    pattern is `^`-anchored, so no rewrite can cascade into another: once
    `mnu/hr` becomes `million-units/hr` it no longer starts with `mu` or `u`.

    Parameters
    ----------
    col : str
        Unit column to rewrite.

    Returns
    -------
    str
        Nested `regexp_replace` expression.

    Examples
    --------
    >>> expr = _canonical_spelling_expr('_base_unit')
    >>> 'million-units' in expr and 'milli-units' in expr
    True
    """
    expr = col
    for token in sorted(CANONICAL_UNIT_SPELLING, key=lambda t: (-len(t), t)):
        # Bounded by `/` or the end: a cleaned but unrecognized string such as
        # `unk` must not become `unitsnk`.
        expr = (
            f"regexp_replace({expr}, '^{token}(/|$)', "
            f"'{CANONICAL_UNIT_SPELLING[token]}\\1')"
        )
    return expr
