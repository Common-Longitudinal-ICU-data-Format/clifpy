"""Patient-weight lookup for weight-qualified dose conversions.

Only the XOR cases need it: converting a weighted unit to an unweighted one, or
the reverse. `/lb` <-> `/kg` is a constant factor and never touches this module.

`attach_weight` is the general entry point: an ordered chain of lookup
strategies, the first to yield a weight winning per row. `find_most_recent_weight`
is the original single-purpose as-of lookup (the latest weight at or before
each dose), kept as a thin wrapper with its output contract unchanged. The
time-dependent lookups (`asof`, and `same_hosp_earliest` under a cap) are
window functions over one sorted set of dose and weight rows, not ASOF joins
(see `_window_lookup_ctes`); the others are hash joins. # TODO: check the functions mentioned
"""

from __future__ import annotations

import collections.abc
import math
import numbers
import uuid
from decimal import Decimal
from typing import Sequence

import duckdb
import numpy as np

from clifpy.utils.logging_config import get_logger
from ._columns import validate_column_name, quote_identifier, WEIGHT_OUTPUT_COLUMNS
from ._inputs import (
    FrameLike,
    check_no_shadowing_tables,
    column_names,
    on_default_connection,
    pandas_index,
    read_input,
)
from ._outputs import ReturnFormat, emit, resolve_return_format, with_weight_time_zone

logger = get_logger('utils.unit_converter')

# Every strategy `attach_weight` understands, in the canonical order the joins
# are emitted. The caller's `fallbacks` order decides precedence, not this one.
WEIGHT_FALLBACKS = (
    'asof',
    'same_hosp_earliest',
    'prior_hosp_latest',
    'later_hosp_earliest',
)

# Strategies that look across a patient's other stays. `vitals` carries no
# `patient_id`, so these need the hospitalization table to link stays.
_CROSS_HOSP_FALLBACKS = frozenset({'prior_hosp_latest', 'later_hosp_earliest'})

_VITALS_REQUIRED_COLS = ('recorded_dttm', 'vital_category', 'vital_value')
_HOSP_REQUIRED_COLS = ('patient_id', 'admission_dttm')
# One definition, in _columns: the converter's column-name rules need it too.
_WEIGHT_OUTPUT_COLS = WEIGHT_OUTPUT_COLUMNS

_US_PER_HOUR = 3_600_000_000
_US_PER_DAY = 86_400_000_000
# Longest staleness cap kept as a cap: 2**62 us, about 146,000 years.
# epoch_us() is BIGINT and `epoch_us(time_col) - cap` must not overflow it.
# A longer cap can never bind on real timestamps, so it means "no cap".
_MAX_STALENESS_US = 2**62


def _require_columns(df, required: Sequence[str], name: str) -> None:
    """Raise if `df` (polars, relation or pandas) lacks any of `required`."""
    available = column_names(df, name)
    missing = [c for c in required if c not in available]
    if missing:
        raise ValueError(
            f"{name} is missing required column(s) {missing}; "
            f"available: {sorted(available)}"
        )


def _coerce_max_staleness_days(value) -> float | None:
    """Validate a staleness cap; return it in days as a float, or None for no cap.

    Accepts any real number of days: int, float, Fraction, numpy integer and
    floating scalars, and Decimal. Rejects bool (numpy's too), NaN, negatives
    and non-numbers with a ValueError naming the problem. A cap too long for
    the microsecond arithmetic in :func:`attach_weight` (``float('inf')``,
    ``1e300``, ``10**400``) can never bind on real timestamps and is returned
    as None.
    """
    if value is None:
        return None
    if isinstance(value, (bool, np.bool_)):
        raise ValueError(
            f"max_staleness_days must be a number of days or None, not a bool; "
            f"got {value!r}"
        )
    if not isinstance(value, (numbers.Real, Decimal)):
        raise ValueError(
            f"max_staleness_days must be a real number of days or None; got "
            f"{value!r} ({type(value).__name__})"
        )
    try:
        days = float(value)
    except OverflowError:  # an int too large for a float
        days = math.inf if value > 0 else -math.inf
    if math.isnan(days):
        raise ValueError(
            "max_staleness_days must not be NaN; pass None for no staleness limit"
        )
    if days < 0:
        raise ValueError(f"max_staleness_days must be >= 0; got {value!r}")
    if days * _US_PER_DAY > _MAX_STALENESS_US:
        logger.debug(
            f"max_staleness_days={value!r} is longer than any real time span; "
            f"treating it as no limit"
        )
        return None
    return days


def _tz_awareness(rel: duckdb.DuckDBPyRelation, col: str) -> tuple:
    """Return ``(is_tz_aware, type_name)`` for column `col` of relation `rel`.

    Reads the relation's schema only: no rows are scanned. `attach_weight`
    calls it once its inputs are relations on DuckDB's default connection, so
    one rule covers every input type. A pandas frame (numpy- or
    pyarrow-backed) or a polars frame arrives through Arrow, where a zoned
    timestamp becomes TIMESTAMP WITH TIME ZONE and a naive one TIMESTAMP,
    TIMESTAMP_NS, TIMESTAMP_MS or TIMESTAMP_S.
    """
    type_name = str(dict(zip(rel.columns, rel.types))[col])
    return type_name.endswith('WITH TIME ZONE'), type_name


def _retype_time_columns(
    med_df: duckdb.DuckDBPyRelation,
    time_col: str,
    vitals_df: duckdb.DuckDBPyRelation,
    hospitalization_df: duckdb.DuckDBPyRelation | None = None,
) -> tuple:
    """Give a time column that holds no times the type of the column it pairs with.

    Returns ``(med_df, vitals_df, hospitalization_df)``. An Arrow null-typed
    column (``pd.DataFrame(columns=...)``, ``pl.DataFrame({c: []})``, a pandas
    column of only None) reaches DuckDB as an untyped INTEGER, which
    `_require_matching_tz` would call naive and `epoch_us()` cannot bind. So a
    time column of `med_df` (`time_col`), `vitals_df` (`recorded_dttm`) or, when
    given, `hospitalization_df` (`admission_dttm`) whose type is not a DATE or
    TIMESTAMP and that holds no non-null value is CAST in a projection:
    `med_df`'s and `vitals_df`'s pair with each other (TIMESTAMP when both are
    empty), `admission_dttm` takes `med_df`'s resulting type. Only the
    comparisons and arithmetic see the cast; other columns are untouched.

    A wrong-typed column that does hold values (text, a genuine integer) is
    never parsed silently. Reads relation types, and runs one
    ``IS NOT NULL ... LIMIT 1`` probe for each wrong-typed column.

    Raises
    ------
    ValueError
        Naming the input, the column and its type, when it holds values.
    """
    def is_temporal(rel, col) -> bool:
        type_name = str(dict(zip(rel.columns, rel.types))[col])
        return type_name == 'DATE' or type_name.startswith('TIMESTAMP')

    def type_of(rel, col) -> str:
        return str(dict(zip(rel.columns, rel.types))[col])

    def holds_no_times(rel, col, name) -> bool:
        """True for an empty untyped column; raises for text or numbers."""
        probe = rel.filter(f"{quote_identifier(col)} IS NOT NULL").limit(1).fetchone()
        if probe is not None:
            raise ValueError(
                f"{name} column {col!r} has type {type_of(rel, col)}, not a date "
                f"or timestamp. Parse it first (polars: .str.to_datetime(...); "
                f"DuckDB: strptime(...) or CAST(... AS TIMESTAMP)); text is not "
                f"cast to timestamps silently."
            )
        return True

    def recast(rel, col, target):
        qc = quote_identifier(col)
        return rel.project(
            ', '.join(
                f"CAST({qc} AS {target}) AS {qc}" if c == col else quote_identifier(c)
                for c in rel.columns
            )
        )

    med_empty = (
        not is_temporal(med_df, time_col)
        and holds_no_times(med_df, time_col, 'med_df')
    )
    vit_empty = (
        not is_temporal(vitals_df, 'recorded_dttm')
        and holds_no_times(vitals_df, 'recorded_dttm', 'vitals_df')
    )
    if med_empty:
        target = 'TIMESTAMP' if vit_empty else type_of(vitals_df, 'recorded_dttm')
        med_df = recast(med_df, time_col, target)
    if vit_empty:
        vitals_df = recast(vitals_df, 'recorded_dttm', type_of(med_df, time_col))
    if (
        hospitalization_df is not None
        and not is_temporal(hospitalization_df, 'admission_dttm')
        and holds_no_times(hospitalization_df, 'admission_dttm', 'hospitalization_df')
    ):
        hospitalization_df = recast(
            hospitalization_df, 'admission_dttm', type_of(med_df, time_col),
        )
    return med_df, vitals_df, hospitalization_df


def _require_matching_tz(
    med_df: duckdb.DuckDBPyRelation,
    time_col: str,
    vitals_df: duckdb.DuckDBPyRelation,
) -> None:
    """Raise if exactly one of `med_df[time_col]` / `vitals_df.recorded_dttm` is tz-aware.

    DuckDB compares a TIMESTAMP with a TIMESTAMPTZ by reading the naive side in
    the session TimeZone, which clifpy itself moves (load_data pins UTC, the
    converter pins the input's zone), while the staleness gate and
    `_weight_age_hours` read it as UTC through epoch_us(). The weight picked
    would depend on that setting. Two aware columns in different zones, or two
    naive ones of different precision, compare correctly and pass.
    """
    med_aware, med_type = _tz_awareness(med_df, time_col)
    vit_aware, vit_type = _tz_awareness(vitals_df, 'recorded_dttm')
    if med_aware == vit_aware:
        return
    med_side = f"med_df column {time_col!r} ({med_type})"
    vit_side = f"vitals_df column 'recorded_dttm' ({vit_type})"
    aware_side, naive_side = (
        (med_side, vit_side) if med_aware else (vit_side, med_side)
    )
    raise ValueError(
        f"{aware_side} is tz-aware but {naive_side} is naive. DuckDB would "
        f"compare them by reading the naive one in the session TimeZone, so the "
        f"weight picked would depend on that setting. Localize the naive column "
        f"to the site's zone (polars: .dt.replace_time_zone(tz); DuckDB: "
        f"timezone(tz, col); pandas: .dt.tz_localize(tz)), or make both naive."
    )


def _validate_weight_args(
    fallbacks: Sequence[str],
    hospitalization_df,
    id_name: str,
    max_staleness_days: float | None,
) -> tuple:
    """Validate the weight-lookup arguments and return `fallbacks` as a tuple.

    Shared by :func:`attach_weight` and the converter, which calls it up front
    so a bad chain fails before any temp table is created. Duplicate names are
    dropped, keeping the first occurrence. `max_staleness_days` is only checked
    here; callers convert it with :func:`_coerce_max_staleness_days`.
    """
    validate_column_name(id_name, 'id_name')
    if isinstance(fallbacks, str):
        raise ValueError(
            f"fallbacks must be a sequence of strategy names, not a str; "
            f"did you mean ({fallbacks!r},)?"
        )
    if isinstance(fallbacks, collections.abc.Set):
        # The chain's order IS its precedence, and a set of strings iterates in
        # an order that changes with PYTHONHASHSEED, i.e. between runs.
        raise TypeError(
            f"fallbacks must be an ordered sequence such as a tuple or list, "
            f"not a {type(fallbacks).__name__}: the order sets precedence, and "
            f"a set's order changes between Python runs"
        )
    fallbacks = tuple(dict.fromkeys(fallbacks))
    if not fallbacks:
        raise ValueError(
            f"fallbacks must name at least one strategy from {WEIGHT_FALLBACKS}"
        )
    unknown = [f for f in fallbacks if f not in WEIGHT_FALLBACKS]
    if unknown:
        raise ValueError(
            f"unknown weight fallback(s) {unknown}; "
            f"choose from {WEIGHT_FALLBACKS}"
        )

    cross_hosp = [f for f in fallbacks if f in _CROSS_HOSP_FALLBACKS]
    if cross_hosp and hospitalization_df is None:
        raise ValueError(
            f"fallback(s) {cross_hosp} look across a patient's other stays and "
            f"need hospitalization_df (with {id_name}, patient_id, "
            f"admission_dttm); vitals carry no patient_id"
        )
    if hospitalization_df is not None:
        _require_columns(
            hospitalization_df, (id_name, *_HOSP_REQUIRED_COLS),
            'hospitalization_df',
        )

    _coerce_max_staleness_days(max_staleness_days)
    return fallbacks


def _warn_if_cross_stay_uncovered(
    med_df, vitals_df, hospitalization_df, id_name: str, cross_hosp: Sequence[str],
) -> None:
    """WARN when the cross-stay strategies can only read stays inside `med_df`.

    `prior_hosp_latest` and `later_hosp_earliest` read each patient's OTHER
    stays, so `hospitalization_df` and `vitals_df` should cover those stays,
    not only the ones in `med_df`. Loading either filtered to the cohort's IDs
    -- clifpy's usual pattern -- hides every stay outside the cohort, which
    reads exactly like "no other stay has a weight". A readmission that is
    itself inside `med_df` can still borrow from its sibling stay, so the
    warning claims only that stays outside `med_df` are out of reach. One
    aggregate query over the three inputs. A heuristic, so it warns and never raises:
    if the query itself fails, the check is skipped with a DEBUG line and the
    join below reports the real problem.
    """
    idq = quote_identifier(id_name)
    try:
        n_other_stays, n_other_weighted = duckdb.sql(f"""
            -- the stays of med_df's patients that med_df itself does not touch
            FROM hospitalization_df h
            LEFT JOIN (
                FROM vitals_df
                SELECT DISTINCT {idq}
                WHERE vital_category = 'weight_kg'
                    AND vital_value > 0
                    AND isfinite(CAST(vital_value AS DOUBLE))
                    AND recorded_dttm IS NOT NULL
            ) w ON h.{idq} = w.{idq}
            SELECT
                n_other_stays: COUNT(DISTINCT h.{idq})
                , n_other_weighted: COUNT(DISTINCT w.{idq})
            WHERE h.{idq} NOT IN (FROM med_df SELECT {idq} WHERE {idq} IS NOT NULL)
                AND h.patient_id IN (
                    FROM hospitalization_df hm
                    SEMI JOIN med_df m ON hm.{idq} = m.{idq}
                    SELECT hm.patient_id
                )
        """).fetchone()
    except duckdb.Error as exc:
        logger.debug(f"Skipped the cross-stay coverage check: {exc}")
        return
    advice = (
        f"load it for every stay of the cohort's patients, not only the "
        f"cohort's {id_name} values"
    )
    if n_other_stays == 0:
        logger.warning(
            f"weight fallback(s) {list(cross_hosp)} read each patient's other "
            f"stays, but hospitalization_df lists no stay of med_df's patients "
            f"outside med_df, so these strategies can only read stays inside "
            f"med_df. If hospitalization_df was filtered to the cohort, {advice}."
        )
    elif n_other_weighted == 0:
        logger.warning(
            f"weight fallback(s) {list(cross_hosp)} read each patient's other "
            f"stays, but vitals_df has no weight_kg on any of the "
            f"{n_other_stays} stay(s) of med_df's patients outside med_df, so "
            f"these strategies can only find weights charted in stays inside "
            f"med_df. If vitals_df was filtered to the cohort, {advice}."
        )


def _window_lookup_ctes(
    source: str,
    idq: str,
    tq: str,
    *,
    backward: bool,
    cap_us: int | None,
    map_ids: bool,
    suffix: str,
) -> tuple:
    """CTEs for the per-dose lookups: `asof`, and `same_hosp_earliest` under a cap.

    Returns ``(ctes, picks, hidden)``: the CTE texts, the last of which,
    `_doses`, holds every `source` row with all its columns plus each
    lookup's weight and recorded time; ``{strategy: (weight column,
    recorded-time column)}`` for the lookups asked for; and the names of the
    added columns, which the caller leaves out of its result.

    Both lookups are windows over one set of dose rows and weight rows,
    sorted per stay, not ASOF joins. DuckDB estimates an Arrow scan (every
    polars or pandas input) at about one row, below
    `asof_loop_join_threshold`, and plans such an ASOF join as a
    NESTED_LOOP_JOIN, whose time grows with med rows times weight rows. A
    window stays lazy and needs no connection setting. The running
    `arg_max`/`arg_min` frames scale with the sort; `last_value(... IGNORE
    NULLS)` rescans each run of doses and turns quadratic in a large stay.

    - `backward` (`asof`): the latest weight at or before the dose, ordered
      on the timestamps in the type DuckDB compares them in.
    - `cap_us` (`same_hosp_earliest` under a cap): the first weight charted
      no earlier than `time_col - cap`, on epoch_us() keys, so neither DST
      nor the session TimeZone moves the boundary.

    A dose with no time or no ID has no stay to sort into, so it gets no
    weight from either lookup. When the two ID columns differ in type
    (`map_ids`), each weight takes the ID of the dose stay it equals: `=`
    and a UNION cast mixed types differently (`'01' = 1` holds, while a
    UNION turns 1 into `'1'`), so stays match exactly as `=` matches them.
    """
    def name(part: str) -> str:
        return f"_aw_{part}_{suffix}"

    # union columns: sort keys, then the weight row's value and time
    stay, is_dose, at, from_us, kg, rec = map(
        name, ('stay', 'is_dose', 'at', 'from_us', 'kg', 'rec'),
    )
    dose_keys, weight_keys, key_cols, windows, lookups = [], [], [], [], []
    picks = {}
    if backward:
        asof_kg, asof_rec = name('asof_kg'), name('asof_rec')
        key_cols.append(at)
        dose_keys.append(f", {at}: {tq}")
        weight_keys.append(f", {at}: w.recorded_dttm")
        # The latest weight in the frame up to and including the dose.
        windows.append(f"""
        _back AS (
            PARTITION BY {stay}
            ORDER BY {at} ASC
                , {is_dose} ASC  -- weight first at its instant: AT or before
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""")
        lookups.append(f"""
            , {asof_kg}: arg_max({kg}, {rec}) OVER _back
            , {asof_rec}: max({rec}) OVER _back""")
        picks['asof'] = (asof_kg, asof_rec)
    if cap_us is not None:
        she_kg, she_rec = name('she_kg'), name('she_rec')
        key_cols.append(from_us)
        dose_keys.append(f", {from_us}: epoch_us({tq}) - {cap_us}")
        weight_keys.append(f", {from_us}: epoch_us(w.recorded_dttm)")
        # The earliest weight at or after the dose's cap key, read as a
        # running frame over the keys sorted DESCENDING: a frame that runs to
        # the partition's end (CURRENT ROW .. UNBOUNDED FOLLOWING) makes
        # min() superlinear in one large stay.
        windows.append(f"""
        _fwd AS (
            PARTITION BY {stay}
            ORDER BY {from_us} DESC
                , {is_dose} ASC  -- weight first at its key: the cap is inclusive
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)""")
        lookups.append(f"""
            , {she_kg}: arg_min({kg}, {rec}) OVER _fwd
            , {she_rec}: min({rec}) OVER _fwd""")
        picks['same_hosp_earliest'] = (she_kg, she_rec)
    hidden = [col for pair in picks.values() for col in pair]

    if map_ids:
        weights = (
            f"_weights w JOIN (FROM med_df SELECT DISTINCT {idq}) d "
            f"ON w.{idq} = d.{idq}"
        )
        weight_stay = f"d.{idq}"
    else:
        weights, weight_stay = "_weights w", f"w.{idq}"
    ctes = [f"""
    _lookup_rows AS (
        -- every dose, then one row per weight; each lookup is a window
        -- over this one set, sorted per stay
        FROM {source}
        SELECT *
            -- no time or no ID: no stay to sort into, so no weight
            , {stay}: CASE WHEN {tq} IS NOT NULL THEN {idq} END
            , {is_dose}: TRUE
            {' '.join(dose_keys)}
        UNION ALL BY NAME
        FROM {weights}
        SELECT {stay}: {weight_stay}
            , {is_dose}: FALSE
            , {kg}: w.weight_kg
            , {rec}: w.recorded_dttm
            {' '.join(weight_keys)}
        WHERE w.{idq} IS NOT NULL
    )""", f"""
    _lookups AS (
        FROM _lookup_rows
        SELECT * EXCLUDE ({', '.join((stay, kg, rec, *key_cols))})
            {''.join(lookups)}
        WINDOW {','.join(windows)}
    )""", f"""
    _doses AS (
        FROM _lookups SELECT * EXCLUDE ({is_dose}) WHERE {is_dose}
    )"""]
    return ctes, picks, hidden


def attach_weight(
    med_df: FrameLike,
    vitals_df: FrameLike,
    *,
    id_name: str = 'hospitalization_id',
    time_col: str = 'admin_dttm',
    fallbacks: Sequence[str] = ('asof',),
    hospitalization_df: FrameLike | None = None,
    max_staleness_days: float | None = None,
    return_format: ReturnFormat = 'polars',
) -> FrameLike:
    """Attach a patient weight to every row via an ordered fallback chain.

    For each row of `med_df`, the strategies in `fallbacks` are tried in order
    and the **first** that yields a weight wins. Rows no strategy can serve get
    NULL weight and NULL provenance.

    Strategies (see `WEIGHT_FALLBACKS`):

    - `'asof'`: latest weight recorded at or before `time_col` in the same
      `id_name`. A row with a NULL `time_col` has no "at or before", so
      `asof` never serves it.
    - `'same_hosp_earliest'`: earliest weight in the same `id_name`. Recovers
      stays where the drug started before anyone charted a weight -- what
      `fallback_on_earliest=True` has always done. With `max_staleness_days`,
      the earliest weight *eligible for that row*.
    - `'prior_hosp_latest'`: latest weight from the patient's *other* stays
      admitted strictly before this one.
    - `'later_hosp_earliest'`: earliest weight from the patient's *other* stays
      admitted at or after this one. # TODO: do the other ones consider max_staleness_days?

    Parameters
    ----------
    med_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Rows needing a weight, with at least `{id_name}` and `{time_col}`.
        A relation may come from any DuckDB connection; one from a connection
        other than the default is copied through Arrow (logged at INFO).
    vitals_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Vitals with `{id_name}`, `recorded_dttm`, `vital_category`,
        `vital_value`. Only `vital_category = 'weight_kg'` rows with a
        positive, finite value are used.
    id_name : str, default 'hospitalization_id'
        Stay-level ID shared by `med_df`, `vitals_df` and `hospitalization_df`.
    time_col : str, default 'admin_dttm'
        Timestamp in `med_df` the weight is looked up relative to.
    fallbacks : tuple or list of str, default ('asof',)
        Ordered strategy names from `WEIGHT_FALLBACKS`. A set raises
        TypeError: its order, and so the precedence, changes between runs.
        `('asof', 'same_hosp_earliest')` is the recommended default for
        conversion.
    hospitalization_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame, optional
        `{id_name}`, `patient_id`, `admission_dttm`. Required by the two
        cross-stay strategies.
    max_staleness_days : real number, optional
        When set, a weight charted before `time_col` is ineligible for that row
        if it is more than this many days old; weights charted after the dose
        always stay eligible. Per strategy:

        - `'asof'`, `'prior_hosp_latest'`: an ineligible pick counts as
          missing and the row falls through. Each picks its latest candidate,
          so nothing older could qualify instead.
        - `'same_hosp_earliest'`: the earliest eligible weight, per row -- the
          first inside the window, else the first after the dose.
        - `'later_hosp_earliest'`: not gated. Its weights come from stays
          admitted at or after this one, usually after the dose, but a twin
          admission or an overlapping stay can chart one before it.

        None (default) disables the check entirely. It does not make a row
        with a NULL `time_col` matchable: `asof` never serves such a row, with
        or without a limit, and under a limit neither do `same_hosp_earliest`
        and `prior_hosp_latest`. Uncapped `same_hosp_earliest` and
        `prior_hosp_latest`, and `later_hosp_earliest`, do not read the dose
        time, so they still can.
        Any real number of days works (int, float, numpy scalar, Decimal);
        `float('inf')`, or a cap longer than about 146,000 years, can never
        bind and is treated exactly as None.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, default 'polars'
        Output format, as for :func:`clifpy.utils.io.load_data`. ``'pandas'``
        is deprecated (DeprecationWarning) and retires in the next release.

    Returns
    -------
    pl.DataFrame, pl.LazyFrame, duckdb.DuckDBPyRelation or pd.DataFrame
        In the format `return_format` names (a polars DataFrame by default, a
        lazy relation on the default connection for ``'duckdb'``): every
        `med_df` column, unchanged and in order, plus

        - `weight_kg` (DOUBLE): the chosen weight, NULL if none
        - `_weight_source` (VARCHAR): the winning strategy name, NULL if none
        - `_weight_recorded_dttm`: when the chosen weight was charted,
          labelled in `time_col`'s zone when that column is tz-aware
        - `_weight_age_hours` (DOUBLE): hours from `_weight_recorded_dttm` to
          `time_col`; negative when the weight was charted after the dose

        Exactly one output row per input row. The eager formats come back in
        input order; a `'duckdb'` relation has no row order of its own, as for
        the converter.

    Raises
    ------
    ValueError
        Unknown or empty `fallbacks`; a cross-stay strategy without
        `hospitalization_df`; missing required columns; `med_df` already
        carrying one of the output columns; invalid `max_staleness_days`; an
        unknown `return_format`; a table or view on DuckDB's default
        connection named like a variable this function's SQL reads (`med_df`,
        `vitals_df`, `hospitalization_df`); `time_col` and `recorded_dttm`
        disagreeing on tz-awareness.
    TypeError
        `fallbacks` given as a set or frozenset. Also an input that is not a
        polars DataFrame or LazyFrame, a DuckDB relation or a pandas
        DataFrame.

    Notes
    -----
    - Weights of zero or below, NaN and infinite weights are discarded (they
      would divide by zero, flip the sign of a converted dose or make it NaN
      or infinite). Several weights charted at the same
      instant in the same stay are averaged, so ties resolve deterministically.
    - Timestamps are never cast. Only `return_format='duckdb'` sets the session
      `TimeZone`, pinning it to `time_col`'s zone so a later `.pl()` shows that
      zone. A TIMESTAMPTZ `time_col` stays TIMESTAMPTZ with the same instants.
      `time_col` and `recorded_dttm` must both be tz-aware or both naive
      (zones and precisions may differ). A mix raises ValueError, because
      DuckDB would compare a TIMESTAMP with a TIMESTAMPTZ by reading the naive
      side in the session zone.
    - Two different stays with identical `admission_dttm` count each other as
      *later*, never *prior* (strict `<` for prior, `>=` for later). Stays with
      a NULL `admission_dttm` are not linked either way.
    - Cross-stay weights can be months old. `_weight_age_hours` carries that
      distance so it can be filtered or reported.
    - The cross-stay strategies read each patient's *other* stays, so
      `hospitalization_df` and `vitals_df` must cover those stays, not only the
      stays in `med_df`. When `hospitalization_df` lists no stay of a `med_df`
      patient outside `med_df`, or `vitals_df` has no weight on any of them, a
      WARNING says the strategies can only read stays inside `med_df` (one
      aggregate query, run when this function is called).
    - Converting this output keeps its `_weight_source` and
      `_weight_age_hours` as they are: the converter writes none of its own
      next to them, returns them in its lean output and groups its counts
      table by `_weight_source`. A `weight_kg` supplied without
      `_weight_source` reads `'prefilled'` there instead, and a converter
      lookup labels rows needing no weight `'not_needed'`.

    Examples
    --------
    >>> rel = attach_weight(                             # doctest: +SKIP
    ...     mac_df, vitals_df,
    ...     fallbacks=('asof', 'same_hosp_earliest', 'prior_hosp_latest'),
    ...     hospitalization_df=hosp_df, max_staleness_days=7)
    """
    fmt = resolve_return_format(return_format)
    fallbacks = _validate_weight_args(
        fallbacks, hospitalization_df, id_name, max_staleness_days,
    )
    max_staleness_days = _coerce_max_staleness_days(max_staleness_days)
    validate_column_name(time_col, 'time_col')
    _require_columns(med_df, (id_name, time_col), 'med_df')
    _require_columns(vitals_df, (id_name, *_VITALS_REQUIRED_COLS), 'vitals_df')
    clash = [c for c in column_names(med_df, 'med_df') if str(c).lower() in _WEIGHT_OUTPUT_COLS]
    if clash:
        raise ValueError(
            f"med_df already has column(s) {clash}; drop them before attaching "
            f"weight (a pre-attached weight_kg should be used as-is instead)"
        )

    # The SQL below reads med_df, vitals_df and hospitalization_df by name.
    check_no_shadowing_tables()

    # Read every input onto DuckDB's default connection with no pandas
    # operation (pandas through Arrow); med_df's zones are kept so the eager
    # formats come back labelled as the input was. A relation from a
    # caller-owned connection is copied through Arrow -- vitals and
    # hospitalization cut to the rows and columns used here first.
    idx = pandas_index(med_df)
    med_df, input_tz_map = read_input(med_df, 'med_df', detach=fmt == 'duckdb')
    vitals_df = on_default_connection(
        vitals_df, 'vitals_df', columns=(id_name, *_VITALS_REQUIRED_COLS),
        where="vital_category = 'weight_kg'", detach=fmt == 'duckdb',
    )
    hospitalization_df = on_default_connection(
        hospitalization_df, 'hospitalization_df',
        columns=(id_name, *_HOSP_REQUIRED_COLS), detach=fmt == 'duckdb',
    )

    # Every input is a relation on the default connection by now, whatever the
    # caller passed, so one schema read covers pandas, polars and relations.
    # An untyped empty time column takes its partner's type first, so it is
    # neither called naive below nor left unbound in epoch_us().
    med_df, vitals_df, hosp_retyped = _retype_time_columns(
        med_df, time_col, vitals_df,
        hospitalization_df if _CROSS_HOSP_FALLBACKS & set(fallbacks) else None,
    )
    if _CROSS_HOSP_FALLBACKS & set(fallbacks):
        hospitalization_df = hosp_retyped
    _require_matching_tz(med_df, time_col, vitals_df)

    cross_hosp = [f for f in fallbacks if f in _CROSS_HOSP_FALLBACKS]
    if cross_hosp:
        _warn_if_cross_stay_uncovered(
            med_df, vitals_df, hospitalization_df, id_name, cross_hosp,
        )

    logger.info(f"Attaching patient weights (fallbacks: {', '.join(fallbacks)})...")
    idq = quote_identifier(id_name)
    tq = quote_identifier(time_col)
    # Whole microseconds, compared on epoch_us(): elapsed time between
    # instants, so neither DST nor the session TimeZone can move the boundary.
    limit_us = (
        None if max_staleness_days is None
        else int(round(max_staleness_days * _US_PER_DAY))
    )

    # Positive, finite weights only, one row per (stay, instant). Collapsing
    # duplicates here makes every ARG_MIN/ARG_MAX below deterministic, the
    # window lookups' included. isfinite() is needed on top of `> 0`: DuckDB
    # sorts NaN above every number, so a NaN in a DuckDB relation passes it,
    # and +inf from any input does too.
    ctes = [f"""
    _weights AS (
        FROM vitals_df
        SELECT {idq}, recorded_dttm, weight_kg: AVG(vital_value)
        WHERE vital_category = 'weight_kg'
            AND vital_value IS NOT NULL
            AND vital_value > 0
            AND isfinite(CAST(vital_value AS DOUBLE))
            AND recorded_dttm IS NOT NULL
        GROUP BY {idq}, recorded_dttm
    )"""]
    joins = []

    # The eager formats come back in input order: the window lookups and hash
    # joins below emit rows grouped by stay and time, so number the input
    # rows. A bare row_number() OVER () is a streaming window, numbered in
    # scan order. The random suffix keeps the name clear of every caller
    # column. A 'duckdb' relation has no row order of its own (the converter
    # re-sorts on its own ordinal), so it skips the numbering and the sort,
    # which would only block pushdown. `emit` does the one sort and drops the
    # ordinal.
    suffix = uuid.uuid4().hex[:8]
    ord_col = None if fmt == 'duckdb' else f"_attach_weight_ord_{suffix}"
    source = (
        "med_df" if ord_col is None
        else f"(FROM med_df SELECT *, {ord_col}: row_number() OVER ())"
    )

    # `asof`, and `same_hosp_earliest` under a cap, depend on each dose's
    # time: window lookups over the dose rows themselves (see
    # _window_lookup_ctes for why not ASOF joins). Under a cap the earliest
    # ELIGIBLE weight is the first charted no earlier than (time_col - cap);
    # weights after the dose satisfy that trivially.
    picks, hidden = {}, []
    capped_earliest = 'same_hosp_earliest' in fallbacks and limit_us is not None
    if 'asof' in fallbacks or capped_earliest:
        med_id_type = dict(zip(med_df.columns, med_df.types))[id_name]
        vitals_id_type = dict(zip(vitals_df.columns, vitals_df.types))[id_name]
        # An empty med_df has no stay to map weights onto. Skipping the
        # mapping then also skips its cast, so a weight ID that cannot be cast
        # to the dose ID's type (text 'abc' against BIGINT) does not raise for
        # an empty input, as the ASOF join this replaced never compared it.
        # One LIMIT 1 probe, run only when the two ID types differ. For
        # return_format='duckdb' the rule is fixed here, at call time: a
        # relation that is empty now and filled before the result is
        # evaluated is matched without the mapping, by the UNION's cast
        # ('1' does not equal '01').
        map_ids = (
            str(med_id_type) != str(vitals_id_type)
            and med_df.limit(1).fetchone() is not None
        )
        lookup_ctes, picks, hidden = _window_lookup_ctes(
            source, idq, tq,
            backward='asof' in fallbacks,
            cap_us=limit_us if capped_earliest else None,
            map_ids=map_ids,
            suffix=suffix,
        )
        ctes += lookup_ctes
        source = "_doses"
        picks = {
            name: (f"m.{kg_col}", f"m.{rec_col}")
            for name, (kg_col, rec_col) in picks.items()
        }

    if 'same_hosp_earliest' in fallbacks and limit_us is None:
        ctes.append(f"""
    _same_hosp_earliest AS (
        FROM _weights w
        SELECT w.{idq}
            , recorded_dttm: MIN(w.recorded_dttm)
            , weight_kg: ARG_MIN(w.weight_kg, w.recorded_dttm)
        GROUP BY w.{idq}
    )""")
        joins.append(f"""
        LEFT JOIN _same_hosp_earliest _she ON m.{idq} = _she.{idq}""")
        picks['same_hosp_earliest'] = ('_she.weight_kg', '_she.recorded_dttm')

    if _CROSS_HOSP_FALLBACKS & set(fallbacks):
        ctes.append(f"""
    _stays AS (
        -- the stays med_df actually touches, tagged with patient + admission
        FROM hospitalization_df h
        SEMI JOIN (FROM med_df SELECT DISTINCT {idq}) s ON h.{idq} = s.{idq}
        SELECT h.{idq}, h.patient_id, h.admission_dttm
    )""")
        ctes.append(f"""
    _weights_by_stay AS (
        FROM _weights w
        JOIN hospitalization_df h ON w.{idq} = h.{idq}
        SELECT w.{idq}, h.patient_id, h.admission_dttm, w.recorded_dttm, w.weight_kg
    )""")

    # Each cross-stay candidate is built in two steps: average weights sharing
    # an instant (across the eligible stays), then pick the latest / earliest
    # instant. One row per stay, so the final LEFT JOIN cannot fan out.
    cross_hosp_spec = {
        # name: (alias, admission comparison, instant aggregate, value aggregate)
        'prior_hosp_latest': ('_phl', '<', 'MAX', 'ARG_MAX'),
        'later_hosp_earliest': ('_lhe', '>=', 'MIN', 'ARG_MIN'),
    }
    for name, (alias, cmp, pick_dttm, pick_value) in cross_hosp_spec.items():
        if name not in fallbacks:
            continue
        ctes.append(f"""
    {alias}_by_instant AS (
        FROM _stays s
        JOIN _weights_by_stay w
            ON w.patient_id = s.patient_id
            AND w.{idq} <> s.{idq}
            AND w.admission_dttm {cmp} s.admission_dttm
        SELECT s.{idq}, w.recorded_dttm, weight_kg: AVG(w.weight_kg)
        GROUP BY s.{idq}, w.recorded_dttm
    )""")
        ctes.append(f"""
    {alias}_pick AS (
        FROM {alias}_by_instant c
        SELECT c.{idq}
            , recorded_dttm: {pick_dttm}(c.recorded_dttm)
            , weight_kg: {pick_value}(c.weight_kg, c.recorded_dttm)
        GROUP BY c.{idq}
    )""")
        joins.append(f"""
        LEFT JOIN {alias}_pick {alias} ON m.{idq} = {alias}.{idq}""")
        picks[name] = (f"{alias}.weight_kg", f"{alias}.recorded_dttm")

    # Under a cap, `asof` and `prior_hosp_latest` each hold their LATEST
    # candidate, so gating that one pick is the whole rule: anything older is
    # staler. `same_hosp_earliest` is gated inside its window lookup above,
    # and `later_hosp_earliest` is never gated.
    gated = {'asof', 'prior_hosp_latest'}
    # One (name, weight, recorded time, usable-condition) per strategy, in the
    # CALLER's order: the first usable candidate wins every CASE below.
    branches = []
    for name in fallbacks:
        kg_col, rec_col = picks[name]
        usable = f"{kg_col} IS NOT NULL"
        if name in gated and limit_us is not None:
            usable += (
                f" AND epoch_us(m.{tq}) - epoch_us({rec_col})"
                f" <= {limit_us}"
            )
        branches.append((name, kg_col, rec_col, usable))

    def _case(pick) -> str:
        whens = ' '.join(
            f"WHEN {usable} THEN {pick(name, kg_col, rec_col)}"
            for name, kg_col, rec_col, usable in branches
        )
        return f"CASE {whens} END"

    select_cols = f"m.* EXCLUDE ({', '.join(hidden)})" if hidden else "m.*"
    q = f"""
    WITH {','.join(ctes)}
    FROM {source} m
    {''.join(joins)}
    SELECT {select_cols}
        , weight_kg: CAST({_case(lambda n, kg, rec: kg)} AS DOUBLE)
        , _weight_source: {_case(lambda n, kg, rec: f"'{n}'")}
        , _weight_recorded_dttm: {_case(lambda n, kg, rec: rec)}
        -- instant difference, so the session TimeZone cannot shift it
        , _weight_age_hours:
            (epoch_us(m.{tq}) - epoch_us(_weight_recorded_dttm)) / {_US_PER_HOUR}.0
    """
    result = duckdb.sql(q)
    logger.info("Weight attachment complete")
    # _weight_recorded_dttm comes from vitals_df; label it like time_col so one
    # result never mixes zones (_outputs.with_weight_time_zone).
    return emit(
        result, fmt, tz_map=with_weight_time_zone(input_tz_map, time_col),
        order_by=ord_col, drop=(ord_col,) if ord_col else (), pandas_index=idx,
        time_col=time_col,
    )


def find_most_recent_weight(
    med_df: FrameLike,
    vitals_df: FrameLike,
    id_name: str = 'hospitalization_id',
    fallback_on_earliest: bool = False,
    ) -> duckdb.DuckDBPyRelation:
    """Find the most recent weight for each medication administration: an as-of lookup.

    Single-purpose utility: for each row in `med_df`, attach the most recent
    `weight_kg` recorded **at or before** `admin_dttm` for the same hospitalization.
    A row with a NULL `admin_dttm` has no "at or before", so it gets no
    weight, or the stay's earliest one with `fallback_on_earliest=True`.

    Parameters
    ----------
    med_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Medication-admin rows with at least `{id_name}` and `admin_dttm` columns.
    vitals_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Vitals rows with `{id_name}`, `recorded_dttm`, `vital_category`, `vital_value`.
    id_name : str, default 'hospitalization_id'
        ID column to join on.
    fallback_on_earliest : bool, default False
        When True, rows with no weight at or before `admin_dttm` (med admin
        precedes the first charted weight — common when documentation lags
        admission) fall back to the **earliest** charted weight for that
        hospitalization. Surfaced in the new `_weight_source` column.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Input columns plus:

        - `weight_kg`: matched weight value (NULL if no prior or fallback weight)
        - `_weight_recorded_dttm`: timestamp of matched weight
        - `_weight_source`: 'asof' | 'earliest_fallback' | NULL — provenance for QA

    Notes
    -----
    Caller is responsible for filtering `med_df` to rows that actually need
    weight before calling. This function does not know about `_needs_wt`.

    A thin wrapper over :func:`attach_weight` with
    `fallbacks=('asof',)` or `('asof', 'same_hosp_earliest')`. Like it,
    non-positive and non-finite weights are ignored and same-instant
    duplicates are averaged.
    Use `attach_weight` directly for cross-stay fallbacks or a staleness limit.
    """
    logger.info("Finding most recent weights...")
    fallbacks = ('asof', 'same_hosp_earliest') if fallback_on_earliest else ('asof',)
    attached = attach_weight(
        med_df, vitals_df, id_name=id_name, fallbacks=fallbacks,
        return_format='duckdb',
    )
    idq = quote_identifier(id_name)
    result = duckdb.sql(f"""
        FROM attached
        SELECT * EXCLUDE (_weight_source, _weight_age_hours)
            -- keep the legacy provenance label for the same-stay fallback
            , _weight_source: CASE _weight_source
                WHEN 'same_hosp_earliest' THEN 'earliest_fallback' -- TODO: not sure why this rename is needed
                ELSE _weight_source END
        ORDER BY {idq} ASC, admin_dttm ASC, med_category ASC
    """)
    logger.info("Weight lookup complete")
    return result
