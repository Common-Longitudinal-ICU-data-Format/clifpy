"""Patient-weight lookup for weight-qualified dose conversions.

Only the XOR cases need it: converting a weighted unit to an unweighted one, or
the reverse. `/lb` <-> `/kg` is a constant factor and never touches this module.

`attach_weight` is the general entry point: an ordered chain of lookup
strategies, the first to yield a weight winning per row. `find_most_recent_weight`
is the original single-purpose ASOF lookup, kept as a thin wrapper with its
output contract unchanged.
"""

import math
from typing import Sequence

import pandas as pd
import duckdb

from clifpy.utils.logging_config import get_logger
from ._columns import validate_column_name, quote_identifier

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
_WEIGHT_OUTPUT_COLS = (
    'weight_kg', '_weight_source', '_weight_recorded_dttm', '_weight_age_hours',
)

_US_PER_HOUR = 3_600_000_000
_US_PER_DAY = 86_400_000_000


def _require_columns(df, required: Sequence[str], name: str) -> None:
    """Raise if `df` (pandas or relation) lacks any of `required`."""
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"{name} is missing required column(s) {missing}; "
            f"available: {sorted(df.columns)}"
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
    dropped, keeping the first occurrence.
    """
    validate_column_name(id_name, 'id_name')
    if isinstance(fallbacks, str):
        raise ValueError(
            f"fallbacks must be a sequence of strategy names, not a str; "
            f"did you mean ({fallbacks!r},)?"
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

    if max_staleness_days is not None:
        if (
            isinstance(max_staleness_days, bool)
            or not isinstance(max_staleness_days, (int, float))
            or math.isnan(max_staleness_days)
            or max_staleness_days < 0
        ):
            raise ValueError(
                f"max_staleness_days must be a non-negative number or None; "
                f"got {max_staleness_days!r}"
            )
    return fallbacks


def attach_weight(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    vitals_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    *,
    id_name: str = 'hospitalization_id',
    time_col: str = 'admin_dttm',
    fallbacks: Sequence[str] = ('asof',),
    hospitalization_df: pd.DataFrame | duckdb.DuckDBPyRelation | None = None,
    max_staleness_days: float | None = None,
) -> duckdb.DuckDBPyRelation:
    """Attach a patient weight to every row via an ordered fallback chain.

    For each row of `med_df`, the strategies in `fallbacks` are tried in order
    and the **first** that yields a weight wins. Rows no strategy can serve get
    NULL weight and NULL provenance.

    Strategies (see `WEIGHT_FALLBACKS`):

    - `'asof'`: latest weight recorded at or before `time_col` in the same
      `id_name`. Honors `max_staleness_days`.
    - `'same_hosp_earliest'`: earliest weight in the same `id_name`. Recovers
      stays where the drug started before anyone charted a weight -- what
      `fallback_on_earliest=True` has always done.
    - `'prior_hosp_latest'`: latest weight from the patient's *other* stays
      admitted strictly before this one.
    - `'later_hosp_earliest'`: earliest weight from the patient's *other* stays
      admitted at or after this one.

    Parameters
    ----------
    med_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Rows needing a weight, with at least `{id_name}` and `{time_col}`.
    vitals_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Vitals with `{id_name}`, `recorded_dttm`, `vital_category`,
        `vital_value`. Only `vital_category = 'weight_kg'` rows with a
        positive value are used.
    id_name : str, default 'hospitalization_id'
        Stay-level ID shared by `med_df`, `vitals_df` and `hospitalization_df`.
    time_col : str, default 'admin_dttm'
        Timestamp in `med_df` the weight is looked up relative to.
    fallbacks : sequence of str, default ('asof',)
        Ordered strategy names from `WEIGHT_FALLBACKS`.
        `('asof', 'same_hosp_earliest')` is the recommended default for
        conversion.
    hospitalization_df : pd.DataFrame or duckdb.DuckDBPyRelation, optional
        `{id_name}`, `patient_id`, `admission_dttm`. Required by the two
        cross-stay strategies.
    max_staleness_days : float, optional
        When set, an `'asof'` match more than this many days older than
        `time_col` is treated as missing and the row falls through to the next
        strategy. Other strategies are not gated.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Lazy relation on the default connection: every `med_df` column,
        unchanged and in order, plus

        - `weight_kg` (DOUBLE): the chosen weight, NULL if none
        - `_weight_source` (VARCHAR): the winning strategy name, NULL if none
        - `_weight_recorded_dttm`: when the chosen weight was charted
        - `_weight_age_hours` (DOUBLE): hours from `_weight_recorded_dttm` to
          `time_col`; negative when the weight was charted after the dose

        Exactly one output row per input row. Row order is not guaranteed.

    Raises
    ------
    ValueError
        Unknown or empty `fallbacks`; a cross-stay strategy without
        `hospitalization_df`; missing required columns; `med_df` already
        carrying one of the output columns; invalid `max_staleness_days`.

    Notes
    -----
    - Weights of zero or below are discarded (they would divide by zero or flip
      the sign of a converted dose). Several weights charted at the same
      instant in the same stay are averaged, so ties resolve deterministically.
    - Timestamps are never cast and the session `TimeZone` is never set. A
      TIMESTAMPTZ `time_col` stays TIMESTAMPTZ with the same instants. Keep
      `time_col` and `recorded_dttm` the same type: DuckDB compares a TIMESTAMP
      with a TIMESTAMPTZ by reading the naive side in the session zone.
    - Two different stays with identical `admission_dttm` count each other as
      *later*, never *prior* (strict `<` for prior, `>=` for later). Stays with
      a NULL `admission_dttm` are not linked either way.
    - Cross-stay weights can be months old. `_weight_age_hours` carries that
      distance so it can be filtered or reported.

    Examples
    --------
    >>> rel = attach_weight(                             # doctest: +SKIP
    ...     mac_df, vitals_df,
    ...     fallbacks=('asof', 'same_hosp_earliest', 'prior_hosp_latest'),
    ...     hospitalization_df=hosp_df, max_staleness_days=7)
    """
    fallbacks = _validate_weight_args(
        fallbacks, hospitalization_df, id_name, max_staleness_days,
    )
    validate_column_name(time_col, 'time_col')
    _require_columns(med_df, (id_name, time_col), 'med_df')
    _require_columns(vitals_df, (id_name, *_VITALS_REQUIRED_COLS), 'vitals_df')
    clash = [c for c in _WEIGHT_OUTPUT_COLS if c in med_df.columns]
    if clash:
        raise ValueError(
            f"med_df already has column(s) {clash}; drop them before attaching "
            f"weight (a pre-attached weight_kg should be used as-is instead)"
        )

    logger.info(f"Attaching patient weights (fallbacks: {', '.join(fallbacks)})...")
    idq = quote_identifier(id_name)
    tq = quote_identifier(time_col)

    # Positive weights only, one row per (stay, instant). Collapsing duplicates
    # here makes every ARG_MIN/ARG_MAX and the ASOF match below deterministic.
    ctes = [f"""
    _weights AS (
        FROM vitals_df
        SELECT {idq}, recorded_dttm, weight_kg: AVG(vital_value)
        WHERE vital_category = 'weight_kg'
            AND vital_value IS NOT NULL
            AND vital_value > 0
            AND recorded_dttm IS NOT NULL
        GROUP BY {idq}, recorded_dttm
    )"""]
    joins = []

    if 'asof' in fallbacks:
        joins.append(f"""
        ASOF LEFT JOIN _weights _asof
            ON m.{idq} = _asof.{idq}
            AND _asof.recorded_dttm <= m.{tq}""")

    if 'same_hosp_earliest' in fallbacks:
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

    # One (alias, usable-condition) per strategy, in the CALLER's order: the
    # first usable candidate wins every CASE below.
    aliases = {
        'asof': '_asof',
        'same_hosp_earliest': '_she',
        'prior_hosp_latest': '_phl',
        'later_hosp_earliest': '_lhe',
    }
    branches = []
    for name in fallbacks:
        alias = aliases[name]
        usable = f"{alias}.weight_kg IS NOT NULL"
        if name == 'asof' and max_staleness_days is not None:
            limit_us = float(max_staleness_days) * _US_PER_DAY
            usable += (
                f" AND epoch_us(m.{tq}) - epoch_us({alias}.recorded_dttm)"
                f" <= {limit_us}"
            )
        branches.append((name, alias, usable))

    def _case(pick) -> str:
        whens = ' '.join(
            f"WHEN {usable} THEN {pick(name, alias)}"
            for name, alias, usable in branches
        )
        return f"CASE {whens} END"

    q = f"""
    WITH {','.join(ctes)}
    FROM med_df m
    {''.join(joins)}
    SELECT m.*
        , weight_kg: CAST({_case(lambda n, a: f'{a}.weight_kg')} AS DOUBLE)
        , _weight_source: {_case(lambda n, a: f"'{n}'")}
        , _weight_recorded_dttm: {_case(lambda n, a: f'{a}.recorded_dttm')}
        -- instant difference, so the session TimeZone cannot shift it
        , _weight_age_hours:
            (epoch_us(m.{tq}) - epoch_us(_weight_recorded_dttm)) / {_US_PER_HOUR}.0
    """
    result = duckdb.sql(q)
    logger.info("Weight attachment complete")
    return result


def find_most_recent_weight(
    med_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    vitals_df: pd.DataFrame | duckdb.DuckDBPyRelation,
    id_name: str = 'hospitalization_id',
    fallback_on_earliest: bool = False,
    ) -> duckdb.DuckDBPyRelation:
    """Find the most recent weight for each medication administration via ASOF join.

    Single-purpose utility: for each row in `med_df`, attach the most recent
    `weight_kg` recorded **at or before** `admin_dttm` for the same hospitalization.

    Parameters
    ----------
    med_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Medication-admin rows with at least `{id_name}` and `admin_dttm` columns.
    vitals_df : pd.DataFrame or duckdb.DuckDBPyRelation
        Vitals rows with `{id_name}`, `recorded_dttm`, `vital_category`, `vital_value`.
    id_name : str, default 'hospitalization_id'
        ID column to join on.
    fallback_on_earliest : bool, default False
        When True, rows whose ASOF returns NULL (med admin precedes the first
        charted weight — common when documentation lags admission) fall back to
        the **earliest** charted weight for that hospitalization. Surfaced in
        the new `_weight_source` column.

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
    non-positive weights are ignored and same-instant duplicates are averaged.
    Use `attach_weight` directly for cross-stay fallbacks or a staleness limit.
    """
    logger.info("Finding most recent weights...")
    fallbacks = ('asof', 'same_hosp_earliest') if fallback_on_earliest else ('asof',)
    attached = attach_weight(
        med_df, vitals_df, id_name=id_name, fallbacks=fallbacks,
    )
    idq = quote_identifier(id_name)
    result = duckdb.sql(f"""
        FROM attached
        SELECT * EXCLUDE (_weight_source, _weight_age_hours)
            -- keep the legacy provenance label for the same-stay fallback
            , _weight_source: CASE _weight_source
                WHEN 'same_hosp_earliest' THEN 'earliest_fallback'
                ELSE _weight_source END
        ORDER BY {idq}, admin_dttm, med_category
    """)
    logger.info("Weight lookup complete")
    return result
