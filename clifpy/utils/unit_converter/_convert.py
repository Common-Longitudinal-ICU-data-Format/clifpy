"""Orchestration: the public `convert_dose_units_by_med_category` entry point.

Wires stage 1, the lazy weight join and stage 2 together, manages the DuckDB
temp-table lifecycle, and preserves timezone metadata across the pipeline.
Tables enter through `_inputs` (pandas only through Arrow) and results leave
through `_outputs`, in the format `return_format` names.
"""

from __future__ import annotations

import logging
import uuid

import duckdb
import polars as pl
import pyarrow as pa
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple
from typing import Tuple, List, Dict, Union, Literal, Collection, Sequence, overload
from duckdb import DuckDBPyRelation

from clifpy.utils.logging_config import get_logger
from clifpy.utils._duckdb_helpers import _drop_temp_table

from ._grammar import (
    ALL_ACCEPTABLE_UNITS,
    AMOUNT_UNITS_STR,
    RATE_UNITS_STR,
    STANDALONE_AMOUNT_UNITS,
    _canonical_spelling_expr,
    _convert_set_to_str_for_sql,
)
from ._clean import _clean_dose_unit_formats_duckdb, _clean_dose_unit_names_duckdb
from ._columns import (
    INTERNAL_CATEGORY_COL,
    INTERNAL_UNIT_COL,
    build_rename_map,
    quote_identifier,
    resolve_output_names,
    rename_to_internal,
    rename_from_internal,
    validate_column_name,
)
from ._inputs import (
    FrameLike,
    arrow_tz_map,
    check_no_shadowing_tables,
    column_names,
    is_foreign_relation,
    pandas_index,
    relation_to_arrow,
    to_arrow,
)
from ._outputs import ReturnFormat, emit, resolve_return_format, to_pandas, with_weight_time_zone
from ._base import _normalize_countable_units, _standardize_to_base
from ._weight import find_most_recent_weight, attach_weight, _validate_weight_args
from ._preferred import (
    UNLISTED_MODES,
    _convert_base_units_to_preferred_units,
    _weight_lookup_filter,
    _weight_unusable_predicate,
)
from ._counts import _create_unit_conversion_counts_table, counts_output_names
from ._targets import (
    _warn_about_targets,
    _is_na_token,
    load_dose_unit_targets,
)

if TYPE_CHECKING:  # type checkers only: the pandas overload's return type
    import pandas

logger = get_logger('utils.unit_converter')

# Hidden column holding each input row's position (0, 1, 2, ...). Added when
# the input is read into DuckDB, carried through every stage, used to return
# the eager formats in input order, and dropped before anything is returned.
# Reserved: an input column with this name is refused.
INPUT_ORDINAL_COL = '_input_row_ord'
_INPUT_TABLE_PREFIX = '_med_unit_input_'


class CapturedInput(NamedTuple):
    """The caller's med table, read into DuckDB once by :func:`_capture_input`."""

    # A relation on DuckDB's default connection, carrying INPUT_ORDINAL_COL.
    rel: DuckDBPyRelation
    # {column: zone} of the input's tz-aware timestamp columns.
    tz_map: Dict[str, str]
    # The caller's pandas index (pandas input only), for return_format='pandas'.
    pandas_index: Any


def _reject_ordinal_column(names) -> None:
    """Raise if the input already has a column named INPUT_ORDINAL_COL."""
    if any(str(name).lower() == INPUT_ORDINAL_COL for name in names):
        raise ValueError(
            f"med_df already has a column named {INPUT_ORDINAL_COL!r}; the "
            f"converter reserves that name to keep input row order. Rename or "
            f"drop it."
        )


def _with_arrow_ordinal(table: pa.Table) -> pa.Table:
    """Append INPUT_ORDINAL_COL = 0..n-1 to an Arrow table, in its row order."""
    return table.append_column(
        INPUT_ORDINAL_COL,
        pl.int_range(0, table.num_rows, dtype=pl.Int64, eager=True).to_arrow(),
    )


def _capture_input(med_df: FrameLike, *, detach: bool = False) -> CapturedInput:
    """Read ``med_df`` into DuckDB once, as a default-connection relation with INPUT_ORDINAL_COL.

    - DuckDB relation on the default connection: projected lazily, with
      ``row_number() OVER ()`` as the ordinal (a relation has no row order
      of its own). Its zones are its connection's TimeZone, read from a
      zero-row Arrow export.
    - polars DataFrame or LazyFrame, pandas DataFrame, or a relation from
      another connection (run there, logged at INFO): copied once into Arrow
      by :func:`~clifpy.utils.unit_converter._inputs.to_arrow` (pandas through
      its Arrow boundary), the ordinal appended as the row position, and
      scanned from that Arrow table by reference. The zones come from the
      Arrow types.

    Neither form has a catalog name, so no later call can overwrite or drop
    what it reads, and no same-named catalog table can stand in for it.

    Raises
    ------
    ValueError
        The input already has a column named INPUT_ORDINAL_COL.
    TypeError
        Any other input type.
    """
    _reject_ordinal_column(column_names(med_df, 'med_df'))
    if isinstance(med_df, DuckDBPyRelation) and not is_foreign_relation(med_df):
        return CapturedInput(
            med_df.project(f"*, row_number() OVER () - 1 AS {INPUT_ORDINAL_COL}"),
            arrow_tz_map(relation_to_arrow(med_df.limit(0)).schema),
            None,
        )
    table = to_arrow(med_df, 'med_df', detach=detach)
    return CapturedInput(
        duckdb.from_arrow(_with_arrow_ordinal(table)),
        arrow_tz_map(table.schema),
        pandas_index(med_df),
    )


def _register_input(source: DuckDBPyRelation) -> str:
    """Copy a captured input into a new temp table on the default connection; return its name.

    The name is unique per call (``_med_unit_input_<8 hex>``), so no other
    call can overwrite the table or drop it, and the caller drops exactly the
    tables it created. ``source`` is :func:`_capture_input`'s relation, never
    resolved by a Python variable name, so a same-named catalog table cannot
    stand in for it.
    """
    name = f"{_INPUT_TABLE_PREFIX}{uuid.uuid4().hex[:8]}"
    view = f"_med_unit_view_{uuid.uuid4().hex[:8]}"
    duckdb.register(view, source)
    try:
        # No OR REPLACE: on a name collision, fail instead of overwriting.
        duckdb.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM {view}")
    finally:
        duckdb.unregister(view)
    return name


def _coerce_text_columns(rel: DuckDBPyRelation) -> DuckDBPyRelation:
    """CAST the category and unit columns to VARCHAR when DuckDB typed them otherwise.

    An empty text column, or one that is entirely null, can reach DuckDB
    untyped (Arrow ``null``, read as INTEGER), and ``lower(INTEGER)`` in the
    unit cleaner then fails to bind. ENUM (a categorical column) is left
    alone: it already compares as text.
    """
    casts = [
        f"CAST({col} AS VARCHAR) AS {col}"
        for col, dtype in zip(rel.columns, rel.types)
        if col in (INTERNAL_CATEGORY_COL, INTERNAL_UNIT_COL)
        and dtype.id not in ('varchar', 'enum')
    ]
    if not casts:
        return rel
    return rel.project(f"* REPLACE ({', '.join(casts)})")


def _text_or_none(value) -> str | None:
    """``str(value)``, or None for a missing value (None, or a float NaN)."""
    if value is None or (isinstance(value, float) and value != value):
        return None
    return str(value)


def _preferred_units_relation(preferred_units) -> DuckDBPyRelation:
    """``preferred_units`` as a small relation: the caller's spelling and its normalised form.

    Columns ``med_category``, ``_preferred_unit`` (the caller's spelling, kept
    for output) and ``_preferred_unit_clean`` (the form stage 2's regexes
    match), all VARCHAR even for an empty dict, in the dict's order. Built off
    the dict, never the med table, and materialized once into Arrow: the
    validation and the preferred-unit join both scan it.
    """
    items = list((preferred_units or {}).items())
    table = pa.table({
        'med_category': pa.array([_text_or_none(k) for k, _ in items], pa.string()),
        '_preferred_unit': pa.array([_text_or_none(v) for _, v in items], pa.string()),
    })
    rel = _clean_dose_unit_formats_duckdb(
        duckdb.from_arrow(table), col='_preferred_unit', out_col='_preferred_unit_clean',
    )
    rel = _clean_dose_unit_names_duckdb(rel, col='_preferred_unit_clean')
    return duckdb.from_arrow(relation_to_arrow(rel))


def _emit_output(
    result_rel: DuckDBPyRelation,
    counts_rel: DuckDBPyRelation,
    *,
    fmt: str,
    captured: CapturedInput,
    time_col: str,
):
    """Hand back ``(converted, counts)`` in ``fmt``, without INPUT_ORDINAL_COL.

    The eager formats come back in input order, sorted on the ordinal, with
    the input's zones; ``_weight_recorded_dttm`` (``show_intermediate``) takes
    the zone of the caller's ``time_col``. pandas output also gets the
    caller's index back. Every stage is one row out per row in, so the
    ordinal is complete.
    """
    converted = emit(
        result_rel, fmt,
        tz_map=with_weight_time_zone(captured.tz_map, time_col),
        order_by=INPUT_ORDINAL_COL,
        drop=(INPUT_ORDINAL_COL,),
        pandas_index=captured.pandas_index,
        time_col=time_col,
    )
    return converted, emit(counts_rel, fmt)


def _log_weight_sources(needs_wt_joined, weight_fallbacks, fallback_on_earliest) -> None:
    """INFO-log how many rows needing weight each lookup strategy supplied.

    Labels follow the chain order, zeros included, then `none` for rows left
    without a weight. A cross-stay strategy reading 0 is how a vitals_df or
    hospitalization_df loaded for the cohort's own stays shows up.

    Built with the relation API, not SQL text, so no replacement-scan name is
    involved. The caller materializes the rows needing weight once (Arrow)
    before calling, so the counts are a group-by on that in-memory table and
    the weight join runs only once; the INFO gate skips even that when INFO
    is off.
    """
    if weight_fallbacks is not None:
        labels = tuple(weight_fallbacks)
    elif fallback_on_earliest:
        labels = ('asof', 'earliest_fallback')
    else:
        labels = ('asof',)
    source = "COALESCE(_weight_source, 'none')"
    counts = dict(
        needs_wt_joined.aggregate(f"{source}, COUNT(*)", source).fetchall()
    )
    logger.info(
        "Weight sources for %d row(s) needing weight: %s",
        sum(counts.values()),
        ', '.join(f"{label}={counts.get(label, 0)}" for label in (*labels, 'none')),
    )


# The stubs sit directly above the implementation -- a type checker treats
# stubs followed by any other code as having no implementation -- and each
# lists every parameter: one missing from the stubs is a type error at every
# call that passes it. They key on `return_format`, as load_data's do: only the
# 'polars' stub defaults it, so a call that leaves it out types as polars.
# `return_rel`, the deprecated alias for 'duckdb', is a plain bool in each.
@overload
def convert_dose_units_by_med_category(
    med_df: FrameLike,
    vitals_df: FrameLike | None = ...,
    preferred_units: dict | None = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: bool = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
    *,
    countable_units: Collection[str] | None = ...,
    category_col: str = ...,
    dose_col: str = ...,
    unit_col: str = ...,
    time_col: str = ...,
    converted_dose_col: str | None = ...,
    converted_unit_col: str | None = ...,
    weight_fallbacks: Sequence[str] | None = ...,
    hospitalization_df: FrameLike | None = ...,
    weight_max_staleness_days: float | None = ...,
    unlisted: Literal['base', 'keep'] = ...,
    allow_weight_variant: bool = ...,
    return_format: Literal['polars'] = ...,
) -> Tuple[pl.DataFrame, pl.DataFrame]: ...


@overload
def convert_dose_units_by_med_category(
    med_df: FrameLike,
    vitals_df: FrameLike | None = ...,
    preferred_units: dict | None = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: bool = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
    *,
    countable_units: Collection[str] | None = ...,
    category_col: str = ...,
    dose_col: str = ...,
    unit_col: str = ...,
    time_col: str = ...,
    converted_dose_col: str | None = ...,
    converted_unit_col: str | None = ...,
    weight_fallbacks: Sequence[str] | None = ...,
    hospitalization_df: FrameLike | None = ...,
    weight_max_staleness_days: float | None = ...,
    unlisted: Literal['base', 'keep'] = ...,
    allow_weight_variant: bool = ...,
    return_format: Literal['polars_lazy'],
) -> Tuple[pl.LazyFrame, pl.LazyFrame]: ...


@overload
def convert_dose_units_by_med_category(
    med_df: FrameLike,
    vitals_df: FrameLike | None = ...,
    preferred_units: dict | None = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: bool = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
    *,
    countable_units: Collection[str] | None = ...,
    category_col: str = ...,
    dose_col: str = ...,
    unit_col: str = ...,
    time_col: str = ...,
    converted_dose_col: str | None = ...,
    converted_unit_col: str | None = ...,
    weight_fallbacks: Sequence[str] | None = ...,
    hospitalization_df: FrameLike | None = ...,
    weight_max_staleness_days: float | None = ...,
    unlisted: Literal['base', 'keep'] = ...,
    allow_weight_variant: bool = ...,
    return_format: Literal['duckdb'],
) -> Tuple[DuckDBPyRelation, DuckDBPyRelation]: ...


@overload
def convert_dose_units_by_med_category(
    med_df: FrameLike,
    vitals_df: FrameLike | None = ...,
    preferred_units: dict | None = ...,
    show_intermediate: bool = ...,
    override: bool = ...,
    return_rel: bool = ...,
    id_name: str = ...,
    fallback_on_earliest: bool = ...,
    *,
    countable_units: Collection[str] | None = ...,
    category_col: str = ...,
    dose_col: str = ...,
    unit_col: str = ...,
    time_col: str = ...,
    converted_dose_col: str | None = ...,
    converted_unit_col: str | None = ...,
    weight_fallbacks: Sequence[str] | None = ...,
    hospitalization_df: FrameLike | None = ...,
    weight_max_staleness_days: float | None = ...,
    unlisted: Literal['base', 'keep'] = ...,
    allow_weight_variant: bool = ...,
    return_format: Literal['pandas'],
) -> Tuple['pandas.DataFrame', 'pandas.DataFrame']: ...


def convert_dose_units_by_med_category(
    med_df: FrameLike,
    vitals_df: FrameLike | None = None,
    preferred_units: dict | None = None,
    show_intermediate: bool = False,
    override: bool = False,
    return_rel: bool = False,
    id_name: str = 'hospitalization_id',
    fallback_on_earliest: bool = False,
    *,
    countable_units: Collection[str] | None = None,
    category_col: str = 'med_category',
    dose_col: str = 'med_dose',
    unit_col: str = 'med_dose_unit',
    time_col: str = 'admin_dttm',
    converted_dose_col: str | None = None,
    converted_unit_col: str | None = None,
    weight_fallbacks: Sequence[str] | None = None,
    hospitalization_df: FrameLike | None = None,
    weight_max_staleness_days: float | None = None,
    unlisted: Literal['base', 'keep'] = 'base',
    allow_weight_variant: bool = False,
    return_format: ReturnFormat = 'polars',
) -> Union[
    Tuple[pl.DataFrame, pl.DataFrame],
    Tuple[pl.LazyFrame, pl.LazyFrame],
    Tuple[DuckDBPyRelation, DuckDBPyRelation],
    Tuple['pandas.DataFrame', 'pandas.DataFrame'],
]:
    """Convert medication dose units to preferred units, weight-aware and DuckDB-native.

    Two-stage pipeline:

    1. **Standardize to base units** (weight-preserving). Stage 1 normalizes amount
       and time but keeps the weight qualifier (`/kg`, `/lb`, or none) verbatim.
    2. **Convert to preferred units**. Stage 2 applies amount/time factors and a
       9-case weight-transition factor. Patient weight (`weight_kg`) is consumed
       *only* when source and target differ in weight-qualifier presence.

    Performance follows `docs/duckdb_perf_guide.md`: the input is read into
    DuckDB once (boundary 2a; the eager formats copy it into a per-call temp
    table), validations use ANTI JOIN + fetchall (no DataFrame), and the lazy
    weight lookup runs only on the rows that need weight (window functions,
    not ASOF joins) and `UNION ALL BY NAME`s the rest back. Nothing is
    computed in pandas: a pandas input reaches DuckDB through Arrow, and the
    deprecated pandas output is converted from polars at the very end.

    Parameters
    ----------
    med_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Medication data, read into DuckDB once at the start of the call (see
        Notes), with required columns:

        - `med_dose`: original dose values (numeric)
        - `med_dose_unit`: original dose unit strings
        - `med_category`: medication category (e.g., 'propofol')
        - `weight_kg`: optional patient weight. If absent and any conversion
          requires patient weight, vitals_df is consulted (lazy join).
        - `admin_dttm`, `{id_name}`: required when our internal weight lookup
          fires (i.e., `weight_kg` not in `med_df` and at least one row needs weight).

    vitals_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame, optional
        Vitals data for weight lookup. Required only if `weight_kg` is missing
        AND at least one convertible row needs a weighted ↔ unweighted
        transition. Rows stage 2 refuses anyway (an unrecognized unit such as
        `mg/kg/dose`, rate ↔ amount, mass ↔ volume) never need it.
    preferred_units : dict, optional
        `{med_category: target_unit_string}`. Categories without an entry use
        their base unit as the target. An entry whose unit is blank or a
        placeholder (`''`, `'unknown'`, `'nan'`) is an explicit request that
        cannot be met: it raises, or with `override=True` its rows report
        `user-preferred unit ... is not recognized`. It never falls back to
        the base unit.
    show_intermediate : bool, default False
        If True, retain QA columns (`_amount_multiplier_preferred`, etc.).
    override : bool, default False
        If True, log warnings instead of raising on validation failures.
    return_rel : bool, default False
        Deprecated alias for ``return_format='duckdb'`` (DeprecationWarning).
        Together with ``return_format='polars_lazy'`` or ``'pandas'`` it
        raises ValueError.
    id_name : str, default 'hospitalization_id'
        ID column name for the weight lookup.
    fallback_on_earliest : bool, default False
        Forwarded to `find_most_recent_weight` when our internal lookup fires.
        When True, rows with no weight at or before the dose fall back to the
        earliest charted weight for the same hospitalization (handles
        documentation lag).
        Equivalent to `weight_fallbacks=('asof', 'same_hosp_earliest')` except
        for the provenance label (`'earliest_fallback'`). Cannot be combined
        with `weight_fallbacks`.
    countable_units : collection of str, optional
        Unit strings to classify as countable dosage forms (`_unit_class =
        'countable'`; the dose passes through unconverted). None (default)
        uses `DEFAULT_COUNTABLE_UNITS`. A collection REPLACES the defaults
        rather than extending them, so pass
        `set(DEFAULT_COUNTABLE_UNITS) | {'troche'}` to extend. Tokens are
        cleaned exactly like the unit column (lowercased, whitespace and a
        trailing period removed, spelling variants rewritten), so pass them as
        the data spells them. A single string raises TypeError. A countable
        form is a valid target too: a row already in it reads 'success' with
        its dose copied; any other row fails on unit class.
    category_col, dose_col, unit_col, time_col : str
        Input column names for the category, dose, unit and administration
        time. Defaults `med_category`, `med_dose`, `med_dose_unit`,
        `admin_dttm`. Must be plain SQL identifiers.
    converted_dose_col, converted_unit_col : str, optional
        Output column names. Defaults `<dose_col>_converted`,
        `<unit_col>_converted` (`med_dose_converted`,
        `med_dose_unit_converted` for the default pair). Passing `dose_col`
        (and/or `unit_col`) overwrites that source column in place; an output
        name any other input column already has raises `ValueError`. For a
        pair other than `med_dose` the QA columns end in `__<dose_col>`; see
        Notes.
    weight_fallbacks : tuple or list of str, optional
        Ordered weight-lookup strategies from
        :data:`~clifpy.utils.unit_converter.WEIGHT_FALLBACKS`, passed to
        :func:`~clifpy.utils.unit_converter.attach_weight` when our internal
        lookup fires. The first strategy yielding a weight wins per row and is
        recorded in `_weight_source`. `('asof', 'same_hosp_earliest')` is the
        recommended choice. Default None keeps the `fallback_on_earliest` path.
    hospitalization_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame, optional
        `{id_name}`, `patient_id`, `admission_dttm`. Required when
        `weight_fallbacks` names `'prior_hosp_latest'` or
        `'later_hosp_earliest'`.
    weight_max_staleness_days : real number, optional
        Make a weight charted more than this many days before the dose
        ineligible for every strategy except `'later_hosp_earliest'`; weights
        charted after the dose stay eligible. See
        :func:`~clifpy.utils.unit_converter.attach_weight`. Requires
        `weight_fallbacks`.
    unlisted : {'base', 'keep'}, default 'base'
        Rows whose category has no entry in `preferred_units`. 'base' converts
        them to their base unit (the released behaviour); 'keep' copies the
        dose and only cleans the unit spelling (`Units/Hour` -> `units/hr`),
        so nothing is converted without a target written out. Missing,
        countable and unrecognized units report their `_convert_status`
        either way.
    allow_weight_variant : bool, default False
        Accept a target's weight-adjusted or weight-naive variant instead of
        crossing the weight axis. With target `mcg/hr`, a `mcg/hr` or
        `mcg/kg/hr` row stays as charted and `mg/kg/hr` becomes `mcg/kg/hr`
        (not `mcg/hr`); with target `mcg/kg/min`, `mg/hr` becomes `mcg/min`.
        A per-lb source becomes the `/kg` variant (stage 1 already folds
        `/lb` into `/kg`), and a variant's label is written in mCIDE spelling
        (`units/kg/hr`). No row then needs a weight, so no weight is looked
        up and `vitals_df` is optional. Rows whose target is standalone
        (`ppm`, `cells`), unrecognized, or of another unit class are
        unaffected. `_convert_status_details` reads '... the weight variant
        of the target' where the variant was used.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, default 'polars'
        Output format, as for :func:`clifpy.utils.io.load_data`:

        - ``'polars'``: polars DataFrames.
        - ``'polars_lazy'``: polars LazyFrames over the materialized result
          (the conversion has already run).
        - ``'duckdb'``: lazy DuckDB relations on the default connection. A
          polars or pandas input, or a relation from another connection, is
          copied during the call, so a later change to it cannot reach the
          result. A relation on the default connection is read in place each
          time the result is evaluated: later changes to its tables show
          through and dropping them breaks the result, so materialize first
          if you will change them. Like any relation, they have no row order.
        - ``'pandas'``: deprecated; retires in the next release
          (DeprecationWarning). The polars result converted at the very end;
          pandas input keeps its index.

    Returns
    -------
    Tuple[pl.DataFrame, pl.DataFrame]
        Tuple of `(converted, counts)`, both in the format `return_format`
        names: polars DataFrames by default, LazyFrames for 'polars_lazy',
        lazy relations for 'duckdb' (or the deprecated `return_rel=True`),
        and pandas DataFrames for the deprecated 'pandas'.

        The converted frame is lean by default: the caller's columns plus
        `med_dose_converted`, `med_dose_unit_converted`, `_convert_status`
        (byte-identical to earlier releases), `_convert_status_details` (how a
        successful row reached its unit, `CONVERT_STATUS_DETAILS`, or the
        failure reason again), `weight_kg`, `_weight_source` (the lookup
        strategy that found the weight, `'prefilled'` for a usable caller
        `weight_kg` with no provenance of its own, `'not_needed'` where the
        conversion needs no weight, NULL where one was needed and none found)
        and `_weight_age_hours` (hours from the weight's charting to the dose,
        NULL unless a lookup found it). With
        `show_intermediate=True`, also the QA columns `_clean_unit`,
        `_unit_class`, `_base_unit`, `_needs_wt`, `_weight_recorded_dttm`,
        `_amount_multiplier_preferred`, etc. Weight columns that arrive on
        `med_df` (`weight_kg` and
        :func:`~clifpy.utils.unit_converter.attach_weight`'s `_weight_source`,
        `_weight_recorded_dttm`, `_weight_age_hours`) come back unchanged in
        either mode, and the converter then writes no `_weight_source` of
        its own.

        In the eager formats the converted frame has exactly one row per input
        row, in input order. A zero-row input returns zero-row results with
        the same columns.

        `counts` has one row per combination of category, original unit,
        `_clean_unit`, `_base_unit`, `_unit_class`, `_preferred_unit`,
        converted unit, `_convert_status`, `_convert_status_details` and
        `_weight_source`, plus a `count` column, sorted by category and then
        by descending count. It keeps the QA columns the lean rows drop, and
        joins back onto the rows on the columns they share. Its columns carry
        the converted frame's names: the caller's `category_col`, `unit_col`
        and `converted_unit_col`, for a pair other than `med_dose` the QA
        columns' `__<dose_col>` names, and `_weight_source` bare (shared by
        every pair). When `converted_unit_col` equals
        `unit_col` (an in-place overwrite) the converted unit is shown as
        `<unit_col>_converted`.

    Raises
    ------
    ValueError
        Required columns missing, or validation failures (when `override=False`).
        Also, whatever `override`, a column-name collision: an output name of
        this call that an input column already has (typically the same column
        pair converted twice), an input column starting with `_saved__`,
        `_weight_*` provenance columns without `weight_kg`, or an output name
        equal to a column the converter writes, or to the other output name.
        An input column or output name `_input_row_ord` (the hidden column
        the converter uses to keep input row order) is such a collision too.
        Also raised for an unknown `return_format`, for `return_rel=True` with
        a conflicting `return_format`, and when a table or view on DuckDB's
        default connection is named like a variable the converter's SQL reads
        (`_inputs.REPLACEMENT_SCAN_NAMES`), which DuckDB would read in place
        of your data.
    TypeError
        `med_df` is not a polars DataFrame or LazyFrame, a DuckDB relation or
        a pandas DataFrame.

    Notes
    -----
    Only the first eight parameters (`med_df` to `fallback_on_earliest`) may
    be passed by position; every later one, `return_format` included, is
    keyword-only.

    User-prefilled `weight_kg` is honored: if the column is present in
    `med_df`, our lookup is skipped entirely (NULLs are preserved as-is). This
    holds even when `weight_fallbacks` is given; pre-attach with
    :func:`~clifpy.utils.unit_converter.attach_weight` to use one weight
    consistently across several tables. A pre-filled weight that is zero or
    below, NaN or infinite counts as missing: a row that crosses the weight
    axis reports `cannot convert ...: weight_kg is missing` and keeps its
    original dose. The `weight_kg` column itself is returned unchanged.

    Lookup arguments passed alongside a pre-filled `weight_kg` are validated
    first, so `weight_max_staleness_days` still needs `weight_fallbacks` and
    `fallback_on_earliest=True` still cannot be combined with it. Valid ones
    (`weight_fallbacks`, `weight_max_staleness_days`, `hospitalization_df`,
    `fallback_on_earliest=True`) are then ignored, with one WARNING naming
    them. `vitals_df` is not used either: when some rows that need a weight
    have none (NULL, zero or below, NaN or infinite), a WARNING gives their
    count. That count is one aggregate over the input, run inside the call,
    also under `return_format='duckdb'`. The warning covers a `weight_kg`
    that arrives without
    :func:`~clifpy.utils.unit_converter.attach_weight`'s `_weight_age_hours`:
    next to that column the gaps are that chain's verdict and the call stays
    quiet. A pass's `show_intermediate=True` output carries `_weight_source`
    but not `_weight_age_hours`, so a second pass on it still warns.

    When the converter looks weights up itself it logs, at INFO, how many of
    the rows needing a weight each strategy supplied (`none`: no weight).

    Output names follow the converted pair. `converted_dose_col` and
    `converted_unit_col` default to `<dose_col>_converted` and
    `<unit_col>_converted`. The QA columns (`_clean_unit`, `_unit_class`,
    `_convert_status`, and with `show_intermediate=True` every intermediate)
    keep their bare names when `dose_col` is `med_dose` and otherwise end in
    `__<dose_col>`, e.g. `_convert_status__volume_infusion_rate`. `weight_kg`
    and its `_weight_*` provenance are shared by every pair. Converting the
    default pair and then `dose_col='volume_infusion_rate'`,
    `unit_col='volume_infusion_rate_unit'` on the result, or the other way
    round, therefore keeps both results' columns with no other argument (for
    `weight_kg`, see below).

    An input column named like a column the converter writes is set aside
    while the conversion runs and returned unchanged: an earlier pass's
    `med_dose_converted` or `_convert_status`, say, or the `_weight_source`,
    `_weight_recorded_dttm` and `_weight_age_hours` that
    :func:`~clifpy.utils.unit_converter.attach_weight` adds next to
    `weight_kg`. `weight_kg` itself is read as the pre-attached weight, so a
    second pass uses the one the first pass returned. When `weight_kg` was
    missing from the first pass's input, the column that pass returns holds a
    weight only on the rows it converted across the weight axis and is NULL
    elsewhere, so the second pass's weight-crossing rows on those NULLs report
    `weight_kg is missing` and `vitals_df` is not used. Run the pair that
    crosses the weight axis first (if both pairs cross it, attach weights
    first), or, to give both passes one complete weight, attach it first with
    :func:`~clifpy.utils.unit_converter.attach_weight`.

    Passing `converted_dose_col=dose_col` (and/or `converted_unit_col=unit_col`)
    overwrites that source column in place: the raw values are dropped and the
    converted ones take its name. Any other reuse of an existing name raises.

    Input handling (decision D-261022-pandas-free-backend):

    - Every computation runs in DuckDB. A pandas DataFrame is accepted and
      handed to DuckDB through Arrow, untouched; a polars LazyFrame is
      collected once.
    - A pandas object column that mixes Python types (`['mg', 5, None]`,
      common in raw unit and ID columns) has no Arrow type; DuckDB's own
      pandas scanner reads that column as text instead, and a WARNING names
      it.
    - A relation from a connection other than DuckDB's default one (e.g.
      `clifpy.utils.io.new_duckdb_con()`) is run on its own connection and
      copied to the default one through Arrow; the copy is logged at INFO
      with its row count. `vitals_df` and `hospitalization_df` are cut to
      the weight rows and needed columns before such a copy.
    - Timestamp zones: polars and pandas inputs keep their columns' zones. A
      relation carries no zone of its own, so its tz-aware columns come back
      labelled in the TimeZone of the connection that owns it; the instants
      never change. With `show_intermediate=True`, `_weight_recorded_dttm`
      comes from `vitals_df` and is labelled in `time_col`'s zone, so one
      result holds one zone. With `return_format='duckdb'` the default
      connection's TimeZone is pinned to the input's zone, so a later
      `.pl()` shows it.
    """
    # Resolve the output format first: an unknown format, or the deprecated
    # return_rel next to a conflicting return_format, fails before any work.
    fmt = resolve_return_format(return_format, return_rel)
    if unlisted not in UNLISTED_MODES:
        raise ValueError(f"unlisted must be one of {UNLISTED_MODES}; got {unlisted!r}")
    if isinstance(preferred_units, (str, Path)):
        # A file of targets is read by standardize_med_dose_units (or
        # load_dose_unit_targets); a path here used to fail with
        # "'str' object has no attribute 'items'".
        raise TypeError(
            f"preferred_units must be a {{med_category: unit}} dict; got a path "
            f"({preferred_units!r}). Read a targets file with "
            f"standardize_med_dose_units(med_df, targets=...) or "
            f"load_dose_unit_targets(...)."
        )

    # Validate the weight-lookup arguments before any temp table exists, so a
    # bad chain fails fast even when no row turns out to need weight.
    if weight_fallbacks is not None:
        if fallback_on_earliest:
            raise ValueError(
                "pass either fallback_on_earliest=True or weight_fallbacks, not "
                "both; the equivalent chain is "
                "weight_fallbacks=('asof', 'same_hosp_earliest')"
            )
        weight_fallbacks = _validate_weight_args(
            weight_fallbacks, hospitalization_df, id_name, weight_max_staleness_days,
        )
    elif weight_max_staleness_days is not None:
        raise ValueError(
            "weight_max_staleness_days requires weight_fallbacks, e.g. "
            "weight_fallbacks=('asof', 'same_hosp_earliest')"
        )
    elif hospitalization_df is not None:
        logger.warning("hospitalization_df is ignored unless weight_fallbacks is given")

    # Validate every column name and settle the output names before any temp
    # table exists. This runs on every call: rename_to_internal returns early
    # when no name is remapped, and the default names are exactly the ones an
    # earlier conversion's output carries. See _columns.py.
    rename_map = build_rename_map(category_col, dose_col, unit_col, time_col)
    output_names = resolve_output_names(
        column_names(med_df, 'med_df'),
        dose_col=dose_col,
        unit_col=unit_col,
        converted_dose_col=converted_dose_col,
        converted_unit_col=converted_unit_col,
        show_intermediate=show_intermediate,
        category_col=category_col,
        time_col=time_col,
    )
    if output_names.in_place:
        logger.info(
            "Writing the converted values over the source column(s) in place "
            "(converted_dose_col=%r, converted_unit_col=%r)",
            output_names.converted_dose_col, output_names.converted_unit_col,
        )
    # Input columns named like a column the pipeline writes (an earlier
    # pass's output, say) are parked for the run, like a caller column that
    # collides with an internal input name; rename_from_internal restores them.
    rename_map.update(output_names.park)

    # `_weight_source` is returned in the lean output and grouped by in the
    # counts table (D-261119-weight-source). The converter writes its own only
    # when no input column has that name: a caller's copy (attach_weight's
    # output, an earlier pass) is parked and comes back unchanged, and a
    # source column of that name (`time_col='_weight_source'`) keeps it.
    # Likewise `_weight_age_hours` (D-261119-weight-labels).
    input_lower = {str(c).lower() for c in column_names(med_df, 'med_df')}
    weight_source_taken = '_weight_source' in input_lower
    weight_age_taken = '_weight_age_hours' in input_lower
    source_lower = {str(c).lower() for c in (category_col, dose_col, unit_col, time_col)}
    weight_cols_as_sources = tuple(
        c for c in ('_weight_source', '_weight_age_hours') if c in source_lower
    )
    parked_weight_source = next(
        (stashed for caller, stashed in output_names.park.items()
         if caller.lower() == '_weight_source'),
        None,
    )

    # The names the counts table will show: the converted frame's own, so the
    # two join without a rename. Checked before any temp table exists, so a
    # clash fails fast.
    counts_names = counts_output_names(
        category_col=category_col,
        dose_col=dose_col,
        unit_col=unit_col,
        converted_unit_col=output_names.converted_unit_col,
    )

    n_categories = len(preferred_units) if preferred_units else 0
    logger.info(f"Converting dose units for {n_categories} med categories...")

    # The SQL below reads Python variables by name; refuse to run when a
    # same-named table on the default connection would be read instead.
    check_no_shadowing_tables()

    # Read the input into DuckDB once: each row's position (INPUT_ORDINAL_COL)
    # is recorded, and each timestamp column's zone is captured BEFORE the
    # DuckDB round-trip -- DuckDB renders TIMESTAMPTZ in the rendering
    # connection's zone, not necessarily the input's. See docs/tz_dx.md (§9).
    # pandas reaches DuckDB only through Arrow (_inputs.pandas_to_arrow).
    captured = _capture_input(med_df, detach=fmt == 'duckdb')

    # Temp tables THIS call created; `finally` drops exactly these.
    created_tables: List[str] = []
    try:
        # --------------------------------------------------------------
        # Boundary 2a (per docs/duckdb_perf_guide.md): the input is
        # referenced many times downstream (validation, base conversion,
        # preferred-unit join, weight join, conversion), so it is read into
        # DuckDB once.
        #
        # - Eager formats ('polars', 'polars_lazy', 'pandas'): copied into a
        #   temp table under a per-call unique name, which `finally` drops
        #   once the result is materialized.
        # - 'duckdb': the returned relations outlive this call, so they read
        #   the captured input itself -- an Arrow table held by reference, or
        #   the caller's own relation. No catalog name exists for a later
        #   call to overwrite or drop.
        # --------------------------------------------------------------
        if fmt == 'duckdb':
            med_df = captured.rel
        else:
            input_table = _register_input(captured.rel)
            created_tables.append(input_table)
            med_df = duckdb.table(input_table)

        # Project the caller's columns onto the names the pipeline's SQL is
        # written against. Everything downstream then works on fixed literal
        # names, so no caller-supplied identifier ever reaches an internal
        # f-string. The names were validated above; quoting happens inside
        # these two helpers. See _columns.py.
        med_df = _coerce_text_columns(rename_to_internal(med_df, rename_map))

        input_is_empty = med_df.limit(1).fetchone() is None
        if input_is_empty:
            logger.warning(
                "med_df has no rows; returning empty results with the full "
                "output schema"
            )

        # --------------------------------------------------------------
        # Validate requested med_categories via ANTI JOIN (no .to_df()).
        # --------------------------------------------------------------
        # Lookup table carrying BOTH the caller's spelling and its normalised
        # form. Built once, off the (small) preferred_units dict rather than
        # the med table.
        #
        # WARNING: `_preferred_unit_clean` is not cosmetic. Stage 2 matches the
        # preferred unit against the factor regexes, and the caller's raw
        # string does not necessarily match them. mCIDE spelled pitocin's target
        # `milli-units/min` until upstream CLIF d6fe15a0 (2026-09-10); that
        # fails `MU_REGEX = ^(mu)` because it starts "mi" -- the multiplier
        # would fall through to 1 and the dose come out 1000x wrong.
        # `units/kg/hr` only survives by accident, since `^(u)`
        # matches the leading "u" of "units". `_preferred_unit` keeps the
        # caller's spelling for output; `_preferred_unit_clean` drives every
        # regex match and the acceptability check below.
        preferred_units_df = _preferred_units_relation(preferred_units)
        # The countable tokens, cleaned as stage 1 cleans them (it applies the
        # same normalization): a target among them is a valid countable unit.
        countable_tokens = tuple(sorted(_normalize_countable_units(countable_units)))

        if preferred_units:
            requested_categories_df = pa.table({
                'med_category': pa.array(
                    [_text_or_none(c) for c in sorted(preferred_units)], pa.string()),
            })
            extra_rows = duckdb.sql("""
                SELECT med_category
                FROM requested_categories_df
                ANTI JOIN (SELECT DISTINCT med_category FROM med_df) existing
                  ON requested_categories_df.med_category = existing.med_category
            """).fetchall()
            if extra_rows and not input_is_empty:
                extras = sorted(row[0] for row in extra_rows)
                # Standardizing against a schema of a few hundred categories
                # will always list many the data does not contain, so summarise
                # rather than dumping every name into the log.
                shown = ', '.join(extras[:8])
                if len(extras) > 8:
                    shown += f", ... (+{len(extras) - 8} more)"
                error_msg = (
                    f"{len(extras)} med_category value(s) are given a preferred unit "
                    f"but not found in the input med_df: {shown}"
                )
                if override:
                    logger.warning(error_msg)
                else:
                    raise ValueError(error_msg)

            # ----------------------------------------------------------
            # Validate the units the CALLER actually asked for.
            #
            # NOTE: this deliberately runs before the fallback join below.
            # Categories with no entry in `preferred_units` fall back to their
            # own `_base_unit`, and if stage 1 could not map a drug's original
            # unit that unmappable string carries through as its
            # `_preferred_unit`. Validating post-fallback made one
            # unconvertible drug abort a call that never mentioned it -- see
            # clifpy#153, where asking for vasopressor units failed with
            # `{'meq/min'}` because sodium bicarbonate happened to be in the
            # same table. Fallback units are clifpy's own derived values, not
            # user input, and their unconvertibility is already reported per
            # row through `_convert_status`.
            # NOTE: iterate with zip, not itertuples -- pandas renames
            # leading-underscore columns to positional _1/_2 there. The rows
            # are now fetched from DuckDB as plain tuples instead, which
            # carry no column names at all.
            # A countable form (`tablet`, `dose`, ...) is a valid target too:
            # rows already in that form need no conversion.
            bad_by_category = {
                category: raw
                for category, raw, clean in preferred_units_df.project(
                    'med_category, _preferred_unit, _preferred_unit_clean'
                ).fetchall()
                if clean not in ALL_ACCEPTABLE_UNITS and clean not in countable_tokens
            }
            if bad_by_category:
                error_msg = (
                    f"Cannot accommodate the conversion to the following preferred "
                    f"units: {bad_by_category}. Consult the function documentation "
                    f"for a list of acceptable units."
                )
                if override:
                    logger.warning(error_msg)
                else:
                    raise ValueError(error_msg)

        # --------------------------------------------------------------
        # Stage 1: standardize to base units (no weight needed).
        # --------------------------------------------------------------
        try:
            # Internal `u` / `mu` / `mnu` tokens: stage 2 matches on them, and
            # they are spelled out below, after stage 2, where they leave.
            med_df_base, _ = _standardize_to_base(
                med_df, vitals_df, show_intermediate=show_intermediate,
                id_name=id_name, countable_units=countable_units,
                return_format='duckdb', spell_out=False,
            )
        except ValueError as e:
            raise ValueError(f"Error standardizing dose units to base units: {e}")

        # --------------------------------------------------------------
        # Join preferred units onto base table.
        # --------------------------------------------------------------
        # A row whose category has no listed target goes to its base unit
        # ('base', the released behaviour) or to its own cleaned unit ('keep'),
        # where stage 2's identity branch copies the dose unchanged.
        unlisted_target = 'l._clean_unit' if unlisted == 'keep' else 'l._base_unit'
        # allow_weight_variant (D-261119-weight-variant): a listed target is
        # replaced by its weight variant when the row and the target sit on
        # opposite sides of the weight axis (exactly one of them per kg or per
        # lb) -- the variant then takes the source's weight basis, so no
        # weight is needed. `/lb` against `/kg` crosses no axis and is left
        # to the plain conversion. The variant, its mCIDE label and whether
        # it is usable (a recognized, non-standalone target whose variant is
        # an acceptable unit) depend on the target alone, so they are
        # computed once per target on the small preferred-units relation;
        # each row then tests only its own weight basis and unit class. Every
        # other row keeps its target, so stage 2's checks read as before, and
        # _weight_lookup_filter (which reads _preferred_unit_clean) never asks
        # for a weight for a variant row.
        target_clean = 'r._preferred_unit_clean'
        if allow_weight_variant:
            weighted = "regexp_matches(_preferred_unit_clean, '/(kg|lb)(/|$)')"
            variant = (
                f"CASE WHEN {weighted} "
                f"THEN regexp_replace(_preferred_unit_clean, '/(kg|lb)(/|$)', '\\2') "
                f"ELSE regexp_replace(_preferred_unit_clean, '^([^/]+)', '\\1/kg') END"
            )
            standalone = _convert_set_to_str_for_sql(STANDALONE_AMOUNT_UNITS)
            acceptable = _convert_set_to_str_for_sql(ALL_ACCEPTABLE_UNITS)
            # One query (a CTE, not a second Python relation), so the only
            # name its SQL reads is preferred_units_df.
            preferred_units_df = duckdb.from_arrow(relation_to_arrow(duckdb.sql(f"""
                WITH per_target AS (
                    SELECT *
                        , _variant_target_weighted: {weighted}
                        , _variant_unit: {variant}
                        , _variant_target_class: CASE
                            WHEN _preferred_unit_clean IN ('{RATE_UNITS_STR}') THEN 'rate'
                            WHEN _preferred_unit_clean IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
                            END
                    FROM preferred_units_df
                )
                SELECT *
                    , _variant_ok: COALESCE(
                        _variant_target_class IS NOT NULL
                        AND _preferred_unit_clean NOT IN ('{standalone}')
                        AND _variant_unit IN ('{acceptable}'), FALSE)
                    , _variant_label: {_canonical_spelling_expr('_variant_unit')}
                FROM per_target
            """)))
            # `_base_unit` never carries `/lb` (stage 1 folds it into `/kg`).
            source_weighted = "regexp_matches(l._base_unit, '/kg(/|$)')"
            is_variant = (
                f"COALESCE(r._variant_ok AND r._variant_target_class = l._unit_class "
                f"AND ({source_weighted}) != r._variant_target_weighted, FALSE)"
            )
            label_expr = (
                f"CASE WHEN {is_variant} THEN r._variant_label "
                f"ELSE COALESCE(r._preferred_unit, {unlisted_target}) END"
            )
            clean_expr = f"CASE WHEN {is_variant} THEN r._variant_unit ELSE {target_clean} END"
        else:
            is_variant = "FALSE"
            label_expr = f"COALESCE(r._preferred_unit, {unlisted_target})"
            clean_expr = target_clean
        try:
            med_df_preferred = duckdb.sql(f"""
                SELECT l.*
                    -- categories without an explicit preferred unit: base unit or own clean unit
                    , _preferred_unit: {label_expr}
                    -- Marks whether the caller actually requested this unit.
                    -- Stage 2 validates only explicit rows; fallback units are
                    -- derived by clifpy and are reported per row via
                    -- `_convert_status` instead (clifpy#153).
                    , _preferred_is_explicit: r._preferred_unit IS NOT NULL
                    -- `_base_unit` is already canonical, so the fallback needs
                    -- no further normalisation. Fall back only when the caller
                    -- gave no target: an explicit target that normalises to
                    -- NULL ('', ' ', 'unknown', 'nan') must stay NULL so stage 2
                    -- reports "user-preferred unit ... is not recognized"
                    -- instead of converting to base units under the caller's
                    -- blank label.
                    , _preferred_unit_clean: CASE
                        WHEN r._preferred_unit IS NULL THEN {unlisted_target}
                        ELSE {clean_expr} END
                    -- TRUE where allow_weight_variant replaced the target by
                    -- its weight variant (for `_convert_status_details`).
                    , _preferred_is_weight_variant: {is_variant}
                FROM med_df_base l
                LEFT JOIN preferred_units_df r USING (med_category)
            """)
        except Exception as e:
            raise ValueError(f"Error joining preferred units: {e}")

        # --------------------------------------------------------------
        # Lazy weight join.
        # Skip entirely if the user pre-filled `weight_kg` (their strategy wins).
        # Otherwise, look weights up only for the rows that need them; UNION
        # ALL BY NAME the rest with NULL weight values. Empty-needs case skips
        # the lookup.
        # --------------------------------------------------------------
        if 'weight_kg' not in med_df_preferred.columns:
            # Only rows stage 2 will actually multiply by weight_kg: both units
            # recognized, same class and subclass, weight qualifier on exactly
            # one side, all read in normalised form. See _weight_lookup_filter.
            needs_wt_filter = _weight_lookup_filter()

            # fetchone()-based gate: cheap single-tuple materialization; no DataFrame.
            any_needs_wt = duckdb.sql(f"""
                SELECT 1 FROM med_df_preferred
                WHERE {needs_wt_filter}
                LIMIT 1
            """).fetchone() is not None

            if any_needs_wt:
                if vitals_df is None:
                    error_msg = (
                        "weight_kg is missing from med_df and at least one conversion "
                        "requires patient weight (weighted <-> unweighted transition), "
                        "but vitals_df=None. Either pre-fill med_df['weight_kg'] or "
                        "provide vitals_df."
                    )
                    if override:
                        logger.warning(error_msg)
                        # fall through with weight_kg=NULL; the conversion will
                        # mark these rows as failed via _convert_status.
                        # Rows that need a weight read NULL (needed, none
                        # found); the others 'not_needed'.
                        med_df_preferred = duckdb.sql(f"""
                            SELECT *
                                , CAST(NULL AS DOUBLE) AS weight_kg
                                , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                                , CASE WHEN {needs_wt_filter} THEN CAST(NULL AS VARCHAR)
                                    ELSE 'not_needed' END AS _weight_source
                                , CAST(NULL AS DOUBLE) AS _weight_age_hours
                            FROM med_df_preferred
                        """)
                    else:
                        raise ValueError(error_msg)
                else:
                    # Split, look weights up for the needs-wt subset only,
                    # UNION ALL BY NAME the rest.
                    needs_wt_subset = duckdb.sql(f"""
                        SELECT * FROM med_df_preferred WHERE {needs_wt_filter}
                    """)
                    no_wt_subset = duckdb.sql(f"""
                        SELECT *
                            , CAST(NULL AS DOUBLE) AS weight_kg
                            , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                            , 'not_needed' AS _weight_source
                            , CAST(NULL AS DOUBLE) AS _weight_age_hours
                        FROM med_df_preferred
                        WHERE NOT ({needs_wt_filter})
                    """)
                    if weight_fallbacks is not None:
                        # The time column is already renamed to the internal
                        # `admin_dttm` here, which is attach_weight's default.
                        needs_wt_attached = attach_weight(
                            needs_wt_subset,
                            vitals_df,
                            id_name=id_name,
                            fallbacks=weight_fallbacks,
                            hospitalization_df=hospitalization_df,
                            max_staleness_days=weight_max_staleness_days,
                            return_format='duckdb',
                        )
                        needs_wt_joined = needs_wt_attached
                    else:
                        needs_wt_attached = find_most_recent_weight(
                            needs_wt_subset,
                            vitals_df,
                            id_name=id_name,
                            fallback_on_earliest=fallback_on_earliest,
                        )
                        # The legacy wrapper keeps its output contract (no
                        # age); attach_weight's formula, an instant
                        # difference the session TimeZone cannot shift.
                        needs_wt_joined = duckdb.sql("""
                            SELECT *
                                , _weight_age_hours: (epoch_us(admin_dttm)
                                    - epoch_us(_weight_recorded_dttm)) / 3600000000.0
                            FROM needs_wt_attached
                        """)
                    if logger.isEnabledFor(logging.INFO):
                        if fmt != 'duckdb':
                            # Materialize once: the count and the UNION ALL
                            # below would otherwise each execute the weight
                            # lookup. A 'duckdb' result stays lazy, so
                            # the log level cannot change which rows it reads.
                            needs_wt_joined = duckdb.from_arrow(
                                relation_to_arrow(needs_wt_joined)
                            )
                        _log_weight_sources(
                            needs_wt_joined, weight_fallbacks, fallback_on_earliest,
                        )
                    med_df_preferred = duckdb.sql("""
                        SELECT * FROM needs_wt_joined
                        UNION ALL BY NAME
                        SELECT * FROM no_wt_subset
                    """)
            else:
                # No row needs weight. Add placeholders for schema consistency.
                logger.debug("No rows require patient weight; skipping vitals join.")
                med_df_preferred = duckdb.sql("""
                    SELECT *
                        , CAST(NULL AS DOUBLE) AS weight_kg
                        , CAST(NULL AS TIMESTAMP) AS _weight_recorded_dttm
                        , 'not_needed' AS _weight_source
                        , CAST(NULL AS DOUBLE) AS _weight_age_hours
                    FROM med_df_preferred
                """)
        else:
            # User pre-filled weight_kg; trust it as-is. Lookup arguments passed
            # alongside it then do nothing, so name them rather than let a
            # requested cap or chain vanish silently.
            ignored = [
                name for name, given in (
                    ('weight_fallbacks', weight_fallbacks is not None),
                    ('weight_max_staleness_days', weight_max_staleness_days is not None),
                    ('hospitalization_df', hospitalization_df is not None),
                    ('fallback_on_earliest', bool(fallback_on_earliest)),
                )
                if given
            ]
            if ignored:
                logger.warning(
                    "med_df already has a weight_kg column, which is used as-is; "
                    f"{', '.join(ignored)} {'is' if len(ignored) == 1 else 'are'} "
                    "ignored. Drop weight_kg and its _weight_* provenance columns "
                    "to let the converter look weights up."
                )
            # vitals_df is not used either. It stays off the list above: the
            # orchestrator always passes one, so naming it there would warn on
            # every call over pre-attached weights. Warn only when it could
            # have helped: rows stage 2 multiplies by weight (the lookup's own
            # predicate) whose weight_kg stage 2 cannot use. One aggregate,
            # run here also under return_format='duckdb'.
            # A row whose input `_weight_source` is NULL was looked up and none
            # was found (attach_weight's chain, or an earlier pass's lookup):
            # that is the chain's verdict (a cap, or no weight anywhere), the
            # provenance already records it, and neither the cause nor the
            # remedies below apply. A row reading 'not_needed' is an earlier
            # pass's placeholder, the case this warning is for, and so is a
            # weight_kg with no provenance at all.
            # NOTE: keyed on the caller's `_weight_source` (parked here under
            # the caller's spelling), not on the presence of
            # `_weight_age_hours`: every converter pass now returns both
            # (D-261119-weight-labels), so only the label tells a lookup's
            # gap from a placeholder.
            needs_warning_gate = (
                f"{quote_identifier(parked_weight_source)} = 'not_needed'"
                if parked_weight_source is not None else "TRUE"
            )
            if vitals_df is not None:
                (n_unweighted,) = duckdb.sql(f"""
                    SELECT count(*) FROM med_df_preferred
                    WHERE {_weight_lookup_filter()} AND {_weight_unusable_predicate()}
                        AND {needs_warning_gate}
                """).fetchone()
                if n_unweighted:
                    logger.warning(
                        "med_df already has a weight_kg column, so vitals_df is not "
                        f"used; {n_unweighted} row(s) that need a weight have none "
                        "and will report 'weight_kg is missing'. A likely cause: a "
                        "weight_kg returned by an earlier conversion pass is NULL "
                        "on the rows that pass did not convert across the weight "
                        "axis (_weight_source = 'not_needed'). Drop weight_kg and "
                        "its _weight_* provenance columns first, attach "
                        "weights with attach_weight before converting, or run the pair that "
                        "crosses the weight axis first (if both pairs cross it, "
                        "attach weights instead)."
                    )
            # The caller's weight has no provenance of its own: label a
            # usable one 'prefilled'; an unusable one (NULL, zero or below,
            # NaN, infinite) reads NULL, as stage 2 treats it as missing. Its
            # age is unknown.
            if not weight_source_taken:
                med_df_preferred = duckdb.sql(f"""
                    SELECT *
                        , CASE WHEN NOT ({_weight_unusable_predicate()})
                            THEN 'prefilled' END AS _weight_source
                    FROM med_df_preferred
                """)
            if not weight_age_taken:
                med_df_preferred = duckdb.sql("""
                    SELECT *, CAST(NULL AS DOUBLE) AS _weight_age_hours
                    FROM med_df_preferred
                """)

        # --------------------------------------------------------------
        # Stage 2: convert to preferred units.
        # --------------------------------------------------------------
        try:
            logger.debug("Converting to preferred units...")
            med_df_converted = _convert_base_units_to_preferred_units(
                med_df_preferred, override=override, show_intermediate=show_intermediate,
                unlisted=unlisted, countable_units=countable_tokens,
                # the explicit targets were checked up front, above
                targets_validated=True,
            )
        except ValueError as e:
            raise ValueError(f"Error converting dose units to preferred units: {e}")

        # --------------------------------------------------------------
        # Counts table.
        # --------------------------------------------------------------
        # What users see spells the unit family out (`units/hr`, not `u/hr`):
        # the cleaned and base units, an unlisted row's own target, and a
        # failed row's unit (its cleaned source unit). A target the caller
        # wrote is echoed as written, on its rows and in `_preferred_unit`.
        med_df_converted = med_df_converted.project(
            "* REPLACE ("
            f"{_canonical_spelling_expr('_clean_unit')} AS _clean_unit, "
            f"{_canonical_spelling_expr('_base_unit')} AS _base_unit, "
            f"{_canonical_spelling_expr('_preferred_unit_clean')} AS _preferred_unit_clean, "
            "CASE WHEN _preferred_is_explicit THEN _preferred_unit "
            f"ELSE {_canonical_spelling_expr('_preferred_unit')} END AS _preferred_unit, "
            "CASE WHEN _convert_status = 'success' THEN med_dose_unit_converted "
            f"ELSE {_canonical_spelling_expr('med_dose_unit_converted')} END "
            "AS med_dose_unit_converted)"
        )

        # The counts table groups by the row's weight provenance: the
        # converter's own `_weight_source`, else the caller's parked copy,
        # else NULL (a source column named `_weight_source` with a
        # pre-filled weight).
        counts_input = med_df_converted
        if '_weight_source' not in med_df_converted.columns:
            provenance = (
                f"CAST({quote_identifier(parked_weight_source)} AS VARCHAR)"
                if parked_weight_source is not None else "CAST(NULL AS VARCHAR)"
            )
            counts_input = duckdb.sql(
                f"SELECT *, {provenance} AS _weight_source FROM med_df_converted"
            )
        try:
            convert_counts_df = _create_unit_conversion_counts_table(
                counts_input,
                # The review columns first, then `count`, then the QA detail.
                group_by=[
                    'med_category', 'med_dose_unit',
                    '_preferred_unit', 'med_dose_unit_converted',
                    '_convert_status_details', '_weight_source',
                    '_clean_unit', '_base_unit', '_unit_class', '_convert_status',
                ],
                output_names=counts_names,
                count_after='_weight_source',
            )
        except ValueError as e:
            raise ValueError(f"Error creating unit conversion counts table: {e}")

        logger.info("Dose unit conversion complete")

        # --------------------------------------------------------------
        # Output column hygiene, on the pipeline's own names. It runs before
        # the rename below, which gives another pair's columns their
        # `__<dose_col>` suffix and un-parks the caller's columns: a caller's
        # `_weight_source` next to `weight_kg` still carries `_saved__` here,
        # so it is never taken for the converter's own QA column.
        # --------------------------------------------------------------
        # Default (show_intermediate=False): drop QA columns the user didn't
        # ask for. The counts table, built above, still groups by
        # `_clean_unit` and `_unit_class`. `_weight_source` and
        # `_weight_age_hours` stay, unless a source column of that name takes
        # it back (see weight_cols_as_sources).
        possible_cols_to_exclude = set() if show_intermediate else {
            '_clean_unit', '_unit_class',
            '_weight_recorded_dttm',
            *weight_cols_as_sources,
            '_needs_wt',
            '_base_dose', '_base_unit',
            '_base_wt', '_pref_wt',
            '_preferred_unit',
            '_preferred_unit_clean',
            '_preferred_is_explicit',
            '_preferred_is_weight_variant',
            '_unit_class_preferred',
            '_unit_subclass', '_unit_subclass_preferred',
            '_amount_multiplier', '_time_multiplier', '_weight_multiplier',
            '_amount_multiplier_preferred', '_time_multiplier_preferred',
            '_weight_multiplier_preferred',
        }
        existing_cols = set(med_df_converted.columns)
        cols_to_exclude = tuple(possible_cols_to_exclude & existing_cols)

        if cols_to_exclude:
            med_df_converted = duckdb.sql(f"""
                SELECT * EXCLUDE {cols_to_exclude}
                FROM med_df_converted
            """)

        # Restore the caller's column names and apply the requested output
        # names. The counts table already shows the converted frame's names
        # (see counts_output_names), so the two join without a rename.
        med_df_converted = rename_from_internal(
            med_df_converted, rename_map, output_names.output_map
        )

        # --------------------------------------------------------------
        # Final return.
        # --------------------------------------------------------------
        if show_intermediate:
            return _emit_output(
                med_df_converted, convert_counts_df, fmt=fmt, captured=captured,
                time_col=time_col,
            )

        result_rel = med_df_converted

        return _emit_output(
            result_rel, convert_counts_df, fmt=fmt, captured=captured,
            time_col=time_col,
        )

    finally:
        # Drop only the temp tables this call created. 'duckdb' creates none,
        # and tables other calls or modules registered are never touched.
        for name in created_tables:
            _drop_temp_table(name)


def standardize_med_dose_units(
    med_df: FrameLike,
    targets: Union[str, Path, Dict[str, str]],
    *,
    vitals_df: FrameLike | None = None,
    reader=None,
    category_col: str = 'med_category',
    unit_col: str = 'med_dose_unit',
    targets_category_col: str = 'med_category',
    targets_unit_col: str | None = None,
    override: bool = True,
    unlisted: Literal['base', 'keep'] = 'keep',
    allow_weight_variant: bool = False,
    return_format: ReturnFormat = 'polars',
    **kwargs,
) -> Union[
    Tuple[pl.DataFrame, pl.DataFrame],
    Tuple[pl.LazyFrame, pl.LazyFrame],
    Tuple[DuckDBPyRelation, DuckDBPyRelation],
    Tuple['pandas.DataFrame', 'pandas.DataFrame'],
]:
    """Standardize a medication table against an external mCIDE schema.

    Convenience wrapper over :func:`convert_dose_units_by_med_category` that
    takes the per-category target units from a targets file (an mCIDE CSV, a
    URL, YAML, JSON, Parquet) or a dict. A category the targets do not list is
    left as charted (`unlisted='keep'`): its dose is copied and only its unit
    spelling is cleaned.

    Parameters
    ----------
    med_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame
        Medication administration table, as for
        :func:`convert_dose_units_by_med_category`.
    targets : str, Path, or dict
        Targets file path/URL (read by
        :func:`~clifpy.utils.unit_converter.load_dose_unit_targets`), or an
        already-parsed ``{med_category: target_unit}`` mapping. In both forms an entry whose
        category or unit is blank or an NA token (`''`, `'NA'`, `'nan'`,
        `None`, ...) means "no target": it is dropped, and that category is
        unlisted (see `unlisted`).
    vitals_df : pl.DataFrame, pl.LazyFrame, DuckDBPyRelation or pd.DataFrame, optional
        Vitals table, needed only when a conversion crosses the weight axis
        (weighted to unweighted or the reverse).
    reader : callable, optional
        Override the schema reader; see
        :func:`~clifpy.utils.unit_converter.load_dose_unit_targets`.
    category_col, unit_col : str
        Column names in `med_df`. Forwarded to the converter.
    targets_category_col : str, default 'med_category'
        Category column of the *targets file* (not of `med_df`). Ignored for a
        dict.
    targets_unit_col : str, optional
        Target-unit column of the targets file. None picks
        `'volume_infusion_rate_units'` when `dose_col` (in `**kwargs`) is
        `volume_infusion_rate`, and `'med_dose_unit'` otherwise -- the two
        unit columns of the mCIDE continuous file. Ignored for a dict.
    override : bool, default True
        Note this default differs from
        :func:`convert_dose_units_by_med_category`, which defaults to False.
        The converter raises for a category that has a target but is absent
        from the data, and a schema always lists categories a given table
        lacks (the CLIF 3.0 mCIDE files list dozens to hundreds); with
        `override=True` they are summarised in one warning instead. The
        reverse case never raises (clifpy#153): data categories the targets
        omit are handled by `unlisted`.

        `override=True` also turns two other errors into a warning plus a
        per-row `_convert_status`: a schema target the unit grammar cannot
        parse ('user-preferred unit ... is not recognized'), and a conversion
        that needs patient weight when the input has no `weight_kg` column
        and no `vitals_df` is given ('... weight_kg is missing'). Pass
        `vitals_df` (the orchestrator always does), or `override=False` to
        make all three raise.
    unlisted : {'base', 'keep'}, default 'keep'
        Rows whose category the targets do not list. 'keep' (the default here)
        copies the dose and cleans the unit spelling; 'base' converts them to
        clifpy's base unit, as :func:`convert_dose_units_by_med_category` does
        by default.
    allow_weight_variant : bool, default False
        Accept each target's weight-adjusted or weight-naive variant: with an
        mCIDE target of `mcg/hr`, `mcg/hr` and `mcg/kg/hr` rows stay as
        charted and `mg/kg/hr` becomes `mcg/kg/hr`. No weight is then looked
        up, so `vitals_df` is not needed. See
        :func:`convert_dose_units_by_med_category`.
    return_format : {'polars', 'polars_lazy', 'duckdb', 'pandas'}, default 'polars'
        Output format, as for :func:`convert_dose_units_by_med_category`.
    **kwargs
        Passed through to :func:`convert_dose_units_by_med_category`
        (`id_name`, `show_intermediate`, `countable_units`,
        `converted_dose_col`, ...).

    Returns
    -------
    tuple
        `(converted, counts)` in the format `return_format` names, polars
        DataFrames by default, as :func:`convert_dose_units_by_med_category`
        returns them. `_convert_status_details` says how each row reached its
        unit: 'success: converted to the target unit', 'success: already the
        target unit', 'success: no target listed; unit cleaned only', ... (see
        `CONVERT_STATUS_DETAILS`), or repeats the failure reason.

    Notes
    -----
    `return_format` is resolved first, before the targets are read. The
    conversion runs in `'duckdb'` (for `'duckdb'`, which stays lazy) or in
    `'polars'` (for every other format); `'polars_lazy'` and the deprecated
    `'pandas'` are made from the polars frames at the very end.

    The warnings below are computed from `counts` after the conversion; under
    `return_format='duckdb'` that evaluates the pipeline once inside this call.

    - One WARNING names every data category that differs from a listed
      category only in case or surrounding spaces (`'Norepinephrine '` or
      `'NOREPINEPHRINE'` against the listed `'norepinephrine'`), then up to
      eight other unlisted categories (most rows first) and how many more.

    - One WARNING per category whose target fails on unit class
      ('cannot convert rate to amount' / 'cannot convert amount to rate')
      for most of its rows with a recognised unit. That is the signature of
      a targets file from before the 2026-09-09 mCIDE layout, whose
      `med_dose_unit` held amount-only units, or of the other medication
      table's file.

    Examples
    --------
    >>> from clifpy.utils.unit_converter import standardize_med_dose_units
    >>> URL = (                                        # doctest: +SKIP
    ...     'https://raw.githubusercontent.com/'
    ...     'Common-Longitudinal-ICU-data-Format/CLIF/3.0/mCIDE/'
    ...     'medication_admin_continuous/'
    ...     'clif_medication_admin_continuous_med_categories.csv')
    >>> out, counts = standardize_med_dose_units(      # doctest: +SKIP
    ...     mac_df, URL, vitals_df=vitals_df)

    See Also
    --------
    convert_dose_units_by_med_category : The underlying conversion.
    clifpy.utils.unit_converter.load_dose_unit_targets : Schema parsing.
    """
    # Resolve the output format first, as the converter does: an unknown
    # format, or the deprecated return_rel next to a conflicting
    # return_format, fails before the schema is read, and a deprecation
    # warns once, here, at the caller's line.
    fmt = resolve_return_format(return_format, kwargs.pop('return_rel', False))
    dose_col = kwargs.get('dose_col', 'med_dose')
    if targets_unit_col is None:
        # The mCIDE continuous file carries both pairs' targets.
        targets_unit_col = (
            'volume_infusion_rate_units'
            if str(dose_col).lower() == 'volume_infusion_rate' else 'med_dose_unit'
        )
    target_spec = targets

    if isinstance(target_spec, dict):
        # Same rule load_dose_unit_targets applies to schema rows: a blank or
        # NA category or unit means "no target defined", so that category is
        # unlisted (handled by `unlisted`) instead of reaching stage 2 as an
        # explicit, unrecognized unit.
        targets = {
            cat: unit
            for cat, unit in target_spec.items()
            if not (_is_na_token(cat) or _is_na_token(unit))
        }
        if len(targets) < len(target_spec):
            logger.info(
                "Dropped %d dict target(s) whose category or unit is blank/NA",
                len(target_spec) - len(targets),
            )
    else:
        targets = load_dose_unit_targets(
            target_spec,
            reader=reader,
            category_col=targets_category_col,
            unit_col=targets_unit_col,
        )
    if not targets:
        raise ValueError(
            "targets produced no usable targets; check the targets file's "
            "columns and that it is not empty"
        )

    logger.info("Standardizing med dose units against %d target(s)", len(targets))
    # 'duckdb' converts lazily as asked, every other format converts to
    # polars, and 'polars_lazy' and the deprecated 'pandas' are made from the
    # polars frames at the very end.
    converted, counts = convert_dose_units_by_med_category(
        med_df,
        vitals_df=vitals_df,
        preferred_units=targets,
        override=override,
        category_col=category_col,
        unit_col=unit_col,
        unlisted=unlisted,
        allow_weight_variant=allow_weight_variant,
        return_format='duckdb' if fmt == 'duckdb' else 'polars',
        **kwargs,
    )
    _warn_about_targets(
        counts, targets, category_col=category_col, dose_col=dose_col, unlisted=unlisted
    )
    if fmt == 'polars_lazy':
        return converted.lazy(), counts.lazy()
    if fmt == 'pandas':
        # The deprecated shim, at the very end; pandas input keeps its index.
        return to_pandas(converted, pandas_index(med_df)), to_pandas(counts)
    return converted, counts
