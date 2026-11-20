"""Output formats: what the public entry points hand back.

Every result is a DuckDB relation until the very end; :func:`emit` turns it
into the format the caller asked for, mirroring
:func:`clifpy.utils.io.load_data` (decision D-261022-return-format):

- ``'polars'`` (default): a polars DataFrame.
- ``'polars_lazy'``: a polars LazyFrame over that materialized DataFrame. The
  conversion has already run, so it never reads a temp table this package
  has dropped.
- ``'duckdb'``: the relation itself, on DuckDB's default connection.
- ``'pandas'``: deprecated; retires in the next release. The polars result,
  converted at the very end by :func:`to_pandas`, the one place this package
  makes a pandas object.

Timestamps are relabelled in polars (``dt.convert_time_zone``, which keeps
the instant): DuckDB exports TIMESTAMPTZ labelled with its connection's
TimeZone, which need not be the input's zone. polars resolves zones through
chrono-tz, the engine clifpy.utils.io relies on for CLIF-MIMIC's far-future
dates (docs/tz_dx.md §11). ``_weight_recorded_dttm``, which comes from the
vitals table, is labelled in the dose time's zone
(:func:`with_weight_time_zone`), so one result holds one zone.
"""

from __future__ import annotations

import sys
import warnings
from typing import Any, Dict, Literal, Optional, Sequence, Tuple

import duckdb
import polars as pl

from clifpy.utils.logging_config import get_logger
from ._columns import quote_identifier

logger = get_logger('utils.unit_converter')

ReturnFormat = Literal['polars', 'polars_lazy', 'duckdb', 'pandas']

# The formats load_data offers, in its order (clifpy.utils.io.RETURN_FORMATS).
RETURN_FORMATS: Tuple[str, ...] = ('polars', 'polars_lazy', 'duckdb', 'pandas')
DEFAULT_RETURN_FORMAT = 'polars'

# The weight lookup's timestamp (one of _columns.WEIGHT_OUTPUT_COLUMNS).
_WEIGHT_TIME_COL = '_weight_recorded_dttm'

_PACKAGE = __name__.rpartition('.')[0]


def _caller_stacklevel() -> int:
    """A ``stacklevel`` that points a warning at the first frame outside this package.

    The public functions call one another (``standardize_med_dose_units``
    forwards to the converter), so a fixed stacklevel would blame clifpy.
    """
    level, frame = 1, sys._getframe(1)
    while frame is not None and frame.f_globals.get('__name__', '').startswith(_PACKAGE):
        frame = frame.f_back
        level += 1
    return level


def resolve_return_format(return_format: str = DEFAULT_RETURN_FORMAT, return_rel: bool = False) -> str:
    """Reconcile the deprecated ``return_rel`` flag with ``return_format``.

    - An unknown ``return_format`` raises ``ValueError``.
    - ``return_rel=True`` is a deprecated alias for ``'duckdb'`` and warns
      (``DeprecationWarning``). ``return_format`` defaults to ``'polars'``, so
      an explicit ``'polars'`` cannot be told from the default and is not a
      conflict. ``'polars_lazy'`` or ``'pandas'`` together with
      ``return_rel=True`` is a conflict and raises ``ValueError``.
    - ``'pandas'`` warns (``DeprecationWarning``) that it retires in the next
      release.

    Returns the effective format.
    """
    if return_format not in RETURN_FORMATS:
        raise ValueError(
            f"Unknown return_format {return_format!r}; use one of "
            f"{', '.join(repr(f) for f in RETURN_FORMATS)}."
        )
    if return_rel:
        if return_format not in (DEFAULT_RETURN_FORMAT, 'duckdb'):
            raise ValueError(
                f"return_rel=True conflicts with return_format={return_format!r}. "
                f"return_rel is a deprecated alias for return_format='duckdb'; "
                f"pass return_format alone."
            )
        warnings.warn(
            "return_rel=True is deprecated; use return_format='duckdb' instead.",
            DeprecationWarning,
            stacklevel=_caller_stacklevel(),
        )
        return 'duckdb'
    if return_format == 'pandas':
        warnings.warn(
            "return_format='pandas' is deprecated and retires in the next "
            "release; use the default 'polars' (call .to_pandas() on the "
            "result where a pandas frame is needed).",
            DeprecationWarning,
            stacklevel=_caller_stacklevel(),
        )
    return return_format


def with_weight_time_zone(tz_map: Dict[str, str], time_col: str) -> Dict[str, str]:
    """``tz_map`` plus ``_weight_recorded_dttm`` labelled in ``time_col``'s zone.

    The weight time comes from ``vitals_df`` through DuckDB, which labels a
    TIMESTAMPTZ in the session TimeZone, so one result could mix zones
    (``admin_dttm`` in US/Eastern, ``_weight_recorded_dttm`` in whatever the
    session holds). Labelling it like the dose time keeps one zone per result;
    the instants never change. A zone the input already gave that column (a
    pre-attached weight) is kept, and a naive ``time_col`` adds nothing.
    """
    zones = dict(tz_map)
    if time_col in tz_map:
        zones.setdefault(_WEIGHT_TIME_COL, tz_map[time_col])
    return zones


def pin_default_tz(tz_map: Dict[str, str], time_col: Optional[str] = None) -> None:
    """Pin the default connection's TimeZone so a caller's later ``.pl()`` on a
    returned relation renders TIMESTAMPTZ in the input's zone.

    Persistent by design (a deferred render cannot be intercepted). Best-effort:
    swallows errors so returning the relation never fails on an exotic zone. Only
    used for ``return_format='duckdb'`` with tz-aware input. See docs/tz_dx.md (§9).

    The zone pinned is that of ``time_col`` (the caller's dose-time column) when
    it is tz-aware, else of ``admin_dttm``, else the first zone in ``tz_map``.
    """
    if not tz_map:
        return
    tz = (tz_map.get(time_col) if time_col else None) or tz_map.get('admin_dttm') \
        or next(iter(tz_map.values()))
    try:
        duckdb.execute(f"SET TimeZone = '{tz}'")
    except Exception:
        logger.warning(f"Could not pin default-connection TimeZone to '{tz}' for deferred render")


def relabel_tz(frame: pl.DataFrame, tz_map: Dict[str, str]) -> pl.DataFrame:
    """Label each column named in ``tz_map`` in its input zone; instants never change.

    DuckDB exports TIMESTAMPTZ labelled with its connection's TimeZone. The
    conversion is emitted for every listed tz-aware column, even one already
    in its zone (see the CRITICAL note in
    clifpy.utils.io._convert_dttm_cols_polars). A column absent from the
    frame, or not tz-aware there, is left alone.
    """
    exprs = [
        pl.col(col).dt.convert_time_zone(tz)
        for col, tz in tz_map.items()
        if isinstance(frame.schema.get(col), pl.Datetime)
        and frame.schema[col].time_zone is not None
    ]
    return frame.with_columns(exprs) if exprs else frame


def to_pandas(frame: pl.DataFrame, index: Any = None) -> Any:
    """The deprecated ``return_format='pandas'`` shim: the one place a pandas object is made.

    Converts the finished polars result at the very end. ``index`` is the
    caller's pandas index (pandas input only); it is restored when the result
    has exactly one row per input row, which every pipeline stage guarantees.
    """
    out = frame.to_pandas()
    if index is not None:
        if len(out) != len(index):
            raise RuntimeError(
                f"conversion returned {len(out)} rows for {len(index)} input "
                f"rows; the caller's index cannot be restored"
            )
        out.index = index
    return out


def emit(
    rel: duckdb.DuckDBPyRelation,
    fmt: str,
    *,
    tz_map: Optional[Dict[str, str]] = None,
    order_by: Optional[str] = None,
    drop: Sequence[str] = (),
    pandas_index: Any = None,
    time_col: Optional[str] = None,
) -> Any:
    """Hand a finished relation back in ``fmt`` (already resolved).

    ``drop`` names hidden columns to leave out. ``order_by`` sorts the eager
    formats; a relation has no row order, so ``'duckdb'`` ignores it.
    ``tz_map`` relabels the eager formats' timestamp columns and, for
    ``'duckdb'``, pins the default connection's TimeZone instead, to the zone
    of ``time_col`` when given (see ``pin_default_tz``).
    """
    tz_map = tz_map or {}
    excluded = ', '.join(quote_identifier(c) for c in drop)
    if fmt == 'duckdb':
        pin_default_tz(tz_map, time_col)
        return rel.project(f"* EXCLUDE ({excluded})") if drop else rel
    # NOTE: DuckDB INTERVAL exports as Arrow month_day_nano_interval, which polars
    # refuses; a caller's duration column (polars Duration, pandas timedelta,
    # relation INTERVAL) is read as exact microseconds and cast back below.
    intervals = [
        c for c, t in zip(rel.columns, rel.types)
        if c not in drop and str(t).upper() == 'INTERVAL'
    ]
    query = "SELECT *" + (f" EXCLUDE ({excluded})" if drop else "")
    if intervals:
        replaced = ', '.join(
            f"epoch_us({quote_identifier(c)}) AS {quote_identifier(c)}" for c in intervals)
        query += f" REPLACE ({replaced})"
    query += " FROM rel"
    if order_by is not None:
        query += f" ORDER BY {quote_identifier(order_by)} ASC"  # explicit: ignores SET default_order
    frame = duckdb.sql(query).pl()
    if intervals:
        frame = frame.with_columns(pl.col(c).cast(pl.Duration('us')) for c in intervals)
    frame = relabel_tz(frame, tz_map)
    if fmt == 'polars_lazy':
        return frame.lazy()
    if fmt == 'pandas':
        return to_pandas(frame, pandas_index)
    return frame
