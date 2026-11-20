"""Input boundary: the table types the package accepts.

Every computation in this package runs in DuckDB (preferred) or polars, and
every table enters through this module. A polars DataFrame or LazyFrame, a
DuckDB relation (from any connection), or a pandas DataFrame is accepted. pandas is accepted here
and nowhere else: :func:`pandas_to_arrow` hands it to Arrow untouched, and
this package never imports pandas (decision D-261022-pandas-input). A
relation built on a caller-owned connection (clifpy's ``new_duckdb_con()``
pattern) cannot be scanned by the default connection, so it is run on its
own connection and copied across through Arrow, and the copy is logged.

Arrow carries each timestamp column's zone in its type, so a table's zones
are read from its Arrow copy (:func:`arrow_tz_map`). A DuckDB relation
carries no zone of its own: its TIMESTAMPTZ columns are labelled with the
TimeZone of the connection that owns it, which is what that connection's own
``.pl()`` shows.

The package's SQL reads Python variables by name through DuckDB's
replacement scan, and DuckDB resolves a table or view in the catalog
*before* a Python variable of the same name: a table called ``med_df`` on the
default connection would silently replace the data scanned as ``med_df``.
:func:`check_no_shadowing_tables` refuses to run instead.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple, Union

import duckdb
import polars as pl
import pyarrow as pa

from clifpy.utils.logging_config import get_logger
from ._columns import quote_identifier

if TYPE_CHECKING:  # type checkers only; this package never imports pandas at runtime
    import pandas

logger = get_logger('utils.unit_converter')

# Every table type the public entry points accept. pandas is named by a
# forward reference, so nothing here imports it.
FrameLike = Union['pandas.DataFrame', pl.DataFrame, pl.LazyFrame, duckdb.DuckDBPyRelation]

# Python variables the package's SQL reads by name through DuckDB's replacement
# scan, lowercase (DuckDB identifiers are case-insensitive). A catalog table or
# view with one of these names would be read in place of the variable.
# test_replacement_scan_names_cover_every_sql_source keeps this set in step
# with the FROM / JOIN clauses in the package: a new query that reads a Python
# variable by name adds that name here.
REPLACEMENT_SCAN_NAMES = frozenset({
    'relation',                                         # _clean.py
    'rel',                                              # _columns.py, _outputs.py
    'med_df',                                           # _base, _counts, _preferred, _weight, _convert
    'acceptable_units_relation',                        # _preferred.py
    'vitals_df', 'hospitalization_df', 'attached',      # _weight.py
    'requested_categories_df', 'preferred_units_df',    # _convert.py
    'med_df_base', 'med_df_preferred', 'med_df_converted',
    'needs_wt_attached', 'needs_wt_joined', 'no_wt_subset',
})


def is_pandas_frame(obj: Any) -> bool:
    """True for a pandas DataFrame, recognised by its class's module, not by an import."""
    cls = type(obj)
    return cls.__name__ == 'DataFrame' and cls.__module__.partition('.')[0] == 'pandas'


# What pyarrow raises for a pandas column it has no type for (an object column
# mixing str and int, say).
_ARROW_CONVERSION_ERRORS = (pa.ArrowInvalid, pa.ArrowTypeError)


def pandas_to_arrow(
    df: Any,
    columns: Optional[Sequence[str]] = None,
    *,
    detach: bool = False,
) -> pa.Table:
    """The pandas boundary: hand a pandas DataFrame to Arrow, untouched.

    The one function in this package that reads a pandas object's data.
    ``preserve_index=False`` leaves the index behind (:func:`pandas_index`
    keeps it for the deprecated pandas output); ``columns`` limits the copy.
    A tz-aware column keeps its zone in the Arrow type.

    An object column mixing Python types (``['mg', 5, None]``, common in raw
    EHR unit and ID columns) has no Arrow type, and pyarrow refuses the whole
    frame. Only those columns are then read by DuckDB's own pandas scanner,
    which reads mixed text and numbers as VARCHAR, as clifpy's
    ``CREATE TEMP TABLE ... FROM med_df`` did before this boundary existed.
    The scan runs on a short-lived private connection that keeps insertion
    order, so the columns line up with the others whatever the session's
    settings. Every other column still goes through pyarrow, so its type and zone are
    unchanged. A WARNING names the columns read that way.

    ``detach=True`` deep-copies the selected columns first. Arrow shares a
    numeric numpy buffer with the frame, so without the copy an in-place edit
    of the caller's frame would reach a lazy result built on the table.
    """
    names = list(df.columns) if columns is None else list(columns)
    if detach:
        df = df[names].copy()
    try:
        return pa.Table.from_pandas(df, columns=names, preserve_index=False)
    except _ARROW_CONVERSION_ERRORS as error:
        whole_frame_error = error
    arrays, failed = {}, []
    for name in names:
        try:
            arrays[name] = pa.Table.from_pandas(
                df, columns=[name], preserve_index=False,
            ).column(0)
        except _ARROW_CONVERSION_ERRORS:
            failed.append(name)
    if not failed:
        raise whole_frame_error
    logger.warning(
        "pandas column(s) %s mix Python types that Arrow cannot convert; "
        "DuckDB's pandas scanner reads them instead (mixed text and numbers "
        "become text)",
        [str(name) for name in failed],
    )
    # NOTE: a private connection with the setting stated outright. On the
    # default connection a session's `SET preserve_insertion_order = false`
    # lets the parallel scan return rows out of order, and the columns are
    # zipped back by position. Never change a setting on the default one.
    with duckdb.connect(config={'preserve_insertion_order': True}) as con:
        scanned = relation_to_arrow(
            con.from_df(df).project(
                ', '.join(quote_identifier(str(name)) for name in failed)
            )
        )
    if scanned.num_rows != len(df):
        raise RuntimeError(
            f"DuckDB's pandas scanner returned {scanned.num_rows} rows for a "
            f"{len(df)}-row frame; refusing to align the columns by position"
        )
    for name in failed:
        arrays[name] = scanned.column(str(name))
    return pa.table({str(name): arrays[name] for name in names})


def pandas_index(obj: Any) -> Any:
    """The caller's pandas index, for ``return_format='pandas'``; None for any other input."""
    return obj.index if is_pandas_frame(obj) else None


def _not_a_table(obj: Any, param: str) -> TypeError:
    """The TypeError for an input that is not an accepted table, naming ``param``."""
    return TypeError(
        f"{param} must be a polars DataFrame or LazyFrame, a DuckDB relation "
        f"or a pandas DataFrame; got {type(obj).__name__}"
    )


def column_names(df: Any, param: str = 'df') -> List[str]:
    """Column names of a pandas, polars, pyarrow or DuckDB table.

    A polars LazyFrame is read through ``collect_schema()``: its ``.columns``
    resolves the schema behind a PerformanceWarning on every access.

    Raises
    ------
    TypeError
        Anything else, naming ``param`` (the same error as :func:`to_arrow`).
    """
    if isinstance(df, pl.LazyFrame):
        return df.collect_schema().names()
    if isinstance(df, pa.Table):
        return list(df.column_names)
    if is_pandas_frame(df) or isinstance(df, (pl.DataFrame, duckdb.DuckDBPyRelation)):
        return [str(c) for c in df.columns]
    raise _not_a_table(df, param)


def relation_to_arrow(rel: duckdb.DuckDBPyRelation) -> pa.Table:
    """Run ``rel`` on the connection that owns it and return a ``pyarrow.Table``.

    ``to_arrow_table()`` is the duckdb >= 1.4 spelling and
    ``fetch_arrow_table()`` the older one (deprecated in 1.5). ``rel.arrow()``
    is not used: in 1.5 it returns a one-shot ``RecordBatchReader``.
    """
    to_table = getattr(rel, 'to_arrow_table', None)
    return to_table() if to_table is not None else rel.fetch_arrow_table()


def is_foreign_relation(rel: duckdb.DuckDBPyRelation) -> bool:
    """True when ``rel`` belongs to a connection other than DuckDB's default one.

    A relation exposes no handle to its connection, so this registers it on
    the default connection under a throwaway name: DuckDB refuses a relation
    from another connection with InvalidInputException. The probe view is
    removed straight away.
    """
    probe = f"_med_unit_probe_{uuid.uuid4().hex[:8]}"
    try:
        duckdb.register(probe, rel)
    except duckdb.InvalidInputException:
        return True
    duckdb.unregister(probe)
    return False


def arrow_tz_map(schema: pa.Schema) -> Dict[str, str]:
    """``{column: zone}`` for every tz-aware timestamp column of an Arrow schema."""
    return {
        field.name: field.type.tz
        for field in schema
        if pa.types.is_timestamp(field.type) and field.type.tz is not None
    }


def _project(rel: duckdb.DuckDBPyRelation, columns: Optional[Sequence[str]]):
    """``rel`` limited to ``columns`` (all of them when None)."""
    if columns is None:
        return rel
    return rel.project(', '.join(quote_identifier(c) for c in columns))


def to_arrow(
    obj: Any,
    param: str,
    *,
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    detach: bool = False,
) -> pa.Table:
    """Copy one accepted input into a ``pyarrow.Table``, with no pandas operation.

    - pandas DataFrame: :func:`pandas_to_arrow`.
    - polars DataFrame or LazyFrame: ``columns`` selected (lazily for a
      LazyFrame, which is then collected once) and ``to_arrow()``, which
      shares polars' buffers.
    - DuckDB relation: ``where`` (SQL) and ``columns`` applied on the
      connection that owns it, which then runs it. A relation from another
      connection is logged at INFO with its row count, since the copy costs
      memory.

    ``where`` only shrinks a relation's copy; the package's SQL applies the
    same filter to every input anyway. ``detach`` only affects a pandas
    DataFrame (see :func:`pandas_to_arrow`).

    Raises
    ------
    TypeError
        Any other type, naming ``param``.
    """
    if is_pandas_frame(obj):
        return pandas_to_arrow(obj, columns, detach=detach)
    if isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
        if columns is not None:
            obj = obj.select(list(columns))
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        return obj.to_arrow()
    if isinstance(obj, duckdb.DuckDBPyRelation):
        foreign = is_foreign_relation(obj)
        if where is not None:
            obj = obj.filter(where)
        table = relation_to_arrow(_project(obj, columns))
        if foreign:
            logger.info(
                "%s is a relation from another DuckDB connection; copying its "
                "%d row(s) to the default connection through Arrow",
                param, table.num_rows,
            )
        return table
    raise _not_a_table(obj, param)


def read_input(
    obj: Any,
    param: str,
    *,
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    detach: bool = False,
) -> Tuple[duckdb.DuckDBPyRelation, Dict[str, str]]:
    """``obj`` as a relation on DuckDB's default connection, plus its zone map.

    A relation on the default connection stays lazy (``where`` and
    ``columns`` applied to it); its zone map is the connection's TimeZone,
    read from a zero-row Arrow export. Every other input, a relation from
    another connection included, is copied once by :func:`to_arrow` and
    scanned from that Arrow table by reference, so no catalog name exists for
    it. A copied relation keeps its own connection's TimeZone as its zones.
    """
    if isinstance(obj, duckdb.DuckDBPyRelation) and not is_foreign_relation(obj):
        rel = _project(obj if where is None else obj.filter(where), columns)
        return rel, arrow_tz_map(relation_to_arrow(rel.limit(0)).schema)
    table = to_arrow(obj, param, columns=columns, where=where, detach=detach)
    return duckdb.from_arrow(table), arrow_tz_map(table.schema)


def on_default_connection(
    obj: Any,
    param: str,
    *,
    columns: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    detach: bool = False,
) -> Optional[duckdb.DuckDBPyRelation]:
    """``obj`` as a relation DuckDB's default connection can scan; None stays None.

    :func:`read_input` without the zone map.
    """
    if obj is None:
        return None
    if isinstance(obj, duckdb.DuckDBPyRelation) and not is_foreign_relation(obj):
        return _project(obj if where is None else obj.filter(where), columns)
    return duckdb.from_arrow(
        to_arrow(obj, param, columns=columns, where=where, detach=detach)
    )

def check_no_shadowing_tables() -> None:
    """Refuse to run when a catalog object would shadow one of the package's variables.

    Looks at the tables and views DuckDB's default connection resolves
    without a qualifier (the temp schema and the search path of the current
    database), case-insensitively.

    Raises
    ------
    ValueError
        Naming every such table or view whose name is in
        :data:`REPLACEMENT_SCAN_NAMES`.
    """
    found = duckdb.sql("""
        FROM (
            FROM duckdb_tables() SELECT database_name, schema_name, name: table_name
            UNION ALL
            FROM duckdb_views() SELECT database_name, schema_name, view_name
            WHERE NOT internal
        )
        SELECT DISTINCT name
        WHERE database_name IN ('temp', current_database())
            AND list_contains(current_schemas(true), schema_name)
        ORDER BY name
    """).fetchall()
    clash = [name for (name,) in found if name.lower() in REPLACEMENT_SCAN_NAMES]
    if clash:
        raise ValueError(
            f"DuckDB's default connection has table(s) or view(s) named {clash}. "
            f"The unit converter reads Python variables with these names through "
            f"DuckDB's replacement scan, and DuckDB resolves a catalog object "
            f"first, so your table would be read instead of the data you passed. "
            f"Rename or drop it, or keep your own tables on a separate connection "
            f"(clifpy.utils.io.new_duckdb_con())."
        )
