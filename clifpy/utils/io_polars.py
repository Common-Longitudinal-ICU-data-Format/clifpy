"""Deprecated polars I/O shims.

Every function here now delegates to :func:`clifpy.utils.io.load_data`, which gained
native polars output in 0.6.0. This module is kept only so existing imports keep
working; it will be removed in a future release.

Migration
---------
=========================================  ================================================
old                                        new
=========================================  ================================================
``load_data_polars(t, p, f)``              ``load_data(t, p, f, return_format='polars_lazy')``
``load_data_polars(..., lazy=False)``      ``load_data(..., return_format='polars')``
``load_parquet_polars(path, ...)``         ``load_data(table, dir, 'parquet', ...)``
``load_csv_polars(path, ...)``             ``load_data(table, dir, 'csv', ...)``
``load_clif_table_polars(dir, table)``     ``load_data(table, dir, ...)``
=========================================  ================================================

Note that ``lazy`` defaults to ``True`` here but ``load_data`` defaults to eager
``'polars'`` -- the shims preserve the old default.

Two behaviour notes for anyone comparing old and new output:

- **Datetime precision.** These shims pass ``time_unit='ns'`` to preserve the old
  ``standardize_datetime_columns`` behaviour. ``load_data`` keeps the source unit
  (``us``) by default. This matters for ``join_asof``, which requires exact dtype match.
- **CSV timestamps are now parsed.** ``load_csv_polars`` used ``pl.scan_csv`` without
  ``try_parse_dates=True``, so ``*_dttm`` columns came back as ``String`` and were
  silently skipped by the tz converter. CSV now goes through DuckDB's reader and yields
  proper tz-aware ``Datetime``. This is a fix, not a regression.
"""

import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import polars as pl

from .io import load_data, _cast_id_cols_to_utf8  # noqa: F401  (re-exported below)

__all__ = [
    'load_parquet_polars',
    'load_csv_polars',
    'load_data_polars',
    'load_clif_table_polars',
    '_cast_id_cols_to_utf8',
]


def _warn(old: str, new: str) -> None:
    warnings.warn(
        f"{old} is deprecated and will be removed in a future release; use {new} instead.",
        DeprecationWarning,
        stacklevel=3,
    )


def _fmt(lazy: bool) -> str:
    return 'polars_lazy' if lazy else 'polars'


def _split(file_path: Union[str, Path]) -> tuple:
    """Split ``.../clif_<table>.<ext>`` into ``(directory, table_name, filetype)``."""
    p = Path(file_path)
    stem, filetype = p.stem, p.suffix.lstrip('.')
    table_name = stem[len('clif_'):] if stem.startswith('clif_') else stem
    return str(p.parent), table_name, filetype


def load_parquet_polars(
    file_path: Union[str, Path],
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Deprecated. Use ``load_data(table, dir, 'parquet', return_format=...)``."""
    _warn('load_parquet_polars', "load_data(..., return_format='polars'|'polars_lazy')")
    directory, table_name, filetype = _split(file_path)
    return load_data(
        table_name, directory, filetype or 'parquet',
        sample_size=sample_size, columns=columns, filters=filters,
        site_tz=site_tz, verbose=verbose,
        return_format=_fmt(lazy), time_unit='ns',
    )


def load_csv_polars(
    file_path: Union[str, Path],
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    sample_size: Optional[int] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Deprecated. Use ``load_data(table, dir, 'csv', return_format=...)``."""
    _warn('load_csv_polars', "load_data(..., return_format='polars'|'polars_lazy')")
    directory, table_name, filetype = _split(file_path)
    return load_data(
        table_name, directory, filetype or 'csv',
        sample_size=sample_size, columns=columns, filters=filters,
        site_tz=site_tz, verbose=verbose,
        return_format=_fmt(lazy), time_unit='ns',
    )


def load_data_polars(
    table_name: str,
    table_path: Union[str, Path],
    table_format_type: str,
    sample_size: Optional[int] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Dict[str, Any]] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
    verbose: bool = False,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Deprecated. Use ``load_data(..., return_format='polars'|'polars_lazy')``."""
    _warn('load_data_polars', "load_data(..., return_format='polars'|'polars_lazy')")
    return load_data(
        table_name, str(table_path), table_format_type,
        sample_size=sample_size, columns=columns, filters=filters,
        site_tz=site_tz, verbose=verbose,
        return_format=_fmt(lazy), time_unit='ns',
    )


def load_clif_table_polars(
    data_directory: Union[str, Path],
    table_name: str,
    filetype: str = 'parquet',
    hospitalization_ids: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    site_tz: Optional[str] = None,
    lazy: bool = True,
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """Deprecated. Use ``load_data(..., filters={'hospitalization_id': [...]})``."""
    _warn('load_clif_table_polars', "load_data(..., return_format='polars'|'polars_lazy')")
    filters = {'hospitalization_id': hospitalization_ids} if hospitalization_ids else None
    return load_data(
        table_name, str(data_directory), filetype,
        columns=columns, filters=filters, site_tz=site_tz,
        return_format=_fmt(lazy), time_unit='ns',
    )
