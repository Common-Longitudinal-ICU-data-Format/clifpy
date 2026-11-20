"""Normalisation of raw dose-unit strings into clifpy's internal tokens.

Two passes, in order, both in DuckDB: `_clean_dose_unit_formats_duckdb`
handles casing/whitespace and empty-to-NULL, then
`_clean_dose_unit_names_duckdb` rewrites spelling variants
(`milliliter/hour` -> `ml/hr`) via `UNIT_NAMING_VARIANTS`.
"""

import duckdb

from ._grammar import UNIT_NAMING_VARIANTS, MISSING_UNIT_PLACEHOLDERS


def _clean_dose_unit_formats_duckdb(
    relation: duckdb.DuckDBPyRelation,
    col: str = 'med_dose_unit',
    out_col: str = '_clean_unit'
) -> duckdb.DuckDBPyRelation:
    """Clean dose unit formatting in DuckDB.

    Removes whitespace, converts to lowercase, and replaces empty strings with NULL.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Input data containing the column to clean. Any table DuckDB can scan
        by name (a polars DataFrame, say) works too.
    col : str, default 'med_dose_unit'
        Name of the column containing dose unit strings.
    out_col : str, default '_clean_unit'
        Name of the column to write the cleaned value into. Parameterised so
        the same chain can normalise the caller's preferred units into
        `_preferred_unit_clean` without clobbering the observed `_clean_unit`.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation with the `out_col` column added.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({'med_dose_unit': ['mL / hr', 'MCG/KG/MIN', ' Mg/Hr ']})
    >>> _clean_dose_unit_formats_duckdb(df).pl()['_clean_unit'].to_list()
    ['ml/hr', 'mcg/kg/min', 'mg/hr']
    """
    # Applied in order:
    #   1. lowercase
    #   2. drop a trailing period      -- `Tablet.` is 182,690 observations on
    #      its own, and without this it classifies differently from `tablet`
    #   3. drop a trailing parenthetical or route qualifier
    #      -- `g (central catheter)`, `units/hr (prepared by dialysis staff)`
    #   4. strip all whitespace
    #   5. map '' and the placeholder strings to NULL, so they report as
    #      'original unit is missing' rather than as unrecognised units
    placeholders = "', '".join(sorted(MISSING_UNIT_PLACEHOLDERS))
    cleaned = (
        f"regexp_replace("
        f"  regexp_replace("
        f"    regexp_replace(lower({col}), '\\.\\s*$', ''),"
        f"    '\\s*\\([^)]*\\)\\s*$|\\s+(given|central line)\\s*$', ''),"
        f"  '\\s+', '', 'g')"
    )
    return duckdb.sql(f"""
        SELECT *,
            CASE
                WHEN {cleaned} IN ('', '{placeholders}') THEN NULL
                ELSE {cleaned}
            END as {out_col}
        FROM relation
    """)


def _clean_dose_unit_names_duckdb(
    relation: duckdb.DuckDBPyRelation,
    col: str = '_clean_unit'
) -> duckdb.DuckDBPyRelation:
    """Clean dose unit name variants in DuckDB.

    Applies regex patterns to convert various unit name variants to their
    standard abbreviated forms.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
        Input relation containing the column to clean (or any table DuckDB can
        scan by name).
    col : str, default '_clean_unit'
        Name of the column containing dose unit strings.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Relation with the column replaced by cleaned values.

    Examples
    --------
    >>> import polars as pl
    >>> df = pl.DataFrame({'_clean_unit': ['milliliter/hour', 'units/minute', 'µg/kg/h']})
    >>> _clean_dose_unit_names_duckdb(df).pl()['_clean_unit'].to_list()
    ['ml/hr', 'u/min', 'mcg/kg/hr']
    """
    # Build nested regexp_replace calls for all patterns
    expr = col
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        expr = f"regexp_replace({expr}, '{pattern}', '{repl}', 'g')"

    return duckdb.sql(f"""
        SELECT * EXCLUDE ({col}), {expr} as {col}
        FROM relation
    """)
