"""The per-(category, unit) conversion summary table.

Imported by both stage 1 and the orchestrator, so it lives on its own to keep
the package's import graph acyclic.
"""

from __future__ import annotations

import duckdb
from typing import Dict, List, Mapping, Optional

from clifpy.utils.logging_config import get_logger

from ._columns import pair_column_name, quote_identifier, validate_column_name

logger = get_logger('utils.unit_converter')

# The frequency column every counts table ends with.
COUNT_COL = 'count'

# The converter's counts-table QA columns, by internal name. Each is shown
# under its per-pair name (pair_column_name), the name the converted frame
# gives it.
_COUNTS_QA_COLUMNS = (
    '_clean_unit', '_base_unit', '_unit_class', '_preferred_unit', '_convert_status',
    '_convert_status_details',
)


def counts_output_names(
    *,
    category_col: str,
    dose_col: str,
    unit_col: str,
    converted_unit_col: str,
) -> Dict[str, str]:
    """Map the converter's counts-table columns to the names the caller sees.

    The counts table is computed on the pipeline's internal names and shown
    under the names the converted frame carries, so it joins back onto that
    frame without a rename:

    - `med_category`, `med_dose_unit` and `med_dose_unit_converted` under the
      caller's `category_col`, `unit_col` and `converted_unit_col`.
    - The QA columns `_clean_unit`, `_base_unit`, `_unit_class`,
      `_preferred_unit`, `_convert_status` and `_convert_status_details`
      under their per-pair names: bare when `dose_col` is `med_dose`,
      `<name>__<dose_col>` for any other pair
      (:func:`~clifpy.utils.unit_converter._columns.pair_column_name`).
    - `_weight_source`, the row's weight provenance, bare for every pair:
      the weight is shared by every column pair.

    With the default names the mapping is the identity.

    An in-place overwrite (`converted_unit_col` equal to `unit_col`, compared
    case-insensitively as DuckDB compares identifiers) would give the source
    unit and the converted unit one name, so the converted unit is shown as
    `<unit_col>_converted` instead.

    Parameters
    ----------
    category_col, dose_col, unit_col, converted_unit_col : str
        The converter's column-name arguments, `converted_unit_col` as
        :func:`~clifpy.utils.unit_converter._columns.resolve_output_names`
        resolved it (never None).

    Returns
    -------
    dict
        `{internal_name: shown_name}` for the ten caller-facing columns.

    Raises
    ------
    ValueError
        If a name is not a plain SQL identifier, or two shown names (the
        trailing `count` column included) collide case-insensitively.

    Examples
    --------
    >>> names = counts_output_names(category_col='drug', dose_col='rate',
    ...                             unit_col='rate_unit', converted_unit_col='rate_unit')
    >>> names['med_dose_unit_converted'], names['_convert_status']
    ('rate_unit_converted', '_convert_status__rate')
    """
    validate_column_name(category_col, 'category_col')
    validate_column_name(dose_col, 'dose_col')
    validate_column_name(unit_col, 'unit_col')
    validate_column_name(converted_unit_col, 'converted_unit_col')
    shown_converted = (
        f'{unit_col}_converted'
        if converted_unit_col.lower() == unit_col.lower()
        else converted_unit_col
    )
    names = {
        'med_category': category_col,
        'med_dose_unit': unit_col,
        **{qa: pair_column_name(qa, dose_col) for qa in _COUNTS_QA_COLUMNS},
        'med_dose_unit_converted': shown_converted,
        # shared by every pair (WEIGHT_OUTPUT_COLUMNS), so shown bare
        '_weight_source': '_weight_source',
    }
    shown = [*names.values(), COUNT_COL]
    lowered = [name.lower() for name in shown]
    clashes = sorted({name for name in shown if lowered.count(name.lower()) > 1})
    if clashes:
        raise ValueError(
            f"the counts table cannot show these column names, which collide "
            f"case-insensitively: {clashes}. It reports category_col, unit_col, "
            f"converted_unit_col, the pair's QA columns and '{COUNT_COL}' side "
            f"by side; pass distinct names."
        )
    return names


def _create_unit_conversion_counts_table(
    med_df: duckdb.DuckDBPyRelation,
    group_by: List[str],
    output_names: Optional[Mapping[str, str]] = None,
    count_after: Optional[str] = None,
    ) -> duckdb.DuckDBPyRelation:
    """Create summary table of unit conversion counts.

    Generates a grouped summary showing the frequency of each unit conversion
    pattern, useful for data quality assessment and identifying common or
    problematic unit patterns.

    Parameters
    ----------
    med_df : duckdb.DuckDBPyRelation
        Converted data with required columns from conversion process:

        - med_dose_unit: Original unit string
        - _clean_unit: Cleaned unit string
        - _base_unit: base standard unit
        - _unit_class: Classification (rate/amount/countable/unrecognized)
    group_by : List[str]
        List of columns to group by.
    output_names : mapping, optional
        `{group_by column: name shown in the result}`, e.g. from
        :func:`counts_output_names`. Columns it omits keep their own name.
    count_after : str, optional
        A `group_by` column the `count` column follows, so the columns after
        it (QA detail) trail the count; by default `count` comes last.

    Returns
    -------
    duckdb.DuckDBPyRelation
        Lazy summary relation: one row per distinct combination of the
        `group_by` columns (named as `output_names` maps them), plus

        - count: Number of occurrences

        Sorted by `med_category` when it is grouped on, then by descending
        count.

    Raises
    ------
    ValueError
        If required columns are missing from input DataFrame.

    Examples
    --------
    >>> # df_base = standardize_dose_to_base_units(med_df, return_format='duckdb')[0]
    >>> # counts = _create_unit_conversion_counts_table(df_base, ['med_dose_unit'])
    >>> # 'count' in counts.columns
    True

    Notes
    -----
    This table is particularly useful for:

    - Identifying unrecognized units that need handling
    - Understanding the distribution of unit types in your data
    - Quality control and validation of conversions
    """
    # check presense of all the group by columns
    # required_columns = {'med_dose_unit', 'med_dose_unit_normalized', 'med_dose_unit_limited', 'unit_class'}
    missing_columns = set(group_by) - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")

    output_names = dict(output_names or {})
    count_pos = (
        group_by.index(count_after) + 2 if count_after is not None else len(group_by) + 1
    )
    items = [
        f'{quote_identifier(col)} AS {quote_identifier(output_names.get(col, col))}'
        for col in group_by
    ]
    items.insert(count_pos - 1, f'COUNT(*) AS {COUNT_COL}')
    select_list = ', '.join(items)
    group_list = ', '.join(quote_identifier(col) for col in group_by)
    # ORDER BY position, not name: a shown name can equal another column's
    # internal name (unit_col='med_category', say), and a named ORDER BY then
    # binds to that alias -- the wrong column. `med_category` precedes the
    # count whenever it is grouped on, so its position is its group_by index.
    if 'med_category' in group_by:
        order_by_clause = f"{group_by.index('med_category') + 1}, {count_pos} DESC"
    else:
        order_by_clause = f"{count_pos} DESC"

    q = f"""
    SELECT {select_list}
    FROM med_df
    GROUP BY {group_list}
    ORDER BY {order_by_clause}
    """
    return duckdb.sql(q)
