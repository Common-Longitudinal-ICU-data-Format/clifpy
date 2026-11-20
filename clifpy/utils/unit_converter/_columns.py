"""Boundary renaming, so the converter can run on arbitrary column names.

The pipeline is written against fixed internal names (`med_category`,
`med_dose`, `med_dose_unit`, `admin_dttm`). Rather than thread caller-supplied
names through every SQL string -- dozens of interpolation sites, every one an
injection surface -- the caller's columns are projected onto the internal names
on the way in and renamed back on the way out.

Two consequences worth stating plainly:

* Every internal SQL string keeps its literal column names, so it has no
  injection surface at all. Caller-supplied identifiers reach SQL in exactly
  two projections, both built here, both validated and quoted.
* Converting a different column pair -- `volume_infusion_rate` /
  `volume_infusion_rate_unit`, say -- needs no change to the pipeline.

Output names follow the pair being converted (:func:`resolve_output_names`).
The converted columns default to `<dose_col>_converted` and
`<unit_col>_converted`, and every QA or intermediate column keeps its bare
name for the default `med_dose` pair and ends in `__<dose_col>` for any other
pair. An input column named like a column the pipeline writes, typically an
earlier pass's output, is parked under `_saved__<name>` for the run and
returned unchanged. So two pairs converted one after the other on the same
frame keep both results.

DuckDB matches column names case-insensitively and silently suffixes a
duplicate (`_convert_status_1`), so a name clash never errors on its own: the
requested name just holds the wrong values. :func:`resolve_output_names`
refuses, before the pipeline runs, every clash that parking cannot resolve.
"""

import re
from typing import Dict, Iterable, NamedTuple, Optional

import duckdb

from clifpy.utils.logging_config import get_logger

logger = get_logger('utils.unit_converter')

# Canonical names the pipeline's SQL is written against.
INTERNAL_CATEGORY_COL = 'med_category'
INTERNAL_DOSE_COL = 'med_dose'
INTERNAL_UNIT_COL = 'med_dose_unit'
INTERNAL_TIME_COL = 'admin_dttm'

# Prefix used to park a caller column that collides with an internal name.
_STASH_PREFIX = '_saved__'

# A SQL identifier we are willing to emit. Deliberately strict: unquoted-safe
# ASCII only. Anything else is refused rather than escaped, because a column
# name needing escaping is far more likely to be an injection attempt than a
# real column.
_SAFE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

# Names stage 2 writes its result under, before rename_from_internal applies
# the caller's converted_dose_col / converted_unit_col.
INTERNAL_CONVERTED_DOSE_COL = 'med_dose_converted'
INTERNAL_CONVERTED_UNIT_COL = 'med_dose_unit_converted'

# The columns a weight lookup adds (attach_weight's output). Every column pair
# shares them, so they never take a pair suffix. A present `weight_kg` is read
# as a pre-attached weight; the other three, next to it, are the caller's and
# are parked like any other column the pipeline writes.
WEIGHT_OUTPUT_COLUMNS = (
    'weight_kg', '_weight_source', '_weight_recorded_dttm', '_weight_age_hours',
)

# The QA columns the default (lean) output keeps, under their default-pair
# names. `_clean_unit` and `_unit_class` are QA: show_intermediate=True and the
# counts table carry them (D-261119-lean-output).
DEFAULT_QA_COLUMNS = ('_convert_status', '_convert_status_details')

# Every QA or intermediate column the pipeline writes for one column pair, by
# its internal (default-pair) name: the default output's two, plus the
# intermediates show_intermediate=True keeps. For a pair other than med_dose
# each is renamed to pair_column_name(name, dose_col). Listed here rather than
# derived from PIPELINE_OWNED_COLUMNS, which also holds the shared weight
# columns and hidden columns that must never be renamed.
# NOTE: a step that writes a new per-pair column onto the med rows must add it
# here; test_custom_pair_columns_are_shared_or_suffixed enforces that.
PAIR_QA_COLUMNS: frozenset[str] = frozenset({
    *DEFAULT_QA_COLUMNS,
    # stage 1 (_clean, _base)
    '_clean_unit', '_unit_class',
    '_base_dose', '_base_unit',
    '_amount_multiplier', '_time_multiplier', '_weight_multiplier',
    # preferred-unit join (_convert)
    '_preferred_unit', '_preferred_unit_clean', '_preferred_is_explicit',
    '_preferred_is_weight_variant',
    # stage 2 (_preferred)
    '_unit_subclass', '_unit_class_preferred', '_unit_subclass_preferred',
    '_base_wt', '_pref_wt', '_needs_wt',
    '_amount_multiplier_preferred', '_time_multiplier_preferred',
    '_weight_multiplier_preferred',
    # stage 2's split of `_convert_status` (_preferred.CONVERT_STATUS_DETAILS)
    '_convert_status_details',
})

# Every column the pipeline writes onto the med rows, lowercase. An input
# column with one of these names would be read, or returned, in place of the
# value this call computes, so resolve_output_names parks it or refuses it.
# A name here that is in neither PAIR_QA_COLUMNS nor WEIGHT_OUTPUT_COLUMNS is
# hidden: it never reaches an output, and an input column of that name is
# refused rather than parked.
# NOTE: a step that writes a new column onto the med rows must add it here or
# to PAIR_QA_COLUMNS; test_pipeline_owned_columns_cover_every_written_column
# enforces that.
PIPELINE_OWNED_COLUMNS: frozenset[str] = frozenset({
    *PAIR_QA_COLUMNS,
    # weight lookup (_weight, _convert); shared by every pair
    *WEIGHT_OUTPUT_COLUMNS,
    # stage 2's result, before rename_from_internal names it
    INTERNAL_CONVERTED_DOSE_COL, INTERNAL_CONVERTED_UNIT_COL,
    # input row order (_convert.INPUT_ORDINAL_COL); dropped before returning
    '_input_row_ord',
})


def validate_column_name(name: str, param: str) -> str:
    """Reject any column name that is not a plain SQL identifier.

    Parameters
    ----------
    name : str
        Caller-supplied column name.
    param : str
        Parameter name, for the error message.

    Returns
    -------
    str
        The name, unchanged, if it is safe.

    Raises
    ------
    ValueError
        If the name is not a plain identifier.

    Examples
    --------
    >>> validate_column_name('volume_infusion_rate', 'dose_col')
    'volume_infusion_rate'
    >>> validate_column_name('x"; DROP TABLE y --', 'dose_col')
    Traceback (most recent call last):
        ...
    ValueError: dose_col must be a plain SQL identifier ...
    """
    if not isinstance(name, str) or not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"{param} must be a plain SQL identifier matching "
            f"[A-Za-z_][A-Za-z0-9_]*; got {name!r}"
        )
    return name


def quote_identifier(name: str) -> str:
    """Quote an identifier for embedding in SQL, doubling embedded quotes.

    Validated names never contain a double quote, but the rename projections
    quote every input column, and those names are the caller's: `note "a"`
    must come back as itself rather than break the SQL.
    """
    return '"' + name.replace('"', '""') + '"'


def build_rename_map(
    category_col: str,
    dose_col: str,
    unit_col: str,
    time_col: str,
) -> Dict[str, str]:
    """Map caller column names to the pipeline's internal names.

    Only entries that actually differ are returned, so the default call is a
    no-op and the rename projections are skipped entirely.
    """
    pairs = [
        ('category_col', category_col, INTERNAL_CATEGORY_COL),
        ('dose_col', dose_col, INTERNAL_DOSE_COL),
        ('unit_col', unit_col, INTERNAL_UNIT_COL),
        ('time_col', time_col, INTERNAL_TIME_COL),
    ]
    mapping = {}
    for param, given, internal in pairs:
        validate_column_name(given, param)
        if given != internal:
            mapping[given] = internal
    return mapping


def pair_column_name(name: str, dose_col: str) -> str:
    """Public name of the per-pair column `name` when `dose_col` is converted.

    The default pair (`dose_col` is `med_dose`, in any letter case) keeps the
    bare name, so its output does not change; any other pair appends
    `__<dose_col>`. It applies to :data:`PAIR_QA_COLUMNS`; the weight columns
    are shared and the converted pair is named by `converted_dose_col` /
    `converted_unit_col`.

    Examples
    --------
    >>> pair_column_name('_convert_status', 'med_dose')
    '_convert_status'
    >>> pair_column_name('_convert_status', 'volume_infusion_rate')
    '_convert_status__volume_infusion_rate'
    """
    if dose_col.lower() == INTERNAL_DOSE_COL:
        return name
    return f'{name}__{dose_col}'


class OutputNames(NamedTuple):
    """What :func:`resolve_output_names` settled for one conversion."""

    converted_dose_col: str
    converted_unit_col: str
    # internal name -> public name, for every name that changes
    output_map: Dict[str, str]
    # caller column -> the name the run sees it under: `_saved__<name>` for a
    # column named like one the pipeline writes, or `weight_kg` for a
    # pre-attached weight spelled in another letter case (`WEIGHT_KG`)
    park: Dict[str, str]
    # an output name equals its own source column: an in-place overwrite
    in_place: bool


def resolve_output_names(
    input_columns: Iterable[str],
    *,
    dose_col: str,
    unit_col: str,
    converted_dose_col: Optional[str] = None,
    converted_unit_col: Optional[str] = None,
    show_intermediate: bool = False,
    category_col: str = INTERNAL_CATEGORY_COL,
    time_col: str = INTERNAL_TIME_COL,
) -> OutputNames:
    """Settle the output names of one conversion, refusing every clash.

    Every comparison is case-insensitive, because DuckDB's are.

    Parameters
    ----------
    input_columns : iterable of str
        Columns of the caller's med table, before any renaming.
    dose_col, unit_col : str
        The source columns being converted.
    converted_dose_col, converted_unit_col : str, optional
        The requested output names. None means `<dose_col>_converted` and
        `<unit_col>_converted`.
    show_intermediate : bool, default False
        Whether the call returns every intermediate column, which makes their
        names output names too.
    category_col, time_col : str, optional
        The other two input columns the run renames onto its internal names,
        `med_category` and `admin_dttm` by default. These four source columns
        are never parked, read as weight provenance or refused as reserved,
        whatever they are called: the run sees them under internal names. An
        output name of this call still clashes with them.

    Returns
    -------
    OutputNames
        - `converted_dose_col`, `converted_unit_col`: the output names.
        - `output_map`: internal name -> public name for every name that
          changes: the converted pair, and for a pair other than `med_dose`
          every column of :data:`PAIR_QA_COLUMNS` (:func:`pair_column_name`).
        - `park`: caller column -> the name the run sees it under. Every input
          column named like a column the pipeline writes is mapped to
          `_saved__<name>`, except the four source columns, which the run
          renames instead. `weight_kg` is not parked: a present one is read
          as a pre-attached weight. Spelled in another letter case
          (`WEIGHT_KG`), it is mapped to `weight_kg`, so the run reads it and
          the caller's spelling comes back at the end.
        - `in_place`: True when an output name equals its own source column
          (`converted_dose_col == dose_col` and/or
          `converted_unit_col == unit_col`), an explicit request to overwrite
          that column.

    Raises
    ------
    ValueError
        One error naming every clash: an output name of this call that an
        input column already has, other than its own source column (the same
        pair converted twice, typically); the two output names being equal; an
        output name equal to a column the converter writes, other than its own
        default; an input column starting with the parking prefix `_saved__`;
        `_weight_*` provenance columns without the `weight_kg` they describe;
        and an input column named like a hidden pipeline column.

    Examples
    --------
    >>> names = resolve_output_names(
    ...     ['med_dose', 'med_dose_unit', 'med_dose_converted',
    ...      'volume_infusion_rate', 'volume_infusion_rate_unit'],
    ...     dose_col='volume_infusion_rate', unit_col='volume_infusion_rate_unit')
    >>> names.converted_dose_col, names.output_map['_convert_status']
    ('volume_infusion_rate_converted', '_convert_status__volume_infusion_rate')
    >>> names.park
    {'med_dose_converted': '_saved__med_dose_converted'}
    """
    columns = [str(c) for c in input_columns]
    by_lower = {c.lower(): c for c in columns}
    # The four columns the run renames onto its internal names. One named
    # like a converter column is never parked, read as weight provenance or
    # refused as reserved: the run sees it under its internal name. An
    # output name of this call still clashes with it (`taken` below).
    sources = {
        name.lower() for name in (category_col, dose_col, unit_col, time_col)
    }
    dose_out = (
        f'{dose_col}_converted' if converted_dose_col is None else converted_dose_col
    )
    unit_out = (
        f'{unit_col}_converted' if converted_unit_col is None else converted_unit_col
    )
    validate_column_name(dose_out, 'converted_dose_col')
    validate_column_name(unit_out, 'converted_unit_col')

    output_map = {
        internal: public
        for internal, public in (
            (INTERNAL_CONVERTED_DOSE_COL, dose_out),
            (INTERNAL_CONVERTED_UNIT_COL, unit_out),
            *((c, pair_column_name(c, dose_col)) for c in sorted(PAIR_QA_COLUMNS)),
        )
        if public != internal
    }
    pair_names = {pair_column_name(c, dose_col).lower() for c in PAIR_QA_COLUMNS}

    problems = []
    in_place = False
    # Lowercase names this call writes, in-place overwrites aside.
    written = {
        pair_column_name(c, dose_col).lower()
        for c in (PAIR_QA_COLUMNS if show_intermediate else DEFAULT_QA_COLUMNS)
    }
    for param, out_col, source_col, default in (
        ('converted_dose_col', dose_out, dose_col, INTERNAL_CONVERTED_DOSE_COL),
        ('converted_unit_col', unit_out, unit_col, INTERNAL_CONVERTED_UNIT_COL),
    ):
        key = out_col.lower()
        if key == source_col.lower():
            in_place = True
        elif key != default and (key in PIPELINE_OWNED_COLUMNS or key in pair_names):
            problems.append(
                f"{param}={out_col!r} is a column the converter writes itself"
            )
        else:
            written.add(key)
    if dose_out.lower() == unit_out.lower():
        problems.append(
            f"converted_dose_col={dose_out!r} and "
            f"converted_unit_col={unit_out!r} name the same column"
        )
    # With no pre-attached weight the call looks one up, and show_intermediate
    # returns the lookup's `_weight_source`, `_weight_age_hours` and
    # `_weight_recorded_dttm`. A source column of one of those names would
    # clash with them; any other input column of those names is stale
    # provenance, refused below. (The lean output drops the converter's own
    # `_weight_source` / `_weight_age_hours` instead, so the source column
    # comes back.)
    if show_intermediate and 'weight_kg' not in by_lower:
        written.update(
            sources & {'_weight_source', '_weight_age_hours', '_weight_recorded_dttm'})
    taken = sorted(by_lower[key] for key in written if key in by_lower)
    if taken:
        problems.append(
            f"med_df already has column(s) {taken} that this call writes, most "
            f"likely because this column pair was converted before; convert "
            f"the original table instead, or drop or rename these columns "
            f"first. Only an output name equal to its own source column "
            f"overwrites in place"
        )

    prefixed = sorted(c for c in columns if c.lower().startswith(_STASH_PREFIX))
    if prefixed:
        problems.append(
            f"input column(s) {prefixed} start with {_STASH_PREFIX!r}, the "
            f"prefix the converter parks columns under; rename them"
        )
    provenance = [c for c in WEIGHT_OUTPUT_COLUMNS if c != 'weight_kg']
    stale_weight = sorted(
        by_lower[c] for c in provenance if c in by_lower and c not in sources
    )
    if stale_weight and 'weight_kg' not in by_lower:
        problems.append(
            f"med_df has weight provenance column(s) {stale_weight} but no "
            f"weight_kg; they would describe a weight this call looks up "
            f"again. Drop them, or keep the weight_kg they belong to"
        )

    # The columns a call may park for the run: every pair's QA and converted
    # columns, and the weight provenance. weight_kg is not one of them: a
    # present one is the pre-attached weight. Any other pipeline column is
    # hidden (dropped before the converter returns), so an input column of
    # that name is refused instead.
    parkable = (
        PAIR_QA_COLUMNS
        | {INTERNAL_CONVERTED_DOSE_COL, INTERNAL_CONVERTED_UNIT_COL}
        | set(provenance)
    )
    hidden = sorted(
        c for c in columns
        if c.lower() in PIPELINE_OWNED_COLUMNS - parkable - {'weight_kg'}
        and c.lower() not in sources
    )
    if hidden:
        problems.append(
            f"input column(s) {hidden} use a name the converter reserves for a "
            f"column of its own; rename them"
        )

    if problems:
        raise ValueError("Column-name collision: " + "; ".join(problems))

    park = {
        c: _STASH_PREFIX + c
        for c in columns
        if c.lower() in parkable and c.lower() not in sources
    }
    # A pre-attached weight spelled in another letter case (`WEIGHT_KG`) runs
    # as `weight_kg`, so the converter reads it instead of looking a weight
    # up again; rename_from_internal gives the caller's spelling back.
    spellings = [c for c in columns if c.lower() == 'weight_kg']
    if (len(spellings) == 1 and spellings[0] != 'weight_kg'
            and 'weight_kg' not in sources):
        park[spellings[0]] = 'weight_kg'
    return OutputNames(dose_out, unit_out, output_map, park, in_place)


def rename_to_internal(
    rel: duckdb.DuckDBPyRelation,
    rename_map: Dict[str, str],
) -> duckdb.DuckDBPyRelation:
    """Project caller columns onto internal names.

    A caller column that collides with an internal name it is not itself
    mapped to is parked under `_saved__<name>` rather than dropped, and
    restored by :func:`rename_from_internal`. Converting
    `volume_infusion_rate` on a frame that also carries a real `med_dose`
    column must not lose the latter.
    """
    if not rename_map:
        return rel

    present = set(rel.columns)
    missing = [src for src in rename_map if src not in present]
    if missing:
        raise ValueError(
            f"column(s) not found in the input: {sorted(missing)}. "
            f"Available: {sorted(present)}"
        )

    targets = {internal.lower() for internal in rename_map.values()}
    # Columns that would be clobbered: they already carry an internal name but
    # are not the source we are mapping onto it. DuckDB matches names
    # case-insensitively, so `MED_DOSE` clobbers `med_dose` just the same.
    to_stash = [
        c for c in present if c.lower() in targets and c not in rename_map
    ]

    projections = []
    for col in rel.columns:
        q = quote_identifier(col)
        if col in rename_map:
            projections.append(f'{q} AS {quote_identifier(rename_map[col])}')
        elif col in to_stash:
            projections.append(f'{q} AS {quote_identifier(_STASH_PREFIX + col)}')
        else:
            projections.append(q)

    if to_stash:
        logger.debug(
            "Parking column(s) %s during conversion to avoid a name collision",
            sorted(to_stash),
        )
    return duckdb.sql(f"SELECT {', '.join(projections)} FROM rel")


def rename_from_internal(
    rel: duckdb.DuckDBPyRelation,
    rename_map: Dict[str, str],
    output_map: Dict[str, str],
) -> duckdb.DuckDBPyRelation:
    """Restore caller column names and apply the output column names.

    Inverts :func:`rename_to_internal`, un-parking every parked column, and
    renames each pipeline column in `output_map` to its public name: the
    converted pair to `converted_dose_col` / `converted_unit_col` and, for a
    pair other than `med_dose`, every QA column to its `__<dose_col>` name.
    `output_map` comes from :func:`resolve_output_names`.
    """
    inverse = {internal: given for given, internal in rename_map.items()}

    if not inverse and not output_map:
        return rel

    # An output name equal to its own source column (`converted_dose_col ==
    # dose_col`, in any letter case) is an explicit in-place overwrite: the
    # source column is dropped and the converted values take its name.
    # resolve_output_names refused every other overlap before the pipeline ran.
    present = set(rel.columns)
    overwritten = {
        public.lower() for internal, public in output_map.items()
        if internal in present
    }
    projections = []
    for col in rel.columns:
        if col in output_map:
            final = output_map[col]
        elif col in inverse:
            final = inverse[col]
        elif col.startswith(_STASH_PREFIX):
            final = col[len(_STASH_PREFIX):]
        else:
            final = col
        if col not in output_map and final.lower() in overwritten:
            continue  # the source column an in-place overwrite replaces
        q = quote_identifier(col)
        projections.append(q if final == col else f'{q} AS {quote_identifier(final)}')
    return duckdb.sql(f"SELECT {', '.join(projections)} FROM rel")
