"""Reading per-category target dose units from an external targets file.

CLIF's mCIDE files define the target unit for each `med_category`. Those files
are **always supplied externally and parsed** -- never vendored into clifpy --
because they change independently of the library and a stale bundled copy would
silently standardise data to the wrong units.

The format is therefore expected to change too. Readers are registered per file
extension, so supporting a new one is a small function plus a
:func:`register_target_reader` call, with nothing else in the package touched:

    >>> def _read_targets_toml(source):          # doctest: +SKIP
    ...     import tomllib
    ...     with open(source, 'rb') as fh:
    ...         data = tomllib.load(fh)
    ...     return pl.DataFrame(data['targets'])
    >>> register_target_reader('.toml', _read_targets_toml)   # doctest: +SKIP

Every reader returns a polars DataFrame (a pandas one is still accepted and
read through the input boundary); validation, filtering and dict-building are
done once in :func:`load_dose_unit_targets`, so a reader never repeats them.
The reader is picked by file extension, ignoring a URL's query string or
fragment and one trailing compression suffix, so `targets.csv.gz` and
`https://.../targets.csv?token=...` both reach the CSV reader.

The module also holds the two QA warnings of
:func:`~clifpy.utils.unit_converter.standardize_med_dose_units`
(:func:`_warn_about_targets`: unlisted categories, and targets whose unit class
does not fit the data), since both are about how a file's targets met the
data. Each row's own outcome is its `_convert_status_details`, computed in
stage 2.
"""

import importlib
import io
import json
from pathlib import Path, PurePosixPath
from typing import Callable, Dict, Optional, Union
from urllib.parse import urlparse

import polars as pl

from clifpy.utils.logging_config import get_logger
from ._clean import _clean_dose_unit_formats_duckdb, _clean_dose_unit_names_duckdb
from ._columns import (
    INTERNAL_CATEGORY_COL, INTERNAL_DOSE_COL, pair_column_name,
)
from ._inputs import FrameLike, check_no_shadowing_tables, to_arrow

logger = get_logger('utils.unit_converter')

TargetReader = Callable[[Union[str, Path]], FrameLike]

# Values that mean "no target defined for this category". The CLIF 3.0
# continuous file uses the literal string 'NA' in volume_infusion_rate_units
# for 5 of its 77 rows. 'pending' is what every one of those cells held
# between CLIF bd26c7fa (2026-07-28) and 454f7e91 (2026-07-30), so a copy
# taken in those two days must read as "no target", not as a unit.
_NA_TOKENS = {'', 'na', 'n/a', 'nan', 'none', 'null', 'pending'}


def _is_na_token(value) -> bool:
    """True when `value` means "no target defined".

    The one rule shared by :func:`load_dose_unit_targets` (schema rows) and
    the dict branch of `standardize_med_dose_units`: None, a float NaN, the
    pandas `NA` scalar, or a string that strips and lowercases into
    `_NA_TOKENS`.
    """
    if value is None:
        return True
    if isinstance(value, float) and value != value:  # NaN is the only float != itself
        return True
    # pandas' NA scalar, which a dict built from a nullable pandas column can
    # hold. Matched by type name because this module never imports pandas
    # (D-261022-pandas-free-backend), and str() of it is '<NA>', not a token.
    if type(value).__name__ == 'NAType':
        return True
    return str(value).strip().lower() in _NA_TOKENS


# Compression suffixes skipped when picking a reader, each with the stdlib
# module that decompresses it. Every built-in reader reads its bytes through
# _read_bytes, which applies it; Parquet does so only for a compressed source
# (a plain .parquet is read natively by polars).
_DECOMPRESSOR_BY_SUFFIX = {'.gz': 'gzip', '.bz2': 'bz2', '.xz': 'lzma'}


def _source_suffixes(source) -> list:
    """Lower-cased file suffixes of a schema path or URL.

    For a URL (`scheme://...`) only its path counts, so a query string
    (`?token=...`) or a fragment (`#L1`) never reaches the suffix. A local
    path is taken verbatim, because `?` and `#` are legal in file names.
    """
    s = str(source)
    path = PurePosixPath(urlparse(s).path) if '://' in s else Path(s)
    return [suffix.lower() for suffix in path.suffixes]


def _compression(source) -> Optional[str]:
    """The stdlib module that decompresses `source` (`'gzip'`, `'bz2'`, `'lzma'`), or None."""
    suffixes = _source_suffixes(source)
    return _DECOMPRESSOR_BY_SUFFIX.get(suffixes[-1]) if suffixes else None


def _schema_format(source) -> str:
    """Registry key for `source`: its last suffix, skipping one compression suffix.

    `targets.csv.gz` -> `'.csv'`; `clif_3.0_targets.json` -> `'.json'`;
    `https://host/targets.csv?token=x` -> `'.csv'`; no suffix -> `''`.
    """
    suffixes = _source_suffixes(source)
    if len(suffixes) >= 2 and suffixes[-1] in _DECOMPRESSOR_BY_SUFFIX:
        return suffixes[-2]
    return suffixes[-1] if suffixes else ''


def _read_bytes(source) -> bytes:
    """Read a local path or an http(s) URL as bytes, decompressed by its suffix.

    The suffix comes from :func:`_compression`, which reads only a URL's
    path, so `https://.../targets.csv.gz?token=...` is decompressed too.
    """
    s = str(source)
    if s.lower().startswith(('http://', 'https://')):
        from urllib.request import urlopen

        with urlopen(s) as fh:  # noqa: S310 - caller-supplied schema location
            raw = fh.read()
    else:
        # expanduser: polars expanded '~' while it was handed the CSV path.
        raw = Path(s).expanduser().read_bytes()
    module = _compression(s)
    if module is not None:
        raw = importlib.import_module(module).decompress(raw)
    return raw


def _read_targets_csv(source) -> pl.DataFrame:
    """Read a CSV schema, every column as text: a local path or an http(s) URL.

    The bytes come from :func:`_read_bytes`, which fetches a URL and
    decompresses by suffix; polars on its own would see neither a `.gz`
    behind a URL's query string nor a `.bz2` / `.xz` file. An empty field
    stays '' and 'NA' stays 'NA': `load_dose_unit_targets` drops both itself.
    """
    return pl.read_csv(
        _read_bytes(source), infer_schema=False, missing_utf8_is_empty_string=True,
    )


def _read_targets_yaml(source) -> pl.DataFrame:
    """Read a YAML schema.

    Accepts a list of records or a mapping of category -> unit, optionally
    under a top-level `targets` key; see :func:`_records_to_frame`.
    """
    import yaml

    text = _read_text(source)
    data = yaml.safe_load(text)
    return _records_to_frame(data)


def _read_targets_json(source) -> pl.DataFrame:
    """Read a JSON schema (list of records, or a category -> unit mapping)."""
    return _records_to_frame(json.loads(_read_text(source)))


def _read_targets_parquet(source) -> pl.DataFrame:
    """Read a Parquet schema.

    A source with a compression suffix (``.parquet.gz``) goes through
    :func:`_read_bytes`, which decompresses it. A plain ``.parquet`` keeps
    polars' native read, so ``~`` paths and cloud URIs (``s3://``) still work.
    """
    if _compression(source) is not None:
        return pl.read_parquet(io.BytesIO(_read_bytes(source)))
    return pl.read_parquet(source)


def _read_text(source) -> str:
    """Read a local path or an http(s) URL as UTF-8 text (a BOM is dropped).

    Fetched and decompressed by :func:`_read_bytes`.
    """
    return _read_bytes(source).decode('utf-8-sig')


# Top-level key a YAML/JSON schema may wrap its targets in. Sibling keys
# (`version`, `source`, ...) are metadata and are ignored.
_WRAPPER_KEY = 'targets'

# The two columns of the frame a mapping-form document produces. They name no
# schema column, so load_dose_unit_targets recognises the mapping form by them
# and renames them to the caller's category_col / unit_col. A polars frame has
# no attrs to carry a flag, so the shape travels in the column names.
_MAPPING_CATEGORY_COL = '_mapping_category'
_MAPPING_UNIT_COL = '_mapping_unit'


def _records_to_frame(data) -> pl.DataFrame:
    """Normalise a parsed YAML/JSON document into a DataFrame.

    Accepted shapes:

    - a list of records (`[{med_category: ..., med_dose_unit: ...}, ...]`),
      the shape a table export produces;
    - a mapping of category -> unit, the natural hand-written shape. A list
      value (`epoprostenol: [ng/kg/min, mcg/kg/min]`) becomes one row per
      unit, so it meets the same first-in-file-order conflict rule as
      duplicate CSV rows;
    - either of the above under a top-level `targets` key, beside metadata
      keys such as `version`.

    The mapping form names no columns, so it comes back as two text columns,
    `_MAPPING_CATEGORY_COL` and `_MAPPING_UNIT_COL`;
    :func:`load_dose_unit_targets` renames them to the `category_col` /
    `unit_col` the caller asked for.

    Raises
    ------
    ValueError
        If the document is neither shape, or a mapping value is itself a
        mapping (e.g. `{norepinephrine: {unit: mcg/kg/min}}`) -- read
        literally that would invent a category named `unit`.
    """
    if isinstance(data, dict) and _WRAPPER_KEY in data:
        data = data[_WRAPPER_KEY]
    if isinstance(data, list):
        return pl.DataFrame(data)
    if not isinstance(data, dict):
        raise ValueError(
            "a YAML/JSON targets file must be a list of records or a mapping of "
            f"category -> unit, optionally under a '{_WRAPPER_KEY}' key; got "
            f"{type(data).__name__}"
        )
    categories, units = [], []
    for category, value in data.items():
        for unit in (value if isinstance(value, list) else [value]):
            if isinstance(unit, (dict, list)):
                raise ValueError(
                    f"targets value for {category!r} is a {type(unit).__name__}; a "
                    "mapping-form targets file maps each category to a unit string or a "
                    "list of unit strings. Use a list of records instead (a "
                    "polars frame's write_json() writes one)."
                )
            categories.append(str(category))
            units.append(str(unit))
    return pl.DataFrame(
        {_MAPPING_CATEGORY_COL: categories, _MAPPING_UNIT_COL: units},
        schema={_MAPPING_CATEGORY_COL: pl.String, _MAPPING_UNIT_COL: pl.String},
    )


_TARGET_READERS: Dict[str, TargetReader] = {
    '.csv': _read_targets_csv,
    '.yaml': _read_targets_yaml,
    '.yml': _read_targets_yaml,
    '.json': _read_targets_json,
    '.parquet': _read_targets_parquet,
}


def register_target_reader(suffix: str, reader: TargetReader) -> None:
    """Register a reader for a schema file extension.

    Parameters
    ----------
    suffix : str
        File extension, e.g. `'.toml'`. Matched case-insensitively; a missing
        leading dot is added. One extension only: a compressed file is read by
        its inner extension's reader, so register `'.csv'`, not `'.csv.gz'`.
    reader : TargetReader
        Callable taking the source path/URL and returning a polars (or
        pandas) DataFrame that contains the category and unit columns.

    Raises
    ------
    TypeError
        If `suffix` is not a string or `reader` is not callable.
    ValueError
        If `suffix` is empty or is not a single extension.

    Notes
    -----
    Replaces any existing reader for that suffix, so the built-ins can be
    overridden.
    """
    if not isinstance(suffix, str):
        raise TypeError(
            f"suffix must be a string such as '.toml'; got {type(suffix).__name__}"
        )
    if not callable(reader):
        raise TypeError(f"reader must be callable; got {type(reader).__name__}")
    key = suffix.strip().lower()
    if not key.startswith('.'):
        key = '.' + key
    if key == '.' or key.count('.') != 1 or any(c.isspace() or c in '/\\' for c in key):
        raise ValueError(
            f"suffix must be a single file extension such as '.toml'; got {suffix!r}"
        )
    _TARGET_READERS[key] = reader
    logger.debug("Registered dose-unit target reader for %s", key)


def available_target_formats() -> list:
    """List the schema file extensions currently registered."""
    return sorted(_TARGET_READERS)


def _normalize_units(units) -> Dict[str, object]:
    """Map each raw unit string to the converter's normalised form.

    Runs the converter's own cleaning chain in DuckDB, so two spellings of
    one unit (`units/hr`, `U/hr`) compare equal and only a real difference
    counts as a conflict. A unit that cleans to nothing maps to None.
    """
    distinct = sorted(set(units))
    if not distinct:
        return {}
    # The cleaning chain reads its input through DuckDB's replacement scan
    # (`relation`); refuse to run when a catalog table would be read instead.
    check_no_shadowing_tables()
    raw = pl.DataFrame({'unit': distinct}, schema={'unit': pl.String})
    rel = _clean_dose_unit_formats_duckdb(raw, col='unit', out_col='_norm')
    rel = _clean_dose_unit_names_duckdb(rel, col='_norm')
    return dict(rel.fetchall())


def load_dose_unit_targets(
    source: Union[str, Path],
    *,
    reader: TargetReader = None,
    category_col: str = 'med_category',
    unit_col: str = 'med_dose_unit',
) -> Dict[str, str]:
    """Parse an external schema into ``{med_category: target_unit}``.

    Parameters
    ----------
    source : str or Path
        Local path or http(s) URL of the schema file. The canonical CLIF 3.0
        files live under `mCIDE/medication_admin_{continuous,intermittent}/`
        in the Common-Longitudinal-ICU-data-Format/CLIF repository. The
        reader is chosen by extension, ignoring a URL's query string or
        fragment and a trailing `.gz`, `.bz2` or `.xz`.
    reader : callable, optional
        Override the reader. When omitted, one is chosen by file extension.
    category_col : str, default 'med_category'
        Column holding the medication category.
    unit_col : str, default 'med_dose_unit'
        Column holding the target unit. Pass e.g.
        `'volume_infusion_rate_units'` to read the volume targets from the
        same file. A mapping-form YAML/JSON file (`category: unit`) names no
        columns, so its keys are read as `category_col` and its values as
        `unit_col`.

    Returns
    -------
    dict
        Mapping of category to target unit, suitable as `preferred_units`.
        Categories are stripped and lower-cased, as CLIF categories are; the
        unit keeps the schema's spelling, which is echoed into the converted
        unit column. Rows whose category or target is blank, `NA` or
        `pending` are dropped.

    Raises
    ------
    ValueError
        If no reader is registered for the extension, the reader returns
        neither a polars nor a pandas DataFrame, a requested column is absent
        from the parsed schema, or (when the schema lists one category with
        differently spelled units) a default-connection table shadows the
        cleaning chain's input.

    Notes
    -----
    The loader expects the mCIDE layout of 2026-09-09 onward, in which
    `med_dose_unit` holds the complete target unit, weight and time axes
    included (`mcg/kg/min` for continuous norepinephrine). An older copy held
    amount-only units (`mcg`); standardizing a continuous table against it
    fails every rate row with 'cannot convert rate to amount', and
    :func:`~clifpy.utils.unit_converter.standardize_med_dose_units` warns.

    A category listed more than once with units that differ after
    normalisation (`units/hr` and `U/hr` do not) keeps its **first occurrence
    in file order**, and a warning names the unit kept. That is deterministic
    for a given file, but it follows the file's row order: reorder the schema
    and the result changes. CLIF 3.0 keys targets on
    `(med_category, med_group)`, and `epoprostenol` and `terbutaline` carry
    one target per route, so set those two explicitly for your cohort.

    Examples
    --------
    >>> targets = load_dose_unit_targets(              # doctest: +SKIP
    ...     'mCIDE/medication_admin_continuous/'
    ...     'clif_medication_admin_continuous_med_categories.csv')
    >>> targets['norepinephrine']                      # doctest: +SKIP
    'mcg/kg/min'
    """
    if reader is None:
        suffix = _schema_format(source)
        reader = _TARGET_READERS.get(suffix)
        if reader is None:
            raise ValueError(
                f"No dose-unit target reader registered for {suffix!r}. "
                f"Registered: {available_target_formats()}. "
                f"Pass reader= explicitly, or call register_target_reader()."
            )

    df = reader(source)
    if not isinstance(df, pl.DataFrame):
        # A reader may still return pandas (or a LazyFrame, or a relation): it
        # crosses the input boundary into Arrow, never through pandas code.
        try:
            df = pl.from_arrow(to_arrow(df, 'the target reader result'))
        except TypeError:
            raise ValueError(
                f"target reader must return a polars or pandas DataFrame; got "
                f"{type(df).__name__}"
            ) from None
    if df.columns == [_MAPPING_CATEGORY_COL, _MAPPING_UNIT_COL]:
        # A mapping-form document names no columns; give its two columns the
        # names the caller asked for.
        df = df.rename({_MAPPING_CATEGORY_COL: category_col, _MAPPING_UNIT_COL: unit_col})

    missing = {category_col, unit_col} - set(df.columns)
    if missing:
        raise ValueError(
            f"targets file is missing column(s) {sorted(missing)}. "
            f"Found: {sorted(df.columns)}"
        )

    rows = []
    dropped = 0
    for cat, unit in zip(df[category_col].to_list(), df[unit_col].to_list()):
        # CLIF categories are lower-case snake_case; a hand-edited schema
        # (' Norepinephrine ') must still match the data.
        cat = str(cat).strip().lower()
        unit = str(unit).strip()
        if _is_na_token(cat) or _is_na_token(unit):
            dropped += 1
            continue
        rows.append((cat, unit))

    targets = {}
    duplicates = []
    for cat, unit in rows:
        if cat not in targets:
            targets[cat] = unit
        elif unit != targets[cat]:
            duplicates.append((cat, unit))

    # A differently spelled duplicate is a conflict only when the two units
    # normalise apart: 'units/hr' and 'U/hr' are one target.
    normalized = _normalize_units(
        [unit for _, unit in duplicates] + [targets[cat] for cat, _ in duplicates]
    )
    conflicts = {}
    for cat, unit in duplicates:
        if normalized[unit] != normalized[targets[cat]]:
            # Keep the FIRST occurrence in file order and surface the
            # conflict rather than resolving it silently. File order is
            # deterministic for one file, not across reorderings of it.
            conflicts.setdefault(cat, [targets[cat]]).append(unit)

    if conflicts:
        # NOTE: this is a clifpy limitation, not a schema defect. CLIF 3.0
        # keys its targets on (med_category, med_group), and the same drug is
        # legitimately dosed differently by route -- epoprostenol is ng/kg/min
        # as an IV infusion and mcg/kg/min inhaled, a factor of 1000;
        # terbutaline is mg inhaled and mcg/kg/min otherwise, not even the same
        # unit class. clifpy currently keys on med_category alone, so it can
        # only carry one target per category.
        #
        # Until composite-key support lands, keep the FIRST occurrence in file
        # order -- deterministic for a given file, but a reordered file
        # changes the result -- and say plainly which rows were collapsed and
        # which unit was kept. See docs/user-guide/med-unit-conversion.md.
        detail = '; '.join(
            f"{cat}: {' vs '.join(units)} (using {targets[cat]})"
            for cat, units in sorted(conflicts.items())
        )
        logger.warning(
            "Targets file %s defines more than one target unit for %d med_category "
            "value(s), most likely distinguished by med_group (route). clifpy "
            "keys on med_category alone and is keeping the first occurrence of "
            "each in file order, so reordering the file changes the result. If "
            "the other applies to your cohort, pass an explicit dict to "
            "override: %s",
            source, len(conflicts), detail,
        )

    logger.info(
        "Loaded %d dose-unit targets from %s (%d row(s) dropped as blank/NA, "
        "%d conflicting duplicate(s))",
        len(targets), source, dropped, len(conflicts),
    )
    return targets


# ---------------------------------------------------------------------------
# QA warnings for standardize_med_dose_units
# ---------------------------------------------------------------------------

# How many categories a summary warning names before "(+N more)".
_MAX_NAMES_IN_WARNING = 8

# What an unlisted row became, by `unlisted` mode, for the warning text.
_UNLISTED_OUTCOME = {
    'keep': "keep their dose, with only the unit spelling cleaned",
    'base': (
        "are converted to clifpy's base units (weight qualifier kept, e.g. "
        "mcg/min, mcg/kg/min, ml/min, units/min)"
    ),
}


def _listed_mask(category_col, listed) -> pl.Expr:
    """TRUE where the category is a key of the targets (compared as text).

    An empty or all-null category column can arrive untyped (polars Null),
    so the column is cast first; a null category is unlisted.
    """
    return pl.col(category_col).cast(pl.String).is_in(sorted(listed)).fill_null(False)


def _warn_categories_without_target(
    summary, category_col, label, targets, *, unlisted='keep'
) -> None:
    """Log one WARNING naming the data categories the targets do not list.

    `summary` is the counts table as a polars DataFrame. A category that
    equals a key of `targets` once stripped and lowercased is likely
    spelling drift: every such one is named first and never truncated; the
    rest follow, most rows first, capped at `_MAX_NAMES_IN_WARNING`.
    """
    listed = {str(c) for c in targets}
    per_category = (
        summary.filter(~_listed_mask(category_col, listed))
        .group_by(category_col)
        .agg(pl.col('count').sum())
    )
    if per_category.height == 0:
        return
    items = sorted(
        ((cat, int(n)) for cat, n in per_category.iter_rows()),
        key=lambda kv: (-kv[1], str(kv[0])),
    )
    normalized_keys = {k.strip().lower() for k in listed}

    def _fmt(pairs):
        return ', '.join(
            f"{cat!r} ({n} row{'' if n == 1 else 's'})" for cat, n in pairs
        )

    drift = [kv for kv in items if str(kv[0]).strip().lower() in normalized_keys]
    others = [kv for kv in items if kv not in drift]
    parts = []
    if drift:
        parts.append(f"likely spelling drift of a listed category: {_fmt(drift)}")
    if others:
        tail = _fmt(others[:_MAX_NAMES_IN_WARNING])
        if len(others) > _MAX_NAMES_IN_WARNING:
            tail += f", ... (+{len(others) - _MAX_NAMES_IN_WARNING} more)"
        parts.append(f"others: {tail}" if drift else tail)
    shown = '; '.join(parts)
    logger.warning(
        "%d %s value(s) in the data have no listed target; their rows %s, "
        "and _convert_status_details reads 'success: no target listed; ...': "
        "%s. Check them for spelling or case drift against the targets' "
        "categories.",
        len(items), label, _UNLISTED_OUTCOME[unlisted], shown,
    )


# Statuses meaning the target's unit class (rate vs amount) disagrees with the
# data's. When they dominate a category, the targets file is the suspect.
_SHAPE_MISMATCH_STATUSES = [
    'cannot convert rate to amount',
    'cannot convert amount to rate',
]


def _warn_target_shape(
    summary, category_col, targets, label, *, dose_col=INTERNAL_DOSE_COL
) -> None:
    """WARN per category whose listed target fails on unit class for most rows.

    `summary` is the counts table as a polars DataFrame. Its `_unit_class`
    and `_convert_status` columns carry the names of the pair `dose_col`
    names (pair_column_name). The denominator is the category's rows with a
    recognised rate or amount unit, so missing and unparseable units do not
    dilute it.
    """
    unit_class_col = pair_column_name('_unit_class', dose_col)
    status_col = pair_column_name('_convert_status', dose_col)
    listed = {str(c) for c in targets}
    per_category = (
        summary.filter(
            _listed_mask(category_col, listed)
            & pl.col(unit_class_col).is_in(['rate', 'amount'])
        )
        .group_by(category_col)
        .agg(
            n=pl.col('count').sum(),
            n_failed=pl.col('count')
            .filter(pl.col(status_col).is_in(_SHAPE_MISMATCH_STATUSES))
            .sum(),
        )
        .sort(category_col)
    )
    for category, n, n_failed in per_category.iter_rows():
        if 2 * n_failed > n:
            logger.warning(
                "Target %r for %s %r fails on unit class for %d of %d "
                "row(s) ('cannot convert rate to amount' / 'cannot convert "
                "amount to rate'); those rows keep their original dose. If many "
                "categories do this, the targets file most likely predates the "
                "2026-09-09 mCIDE layout (med_dose_unit holding amount-only "
                "units such as 'mcg' instead of full rates such as "
                "'mcg/kg/min'), or is the other medication table's file.",
                targets[category], label, category, int(n_failed), int(n),
            )


def _warn_about_targets(
    counts, targets, *, category_col, dose_col=INTERNAL_DOSE_COL, unlisted='keep'
) -> None:
    """Log standardize_med_dose_units' two QA warnings from the counts table.

    Takes the counts table
    :func:`~clifpy.utils.unit_converter.convert_dose_units_by_med_category`
    returned -- a polars DataFrame, or a DuckDBPyRelation under
    `return_format='duckdb'` -- and the `targets` dict, `dose_col` and
    `unlisted` it was given. The counts table is read once, as a small polars
    frame; for a relation that evaluates the pipeline once inside the call.
    Neither output is changed.
    """
    # The counts table names its category column `med_category` unless the
    # converter shows it under the caller's name.
    counts_category_col = (
        category_col if category_col in counts.columns else INTERNAL_CATEGORY_COL
    )
    summary = counts if isinstance(counts, pl.DataFrame) else counts.pl()
    _warn_categories_without_target(
        summary, counts_category_col, category_col, targets, unlisted=unlisted
    )
    _warn_target_shape(
        summary, counts_category_col, targets, label=category_col, dose_col=dose_col
    )
