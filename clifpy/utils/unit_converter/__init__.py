"""Unit converter for standardizing medication dose units.

This package provides utilities for converting medication dose units between
different formats and standardizing them to a common base set. It handles
weight-based dosing, time unit conversions, and various unit name variants.

In general, convert both rate and amount indiscriminately and report them
as well as unrecognized units.

The implementation is split by concern -- `_grammar` (unit vocabulary),
`_clean` (string normalisation), `_sql` (CASE builders), `_inputs` (accepted
input types, the pandas-to-Arrow boundary and the catalog-shadowing guard),
`_outputs` (return formats and time-zone labels), `_columns` (caller column
names and collisions), `_base` (stage 1), `_weight` (patient-weight lookup),
`_preferred` (stage 2), `_counts` (summary table), `_targets` (schema target
readers) and `_convert` (orchestration) -- but the public surface is
re-exported here, so `from clifpy.utils.unit_converter import ...` is
unchanged. Every computation runs in DuckDB or polars; pandas appears only
when a caller passes a pandas DataFrame in or asks for one back.

Public surface (`__all__`; each name has an entry in docs/api/utilities.md):

Conversion
    convert_dose_units_by_med_category -- base units, then per-category
    preferred units, looking patient weight up only where a conversion needs
    it. Only its first eight parameters may be passed by position.
    standardize_med_dose_units -- the same, with the targets read from an
    mCIDE schema file or a dict.
    standardize_dose_to_base_units -- stage 1 alone: clean and convert to base
    units.
    All three, like attach_weight, accept a polars DataFrame or LazyFrame, a
    DuckDB relation or a pandas DataFrame, and take a keyword-only
    `return_format`: 'polars' (the default), 'polars_lazy', 'duckdb' or the
    deprecated 'pandas'.

Patient weight
    attach_weight -- attach `weight_kg` through an ordered fallback chain,
    recording which strategy supplied each weight.
    WEIGHT_FALLBACKS -- the strategy names `attach_weight` accepts.
    find_most_recent_weight -- the as-of weight lookup, with an optional
    fallback to the stay's earliest weight.

Schema targets
    load_dose_unit_targets -- read `{med_category: target_unit}` from a schema.
    register_target_reader, available_target_formats, TargetReader -- add or
    list the schema file formats.

Column names
    validate_column_name -- the identifier check applied to every column-name
    argument.

Conversion status
    AMBIGUOUS_MU_STATUS -- the `_convert_status` of a refused milli-units <->
    million-units conversion.
    CONVERT_STATUS_DETAILS -- the seven `_convert_status_details` values of a
    successful row: DETAILS_CONVERTED_TO_TARGET, DETAILS_ALREADY_TARGET,
    DETAILS_CONVERTED_TO_VARIANT, DETAILS_ALREADY_VARIANT,
    DETAILS_UNLISTED_CLEANED, DETAILS_UNLISTED_TO_BASE,
    DETAILS_UNLISTED_ALREADY_BASE.

Unit vocabulary (read-only constants)
    DEFAULT_COUNTABLE_UNITS, MISSING_UNIT_PLACEHOLDERS, CANONICAL_UNIT_SPELLING,
    UNIT_NAMING_VARIANTS, REGEX_TO_FACTOR_MAPPER, SUBCLASS_SPEC,
    SUBCLASS_REGEX, STANDALONE_SUBCLASSES, TOKEN_TO_BASE_FACTOR,
    TIME_TO_BASE_FACTOR, KG_PER_LB, ACCEPTABLE_BASE_AMOUNT_UNITS,
    STANDALONE_AMOUNT_UNITS, ACCEPTABLE_WEIGHT_UNITS, ACCEPTABLE_TIME_UNITS,
    ACCEPTABLE_AMOUNT_UNITS, ACCEPTABLE_RATE_UNITS, ALL_ACCEPTABLE_UNITS,
    MASS_REGEX, VOLUME_REGEX, UNIT_REGEX, HR_REGEX, WEIGHT_REGEX.

The other names importable from here (the remaining regexes and patterns, the
`_`-prefixed helpers) are internal and may change without notice.
"""

from ._grammar import (
    KG_PER_LB,
    UNIT_NAMING_VARIANTS,
    CANONICAL_UNIT_SPELLING,
    DEFAULT_COUNTABLE_UNITS,
    MISSING_UNIT_PLACEHOLDERS,
    STANDALONE_AMOUNT_UNITS,
    SUBCLASS_SPEC,
    SUBCLASS_REGEX,
    STANDALONE_SUBCLASSES,
    TOKEN_TO_BASE_FACTOR,
    TIME_TO_BASE_FACTOR,
    REGEX_TO_FACTOR_MAPPER,
    AMOUNT_ENDER,
    MASS_REGEX,
    VOLUME_REGEX,
    UNIT_REGEX,
    HR_REGEX,
    MU_REGEX,
    MNU_REGEX,
    MG_REGEX,
    NG_REGEX,
    G_REGEX,
    L_REGEX,
    LB_REGEX,
    KG_REGEX,
    WEIGHT_REGEX,
    AMOUNT_FACTOR_PATTERNS,
    TIME_FACTOR_PATTERNS,
    ACCEPTABLE_WEIGHT_UNITS,
    ACCEPTABLE_TIME_UNITS,
    ACCEPTABLE_BASE_AMOUNT_UNITS,
    ACCEPTABLE_AMOUNT_UNITS,
    ACCEPTABLE_RATE_UNITS,
    ALL_ACCEPTABLE_UNITS,
    RATE_UNITS_STR,
    AMOUNT_UNITS_STR,
    _tokens_alternation,
    _token_regex,
    _weight_qual_clause,
    _acceptable_amount_units,
    _acceptable_rate_units,
    _convert_set_to_str_for_sql,
)
from ._clean import (
    _clean_dose_unit_formats_duckdb,
    _clean_dose_unit_names_duckdb,
)
from ._sql import (
    _concat_builders_by_patterns,
    _pattern_to_factor_builder_for_base,
    _pattern_to_factor_builder_for_preferred,
)
from ._counts import _create_unit_conversion_counts_table
from ._columns import (
    validate_column_name,
    build_rename_map,
    rename_to_internal,
    rename_from_internal,
)
from ._weight import WEIGHT_FALLBACKS, attach_weight, find_most_recent_weight
from ._base import (
    _convert_clean_units_to_base_units,
    standardize_dose_to_base_units,
)
from ._preferred import (
    _convert_base_units_to_preferred_units,
    AMBIGUOUS_MU_STATUS,
    CONVERT_STATUS_DETAILS,
    DETAILS_CONVERTED_TO_TARGET,
    DETAILS_ALREADY_TARGET,
    DETAILS_CONVERTED_TO_VARIANT,
    DETAILS_ALREADY_VARIANT,
    DETAILS_UNLISTED_CLEANED,
    DETAILS_UNLISTED_TO_BASE,
    DETAILS_UNLISTED_ALREADY_BASE,
)
from ._convert import (
    convert_dose_units_by_med_category,
    standardize_med_dose_units,
)
from ._targets import (
    load_dose_unit_targets,
    register_target_reader,
    available_target_formats,
    TargetReader,
)

__all__ = [
    # conversion
    "convert_dose_units_by_med_category",
    "standardize_med_dose_units",
    "standardize_dose_to_base_units",
    # patient weight
    "attach_weight",
    "WEIGHT_FALLBACKS",
    "find_most_recent_weight",
    # targets files
    "load_dose_unit_targets",
    "register_target_reader",
    "available_target_formats",
    "TargetReader",
    # column names
    "validate_column_name",
    # conversion status
    "AMBIGUOUS_MU_STATUS",
    "CONVERT_STATUS_DETAILS",
    "DETAILS_CONVERTED_TO_TARGET",
    "DETAILS_ALREADY_TARGET",
    "DETAILS_CONVERTED_TO_VARIANT",
    "DETAILS_ALREADY_VARIANT",
    "DETAILS_UNLISTED_CLEANED",
    "DETAILS_UNLISTED_TO_BASE",
    "DETAILS_UNLISTED_ALREADY_BASE",
    # unit vocabulary
    "DEFAULT_COUNTABLE_UNITS",
    "MISSING_UNIT_PLACEHOLDERS",
    "CANONICAL_UNIT_SPELLING",
    "UNIT_NAMING_VARIANTS",
    "REGEX_TO_FACTOR_MAPPER",
    "SUBCLASS_SPEC",
    "SUBCLASS_REGEX",
    "STANDALONE_SUBCLASSES",
    "TOKEN_TO_BASE_FACTOR",
    "TIME_TO_BASE_FACTOR",
    "KG_PER_LB",
    "ACCEPTABLE_BASE_AMOUNT_UNITS",
    "STANDALONE_AMOUNT_UNITS",
    "ACCEPTABLE_WEIGHT_UNITS",
    "ACCEPTABLE_TIME_UNITS",
    "ACCEPTABLE_AMOUNT_UNITS",
    "ACCEPTABLE_RATE_UNITS",
    "ALL_ACCEPTABLE_UNITS",
    "MASS_REGEX",
    "VOLUME_REGEX",
    "UNIT_REGEX",
    "HR_REGEX",
    "WEIGHT_REGEX",
]
