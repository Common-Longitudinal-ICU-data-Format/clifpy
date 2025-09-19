"""Unit converter for standardizing medication dose units.

This module provides utilities for converting medication dose units between
different formats and standardizing them to a common base set. It handles
weight-based dosing, time unit conversions, and various unit name variants.

In general, convert both rate and amount indiscriminately and report them
as well as unrecognized units.
"""

from types import NoneType
import pandas as pd
import duckdb
from typing import Set, Tuple, List

UNIT_NAMING_VARIANTS = {
    # time
    '/hr': '/h(r|our)?$',
    '/min': '/m(in|inute)?$',
    # unit -- NOTE: plaural always go first to avoid having result like "us" or "gs"
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass
    'mcg': '^(u|µ|μ)g',
    'g': '^g(rams|ram)?',
    # dose
    # 'dose': '^doses?',
}

AMOUNT_ENDER = "($|/*)"
MASS_REGEX = f"^(mcg|mg|ng|g){AMOUNT_ENDER}"
VOLUME_REGEX = f"^(l|ml){AMOUNT_ENDER}"
UNIT_REGEX = f"^(u|mu){AMOUNT_ENDER}"

# time
HR_REGEX = f"/hr$"

# mass
MU_REGEX = f"^(mu){AMOUNT_ENDER}"
MG_REGEX = f"^(mg){AMOUNT_ENDER}"
NG_REGEX = f"^(ng){AMOUNT_ENDER}"
G_REGEX = f"^(g){AMOUNT_ENDER}"

# volume
L_REGEX = f"^l{AMOUNT_ENDER}"

# weight
LB_REGEX = f"/lb/"
KG_REGEX = f"/kg/"
WEIGHT_REGEX = f"/(lb|kg)/"

REGEX_TO_FACTOR_MAPPER = {
    # time -> /min
    HR_REGEX: '1/60',
    
    # volume -> ml
    L_REGEX: '1000', # to ml

    # unit -> u
    MU_REGEX: '1/1000',
    
    # mass -> mcg
    MG_REGEX: '1000',
    NG_REGEX: '1/1000',
    G_REGEX: '1000000',
    
    # weight -> /kg
    KG_REGEX: 'weight_kg',
    LB_REGEX: 'weight_kg * 2.20462'
}

ACCEPTABLE_AMOUNT_UNITS = {
    "ml", "l", # volume
    "mu", "u", # unit
    "mcg", "mg", "ng", 'g' # mass
    # "dose" # dose
    }

def _acceptable_rate_units() -> Set[str]:
    """Generate all acceptable rate unit combinations.

    Creates a cartesian product of amount units, weight qualifiers, and time units
    to generate all valid rate unit patterns that the converter can handle.

    Returns
    -------
    Set[str]
        Set of all valid rate unit combinations.

    Examples
    --------
    >>> rate_units = _acceptable_rate_units()
    >>> 'mcg/kg/hr' in rate_units
    True
    >>> 'ml/min' in rate_units
    True
    >>> 'tablespoon/hr' in rate_units
    False

    Notes
    -----
    Rate units are combinations of:

    - Amount units: ml, l, mu, u, mcg, mg, ng, g
    - Weight qualifiers: /kg, /lb, or none
    - Time units: /hr, /min
    """
    acceptable_weight_units = {'/kg', '/lb', ''}
    acceptable_time_units = {'/hr', '/min'}
    # find the cartesian product of the three sets
    return {a + b + c for a in ACCEPTABLE_AMOUNT_UNITS for b in acceptable_weight_units for c in acceptable_time_units}

ACCEPTABLE_RATE_UNITS = _acceptable_rate_units()

ALL_ACCEPTABLE_UNITS = ACCEPTABLE_RATE_UNITS | ACCEPTABLE_AMOUNT_UNITS

def _convert_set_to_str_for_sql(s: Set[str]) -> str:
    """Convert a set of strings to SQL IN clause format.

    Transforms a Python set into a comma-separated string suitable for use
    in SQL IN clauses within DuckDB queries.

    Parameters
    ----------
    s : Set[str]
        Set of strings to be formatted for SQL.

    Returns
    -------
    str
        Comma-separated string with items separated by "','".
        Does not include outer quotes - those are added in SQL query.

    Examples
    --------
    >>> units = {'ml/hr', 'mcg/min', 'u/hr'}
    >>> _convert_set_to_str_for_sql(units)
    "ml/hr','mcg/min','u/hr"

    Usage in SQL queries:

    >>> # f"WHERE unit IN ('{_convert_set_to_str_for_sql(units)}')"

    Notes
    -----
    This is a helper function for building DuckDB SQL queries that need to check
    if values are in a set of acceptable units.
    """
    return "','".join(s)

RATE_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_RATE_UNITS)
AMOUNT_UNITS_STR = _convert_set_to_str_for_sql(ACCEPTABLE_AMOUNT_UNITS)

def _clean_dose_unit_formats(s: pd.Series) -> pd.Series:
    """Clean dose unit formatting by removing spaces and converting to lowercase.

    This is the first step in the cleaning pipeline. It standardizes
    the basic formatting of dose units before applying name cleaning.

    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings to clean.

    Returns
    -------
    pd.Series
        Series with cleaned formatting (no spaces, lowercase).

    Examples
    --------
    >>> import pandas as pd
    >>> s = pd.Series(['mL / hr', 'MCG/KG/MIN', ' Mg/Hr '])
    >>> result = _clean_dose_unit_formats(s)
    >>> list(result)
    ['ml/hr', 'mcg/kg/min', 'mg/hr']

    Notes
    -----
    This function is typically used as the first step in the cleaning
    pipeline, followed by _clean_dose_unit_names().
    """
    return s.str.replace(r'\s+', '', regex=True).str.lower().replace('', None, regex=False)
    
def _clean_dose_unit_names(s: pd.Series) -> pd.Series:
    """Clean dose unit name variants to standard abbreviations.

    Applies regex patterns to convert various unit name variants to their
    standard abbreviated forms (e.g., 'milliliter' -> 'ml', 'hour' -> 'hr').

    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings with name variants.
        Should already be format-cleaned (lowercase, no spaces).

    Returns
    -------
    pd.Series
        Series with clean unit names.

    Examples
    --------
    >>> import pandas as pd
    >>> s = pd.Series(['milliliter/hour', 'units/minute', 'µg/kg/h'])
    >>> result = _clean_dose_unit_names(s)
    >>> list(result)
    ['ml/hr', 'u/min', 'mcg/kg/hr']

    Notes
    -----
    Handles conversions including:

    - Time: hour/h -> hr, minute/m -> min
    - Volume: liter/liters/litre/litres -> l
    - Units: units/unit -> u, milli-units -> mu
    - Mass: µg/ug -> mcg, gram -> g

    This function should be applied after _clean_dose_unit_formats().
    """
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s

def _detect_and_classify_clean_dose_units(s: pd.Series) -> dict:
    """Classify and count clean dose units by category.

    Analyzes a series of clean dose units and classifies them into
    rate units, amount units, or unrecognized units, providing counts for each.

    Parameters
    ----------
    s : pd.Series
        Series containing clean dose unit strings.

    Returns
    -------
    dict
        Dictionary with three keys:

        - 'rate_units': dict of recognized rate units and their counts
        - 'amount_units': dict of recognized amount units and their counts
        - 'unrecognized_units': dict of unrecognized units and their counts

    Examples
    --------
    >>> import pandas as pd
    >>> s = pd.Series(['ml/hr', 'ml/hr', 'mcg', 'unknown_unit', None])
    >>> result = _detect_and_classify_clean_dose_units(s)
    >>> result['rate_units']
    {'ml/hr': 2}
    >>> result['amount_units']
    {'mcg': 1}
    >>> result['unrecognized_units']
    {'unknown_unit': 1, None: 1}

    Notes
    -----
    This function includes NaN/None values in the unrecognized category.
    Consider using the more comprehensive _create_unit_conversion_counts_table
    for production use.

    .. deprecated::
        Consider using _create_unit_conversion_counts_table instead.
    """
    counts_dict = s.value_counts(dropna=False).to_dict()
    rate_units_counts = {
        k: v for k, v in counts_dict.items() if k in ACCEPTABLE_RATE_UNITS
    }
    amount_units_counts = {
        k: v for k, v in counts_dict.items() if k in ACCEPTABLE_AMOUNT_UNITS
    }
    unrecognized_units_counts = {
        k: v for k, v in counts_dict.items() 
        if k not in ACCEPTABLE_RATE_UNITS and k not in ACCEPTABLE_AMOUNT_UNITS
    }
    return {
        'rate_units': rate_units_counts,
        'amount_units': amount_units_counts,
        'unrecognized_units': unrecognized_units_counts
    }

def _concat_builders_by_patterns(builder: callable, patterns: list, else_case: str = '1') -> str:
    """Concatenate multiple SQL CASE WHEN statements from patterns.

    Helper function that combines multiple regex pattern builders into a single
    SQL CASE statement for DuckDB queries. Used internally to build conversion
    factor calculations for different unit components (amount, time, weight).

    Parameters
    ----------
    builder : callable
        Function that generates CASE WHEN clauses from regex patterns.
        Should accept a pattern string and return a WHEN...THEN clause.
    patterns : list
        List of regex patterns to process with the builder function.
    else_case : str, default '1'
        Value to use in the ELSE clause when no patterns match.
        Default is '1' (no conversion factor).

    Returns
    -------
    str
        Complete SQL CASE statement with all pattern conditions.

    Examples
    --------
    >>> patterns = ['/hr$', '/min$']
    >>> builder = lambda p: f"WHEN regexp_matches(col, '{p}') THEN factor"
    >>> result = _concat_builders_by_patterns(builder, patterns)
    >>> 'CASE WHEN' in result and 'ELSE 1 END' in result
    True

    Notes
    -----
    This function is used internally by conversion functions to build
    SQL queries that apply different conversion factors based on unit patterns.
    """
    return "CASE " + " ".join([builder(pattern) for pattern in patterns]) + f" ELSE {else_case} END"

def _pattern_to_factor_builder_for_base(pattern: str) -> str:
    """Build SQL CASE WHEN statement for regex pattern matching.

    Helper function that generates SQL CASE WHEN clauses for DuckDB queries
    based on regex patterns and their corresponding conversion factors.

    Parameters
    ----------
    pattern : str
        Regex pattern to match (must exist in REGEX_TO_FACTOR_MAPPER).

    Returns
    -------
    str
        SQL CASE WHEN clause string.

    Raises
    ------
    ValueError
        If the pattern is not found in REGEX_TO_FACTOR_MAPPER.

    Examples
    --------
    >>> clause = _pattern_to_factor_builder_for_base(HR_REGEX)
    >>> 'WHEN regexp_matches' in clause and 'THEN' in clause
    True

    Notes
    -----
    This function is used internally by _convert_clean_dose_units_to_base_units
    to build the SQL query for unit conversion.
    """
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(_clean_unit, '{pattern}') THEN {REGEX_TO_FACTOR_MAPPER.get(pattern)}"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _pattern_to_factor_builder_for_preferred(pattern: str) -> str:
    """Build SQL CASE WHEN statement for preferred unit conversion.

    Generates SQL clauses for converting from base units back to preferred units
    by applying the inverse of the original conversion factor. Used when converting
    from standardized base units to medication-specific preferred units.

    Parameters
    ----------
    pattern : str
        Regex pattern to match in _preferred_unit column.
        Must exist in REGEX_TO_FACTOR_MAPPER dictionary.

    Returns
    -------
    str
        SQL CASE WHEN clause with inverse conversion factor.

    Raises
    ------
    ValueError
        If the pattern is not found in REGEX_TO_FACTOR_MAPPER.

    Examples
    --------
    >>> clause = _pattern_to_factor_builder_for_preferred('/hr$')
    >>> 'WHEN regexp_matches(_preferred_unit' in clause and 'THEN 1/' in clause
    True

    Notes
    -----
    This function applies the inverse of the factor used in
    _pattern_to_factor_builder_for_base, allowing bidirectional conversion
    between unit systems. The inverse is calculated as 1/(original_factor).

    See Also
    --------
    _pattern_to_factor_builder_for_base : Builds patterns for base unit conversion
    """
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(_preferred_unit, '{pattern}') THEN 1/({REGEX_TO_FACTOR_MAPPER.get(pattern)})"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _convert_clean_units_to_base_units(med_df: pd.DataFrame) -> pd.DataFrame:
    """Convert clean dose units to base units.

    Core conversion function that transforms various dose units into a base
    set of standard units (mcg/min, ml/min, u/min for rates; mcg, ml, u for amounts).
    Uses DuckDB for efficient SQL-based transformations.

    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame containing medication data with required columns:

        - _clean_unit: Cleaned unit strings
        - med_dose: Original dose values
        - weight_kg: Patient weight (used for /kg and /lb conversions)

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns:

        - _unit_class: 'rate', 'amount', or 'unrecognized'
        - _amount_multiplier: Factor for amount conversion
        - _time_multiplier: Factor for time conversion (hr to min)
        - _weight_multiplier: Factor for weight-based conversion
        - _base_dose: base dose value
        - _base_unit: base unit string

    Examples
    --------
    >>> import pandas as pd
    >>> df = pd.DataFrame({
    ...     'med_dose': [6, 100],
    ...     '_clean_unit': ['mcg/kg/hr', 'ml/hr'],
    ...     'weight_kg': [70, 80]
    ... })
    >>> result = _convert_clean_dose_units_to_base_units(df)
    >>> 'mcg/min' in result['_base_unit'].values
    True
    >>> 'ml/min' in result['_base_unit'].values
    True

    Notes
    -----
    Conversion targets:

    - Rate units: mcg/min, ml/min, u/min
    - Amount units: mcg, ml, u
    - Unrecognized units: original dose and (cleaned) unit will be preserved

    Weight-based conversions use patient weight from weight_kg column.
    Time conversions: /hr -> /min (divide by 60).
    """
    
    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=[L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX],
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=[HR_REGEX],
        else_case='1'
        )

    weight_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_base,
        patterns=[KG_REGEX, LB_REGEX],
        else_case='1'
        )
    
    q = f"""
    SELECT *
        -- classify and check acceptability first
        , _unit_class: CASE
            WHEN _clean_unit IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN _clean_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END
        -- mark if the input unit is adjusted by weight (e.g. 'mcg/kg/hr')
        , _weighted: CASE
            WHEN regexp_matches(_clean_unit, '{WEIGHT_REGEX}') THEN 1 ELSE 0 END
        -- parse and generate multipliers
        , _amount_multiplier: CASE
            WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({amount_clause}) END 
        , _time_multiplier: CASE
            WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({time_clause}) END 
        , _weight_multiplier: CASE
            WHEN _unit_class = 'unrecognized' THEN 1 ELSE ({weight_clause}) END
        -- calculate the base dose
        , _base_dose: CASE
            -- when the input unit is weighted but weight_kg is missing, keep the original dose
            WHEN _weighted = 1 AND weight_kg IS NULL THEN med_dose
            ELSE med_dose * _amount_multiplier * _time_multiplier * _weight_multiplier 
            END
        -- id the base unit
        , _base_unit: CASE 
            -- when the input unit is weighted but weight_kg is missing, keep the original dose
            WHEN _weighted = 1 AND weight_kg IS NULL THEN _clean_unit
            WHEN _unit_class = 'unrecognized' THEN _clean_unit
            WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{MASS_REGEX}') THEN 'mcg/min'
            WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}') THEN 'ml/min'
            WHEN _unit_class = 'rate' AND regexp_matches(_clean_unit, '{UNIT_REGEX}') THEN 'u/min'
            WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{MASS_REGEX}') THEN 'mcg'
            WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{VOLUME_REGEX}') THEN 'ml'
            WHEN _unit_class = 'amount' AND regexp_matches(_clean_unit, '{UNIT_REGEX}') THEN 'u'
            END
    FROM med_df 
    """
    return duckdb.sql(q).to_df()

def _create_unit_conversion_counts_table(
    med_df: pd.DataFrame,
    group_by: List[str]
    ) -> pd.DataFrame:
    """Create summary table of unit conversion counts.

    Generates a grouped summary showing the frequency of each unit conversion
    pattern, useful for data quality assessment and identifying common or
    problematic unit patterns.

    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame with required columns from conversion process:

        - med_dose_unit: Original unit string
        - _clean_unit: Cleaned unit string
        - _base_unit: base standard unit
        - _unit_class: Classification (rate/amount/unrecognized)
    group_by : List[str]
        List of columns to group by.

    Returns
    -------
    pd.DataFrame
        Summary DataFrame with columns:

        - med_dose_unit: Original unit
        - _clean_unit: After cleaning
        - _base_unit: After conversion
        - _unit_class: Classification
        - count: Number of occurrences

    Raises
    ------
    ValueError
        If required columns are missing from input DataFrame.

    Examples
    --------
    >>> import pandas as pd
    >>> # df_base = standardize_dose_to_base_units(med_df)[0]
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
    
    # build the string that enumerates the group by columns 
    # e.g. 'med_dose_unit, med_dose_unit_normalized, unit_class'
    cols_enum_str = f"{', '.join(group_by)}"
    order_by_clause = f"med_category, count DESC" if 'med_category' in group_by else "count DESC"
    
    q = f"""
    SELECT {cols_enum_str}   
        , COUNT(*) as count
    FROM med_df
    GROUP BY {cols_enum_str}
    ORDER BY {order_by_clause}
    """
    return duckdb.sql(q).to_df()
    

def standardize_dose_to_base_units(
    med_df: pd.DataFrame,
    vitals_df: pd.DataFrame = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Standardize medication dose units to a base set of standard units.

    Main public API function that performs complete dose unit standardization
    pipeline: format cleaning, name cleaning, and unit conversion.
    Returns both base data and a summary table of conversions.

    Parameters
    ----------
    med_df : pd.DataFrame
        Medication DataFrame with required columns:

        - med_dose_unit: Original dose unit strings
        - med_dose: Dose values
        - weight_kg: Patient weights (optional, can be added from vitals_df)

        Additional columns are preserved in output.
    vitals_df : pd.DataFrame, optional
        Vitals DataFrame for extracting patient weights if not in med_df.
        Required columns if weight_kg missing from med_df:

        - hospitalization_id: Patient identifier
        - recorded_dttm: Timestamp of vital recording
        - vital_category: Must include 'weight_kg' values
        - vital_value: Weight values

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        A tuple containing:

        - [0] base medication DataFrame with additional columns:

            * _clean_unit: Cleaned unit string
            * _unit_class: 'rate', 'amount', or 'unrecognized'
            * _base_dose: base dose value
            * _base_unit: base unit
            * amount_multiplier, time_multiplier, weight_multiplier: Conversion factors

        - [1] Summary counts DataFrame showing conversion patterns and frequencies

    Raises
    ------
    ValueError
        If required columns are missing from med_df.

    Examples
    --------
    >>> import pandas as pd
    >>> med_df = pd.DataFrame({
    ...     'med_dose': [6, 100, 500],
    ...     'med_dose_unit': ['MCG/KG/HR', 'mL / hr', 'mg'],
    ...     'weight_kg': [70, 80, 75]
    ... })
    >>> base_df, counts_df = standardize_dose_to_base_units(med_df)
    >>> '_base_unit' in base_df.columns
    True
    >>> 'count' in counts_df.columns
    True

    Notes
    -----
    Standard units for conversion:

    - Rate units: mcg/min, ml/min, u/min (all per minute)
    - Amount units: mcg, ml, u (base units)

    The function automatically handles:

    - Weight-based dosing (/kg, /lb) using patient weights
    - Time conversions (per hour to per minute)
    - Volume conversions (L to mL)
    - Mass conversions (mg, ng, g to mcg)
    - Unit conversions (milli-units to units)

    Unrecognized units are flagged but preserved in the output.
    """
    if 'weight_kg' not in med_df.columns:
        print("No weight_kg column found, adding the most recent from vitals")
        query = """
        SELECT m.*
            , v.vital_value as weight_kg
            , v.recorded_dttm as _weight_recorded_dttm
            , ROW_NUMBER() OVER (
                PARTITION BY m.hospitalization_id, m.admin_dttm, m.med_category
                ORDER BY v.recorded_dttm DESC
                ) as _rn
        FROM med_df m
        LEFT JOIN vitals_df v 
            ON m.hospitalization_id = v.hospitalization_id 
            AND v.vital_category = 'weight_kg' AND v.vital_value IS NOT NULL
            AND v.recorded_dttm <= m.admin_dttm  -- only past weights
        -- rn = 1 for the weight w/ the latest recorded_dttm (and thus most recent)
        QUALIFY (_rn = 1 OR _rn IS NULL) 
        ORDER BY m.hospitalization_id, m.admin_dttm, m.med_category, _rn
        """
        med_df = duckdb.sql(query).to_df()
    
    # check if the required columns are present
    required_columns = {'med_dose_unit', 'med_dose', 'weight_kg'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    med_df['_clean_unit'] = (
        med_df['med_dose_unit'].pipe(_clean_dose_unit_formats)
        .pipe(_clean_dose_unit_names)
    )
    
    med_df_base = _convert_clean_units_to_base_units(med_df)
    convert_counts_df = _create_unit_conversion_counts_table(
        med_df_base, 
        group_by=['med_dose_unit', '_clean_unit', '_base_unit', '_unit_class']
        )
    
    return med_df_base, convert_counts_df
    
def _convert_base_units_to_preferred_units(
    med_df: pd.DataFrame,
    override: bool = False
    ) -> pd.DataFrame:
    """Convert base standardized units to user-preferred units.

    Performs the second stage of unit conversion, transforming from standardized
    base units (mcg/min, ml/min, u/min) to medication-specific preferred units
    while maintaining unit class consistency.

    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame with required columns from first-stage conversion:

        - _base_dose: Dose values in standardized units
        - _base_unit: Standardized unit strings (may be NULL)
        - _preferred_unit: Target unit strings for each medication
        - weight_kg: Patient weights (optional, used for weight-based conversions)
    override : bool, default False
        If True, prints warnings but continues when encountering:

        - Unacceptable preferred units not in ALL_ACCEPTABLE_UNITS
        - Cross-class conversions (e.g., rate to amount)
        - Cross-subclass conversions (e.g., mass to volume)

        If False, raises ValueError for these conditions.

    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns:

        - _unit_class: Classification of base unit ('rate', 'amount', 'unrecognized')
        - _unit_subclass: Subclassification ('mass', 'volume', 'unit', 'unrecognized')
        - _unit_class_preferred: Classification of preferred unit
        - _unit_subclass_preferred: Subclassification of preferred unit
        - _convert_status: Success or failure reason message
        - _amount_multiplier_preferred: Conversion factor for amount units
        - _time_multiplier_preferred: Conversion factor for time units
        - _weight_multiplier_preferred: Conversion factor for weight-based units
        - med_dose_converted: Final converted dose value
        - med_dose_unit_converted: Final unit string after conversion

    Raises
    ------
    ValueError
        If required columns are missing from med_df or if preferred units are not
        in ALL_ACCEPTABLE_UNITS (when override=False).

    Notes
    -----
    Conversion rules enforced:

    - Conversions only allowed within same unit class (rate→rate, amount→amount)
    - Cannot convert between incompatible subclasses (e.g., mass→volume)
    - When conversion fails, falls back to base units and dose values
    - Missing units (NULL) are handled with 'original unit is missing' status

    The function uses DuckDB SQL for efficient processing and applies regex
    pattern matching to classify units and calculate conversion factors.

    See Also
    --------
    _convert_clean_dose_units_to_base_units : First-stage conversion
    convert_dose_units_by_med_category : Public API for complete conversion pipeline
    """
    # check presense of all required columns
    required_columns = {'_base_dose', '_preferred_unit'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    # check user-defined _preferred_unit are in the set of acceptable units
    unacceptable_preferred_units = set(med_df['_preferred_unit']) - ALL_ACCEPTABLE_UNITS - {None}
    if unacceptable_preferred_units:
        error_msg = f"Cannot accommodate the conversion to the following preferred units: {unacceptable_preferred_units}. Consult the function documentation for a list of acceptable units."
        if override:
            print(error_msg)
        else:
            raise ValueError(error_msg)
    
    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=[L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX],
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=[HR_REGEX],
        else_case='1'
        )

    weight_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_preferred,
        patterns=[KG_REGEX, LB_REGEX],
        else_case='1'
        )
    
    unit_class_clause = f"""
    , _unit_class: CASE
        WHEN _base_unit IN ('{RATE_UNITS_STR}') THEN 'rate' 
        WHEN _base_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
        ELSE 'unrecognized' END
    """ if '_unit_class' not in med_df.columns else ''
    
    weighted_clause = f"""
    , _weighted: CASE
        WHEN regexp_matches(_clean_unit, '{WEIGHT_REGEX}') THEN 1 ELSE 0 END
    """ if '_weighted' not in med_df.columns else ''
    
    dose_converted_name = "med_dose" if "med_dose" in med_df.columns else "_base_dose"
    unit_converted_name = "_clean_unit" if "_clean_unit" in med_df.columns else "_base_unit"
    
    q = f"""
    SELECT l.*
        {unit_class_clause}
        , _unit_subclass: CASE 
            WHEN regexp_matches(_base_unit, '{MASS_REGEX}') THEN 'mass'
            WHEN regexp_matches(_base_unit, '{VOLUME_REGEX}') THEN 'volume'
            WHEN regexp_matches(_base_unit, '{UNIT_REGEX}') THEN 'unit'
            ELSE 'unrecognized' END
        , _unit_class_preferred: CASE 
            WHEN _preferred_unit IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN _preferred_unit IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END
        , _unit_subclass_preferred: CASE 
            WHEN regexp_matches(_preferred_unit, '{MASS_REGEX}') THEN 'mass'
            WHEN regexp_matches(_preferred_unit, '{VOLUME_REGEX}') THEN 'volume'
            WHEN regexp_matches(_preferred_unit, '{UNIT_REGEX}') THEN 'unit'
            ELSE 'unrecognized' END
        , _weighted_preferred: CASE
            WHEN regexp_matches(_preferred_unit, '{WEIGHT_REGEX}') THEN 1 ELSE 0 END
        , _convert_status: CASE 
            WHEN _weighted_preferred = 1 AND weight_kg IS NULL 
                THEN 'cannot convert to a weighted unit if weight_kg is missing'
            WHEN _base_unit IS NULL THEN 'original unit is missing'
            WHEN _unit_class == 'unrecognized' OR _unit_subclass == 'unrecognized'
                THEN 'original unit ' || _base_unit || ' is not recognized'
            WHEN _unit_class_preferred == 'unrecognized' OR _unit_subclass_preferred == 'unrecognized'
                THEN 'user-preferred unit ' || _preferred_unit || ' is not recognized'
            WHEN _unit_class != _unit_class_preferred 
                THEN 'cannot convert ' || _unit_class || ' to ' || _unit_class_preferred
            WHEN _unit_subclass != _unit_subclass_preferred
                THEN 'cannot convert ' || _unit_subclass || ' to ' || _unit_subclass_preferred
            WHEN _unit_class == _unit_class_preferred AND _unit_subclass == _unit_subclass_preferred
                -- AND _unit_class != 'unrecognized' AND _unit_subclass != 'unrecognized'
                THEN 'success'
            ELSE 'other error - please report'
            END
        , _amount_multiplier_preferred: {amount_clause}
        , _time_multiplier_preferred: {time_clause}
        , _weight_multiplier_preferred: {weight_clause}
        -- fall back to the base units and dose (i.e. the input) if conversion cannot be accommondated
        , med_dose_converted: CASE
            WHEN _convert_status == 'success' THEN _base_dose * _amount_multiplier_preferred * _time_multiplier_preferred * _weight_multiplier_preferred
            ELSE {dose_converted_name}
            END
        , med_dose_unit_converted: CASE
            WHEN _convert_status == 'success' THEN _preferred_unit
            ELSE {unit_converted_name}
            END
    FROM med_df l
    """
    return duckdb.sql(q).to_df()

def convert_dose_units_by_med_category(
    med_df: pd.DataFrame,
    vitals_df: pd.DataFrame = None,
    preferred_units: dict = None,
    show_intermediate: bool = False,
    override: bool = False
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Convert medication dose units to user-defined preferred units for each med_category.

    This function performs a two-step conversion process:

    1. Standardizes all dose units to a base set of standard units (mcg/min, ml/min, u/min for rates)
    2. Converts from base units to medication-specific preferred units if provided

    The conversion maintains unit class consistency (rates stay rates, amounts stay amounts)
    and handles weight-based dosing appropriately using patient weights.

    Parameters
    ----------
    med_df : pd.DataFrame
        Medication DataFrame with required columns:

        - med_dose: Original dose values (numeric)
        - med_dose_unit: Original dose unit strings (e.g., 'MCG/KG/HR', 'mL/hr')
        - med_category: Medication category identifier (e.g., 'propofol', 'fentanyl')
        - weight_kg: Patient weight in kg (optional, will be extracted from vitals_df if missing)
    vitals_df : pd.DataFrame, optional
        Vitals DataFrame for extracting patient weights if not in med_df.
        Required columns if weight_kg missing from med_df:

        - hospitalization_id: Patient identifier
        - recorded_dttm: Timestamp of vital recording
        - vital_category: Must include 'weight_kg' values
        - vital_value: Weight values
    preferred_units : dict, optional
        Dictionary mapping medication categories to their preferred units.
        Keys are medication category names, values are target unit strings.
        Example: {'propofol': 'mcg/kg/min', 'fentanyl': 'mcg/hr', 'insulin': 'u/hr'}
        If None, uses base units (mcg/min, ml/min, u/min) as defaults.
    show_intermediate : bool, default False
        If False, excludes intermediate calculation columns (multipliers) from output.
        If True, retains all columns including conversion multipliers for debugging.
    override : bool, default False
        If True, prints warning messages for unacceptable preferred units but continues processing.
        If False, raises ValueError when encountering unacceptable preferred units.

    Returns
    -------
    Tuple[pd.DataFrame, pd.DataFrame]
        A tuple containing:

        - [0] Converted medication DataFrame with additional columns:

            * _clean_unit: Cleaned unit format
            * _base_unit: Base unit after first conversion
            * _base_dose: Dose value in base units
            * _preferred_unit: Target unit for medication category
            * med_dose_converted: Final dose value in preferred units
            * med_dose_unit_converted: Final unit string after conversion
            * _unit_class: Classification ('rate', 'amount', or 'unrecognized')
            * _convert_status: Status message indicating success or reason for failure

            If show_intermediate=True, also includes conversion multipliers.

        - [1] Summary counts DataFrame with conversion statistics grouped by medication category

    Raises
    ------
    ValueError
        If required columns (med_dose_unit, med_dose) are missing from med_df,
        if standardization to base units fails, or if conversion to preferred units fails.

    Examples
    --------
    >>> import pandas as pd
    >>> med_df = pd.DataFrame({
    ...     'med_category': ['propofol', 'fentanyl', 'insulin'],
    ...     'med_dose': [200, 2, 5],
    ...     'med_dose_unit': ['MCG/KG/MIN', 'mcg/kg/hr', 'units/hr'],
    ...     'weight_kg': [70, 80, 75]
    ... })
    >>> preferred = {
    ...     'propofol': 'mcg/kg/min',
    ...     'fentanyl': 'mcg/hr',
    ...     'insulin': 'u/hr'
    ... }
    >>> result_df, counts_df = convert_dose_units_by_med_category(med_df, preferred_units=preferred)

    Notes
    -----
    The function handles various unit formats including:

    - Weight-based dosing: /kg, /lb (uses patient weight for conversion)
    - Time conversions: /hr to /min
    - Volume conversions: L to mL
    - Mass conversions: mg, ng, g to mcg
    - Unit conversions: milli-units (mu) to units (u)

    Unrecognized units are preserved but flagged in the _unit_class column.

    Todo
    ----
    Implement config file parsing for default preferred_units.
    """
    try:
        med_df_base, _ = standardize_dose_to_base_units(med_df, vitals_df)
    except ValueError as e:
        raise ValueError(f"Error standardizing dose units to base units: {e}")
    
    # check if the requested med_categories are in the input med_df
    requested_med_categories = set(preferred_units.keys())
    extra_med_categories = requested_med_categories - set(med_df_base['med_category'])
    if extra_med_categories:
        error_msg = f"The following med_categories are given a preferred unit but not found in the input med_df: {extra_med_categories}"
        if override:
            print(error_msg)
        else:
            raise ValueError(error_msg)
    
    try:
        # join the preferred units to the df
        preferred_units_df = pd.DataFrame(preferred_units.items(), columns=['med_category', '_preferred_unit'])
        q = """
        SELECT l.*
            -- for unspecified preferred units, use the base units by default
            , _preferred_unit: COALESCE(r._preferred_unit, l._base_unit)
        FROM med_df_base l
        LEFT JOIN preferred_units_df r USING (med_category)
        """
        med_df_preferred = duckdb.sql(q).to_df()
        
        med_df_converted = _convert_base_units_to_preferred_units(med_df_preferred, override=override)
    except ValueError as e:
        raise ValueError(f"Error converting dose units to preferred units: {e}")
    
    try:
        convert_counts_df = _create_unit_conversion_counts_table(
            med_df_converted, 
            group_by=[
                'med_category',
                'med_dose_unit', '_clean_unit', '_base_unit', '_unit_class',
                '_preferred_unit', 'med_dose_unit_converted', '_convert_status'
                ]
            )
    except ValueError as e:
        raise ValueError(f"Error creating unit conversion counts table: {e}")
    
    if show_intermediate:
        return med_df_converted, convert_counts_df
    else:
        # the default (detailed_output=False) is to drop multiplier columns which likely are not useful for the user
        multiplier_cols = [col for col in med_df_converted.columns if 'multiplier' in col]
        qa_cols = [
            '_rn',
            '_weight_recorded_dttm',
            '_weighted', '_weighted_preferred',
            '_base_dose', '_base_unit',
            '_preferred_unit',
            '_unit_class_preferred',
            '_unit_subclass', '_unit_subclass_preferred'
            ]
        
        cols_to_drop = [c for c in multiplier_cols + qa_cols if c in med_df_converted.columns]
        
        return med_df_converted.drop(columns=cols_to_drop), convert_counts_df
    
    