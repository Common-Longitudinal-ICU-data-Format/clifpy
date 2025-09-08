'''
In general, convert both rate and amount indiscriminately and report them as well as unrecognized units.
'''

import pandas as pd
import duckdb
from typing import Set, Tuple

UNIT_NAMING_VARIANTS = {
    # time
    '/hr': '/h(r|our)?$',
    '/min': '/m(in|inute)?$',
    # unit -- NOTE: plaural always go first to avoid having result like "us"
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass
    'mcg': '^(u|µ|μ)g',
    'g': '^g(rams|ram)?',
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
    }

def acceptable_rate_units() -> Set[str]:
    """
    Generate all acceptable rate unit combinations.
    
    Creates a cartesian product of amount units, weight qualifiers, and time units
    to generate all valid rate unit patterns that the converter can handle.
    
    Returns
    -------
    Set[str]
        Set of all valid rate unit combinations.
        
    Examples
    --------
    >>> rate_units = acceptable_rate_units()
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

ACCEPTABLE_RATE_UNITS = acceptable_rate_units()

ALL_ACCEPTABLE_UNITS = ACCEPTABLE_RATE_UNITS | ACCEPTABLE_AMOUNT_UNITS

def convert_set_to_str_for_sql(s: Set[str]) -> str:
    return "','".join(s)

RATE_UNITS_STR = convert_set_to_str_for_sql(ACCEPTABLE_RATE_UNITS)
AMOUNT_UNITS_STR = convert_set_to_str_for_sql(ACCEPTABLE_AMOUNT_UNITS)

def _normalize_dose_unit_formats(s: pd.Series) -> pd.Series:
    """
    Normalize dose unit formatting by removing spaces and converting to lowercase.
    
    This is the first step in the normalization pipeline. It standardizes
    the basic formatting of dose units before applying name normalization.
    
    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings to normalize.
        
    Returns
    -------
    pd.Series
        Series with normalized formatting (no spaces, lowercase).
        
    Examples
    --------
    >>> s = pd.Series(['mL / hr', 'MCG/KG/MIN', ' Mg/Hr '])
    >>> _normalize_dose_unit_formats(s)
    0        ml/hr
    1    mcg/kg/min
    2        mg/hr
    dtype: object
    
    Notes
    -----
    This function is typically used as the first step in the normalization
    pipeline, followed by _normalize_dose_unit_names().
    """
    return s.str.replace(r'\s+', '', regex=True).str.lower().replace('', None, regex=False)
    
def _normalize_dose_unit_names(s: pd.Series) -> pd.Series:
    """
    Normalize dose unit name variants to standard abbreviations.
    
    Applies regex patterns to convert various unit name variants to their
    standard abbreviated forms (e.g., 'milliliter' -> 'ml', 'hour' -> 'hr').
    
    Parameters
    ----------
    s : pd.Series
        Series containing dose unit strings with name variants.
        Should already be format-normalized (lowercase, no spaces).
        
    Returns
    -------
    pd.Series
        Series with limited unit names.
        
    Examples
    --------
    >>> s = pd.Series(['milliliter/hour', 'units/minute', 'µg/kg/h'])
    >>> _normalize_dose_unit_names(s)
    0        ml/hr
    1        u/min
    2    mcg/kg/hr
    dtype: object
    
    Notes
    -----
    Handles conversions including:
    - Time: hour/h -> hr, minute/m -> min
    - Volume: liter/liters/litre/litres -> l
    - Units: units/unit -> u, milli-units -> mu
    - Mass: µg/ug -> mcg, gram -> g
    
    This function should be applied after _normalize_dose_unit_formats().
    """
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s

def _detect_and_classify_normalized_dose_units(s: pd.Series) -> dict:
    """
    Classify and count normalized dose units by category.
    
    [LIKELY DEPRECATED - Consider using _create_unit_conversion_counts_table instead]
    
    Analyzes a series of normalized dose units and classifies them into
    rate units, amount units, or unrecognized units, providing counts for each.
    
    Parameters
    ----------
    s : pd.Series
        Series containing normalized dose unit strings.
        
    Returns
    -------
    dict
        Dictionary with three keys:
        - 'rate_units': dict of recognized rate units and their counts
        - 'amount_units': dict of recognized amount units and their counts  
        - 'unrecognized_units': dict of unrecognized units and their counts
        
    Examples
    --------
    >>> s = pd.Series(['ml/hr', 'ml/hr', 'mcg', 'unknown_unit', None])
    >>> result = _detect_and_classify_normalized_dose_units(s)
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
    return "CASE " + " ".join([builder(pattern) for pattern in patterns]) + f" ELSE {else_case} END"

def _pattern_to_factor_builder_for_limited(pattern: str) -> str:
    """
    Build SQL CASE WHEN statement for regex pattern matching.
    
    Helper function that generates SQL CASE WHEN clauses for DuckDB queries
    based on regex patterns and their corresponding conversion factors.
    
    Parameters
    ----------
    pattern : str
        Regex pattern to match (must exist in REGEX_MAPPER).
        
    Returns
    -------
    str
        SQL CASE WHEN clause string.
        
    Raises
    ------
    ValueError
        If the pattern is not found in REGEX_MAPPER.
        
    Examples
    --------
    >>> when_then_regex_builder(HR_REGEX)
    "WHEN regexp_matches(med_dose_unit_normalized, '/hr$') THEN 0.016666666666666666"
    
    Notes
    -----
    This function is used internally by _convert_normalized_dose_units_to_limited_units
    to build the SQL query for unit conversion.
    """
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(med_dose_unit_normalized, '{pattern}') THEN {REGEX_TO_FACTOR_MAPPER.get(pattern)}"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _pattern_to_factor_builder_for_preferred(pattern: str) -> str:
    if pattern in REGEX_TO_FACTOR_MAPPER:
        return f"WHEN regexp_matches(med_dose_unit_preferred, '{pattern}') THEN 1/({REGEX_TO_FACTOR_MAPPER.get(pattern)})"
    raise ValueError(f"regex pattern {pattern} not found in REGEX_TO_FACTOR_MAPPER dict")

def _convert_normalized_dose_units_to_limited_units(med_df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert normalized dose units to limited limited units.
    
    Core conversion function that transforms various dose units into a limited
    set of standard units (mcg/min, ml/min, u/min for rates; mcg, ml, u for amounts).
    Uses DuckDB for efficient SQL-based transformations.
    
    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame containing medication data with required columns:
        - med_dose_unit_normalized: Normalized unit strings
        - med_dose: Original dose values
        - weight_kg: Patient weight (used for /kg and /lb conversions)
        
    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns:
        - unit_class: 'rate', 'amount', or 'unrecognized'
        - amount_multiplier: Factor for amount conversion
        - time_multiplier: Factor for time conversion (hr to min)
        - weight_multiplier: Factor for weight-based conversion
        - med_dose_limited: limited dose value
        - med_dose_unit_limited: limited unit string
        
    Examples
    --------
    >>> df = pd.DataFrame({
    ...     'med_dose': [6, 100],
    ...     'med_dose_unit_normalized': ['mcg/kg/hr', 'ml/hr'],
    ...     'weight_kg': [70, 80]
    ... })
    >>> result = _convert_normalized_dose_units_to_limited_units(df)
    >>> result[['med_dose_limited', 'med_dose_unit_limited']]
       med_dose_limited med_dose_unit_limited
    0                  7.0                 mcg/min
    1                  1.67                 ml/min
    
    Notes
    -----
    Conversion targets:
    - Rate units: mcg/min, ml/min, u/min
    - Amount units: mcg, ml, u
    - Unrecognized units: NULL values in limited columns
    
    Weight-based conversions use patient weight from weight_kg column.
    Time conversions: /hr -> /min (divide by 60).
    """
    
    amount_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_limited,
        patterns=[L_REGEX, MU_REGEX, MG_REGEX, NG_REGEX, G_REGEX],
        else_case='1'
        )

    time_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_limited,
        patterns=[HR_REGEX],
        else_case='1'
        )

    weight_clause = _concat_builders_by_patterns(
        builder=_pattern_to_factor_builder_for_limited,
        patterns=[KG_REGEX, LB_REGEX],
        else_case='1'
        )
    
    q = f"""
    SELECT *
        -- classify and check acceptability first
        , unit_class: CASE WHEN med_dose_unit_normalized IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN med_dose_unit_normalized IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END
        -- parse and generate multipliers
        , amount_multiplier: CASE WHEN unit_class = 'unrecognized' THEN 1 ELSE (
            {amount_clause}
            ) END 
        , time_multiplier: CASE WHEN unit_class = 'unrecognized' THEN 1 ELSE (
            {time_clause}
            ) END 
        , weight_multiplier: CASE WHEN unit_class = 'unrecognized' THEN 1 ELSE (
            {weight_clause}
            ) END
        -- calculate the limited dose
        , med_dose_limited: med_dose * amount_multiplier * time_multiplier * weight_multiplier 
        -- id the limited unit
        , med_dose_unit_limited: CASE WHEN unit_class = 'unrecognized' THEN med_dose_unit_normalized
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{MASS_REGEX}') THEN 'mcg/min'
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{VOLUME_REGEX}') THEN 'ml/min'
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{UNIT_REGEX}') THEN 'u/min'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{MASS_REGEX}') THEN 'mcg'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{VOLUME_REGEX}') THEN 'ml'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{UNIT_REGEX}') THEN 'u'
            END
    FROM med_df 
    """
    return duckdb.sql(q).to_df()

def _create_unit_conversion_counts_table(med_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create summary table of unit conversion counts.
    
    Generates a grouped summary showing the frequency of each unit conversion
    pattern, useful for data quality assessment and identifying common or
    problematic unit patterns.
    
    Parameters
    ----------
    med_df : pd.DataFrame
        DataFrame with required columns from conversion process:
        - med_dose_unit: Original unit string
        - med_dose_unit_normalized: Normalized unit string
        - med_dose_unit_limited: limited standard unit
        - unit_class: Classification (rate/amount/unrecognized)
        
    Returns
    -------
    pd.DataFrame
        Summary DataFrame with columns:
        - med_dose_unit: Original unit
        - med_dose_unit_normalized: After normalization
        - med_dose_unit_limited: After conversion
        - unit_class: Classification
        - count: Number of occurrences
        
    Raises
    ------
    ValueError
        If required columns are missing from input DataFrame.
        
    Examples
    --------
    >>> df_limited = standardize_dose_to_limited_units(med_df)[0]
    >>> counts = _create_unit_conversion_counts_table(df_limited)
    >>> counts.head()
       med_dose_unit med_dose_unit_normalized med_dose_unit_limited unit_class  count
    0      MCG/KG/HR              mcg/kg/hr                 mcg/min       rate     15
    1         ml/hr                  ml/hr                  ml/min       rate     10
    2            mg                     mg                     mcg     amount      5
    
    Notes
    -----
    This table is particularly useful for:
    - Identifying unrecognized units that need handling
    - Understanding the distribution of unit types in your data
    - Quality control and validation of conversions
    """
    # check presense of all required columns
    required_columns = {'med_dose_unit', 'med_dose_unit_normalized', 'med_dose_unit_limited', 'unit_class'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    q = """
    SELECT med_dose_unit
        , med_dose_unit_normalized
        , med_dose_unit_limited
        , unit_class
        , COUNT(*) as count
    FROM med_df
    GROUP BY med_dose_unit
        , med_dose_unit_normalized
        , med_dose_unit_limited
        , unit_class
    """
    return duckdb.sql(q).to_df()
    

def standardize_dose_to_limited_units(
    med_df: pd.DataFrame, 
    vitals_df: pd.DataFrame = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Standardize medication dose units to a limited set of standard units.
    
    Main public API function that performs complete dose unit standardization
    pipeline: format normalization, name normalization, and unit conversion.
    Returns both limited data and a summary table of conversions.
    
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
        - [0] limited medication DataFrame with additional columns:
            * med_dose_unit_normalized: Normalized unit string
            * unit_class: 'rate', 'amount', or 'unrecognized'
            * med_dose_limited: limited dose value
            * med_dose_unit_limited: limited unit
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
    >>> limited_df, counts_df = standardize_dose_to_limited_units(med_df)
    >>> limited_df[['med_dose_unit', 'med_dose_limited', 'med_dose_unit_limited']]
       med_dose_unit  med_dose_limited med_dose_unit_limited
    0      MCG/KG/HR                 7.0                 mcg/min
    1        mL / hr                1.67                  ml/min
    2             mg              500000                     mcg
    
    >>> counts_df
       med_dose_unit med_dose_unit_normalized med_dose_unit_limited unit_class  count
    0      MCG/KG/HR              mcg/kg/hr                 mcg/min       rate      1
    1        mL / hr                  ml/hr                  ml/min       rate      1
    2             mg                     mg                     mcg     amount      1
    
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
            , v.recorded_dttm as weight_recorded_dttm
            , ROW_NUMBER() OVER (
                PARTITION BY m.hospitalization_id, m.admin_dttm, m.med_category
                ORDER BY v.recorded_dttm DESC
                ) as rn
        FROM med_df m
        LEFT JOIN vitals_df v 
            ON m.hospitalization_id = v.hospitalization_id 
            AND v.vital_category = 'weight_kg' AND v.vital_value IS NOT NULL
            AND v.recorded_dttm <= m.admin_dttm  -- only past weights
        -- rn = 1 for the weight w/ the latest recorded_dttm (and thus most recent)
        QUALIFY (rn = 1) 
        ORDER BY m.hospitalization_id, m.admin_dttm, m.med_category, rn
        """
        med_df = duckdb.sql(query).to_df()
    
    # check if the required columns are present
    required_columns = {'med_dose_unit', 'med_dose', 'weight_kg'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    med_df['med_dose_unit_normalized'] = (
        med_df['med_dose_unit'].pipe(_normalize_dose_unit_formats)
        .pipe(_normalize_dose_unit_names)
    )
    
    med_df_limited = _convert_normalized_dose_units_to_limited_units(med_df)
    
    return med_df_limited, _create_unit_conversion_counts_table(med_df_limited)
    
def _convert_limited_units_to_preferred_units(
    med_df: pd.DataFrame,
    override: bool = False
    ) -> pd.DataFrame:
    """
    rules:
    - conversion only within the same unit class (e.g. rate only rate and NOT to amount)
    - cannot convert from g to ml
    
    e.g. assumes the presense of 
    - med_dose_limited
    - med_dose_unit_limited
    - med_dose_unit_preferred -- already specified
    
    Override = ignore the error and convert anyway
    """
    # check presense of all required columns
    required_columns = {'med_dose_limited', 'med_dose_unit_preferred'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    # check user-defined med_dose_unit_preferred are in the set of acceptable units
    unacceptable_preferred_units = set(med_df['med_dose_unit_preferred']) - ALL_ACCEPTABLE_UNITS
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
    , unit_class: CASE WHEN med_dose_unit_limited IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN med_dose_unit_limited IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END
    """ if 'unit_class' not in med_df.columns else ''
    
    q = f"""
    SELECT *
        {unit_class_clause}
        , unit_subclass: CASE 
            WHEN regexp_matches(med_dose_unit_limited, '{MASS_REGEX}') THEN 'mass'
            WHEN regexp_matches(med_dose_unit_limited, '{VOLUME_REGEX}') THEN 'volume'
            WHEN regexp_matches(med_dose_unit_limited, '{UNIT_REGEX}') THEN 'unit'
            ELSE 'unrecognized' END
        , unit_class_preferred: CASE WHEN med_dose_unit_preferred IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN med_dose_unit_preferred IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END
        , unit_subclass_preferred: CASE 
            WHEN regexp_matches(med_dose_unit_preferred, '{MASS_REGEX}') THEN 'mass'
            WHEN regexp_matches(med_dose_unit_preferred, '{VOLUME_REGEX}') THEN 'volume'
            WHEN regexp_matches(med_dose_unit_preferred, '{UNIT_REGEX}') THEN 'unit'
            ELSE 'unrecognized' END
        , convert_status: CASE 
            WHEN unit_class == 'unrecognized' OR unit_subclass == 'unrecognized'
                THEN 'original unit ' || med_dose_unit_limited || ' is not recognized'
            WHEN unit_class_preferred == 'unrecognized' OR unit_subclass_preferred == 'unrecognized'
                THEN 'user-defined preferred unit ' || med_dose_unit_preferred || ' is not recognized'
            WHEN unit_class != unit_class_preferred 
                THEN 'cannot convert ' || unit_class || ' to ' || unit_class_preferred
            WHEN unit_subclass != unit_subclass_preferred
                THEN 'cannot convert ' || unit_subclass || ' to ' || unit_subclass_preferred
            WHEN unit_class == unit_class_preferred AND unit_subclass == unit_subclass_preferred
                -- AND unit_class != 'unrecognized' AND unit_subclass != 'unrecognized'
                THEN 'success'
            ELSE 'other error - please report'
            END
        , amount_multiplier_preferred: {amount_clause}
        , time_multiplier_preferred: {time_clause}
        , weight_multiplier_preferred: {weight_clause}
        -- fall back to the limited units and dose (i.e. the input) if conversion cannot be accommondated
        , med_dose_converted: CASE
            WHEN convert_status == 'success' THEN ROUND(med_dose_limited * amount_multiplier_preferred * time_multiplier_preferred * weight_multiplier_preferred, 2)
            ELSE med_dose_limited
            END
        , med_dose_unit_converted: CASE
            WHEN convert_status == 'success' THEN med_dose_unit_preferred
            ELSE med_dose_unit_limited
            END
    FROM med_df l
    -- LEFT JOIN preferred_units_df r USING (med_category)
    """
    return duckdb.sql(q).to_df()

def convert_dose_units_by_med_category(
    med_df: pd.DataFrame,
    vitals_df: pd.DataFrame = None,
    preferred_units: dict = None,
    verbose: bool = False,
    override: bool = False
    ) -> pd.DataFrame:
    """
    Convert medication dose units to user-defined preferred units for each med_category.
    
    This function performs a two-step conversion process:
    1. Standardizes all dose units to a limited set of base units (mcg/min, ml/min, u/min for rates)
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
        - weight_kg: Patient weight in kg (optional, will be extracted from vitals if missing)
        
    preferred_units : dict, optional
        Dictionary mapping medication categories to their preferred units.
        Keys are medication category names, values are target unit strings.
        Example: {'propofol': 'mcg/kg/min', 'fentanyl': 'mcg/hr', 'insulin': 'u/hr'}
        If None, uses limited units (mcg/min, ml/min, u/min) as defaults.
        
    verbose : bool, default False
        If False, excludes intermediate calculation columns (multipliers) from output.
        If True, retains all columns including conversion multipliers for debugging.
        
    Returns
    -------
    pd.DataFrame
        Original DataFrame with additional columns:
        - med_dose_unit_normalized: Standardized unit format
        - med_dose_unit_limited: Base unit after first conversion
        - med_dose_limited: Dose value in base units
        - med_dose_unit_preferred: Target unit for medication category
        - med_dose_preferred: Final dose value in preferred units
        - unit_class: Classification ('rate', 'amount', or 'unrecognized')
        
        If verbose=True, also includes:
        - amount_multiplier, time_multiplier, weight_multiplier: Conversion factors
        - amount_multiplier_preferred, time_multiplier_preferred, weight_multiplier_preferred
        
    Raises
    ------
    ValueError
        - If required columns (med_dose_unit, med_dose) are missing from med_df
        - If standardization to limited units fails
        - If conversion to preferred units fails
        
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
    ...     'propofol': 'mcg/min',
    ...     'fentanyl': 'mcg/hr', 
    ...     'insulin': 'u/hr'
    ... }
    >>> result = convert_dose_units_by_med_category(med_df, preferred)
    >>> result[['med_category', 'med_dose_preferred', 'med_dose_unit_preferred']]
       med_category  med_dose_preferred med_dose_unit_preferred
    0      propofol              14000.0                  mcg/min
    1      fentanyl                160.0                   mcg/hr
    2       insulin                  5.0                     u/hr
    
    Notes
    -----
    NOTE: default is to parse preferred_units from a config file, but will be overridden if preferred_units is provided
    
    The function handles various unit formats including:
    - Weight-based dosing: /kg, /lb (uses patient weight for conversion)
    - Time conversions: /hr to /min
    - Volume conversions: L to mL
    - Mass conversions: mg, ng, g to mcg
    - Unit conversions: milli-units (mu) to units (u)
    
    Unrecognized units are preserved but flagged in the unit_class column.
    
    Developer TODOs
    ---------------
    - [] Implement config file parsing for default preferred_units
    """
    try:
        med_df_limited, _ = standardize_dose_to_limited_units(med_df, vitals_df)
    except ValueError as e:
        raise ValueError(f"Error standardizing dose units to limited units: {e}")
    
    try:
        # join the preferred units to the df
        preferred_units_df = pd.DataFrame(preferred_units.items(), columns=['med_category', 'med_dose_unit_preferred'])
        q = """
        SELECT *
            -- for unspecified preferred units, use the limited units by default
            , med_dose_unit_preferred: COALESCE(r.med_dose_unit_preferred, l.med_dose_unit_limited)
        FROM med_df_limited l
        LEFT JOIN preferred_units_df r USING (med_category)
        """
        med_df_preferred = duckdb.sql(q).to_df()
        
        med_df_converted = _convert_limited_units_to_preferred_units(med_df_preferred, override=override)
    except ValueError as e:
        raise ValueError(f"Error converting dose units to preferred units: {e}")
    
    if verbose:
        return med_df_converted
    # the default (verbose=False) is to drop multiplier columns which likely are not useful for the user
    
    cols_to_drop = [col for col in med_df_converted.columns if 'multiplier' in col]
    return med_df_converted.drop(columns=cols_to_drop)
    
    