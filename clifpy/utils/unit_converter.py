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
    # unit
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass
    'mcg': '^(u|µ)g',
    'g': '^g(ram)?',
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

REGEX_MAPPER = {
    # time -> /min
    HR_REGEX: 1/60,
    
    # volume -> ml
    L_REGEX: 1000, # to ml

    # unit -> u
    MU_REGEX: 1/1000,
    
    # mass -> mcg
    MG_REGEX: 1000,
    NG_REGEX: 1/1000,
    G_REGEX: 1000000,
    
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
    acceptable_weight_units = {'/kg', '/lb', ''}
    acceptable_time_units = {'/hr', '/min'}
    # find the cartesian product of the three sets
    return {a + b + c for a in ACCEPTABLE_AMOUNT_UNITS for b in acceptable_weight_units for c in acceptable_time_units}

ACCEPTABLE_RATE_UNITS = acceptable_rate_units()

ALL_ACCEPTABLE_UNITS = ACCEPTABLE_RATE_UNITS | ACCEPTABLE_AMOUNT_UNITS

def _normalize_dose_unit_formats(s: pd.Series) -> pd.Series:
    '''
    'mL / hr' -> 'ml/hr'
    '''
    return s.str.replace(r'\s+', '', regex=True).str.lower()
    
def _normalize_dose_unit_names(s: pd.Series) -> pd.Series:
    '''
    'milliliter/hour', 'milliliter/h', 'ml/hour', etc. -> 'ml/hr'
    
    e.g. 
    s = pd.Series(['milliliter/hour', 'milliliter/h', 'ml/minute', 'ml/m'])
    '''
    for repl, pattern in UNIT_NAMING_VARIANTS.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s

def _detect_and_classify_normalized_dose_units(s: pd.Series) -> dict:
    '''
    [LIKELY DEPRECATED]
    report the distribution of 1. rate units 2. amount units 3. unrecognized units
    '''
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

def when_then_regex_builder(pattern: str) -> str:
    if pattern in REGEX_MAPPER:
        return f"WHEN regexp_matches(med_dose_unit_normalized, '{pattern}') THEN {REGEX_MAPPER.get(pattern)}"
    raise ValueError(f"regex pattern {pattern} not found in CONVERT_FACTORS dict")

def _convert_normalized_dose_units_to_limited_units(med_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Convert normalized dose units to limited units.
    
    Required columns:
    - med_dose_unit_normalized: Standardized unit pattern
    - weight_kg: Patient weight used for conversion (if applicable)
    - med_dose: Original dose value
    
    Returns:
    - med_dose_converted: Dose value in standardized units
    - med_dose_unit_converted: Standardized unit ('mcg/min', 'ml/min', or 'units/min')
    
    e.g.
    'ml/hr' -> 'ml/min'
    'mcg/kg/hr' -> 'mcg/min'
    'mcg/kg/min' -> 'mcg/min'
    'mcg/kg/min' -> 'mcg/min'
    '''
    # RESUME with a duckdb implementation
    RATE_UNITS_STR = "','".join(ACCEPTABLE_RATE_UNITS)
    AMOUNT_UNITS_STR = "','".join(ACCEPTABLE_AMOUNT_UNITS)
    
    q = f"""
    SELECT *
        -- classify and check acceptability first
        , CASE WHEN med_dose_unit_normalized IN ('{RATE_UNITS_STR}') THEN 'rate' 
            WHEN med_dose_unit_normalized IN ('{AMOUNT_UNITS_STR}') THEN 'amount'
            ELSE 'unrecognized' END as unit_class
        -- parse and generate multipliers
        , CASE WHEN unit_class = 'unrecognized' THEN NULL ELSE (
            CASE {when_then_regex_builder(L_REGEX)}
                {when_then_regex_builder(MU_REGEX)}
                {when_then_regex_builder(MG_REGEX)}
                {when_then_regex_builder(NG_REGEX)}
                {when_then_regex_builder(G_REGEX)}
                ELSE 1 END
            ) END AS amount_multiplier
        , CASE WHEN unit_class = 'unrecognized' THEN NULL ELSE (
            CASE {when_then_regex_builder(HR_REGEX)}
                ELSE 1 END
            ) END AS time_multiplier
        , CASE WHEN unit_class = 'unrecognized' THEN NULL ELSE (
            CASE {when_then_regex_builder(KG_REGEX)}
                {when_then_regex_builder(LB_REGEX)}
                ELSE 1 END
            ) END AS weight_multiplier
        -- calculate the converted dose
        , med_dose * amount_multiplier * time_multiplier * weight_multiplier as med_dose_converted
        -- id the converted unit
        , CASE WHEN unit_class = 'unrecognized' THEN NULL
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{MASS_REGEX}') THEN 'mcg/min'
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{VOLUME_REGEX}') THEN 'ml/min'
            WHEN unit_class = 'rate' AND regexp_matches(med_dose_unit_normalized, '{UNIT_REGEX}') THEN 'u/min'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{MASS_REGEX}') THEN 'mcg'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{VOLUME_REGEX}') THEN 'ml'
            WHEN unit_class = 'amount' AND regexp_matches(med_dose_unit_normalized, '{UNIT_REGEX}') THEN 'u'
            END as med_dose_unit_converted
    FROM med_df 
    """
    return duckdb.sql(q).to_df()

def _create_unit_conversion_counts_table(med_df: pd.DataFrame) -> pd.DataFrame:
    '''    
    Need the following columns:
    - med_dose_unit
    - med_dose_unit_normalized
    - med_dose_unit_converted
    - unit_class
    '''
    # check presense of all required columns
    required_columns = {'med_dose_unit', 'med_dose_unit_normalized', 'med_dose_unit_converted', 'unit_class'}
    missing_columns = required_columns - set(med_df.columns)
    if missing_columns:
        raise ValueError(f"The following column(s) are required but not found: {missing_columns}")
    
    q = """
    SELECT med_dose_unit
        , med_dose_unit_normalized
        , med_dose_unit_converted
        , unit_class
        , COUNT(*) as count
    FROM med_df
    GROUP BY med_dose_unit
        , med_dose_unit_normalized
        , med_dose_unit_converted
        , unit_class
    """
    return duckdb.sql(q).to_df()
    

def standardize_dose_to_limited_units(med_df: pd.DataFrame, vitals_df: pd.DataFrame = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Should handle both rate and amount units and report the distribution
    '''
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
        med_df['med_dose_unit'].pipe(_normalize_dose_unit_formats).pipe(_normalize_dose_unit_names)
    )
    return _convert_normalized_dose_units_to_limited_units(med_df) #, _create_unit_conversion_counts_table(med_df)
    
    
    
    
    
    
    