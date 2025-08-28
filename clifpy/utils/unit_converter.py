'''
In general, would convert both rate and amount indiscriminately and report them as well as unrecognized units.
'''

import pandas as pd
import duckdb
from typing import Set

REGEX_PATTERNS = {
    # time
    '/hr': '/h(r|our)?\\b',
    '/min': '/m(in|inute)?\\b',
    # unit
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    ,
    # mass
    'mcg': '\\b(u|µ)g'
}

CONVERT_FACTORS = {
    # time -> /min
    '/hr': 1/60,
    
    # volume -> ml
    '\\bl(\\b|/)': 1000, # to ml

    # unit -> u
    'mu': 1/1000,
    
    # mass -> mcg
    "mg": 1000,
    "ng": 1/1000,
    "\\bg": 1000000,
    
    # weight -> /kg
    '/lb': 2.20462
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
    for repl, pattern in REGEX_PATTERNS.items():
        s = s.str.replace(pattern, repl, regex=True)
    return s

def _detect_and_classify_normalized_dose_units(s: pd.Series) -> dict:
    '''
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

def _convert_normalized_dose_units_to_limited_units(df: pd.DataFrame) -> pd.DataFrame:
    '''
    'ml/hr' -> 'ml/min'
    'mcg/kg/hr' -> 'mcg/min'
    'mcg/kg/min' -> 'mcg/min'
    'mcg/kg/min' -> 'mcg/min'
    '''
    pass

def standardize_dose_to_limited_units():
    '''
    Should handle both rate and amount units and report the distribution
    '''
    pass