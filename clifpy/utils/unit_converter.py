'''
In general, would convert both rate and amount indiscriminately and report them as well as unrecognized units.
'''

import pandas as pd
import duckdb

REGEX_PATTERNS = {
    # time
    '/hr': '/h(r|our)?\\b',
    '/min': '/m(in|inute)?\\b',
    # unit
    'u': 'u(nits|nit)?',
    # milli
    'm': 'milli-?',
    # volume
    "l": 'l(iters|itres|itre|iter)?'    
}

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
    s = pd.Series(['milliliter/hour', 'milliliters/hour', 'millilitres/hour', 'millilitre/h', 'ml/minute', 'ml/m', 'units/lb/hr', 'unit/lb/hr', 'u/kg/h', 'mcg', 'mg', 'ng', 'milli-units/kg/min', 'milli-unit/kg/min', 'milliunits/kg/min', 'milliunit/kg/min'])
    for repl, pattern in REGEX_PATTERNS.items():
        s = s.str.replace(pattern, repl, regex=True)
    s

def _detect_and_classify_normalized_dose_units():
    '''
    report the distribution of 1. rate units 2. amount units 3. unrecognized units
    '''
    pass
   
def _convert_normalized_dose_units_to_limited_units():
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