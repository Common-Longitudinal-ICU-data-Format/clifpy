"""
Demo data loading module for pyCLIF.

Provides easy access to sample CLIF data for testing, learning, and demonstration.
Similar to sklearn's datasets module.
"""

from .loader import (
    load_demo_clif, 
    load_demo_patient,
    load_demo_adt,
    load_demo_hospitalization,
    load_demo_labs,
    load_demo_vitals,
    load_demo_respiratory_support,
    load_demo_position,
    load_demo_medication_admin_continuous,
    load_demo_patient_assessments,
    list_demo_datasets,
    get_demo_summary
)

__all__ = [
    'load_demo_clif',
    'load_demo_patient', 
    'load_demo_adt',
    'load_demo_hospitalization',
    'load_demo_labs',
    'load_demo_vitals',
    'load_demo_respiratory_support',
    'load_demo_position',
    'load_demo_medication_admin_continuous',
    'load_demo_patient_assessments',
    'list_demo_datasets',
    'get_demo_summary'
]