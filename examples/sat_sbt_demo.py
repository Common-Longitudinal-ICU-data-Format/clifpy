"""
Demonstration of SAT (Spontaneous Awakening Trial) and SBT (Spontaneous Breathing Trial)
flag identification using CLIFpy.

This example shows how to identify SAT and SBT events from CLIF-formatted ICU data.
"""

import pandas as pd
import numpy as np
from clifpy.utils.sat_sbt_flags import (
    identify_sat_events,
    identify_sbt_events,
    calculate_respiratory_stability,
    calculate_hemodynamic_stability
)


def create_sample_data():
    """
    Create sample patient data for demonstration.
    
    In real use, you would load this from your CLIF tables:
    - medication_admin_continuous
    - respiratory_support
    - adt (for location)
    - patient_assessments
    """
    base_time = pd.Timestamp('2024-01-01 20:00:00')
    
    # Create sample data for 2 patients over multiple days
    data = []
    
    # Patient H001 - Will meet SAT and SBT criteria
    for i in range(12):
        event_time = base_time + pd.Timedelta(hours=i)
        data.append({
            'hospitalization_id': 'H001',
            'event_time': event_time,
            'device_category': 'imv',
            'location_category': 'icu',
            # Sedation medications
            'propofol': 20.0 if i < 8 else 0.0,  # Sedation reduced after 8 hours
            'fentanyl': 100.0 if i < 8 else 0.0,
            'midazolam': 0.0,
            'min_sedation_dose_2': 20.0 if i < 8 else 0.0,  # Combined sedation indicator
            # Paralytics
            'cisatracurium': 0.0,
            'vecuronium': 0.0,
            'max_paralytics': 0.0,
            # Respiratory settings
            'mode_category': 'Pressure Support',
            'pressure_support_set': 5.0,
            'peep_set': 5.0,
            'fio2_set': 0.4,
            # Vasopressors
            'norepinephrine': 0.05 if i < 10 else 0.0,
            'epinephrine': 0.0,
            'phenylephrine': 0.0,
            'vasopressin': 0.0,
            'dopamine': 0.0,
        })
    
    # Patient H002 - Will NOT meet criteria (on high sedation throughout)
    for i in range(12):
        event_time = base_time + pd.Timedelta(hours=i)
        data.append({
            'hospitalization_id': 'H002',
            'event_time': event_time,
            'device_category': 'imv',
            'location_category': 'icu',
            # High sedation throughout
            'propofol': 50.0,
            'fentanyl': 200.0,
            'midazolam': 5.0,
            'min_sedation_dose_2': 50.0,
            # No paralytics
            'cisatracurium': 0.0,
            'vecuronium': 0.0,
            'max_paralytics': 0.0,
            # Respiratory settings
            'mode_category': 'Volume Control',
            'pressure_support_set': 15.0,
            'peep_set': 10.0,
            'fio2_set': 0.6,
            # Vasopressors
            'norepinephrine': 0.2,
            'epinephrine': 0.1,
            'phenylephrine': 50.0,
            'vasopressin': 0.04,
            'dopamine': 5.0,
        })
    
    return pd.DataFrame(data)


def main():
    """Run the SAT/SBT demonstration."""
    
    print("=" * 80)
    print("SAT and SBT Flag Identification Demo")
    print("=" * 80)
    print()
    
    # Create sample data
    print("1. Creating sample patient data...")
    cohort = create_sample_data()
    print(f"   Created data for {cohort['hospitalization_id'].nunique()} patients")
    print(f"   Total records: {len(cohort)}")
    print()
    
    # Identify SAT events
    print("2. Identifying Spontaneous Awakening Trial (SAT) events...")
    print("   SAT criteria:")
    print("   - Patient on invasive mechanical ventilation (IMV)")
    print("   - In ICU")
    print("   - Receiving sedation")
    print("   - No paralytics")
    print("   - Conditions met for ≥4 cumulative hours (10 PM - 6 AM window)")
    print()
    
    sat_events = identify_sat_events(cohort, show_progress=False)
    
    print(f"   Found {len(sat_events)} SAT events:")
    if len(sat_events) > 0:
        print(sat_events[['hospitalization_id', 'current_day_key', 'event_time_at_threshold']])
    else:
        print("   No SAT events identified")
    print()
    
    # Calculate stability flags
    print("3. Calculating respiratory and hemodynamic stability...")
    cohort = calculate_respiratory_stability(cohort)
    cohort = calculate_hemodynamic_stability(cohort)
    
    print("   Respiratory stability criteria:")
    print("   - Mode: Pressure Support or CPAP")
    print("   - Pressure Support ≤ 8 cmH2O")
    print("   - PEEP ≤ 8 cmH2O")
    print("   - FiO2 ≤ 0.5")
    print()
    print("   Hemodynamic stability criteria:")
    print("   - Norepinephrine Equivalent (NEE) ≤ 0.1 mcg/kg/min")
    print()
    
    resp_stable = cohort['Respiratory_Stability'].sum()
    hemo_stable = cohort['Hemodynamic_Stability_by_NEE'].sum()
    print(f"   Respiratory stable records: {resp_stable} / {len(cohort)}")
    print(f"   Hemodynamic stable records: {hemo_stable} / {len(cohort)}")
    print()
    
    # Identify SBT events - Standard mode
    print("4. Identifying Spontaneous Breathing Trial (SBT) eligible days...")
    print("   SBT criteria (Standard mode):")
    print("   - Patient on IMV")
    print("   - In ICU")
    print("   - No paralytics")
    print("   - Conditions met for ≥6 cumulative hours (10 PM - 6 AM window)")
    print()
    
    sbt_standard = identify_sbt_events(
        cohort,
        stability_mode='Standard',
        show_progress=False
    )
    
    eligible_days = sbt_standard[sbt_standard['eligible_day'] == 1]
    print(f"   Standard SBT eligible records: {len(eligible_days)}")
    if len(eligible_days) > 0:
        print(f"   Eligible hospitalizations: {eligible_days['hospitalization_id'].unique()}")
    print()
    
    # Identify SBT events - With both stabilities
    print("5. Identifying SBT with stability requirements...")
    print("   SBT criteria (Both stabilities mode):")
    print("   - All standard criteria")
    print("   - + Respiratory stability")
    print("   - + Hemodynamic stability")
    print()
    
    sbt_both = identify_sbt_events(
        cohort,
        stability_mode='Both_stabilities',
        show_progress=False
    )
    
    eligible_days_both = sbt_both[sbt_both['eligible_day'] == 1]
    print(f"   SBT eligible (both stabilities): {len(eligible_days_both)}")
    if len(eligible_days_both) > 0:
        print(f"   Eligible hospitalizations: {eligible_days_both['hospitalization_id'].unique()}")
    print()
    
    # Summary
    print("=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"SAT events identified:                    {len(sat_events)}")
    print(f"SBT eligible days (standard):             {len(eligible_days)}")
    print(f"SBT eligible days (both stabilities):     {len(eligible_days_both)}")
    print()
    
    # Show example usage in real workflow
    print_integration_example()


def print_integration_example():
    """Print example of integration with CLIF Orchestrator."""
    print("=" * 80)
    print("Example: Integration with CLIF Orchestrator")
    print("=" * 80)
    example_code = '''
# In a real workflow, you would:

from clifpy import ClifOrchestrator
from clifpy.utils import (
    identify_sat_events, 
    identify_sbt_events,
    calculate_respiratory_stability,
    calculate_hemodynamic_stability
)

# Load your CLIF data
co = ClifOrchestrator(data_directory='path/to/clif/data')

# Create merged cohort with required tables
# You would typically merge:
# - respiratory_support (for device, mode, settings)
# - medication_admin_continuous (for sedation, vasopressors, paralytics)
# - adt (for location)
# - patient_assessments (optional, for additional clinical data)

# Example merge (pseudo-code):
cohort = (
    co.respiratory_support.df
    .merge(co.medication_admin_continuous.df, 
           on=['hospitalization_id', 'recorded_dttm'], 
           how='outer')
    .merge(co.adt.df,
           on='hospitalization_id',
           how='left')
)

# Calculate stability flags
cohort = calculate_respiratory_stability(cohort)
cohort = calculate_hemodynamic_stability(cohort)

# Identify SAT events
sat_events = identify_sat_events(cohort)

# Identify SBT eligible days
sbt_eligible = identify_sbt_events(
    cohort,
    stability_mode='Both_stabilities'
)

# Use results for analysis
print(f"Total SAT events: {len(sat_events)}")
print(f"Total SBT eligible days: {sbt_eligible['eligible_day'].sum()}")
    '''
    print(example_code)


if __name__ == "__main__":
    main()
