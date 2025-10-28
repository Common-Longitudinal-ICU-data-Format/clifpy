"""
Example: Computing Adult Sepsis Event (ASE) flags using CDC criteria

This example demonstrates how to use the compute_sepsis function to identify
sepsis cases in hospitalized patients based on CDC surveillance criteria.

ASE requires BOTH:
- A. Presumed Infection (blood culture + qualifying antibiotic days)
- B. Organ Dysfunction (vasopressors, mechanical ventilation, or lab criteria)
"""

import pandas as pd
from datetime import datetime, timedelta
from clifpy.utils.sepsis import compute_sepsis

def create_sample_data():
    """Create sample CLIF data for demonstration."""
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    
    # Sample blood culture data
    # Patient with sepsis will have blood culture drawn
    blood_cultures = pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'collect_dttm': [
            base_time,
            base_time + timedelta(hours=6),
            base_time + timedelta(days=1)
        ],
        'fluid_category': ['blood/buffy coat'] * 3
    })
    
    # Sample antibiotic administration data
    # H001: Gets 5 consecutive days of antibiotics (meets QAD ≥4)
    # H002: Gets only 2 days (doesn't meet QAD)
    # H003: Gets 4 days (meets QAD)
    antibiotics_data = []
    
    for day in range(5):
        antibiotics_data.append({
            'hospitalization_id': 'H001',
            'admin_dttm': base_time + timedelta(days=day, hours=8),
            'med_group': 'CMS_sepsis_qualifying_antibiotics'
        })
    
    for day in range(2):
        antibiotics_data.append({
            'hospitalization_id': 'H002',
            'admin_dttm': base_time + timedelta(hours=6) + timedelta(days=day, hours=8),
            'med_group': 'CMS_sepsis_qualifying_antibiotics'
        })
    
    for day in range(4):
        antibiotics_data.append({
            'hospitalization_id': 'H003',
            'admin_dttm': base_time + timedelta(days=1) + timedelta(days=day, hours=8),
            'med_group': 'CMS_sepsis_qualifying_antibiotics'
        })
    
    antibiotics = pd.DataFrame(antibiotics_data)
    
    # Sample hospitalization data
    hospitalization = pd.DataFrame({
        'hospitalization_id': ['H001', 'H002', 'H003'],
        'patient_id': ['P001', 'P002', 'P003'],
        'discharge_dttm': [
            base_time + timedelta(days=10),
            base_time + timedelta(days=5),
            base_time + timedelta(days=12)
        ],
        'discharge_category': ['Home', 'Home', 'Home']
    })
    
    # Sample lab data
    # H001: Will have AKI (creatinine doubling)
    # H003: Will have elevated lactate
    labs_data = [
        # H001 - Baseline creatinine
        {
            'hospitalization_id': 'H001',
            'lab_category': 'creatinine',
            'lab_value_numeric': 1.0,
            'lab_result_dttm': base_time - timedelta(hours=6)
        },
        # H001 - Doubled creatinine (AKI)
        {
            'hospitalization_id': 'H001',
            'lab_category': 'creatinine',
            'lab_value_numeric': 2.5,
            'lab_result_dttm': base_time + timedelta(hours=18)
        },
        # H003 - Elevated lactate
        {
            'hospitalization_id': 'H003',
            'lab_category': 'lactate',
            'lab_value_numeric': 3.5,
            'lab_result_dttm': base_time + timedelta(days=1, hours=6)
        }
    ]
    labs = pd.DataFrame(labs_data)
    
    # Sample continuous medication data (vasopressors)
    # H001: Gets norepinephrine within window
    continuous_meds = pd.DataFrame({
        'hospitalization_id': ['H001'],
        'admin_dttm': [base_time + timedelta(hours=12)],
        'med_category': ['norepinephrine'],
        'med_dose': [0.1]
    })
    
    # Sample respiratory support data
    # H003: Gets invasive mechanical ventilation
    respiratory_support = pd.DataFrame({
        'hospitalization_id': ['H003'],
        'recorded_dttm': [base_time + timedelta(days=1, hours=12)],
        'device_category': ['IMV']
    })
    
    return {
        'blood_cultures': blood_cultures,
        'antibiotics': antibiotics,
        'hospitalization': hospitalization,
        'labs': labs,
        'continuous_meds': continuous_meds,
        'respiratory_support': respiratory_support
    }


def main():
    """Run the sepsis computation example."""
    print("=" * 80)
    print("Adult Sepsis Event (ASE) Computation Example")
    print("=" * 80)
    print()
    
    # Create sample data
    print("Creating sample data...")
    data = create_sample_data()
    
    print(f"  - {len(data['blood_cultures'])} hospitalizations with blood cultures")
    print(f"  - {len(data['antibiotics'])} antibiotic administrations")
    print(f"  - {len(data['labs'])} lab results")
    print(f"  - {len(data['continuous_meds'])} vasopressor administrations")
    print(f"  - {len(data['respiratory_support'])} IMV observations")
    print()
    
    # Compute sepsis
    print("Computing Adult Sepsis Events...")
    sepsis_results = compute_sepsis(
        blood_cultures=data['blood_cultures'],
        antibiotics=data['antibiotics'],
        hospitalization=data['hospitalization'],
        labs=data['labs'],
        continuous_meds=data['continuous_meds'],
        respiratory_support=data['respiratory_support'],
        window_days=2,
        include_lactate=True
    )
    
    print()
    print("Results:")
    print("-" * 80)
    
    if len(sepsis_results) == 0:
        print("No sepsis cases identified")
    else:
        print(f"Identified {len(sepsis_results)} sepsis case(s):")
        print()
        print(sepsis_results.to_string(index=False))
        print()
        
        # Summary statistics
        if 'ase_flag' in sepsis_results.columns:
            n_sepsis = sepsis_results['ase_flag'].sum()
            print(f"\nTotal sepsis cases: {n_sepsis}")
        
        if 'organ_dysfunction_type' in sepsis_results.columns:
            print("\nOrgan dysfunction types:")
            dysfunction_counts = sepsis_results['organ_dysfunction_type'].value_counts()
            for dtype, count in dysfunction_counts.items():
                print(f"  - {dtype}: {count}")
    
    print()
    print("=" * 80)
    print("Interpretation:")
    print("-" * 80)
    print("H001 - Expected to have sepsis:")
    print("  ✓ Presumed infection: Blood culture + 5 days of antibiotics")
    print("  ✓ Organ dysfunction: AKI (creatinine doubled) + vasopressor")
    print()
    print("H002 - Expected to NOT have sepsis:")
    print("  ✗ Insufficient antibiotics: Only 2 days (needs ≥4)")
    print()
    print("H003 - Expected to have sepsis:")
    print("  ✓ Presumed infection: Blood culture + 4 days of antibiotics")
    print("  ✓ Organ dysfunction: Elevated lactate + IMV")
    print("=" * 80)


if __name__ == "__main__":
    main()
