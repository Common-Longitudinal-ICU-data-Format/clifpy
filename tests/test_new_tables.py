#!/usr/bin/env python3
"""
Test script to verify all new table classes can be imported and instantiated.
"""

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from clifpy import (
    CrrtTherapy,
    PatientProcedures,
    MicrobiologySusceptibility,
    EcmoMcs,
    MicrobiologyNonculture,
    CodeStatus,
    MicrobiologyCulture,
    HospitalDiagnosis,
    MedicationAdminIntermittent
)

def test_table_imports():
    """Test that all new table classes can be imported."""
    print("Testing table imports...")
    
    tables_to_test = [
        CrrtTherapy,
        PatientProcedures,
        MicrobiologySusceptibility,
        EcmoMcs,
        MicrobiologyNonculture,
        CodeStatus,
        MicrobiologyCulture,
        HospitalDiagnosis,
        MedicationAdminIntermittent
    ]
    
    for table_class in tables_to_test:
        print(f"✓ {table_class.__name__} imported successfully")
    
    print("\nAll imports successful!")


def test_table_instantiation():
    """Test that all new table classes can be instantiated."""
    print("\nTesting table instantiation...")
    
    temp_dir = "/tmp/test_clif_tables"
    os.makedirs(temp_dir, exist_ok=True)
    
    tables_to_test = [
        ("CrrtTherapy", CrrtTherapy),
        ("PatientProcedures", PatientProcedures),
        ("MicrobiologySusceptibility", MicrobiologySusceptibility),
        ("EcmoMcs", EcmoMcs),
        ("MicrobiologyNonculture", MicrobiologyNonculture),
        ("CodeStatus", CodeStatus),
        ("MicrobiologyCulture", MicrobiologyCulture),
        ("HospitalDiagnosis", HospitalDiagnosis),
        ("MedicationAdminIntermittent", MedicationAdminIntermittent)
    ]
    
    for table_name, table_class in tables_to_test:
        try:
            instance = table_class(
                data_directory=temp_dir,
                filetype='csv',
                timezone='UTC',
                output_directory=temp_dir
            )
            expected_snake_case = table_name[0].lower() + ''.join(['_' + c.lower() if c.isupper() else c for c in table_name[1:]])
            print(f"✓ {table_name} instantiated successfully (table_name: {instance.table_name})")
        except Exception as e:
            print(f"✗ {table_name} failed to instantiate: {str(e)}")
    
    print("\nInstantiation tests complete!")


def test_orchestrator_registration():
    """Test that all tables are registered in ClifOrchestrator."""
    print("\nTesting ClifOrchestrator registration...")
    
    from clifpy.clif_orchestrator import TABLE_CLASSES
    
    expected_tables = [
        'crrt_therapy',
        'patient_procedures',
        'microbiology_susceptibility',
        'ecmo_mcs',
        'microbiology_nonculture',
        'code_status',
        'microbiology_culture',
        'hospital_diagnosis',
        'medication_admin_intermittent'
    ]
    
    for table_name in expected_tables:
        if table_name in TABLE_CLASSES:
            print(f"✓ {table_name} registered in ClifOrchestrator")
        else:
            print(f"✗ {table_name} NOT registered in ClifOrchestrator")
    
    print("\nOrchestrator registration tests complete!")


def main():
    """Run all tests."""
    test_table_imports()
    test_table_instantiation()
    test_orchestrator_registration()
    
    print("\n" + "="*50)
    print("All tests completed!")


if __name__ == "__main__":
    main()