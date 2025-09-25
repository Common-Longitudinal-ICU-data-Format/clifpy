from . import data
# Re-export table classes at package root
from .tables import (
    Patient,
    Adt,
    Hospitalization,
    HospitalDiagnosis,
    Labs,
    RespiratorySupport,
    Vitals,
    MedicationAdminContinuous,
    MedicationAdminIntermittent,
    PatientAssessments,
    Position,
    MicrobiologyCulture,
    CrrtTherapy,
    PatientProcedures,
    MicrobiologySusceptibility,
    EcmoMcs,
    MicrobiologyNonculture,
    CodeStatus,
)
# Re-export ClifOrchestrator at package root
from .clif_orchestrator import ClifOrchestrator

# Re-export commonly used utility functions at package root
from .utils.stitching_encounters import stitch_encounters
from .utils.waterfall import process_resp_support_waterfall
from .utils.wide_dataset import create_wide_dataset, convert_wide_to_hourly
from .utils.comorbidity import calculate_cci
from .utils.outlier_handler import apply_outlier_handling, get_outlier_summary
from .utils.config import load_config
from .utils.io import load_data

# Version info
try:
    from importlib.metadata import version
    __version__ = version("clifpy")
except Exception:
    __version__ = "unknown"

# Public API
__all__ = [
    # Data module
    "data",
    # Table classes
    "Patient",
    "Adt",
    "Hospitalization",
    "HospitalDiagnosis",
    "Labs",
    "RespiratorySupport",
    "Vitals",
    "MedicationAdminContinuous",
    "MedicationAdminIntermittent",
    "PatientAssessments",
    "Position",
    "MicrobiologyCulture",
    "CrrtTherapy",
    "PatientProcedures",
    "MicrobiologySusceptibility",
    "EcmoMcs",
    "MicrobiologyNonculture",
    "CodeStatus",
    # Orchestrator
    "ClifOrchestrator",
    # Utility functions
    "stitch_encounters",
    "process_resp_support_waterfall",
    "create_wide_dataset",
    "convert_wide_to_hourly",
    "calculate_cci",
    "apply_outlier_handling",
    "get_outlier_summary",
    "load_config",
    "load_data",
]