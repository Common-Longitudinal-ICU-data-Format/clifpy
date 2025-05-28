from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class LabOrderCategory(str, Enum):
    ABG = "ABG"
    BMP = "BMP"
    CBC = "CBC"
    COAGS = "Coags"
    LFT = "LFT"
    LACTIC_ACID = "Lactic Acid"
    MISC = "Misc"
    VBG = "VBG"


class LabCategory(str, Enum):
    ALBUMIN = "albumin"
    ALKALINE_PHOSPHATASE = "alkaline_phosphatase"
    ALT = "alt"
    AST = "ast"
    BASOPHILS_PERCENT = "basophils_percent"
    BASOPHILS_ABSOLUTE = "basophils_absolute"
    BICARBONATE = "bicarbonate"
    BILIRUBIN_TOTAL = "bilirubin_total"
    BILIRUBIN_CONJUGATED = "bilirubin_conjugated"
    BILIRUBIN_UNCONJUGATED = "bilirubin_unconjugated"
    BUN = "bun"
    CALCIUM_TOTAL = "calcium_total"
    CALCIUM_IONIZED = "calcium_ionized"
    CHLORIDE = "chloride"
    CREATININE = "creatinine"
    CRP = "crp"
    EOSINOPHILS_PERCENT = "eosinophils_percent"
    EOSINOPHILS_ABSOLUTE = "eosinophils_absolute"
    ESR = "esr"
    FERRITIN = "ferritin"
    GLUCOSE_FINGERSTICK = "glucose_fingerstick"
    GLUCOSE_SERUM = "glucose_serum"
    HEMOGLOBIN = "hemoglobin"
    PHOSPHATE = "phosphate"
    INR = "inr"
    LACTATE = "lactate"
    LDH = "ldh"
    LYMPHOCYTES_PERCENT = "lymphocytes_percent"
    LYMPHOCYTES_ABSOLUTE = "lymphocytes_absolute"
    MAGNESIUM = "magnesium"
    MONOCYTES_PERCENT = "monocytes_percent"
    MONOCYTES_ABSOLUTE = "monocytes_absolute"
    NEUTROPHILS_PERCENT = "neutrophils_percent"
    NEUTROPHILS_ABSOLUTE = "neutrophils_absolute"
    PCO2_ARTERIAL = "pco2_arterial"
    PO2_ARTERIAL = "po2_arterial"
    PCO2_VENOUS = "pco2_venous"
    PH_ARTERIAL = "ph_arterial"
    PH_VENOUS = "ph_venous"
    PLATELET_COUNT = "platelet_count"
    POTASSIUM = "potassium"
    PROCALCITONIN = "procalcitonin"
    PT = "pt"
    PTT = "ptt"
    SO2_ARTERIAL = "so2_arterial"
    SO2_MIXED_VENOUS = "so2_mixed_venous"
    SO2_CENTRAL_VENOUS = "so2_central_venous"
    SODIUM = "sodium"
    TOTAL_PROTEIN = "total_protein"
    TROPONIN_I = "troponin_i"
    TROPONIN_T = "troponin_t"
    WBC = "wbc"


class LabSpecimenCategory(str, Enum):
    pass


class Lab(BaseModel):
    hospitalization_id: str
    lab_order_dttm: datetime
    lab_collect_dttm: datetime
    lab_result_dttm: datetime
    lab_order_name: str
    lab_order_category: LabOrderCategory
    lab_name: str
    lab_category: LabCategory
    lab_value: str
    lab_value_numeric: float
    reference_unit: str
    lab_specimen_name: str
    lab_specimen_category: str  
    lab_loinc_code: str