from pydantic import BaseModel
from datetime import datetime
from enum import Enum


class AdmissionTypeCategory(str, Enum):
    ED = "ed"
    FACILITY = "facility"
    OSH = "osh"
    DIRECT = "direct"
    ELECTIVE = "elective"
    OTHER = "other"


class DischargeCategory(str, Enum):
    ACUTE_CARE_HOSPITAL = "Acute Care Hospital"
    ACUTE_INPATIENT_REHAB = "Acute Inpatient Rehab Facility"
    AMA = "Against Medical Adivce (AMA)"
    ASSISTED_LIVING = "Assisted Living"
    CHEMICAL_DEPENDENCY = "Chemical Dependency"
    EXPIRED = "Expired"
    GROUP_HOME = "Group Home"
    HOME = "Home"
    HOSPICE = "Hospice"
    JAIL = "Jail"
    LTACH = "Long Term Care Hospital (LTACH)"
    MISSING = "Missing"
    OTHER = "Other"
    PSYCHIATRIC_HOSPITAL = "Psychiatric Hospital"
    SHELTER = "Shelter"
    SNF = "Skilled Nursing Facility (SNF)"
    STILL_ADMITTED = "Still Admitted"


class Hospitalization(BaseModel):
    patient_id: str
    hospitalization_id: str
    hospitalization_joined_id: str
    admission_dttm: datetime
    discharge_dttm: datetime
    age_at_admission: int
    admission_type_name: str
    admission_type_category: AdmissionTypeCategory
    discharge_name: str
    discharge_category: DischargeCategory
    zipcode_nine_digit: str
    zipcode_five_digit: str
    census_block_code: str
    census_block_group_code: str
    census_tract: str
    state_code: str
    county_code: str