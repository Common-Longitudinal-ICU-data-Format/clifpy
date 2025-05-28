from pydantic import BaseModel
from datetime import datetime
from enum import Enum


class LocationCategory(str, Enum):
    ED = "ed"
    WARD = "ward"
    STEPDOWN = "stepdown"
    ICU = "icu"
    PROCEDURAL = "procedural"
    L_AND_D = "l&d"
    HOSPICE = "hospice"
    PSYCH = "psych"
    REHAB = "rehab"
    RADIOLOGY = "radiology"
    DIALYSIS = "dialysis"
    OTHER = "other"

class ADT(BaseModel):
    hospitalization_id: str 
    hospital_id: str 
    in_dttm: datetime 
    out_dttm: datetime 
    location_name: str 
    location_category: LocationCategory
    

