from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class VitalCategory(str, Enum):
    SBP = "sbp"
    DBP = "dbp"
    MAP = "map"
    RESPIRATORY_RATE = "respiratory_rate"
    TEMP_C = "temp_c"
    WEIGHT_KG = "weight_kg"
    HEIGHT_IN = "height_in"
    SPO2 = "spo2"
    HEART_RATE = "heart_rate"


class Vital(BaseModel):
    hospitalization_id: str
    recorded_dttm: datetime
    vital_name: str
    vital_category: VitalCategory
    vital_value: float
    meas_site_name: str