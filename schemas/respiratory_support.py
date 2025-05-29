from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class DeviceCategory(str, Enum):
    IMV = "IMV"
    NIPPV = "NIPPV"
    CPAP = "CPAP"
    HIGH_FLOW_NC = "High Flow NC"
    FACE_MASK = "Face Mask"
    TRACH_COLLAR = "Trach Collar"
    NASAL_CANNULA = "Nasal Cannula"
    ROOM_AIR = "Room Air"
    OTHER = "Other"


class ModeCategory(str, Enum):
    ASSIST_CONTROL_VOLUME_CONTROL = "Assist Control-Volume Control"
    PRESSURE_CONTROL = "Pressure Control"
    PRESSURE_REGULATED_VOLUME_CONTROL = "Pressure-Regulated Volume Control"
    SIMV = "SIMV"
    PRESSURE_SUPPORT_CPAP = "Pressure Support/CPAP"
    VOLUME_SUPPORT = "Volume Support"
    BLOW_BY = "Blow by"
    OTHER = "Other"


class RespiratorySupport(BaseModel):
    hospitalization_id: str
    recorded_dttm: datetime
    device_name: str
    device_category: DeviceCategory
    vent_brand_name: str
    mode_name: str
    mode_category: ModeCategory
    tracheostomy: bool
    fio2_set: float = Field(ge=0.21, le=1)
    lpm_set: float = Field(ge=0, le=60)
    tidal_volume_set: float = Field(ge=100, le=3000)
    resp_rate_set: float = Field(ge=0, le=200)
    pressure_control_set: float = Field(ge=-50, le=50)
    pressure_support_set: float = Field(ge=-50, le=50)
    flow_rate_set: float = Field(ge=-50, le=100)
    peak_inspiratory_pressure_set: float = Field(ge=-50, le=100)
    inspiratory_time_set: float = Field(ge=-1, le=50)
    peep_set: float = Field(ge=0, le=30)
    tidal_volume_obs: float = Field(ge=100, le=3000)
    resp_rate_obs: float = Field(ge=0, le=200)
    plateau_pressure_obs: float = Field(ge=0, le=100)
    peak_inspiratory_pressure_obs: float = Field(ge=-50, le=100)
    peep_obs: float = Field(ge=0, le=50)
    minute_vent_obs: float = Field(ge=0, le=40)
    mean_airway_pressure_obs: float = Field(ge=0, le=50)