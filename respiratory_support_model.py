from datetime import datetime
from enum import Enum
from pydantic import BaseModel


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
    fio2_set: float
    lpm_set: float
    tidal_volume_set: float
    resp_rate_set: float
    pressure_control_set: float
    pressure_support_set: float
    flow_rate_set: float
    peak_inspiratory_pressure_set: float
    inspiratory_time_set: float
    peep_set: float
    tidal_volume_obs: float
    resp_rate_obs: float
    plateau_pressure_obs: float
    peak_inspiratory_pressure_obs: float
    peep_obs: float
    minute_vent_obs: float
    mean_airway_pressure_obs: float 