from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class AssessmentCategory(str, Enum):
    AM_PAC = "AM-PAC"
    AMS = "AMS"
    APGAR = "APGAR"
    AVPU = "AVPU"
    BPS = "BPS"
    BRADEN_ACTIVITY = "braden_activity"
    BRADEN_FRICTION = "braden_friction"
    BRADEN_MOBILITY = "braden_mobility"
    BRADEN_MOISTURE = "braden_moisture"
    BRADEN_NUTRITION = "braden_nutrition"
    BRADEN_SENSORY = "braden_sensory"
    BRADEN_TOTAL = "braden_total"
    CAM_INATTENTION = "cam_inattention"
    CAM_LOC = "cam_loc"
    CAM_MENTAL = "cam_mental"
    CAM_THINKING = "cam_thinking"
    CAM_TOTAL = "cam_total"
    CIWA = "CIWA"
    COWS = "COWS"
    CPOT_TOTAL = "cpot_total"
    CPOT_FACIAL = "cpot_facial"
    CPOT_BODY = "cpot_body"
    CPOT_MUSCLE = "cpot_muscle"
    CPOT_VOCALIZATION = "cpot_vocalization"
    DVPRS = "DVPRS"
    GCS_EYE = "gcs_eye"
    GCS_MOTOR = "gcs_motor"
    GCS_TOTAL = "gcs_total"
    GCS_VERBAL = "gcs_verbal"
    ICANS = "ICANS"
    ICSDC = "ICSDC"
    ICSDC_AGITATION = "icsdc_agitation"
    ICSDC_DISORIENTATION = "icsdc_disorientation"
    ICSDC_HALLUCINATION = "icsdc_hallucination"
    ICSDC_INATTENTION = "icsdc_inattention"
    ICSDC_LOC = "icsdc_loc"
    ICSDC_SLEEP = "icsdc_sleep"
    ICSDC_SPEECH = "icsdc_speech"
    ICSDC_SYMPTOMS = "icsdc_symptoms"
    ICSDC_TOTAL = "icsdc_total"
    IMS = "IMS"
    MINDS = "MINDS"
    MORSE_FALL_SCALE = "Morse Fall Scale"
    NRS = "NRS"
    NVPS = "NVPS"
    PAINAD = "PAINAD"
    RASS = "RASS"
    SAS = "SAS"
    SAT_DELIVERY_PASS_FAIL = "sat_delivery_pass_fail"
    SAT_ESCALATING_SEDATION = "sat_escalating_sedation"
    SAT_INTRACRANIAL_PRESSURE = "sat_intracranial_pressure"
    SAT_MYOCARDIAL_ISCHEMIA = "sat_myocardial_ischemia"
    SAT_NEUROMUSCULAR_BLOCKERS = "sat_neuromuscular_blockers"
    SAT_SCREEN_PASS_FAIL = "sat_screen_pass_fail"
    SAT_SEDATIVE_INFUSION = "sat_sedative_infusion"
    SBT_AGITATION = "sbt_agitation"
    SBT_DELIVERY_PASS_FAIL = "sbt_delivery_pass_fail"
    SBT_FAIL_REASON = "sbt_fail_reason"
    SBT_INADEQUATE_OXYGENATION = "sbt_inadequate_oxygenation"
    SBT_INTRACRANIAL_PRESSURE = "sbt_intracranial_pressure"
    SBT_NO_SPONTANEOUS_EFFORT = "sbt_no_spontaneous_effort"
    SBT_SCREEN_PASS_FAIL = "sbt_screen_pass_fail"
    SBT_VASOPRESSOR_USE = "sbt_vasopressor_use"
    TOF = "TOF"
    VAS = "VAS"
    WAT = "WAT"


class AssessmentGroup(str, Enum):
    MOBILITY_ACTIVITY = "Mobility/Activity"
    NEUROLOGICAL = "Neurological"
    PAIN = "Pain"
    NURSING_RISK = "Nursing Risk"
    DELIRIUM = "Delirium"
    WITHDRAWAL = "Withdrawal"
    SEDATION_AGITATION = "Sedation/Agitation"
    SAT_DELIVERY = "SAT Delivery"
    SPONTANEOUS_AWAKENING_TRIAL = "Spontaneous Awakening Trial (SAT)"
    SAT_SCREEN_PASS_FAIL = "SAT Screen Pass/Fail"
    SPONTANEOUS_BREATHING_TRIAL = "Spontaneous Breathing Trial (SBT)"
    SBT_DELIVERY = "SBT Delivery"
    SBT_FAILURE_REASON = "SBT Failure Reason"


class PatientAssessment(BaseModel):
    hospitalization_id: str
    recorded_dttm: datetime
    assessment_name: str
    assessment_category: AssessmentCategory
    assessment_group: AssessmentGroup
    numerical_value: float
    categorical_value: str
    text_value: str 