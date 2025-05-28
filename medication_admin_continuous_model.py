from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class MedicationCategory(str, Enum):
    NOREPINEPHRINE = "norepinephrine"
    EPINEPHRINE = "epinephrine"
    PHENYLEPHRINE = "phenylephrine"
    ANGIOTENSIN = "angiotensin"
    VASOPRESSIN = "vasopressin"
    DOPAMINE = "dopamine"
    DOBUTAMINE = "dobutamine"
    MILRINONE = "milrinone"
    ISOPROTERENOL = "isoproterenol"
    PROPOFOL = "propofol"
    DEXMEDETOMIDINE = "dexmedetomidine"
    KETAMINE = "ketamine"
    MIDAZOLAM = "midazolam"
    FENTANYL = "fentanyl"
    HYDROMORPHONE = "hydromorphone"
    MORPHINE = "morphine"
    REMIFENTANIL = "remifentanil"
    PENTOBARBITAL = "pentobarbital"
    LORAZEPAM = "lorazepam"
    FUROSEMIDE = "furosemide"
    BUMETANIDE = "bumetanide"
    TORSEMIDE = "torsemide"
    HEPARIN = "heparin"
    ARGATROBAN = "argatroban"
    BIVALIRUDIN_ALTEPLASE = "bivalirudin alteplase"
    CANGRELOR = "cangrelor"
    EPTIFIBATIDE = "eptifibatide"
    AMIODARONE = "amiodarone"
    LIDOCAINE = "lidocaine"
    NICARDIPINE = "nicardipine"
    CLEVIDIPINE = "clevidipine"
    NITROGLYCERINE = "nitroglycerine"
    NITROPRUSSIDE = "nitroprusside"
    ESMOLOL = "esmolol"
    DILTIAZEM = "diltiazem"
    LABETALOL = "labetalol"
    PROCAINAMIDE = "procainamide"
    PAPAVERINE = "papaverine"
    ADENOSINE = "adenosine"
    CISATRACURIUM = "cisatracurium"
    VECURONIUM = "vecuronium"
    ROCURONIUM = "rocuronium"
    TREPROSTINIL = "treprostinil"
    EPOPROSTENOL = "epoprostenol"
    ALPROSTADIL = "alprostadil"
    OCTREOTIDE = "octreotide"
    PANTOPRAZOLE = "pantoprazole"
    ESOMEPRAZOLE = "esomeprazole"
    ALBUTEROL = "albuterol"
    TERBUTALINE = "terbutaline"
    IPRATROPIUM = "ipratropium"
    NARCAN = "narcan"
    INSULIN = "insulin"
    THYROID_REPLACEMENT = "thyroid replacement"
    OXYTOCIN = "oxytocin"
    COSYNTROPIN = "cosyntropin"
    REPLETION = "repletion"
    SODIUM_BICARBONATE = "sodium bicarbonate"
    TPN = "tpn"
    TOCOLYTICS = "tocolyttics"
    MAGNESIUM = "magnesium"
    PITOSSIN = "pitossin"
    DEXTROSE = "dextrose"
    SODIUM_CHLORIDE = "sodium chloride"
    AMINOCAPROIC = "aminocaproic"
    AMINOPHYLLINE = "aminophylline"
    NALOXONE = "naloxone"
    PHENTOLAMINE = "phentolamine"
    LIOTHYRONINE = "liothyronine"
    ZIDOVUDINE = "zidovudine"
    TACROLIMUS = "tacrolimus"
    ROPIVACAINE = "ropivacaine"
    BUPIVACAINE = "bupivacaine"
    BACLOFEN = "baclofen"
    LACTATED_RINGERS_SOLUTION = "lactated_ringers_solution"
    ALBUMIN_INFUSION = "albumin_infusion"
    PLASMA_LYTE = "plasma_lyte"
    DEXTROSE_IN_WATER_D5W = "dextrose_in_water_d5w"


class MedicationGroup(str, Enum):
    VASOACTIVES = "vasoactives"
    SEDATION = "sedation"
    DIURETICS = "diuretics"
    ANTICOAGULATION = "anticoagulation"
    CARDIAC = "cardiac"
    PARALYTICS = "paralytics"
    PULMONARY_VASODILATORS = "pulmonary vasaldilators (IV)"
    GASTROINTESTINAL = "gastrointestinal"
    INHALED = "Inhaled"
    ANALGESIA = "analgesia"
    ENDOCRINE = "endocrine"
    FLUIDS_ELECTROLYTES = "fluids_electrolytes"
    OTHERS = "others"


class MedicationRouteCategory(str, Enum):
    # Note: The schema shows empty permissible_values for this category
    # You may want to add specific values here if they become available
    pass


class MedicationActionCategory(str, Enum):
    # Note: The schema shows empty permissible_values for this category
    # You may want to add specific values here if they become available
    pass


class MedicationAdminContinuous(BaseModel):
    hospitalization_id: str
    med_order_id: str
    admin_dttm: datetime
    med_name: str
    med_category: MedicationCategory
    med_group: MedicationGroup
    med_route_name: str
    med_route_category: str  # Using str since permissible_values is empty
    med_dose: float
    med_dose_unit: str
    mar_action_name: str
    mar_action_category: str  # Using str since permissible_values is empty 