from datetime import datetime
from enum import Enum
from pydantic import BaseModel
from typing import List, Optional  # Added Optional for optional fields
from tqdm import tqdm
from pydantic import ValidationError
import pandas as pd
from ..utils.io import load_data

class RaceCategory(str, Enum):
    BLACK_OR_AFRICAN_AMERICAN = "Black or African American"
    WHITE = "White"
    AMERICAN_INDIAN_OR_ALASKA_NATIVE = "American Indian or Alaska Native"
    ASIAN = "Asian"
    NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = "Native Hawaiian or Other Pacific Islander"
    UNKNOWN = "Unknown"
    OTHER = "Other"


class EthnicityCategory(str, Enum):
    HISPANIC = "Hispanic"
    NON_HISPANIC = "Non-Hispanic"
    UNKNOWN = "Unknown"


class SexCategory(str, Enum):
    MALE = "Male"
    FEMALE = "Female"
    UNKNOWN = "Unknown"


class LanguageCategory(str, Enum):
    ENGLISH = "English"
    SPANISH = "Spanish"
    FRENCH = "French"
    HAITIAN_CREOLE = "Haitian Creole"
    ITALIAN = "Italian"
    PORTUGUESE = "Portuguese"
    GERMAN = "German"
    WEST_GERMANIC = "Yiddish, Pennsylvania Dutch, or other West Germanic Languages"
    GREEK = "Greek"
    RUSSIAN = "Russian"
    POLISH = "Polish"
    SERBO_CROATIAN = "Serbo-Croatian"
    SLAVIC = "Ukrainian or other Slavic languages"
    ARMENIAN = "Armenian"
    PERSIAN = "Persian"
    GUJARATI = "Gujarati"
    HINDI = "Hindi"
    URDU = "Urdu"
    PUNJABI = "Punjabi"
    BENGALI = "Bengali"
    INDIC = "Nepali, Marathi, or other Indic languages"
    OTHER_EUROPEAN_INDO_EUROPEAN = "Other European Indo-European languages"
    OTHER_ASIAN_INDO_EUROPEAN = "Other Asian Indo-European languages"
    TELUGU = "Telugu"
    TAMIL = "Tamil"
    DRAVIDIAN = "Malayalam, Kannada, or other Dravidian languages"
    CHINESE = "Chinese"
    JAPANESE = "Japanese"
    KOREAN = "Korean"
    VIETNAMESE = "Vietnamese"
    KHMER = "Khmer"
    TAI_KADAI = "Thai, Lao, or other Tai-Kadai languages"
    OTHER_ASIAN = "Other languages of Asia"
    TAGALOG = "Tagalog"
    AUSTRONESIAN = "Ilocano, Samoan, Hawaiian, or other Austronesian languages"
    ARABIC = "Arabic"
    HEBREW = "Hebrew"
    AFRO_ASIATIC = "Amharic, Somali, or other Afro-Asiatic languages"
    WEST_AFRICAN = "Yoruba, Twi, Igbo, or other languages of Western Africa"
    AFRICAN = "Swahili or other languages of Central, Eastern, and Southern Africa"
    NAVAJO = "Navajo"
    OTHER_NATIVE_AMERICAN = "Other Native languages of North America"
    OTHER_UNSPECIFIED = "Other and unspecified languages"
    SIGN_LANGUAGE = "Sign Language"
    UNKNOWN_OR_NA = "Unknown or NA"


class PatientModel(BaseModel):
    patient_id: str
    birth_date: datetime
    death_dttm: datetime
    race_name: str
    race_category: RaceCategory
    ethnicity_name: str
    ethnicity_category: EthnicityCategory
    sex_name: Optional[str] = None  # Optional, will be suppressed in output if not present
    # To suppress sex_name in output, use: model.dict(exclude_none=True) or model.json(exclude_none=True)
    sex_category: SexCategory
    language_name: str
    language_category: LanguageCategory 


class patient:
    def __init__(self, data=None):
        if data is not None:
            self.df = data
        else:
            self.df = None
        self.records: List[PatientModel] = []
        self.errors = []
        
        if self.df is not None:
            self.validate()

    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = 'parquet'):
        """
        Loads patient data from a file in the specified directory.
        The file is expected to be named 'clif_patient.<table_format_type>'.
        """
        data = load_data('patient', table_path, table_format_type)
        return cls(data)

    def isvalid(self):
        return len(self.errors) == 0
    
    def validate(self):
        self.errors = []
        for idx, row in enumerate(tqdm(self.df.itertuples(index=False), total=len(self.df), desc='Validating Patient Records')):
            try:
                record = PatientModel(**row._asdict())
                self.records.append(record)
            except ValidationError as e:
                self.errors.append({'row': idx, 'error': str(e), 'data': row._asdict()})
        
        if not self.errors:
            print('Validation completed successfully.')
        else:
            print(f'Validation completed with {len(self.errors)} errors. Access `errors` attribute for details.')
