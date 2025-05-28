from datetime import datetime
from enum import Enum
from pydantic import BaseModel


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


class Patient(BaseModel):
    patient_id: str
    birth_date: datetime
    death_dttm: datetime
    race_name: str
    race_category: RaceCategory
    ethnicity_name: str
    ethnicity_category: EthnicityCategory
    sex_name: str
    sex_category: SexCategory
    language_name: str
    language_category: LanguageCategory