from datetime import datetime
from enum import Enum
from pydantic import BaseModel


class PositionCategory(str, Enum):
    PRONE = "prone"
    NOT_PRONE = "not_prone"


class Position(BaseModel):
    hospitalization_id: str
    recorded_dttm: datetime
    position_name: str
    position_category: PositionCategory