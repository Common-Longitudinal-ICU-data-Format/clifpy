from datetime import datetime, timedelta
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
from ..utils.io import load_data
from ..utils.validator import validate_table

class position:
    """Position table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []

        if self.df is not None:
            self.validate()

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet"):
        """Load the position table from *table_path* and build a :class:`position`."""
        data = load_data("position", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *PositionModel.json* spec."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "position")

        # User-friendly status output
        if not self.errors:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

    # ------------------------------------------------------------------
    # Position Specific Methods
    # ------------------------------------------------------------------
    
    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the position data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'position_category_counts': self.df['position_category'].value_counts().to_dict() if 'position_category' in self.df.columns else {},
            'date_range': {
                'earliest': self.df['recorded_dttm'].min() if 'recorded_dttm' in self.df.columns else None,
                'latest': self.df['recorded_dttm'].max() if 'recorded_dttm' in self.df.columns else None
            }
        }
            
        return stats
