from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
from ..utils.io import load_data
from ..utils.validator import validate_table


class adt:
    """ADT (Admission, Discharge, Transfer) table wrapper using lightweight JSON-spec validation."""

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
        """Load the ADT table from *table_path* and build a :class:`adt`."""
        data = load_data("adt", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *ADTModel.json* spec."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "adt")

        # User-friendly status output
        if not self.errors:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

    # ------------------------------------------------------------------
    # ADT Specific Methods (placeholder for future implementation)
    # ------------------------------------------------------------------
    def get_location_categories(self) -> List[str]:
        """Return unique location categories in the dataset."""
        if self.df is None or 'location_category' not in self.df.columns:
            return []
        return self.df['location_category'].dropna().unique().tolist()

    def get_hospital_types(self) -> List[str]:
        """Return unique hospital types in the dataset."""
        if self.df is None or 'hospital_type' not in self.df.columns:
            return []
        return self.df['hospital_type'].dropna().unique().tolist()

    def filter_by_hospitalization(self, hospitalization_id: str) -> pd.DataFrame:
        """Return all ADT records for a specific hospitalization."""
        if self.df is None:
            return pd.DataFrame()
        
        return self.df[self.df['hospitalization_id'] == hospitalization_id].copy()

    def filter_by_location_category(self, location_category: str) -> pd.DataFrame:
        """Return all records for a specific location category (e.g., 'icu', 'ward')."""
        if self.df is None or 'location_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['location_category'] == location_category].copy()

    def filter_by_date_range(self, start_date: datetime, end_date: datetime, 
                           date_column: str = 'in_dttm') -> pd.DataFrame:
        """Return records within a specific date range for a given datetime column."""
        if self.df is None or date_column not in self.df.columns:
            return pd.DataFrame()
        
        # Convert datetime column to datetime if it's not already
        df_copy = self.df.copy()
        df_copy[date_column] = pd.to_datetime(df_copy[date_column])
        
        mask = (df_copy[date_column] >= start_date) & (df_copy[date_column] <= end_date)
        return df_copy[mask]

    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the ADT data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'unique_hospitals': self.df['hospital_id'].nunique() if 'hospital_id' in self.df.columns else 0,
            'location_category_counts': self.df['location_category'].value_counts().to_dict() if 'location_category' in self.df.columns else {},
            'hospital_type_counts': self.df['hospital_type'].value_counts().to_dict() if 'hospital_type' in self.df.columns else {},
            'date_range': {
                'earliest_in': self.df['in_dttm'].min() if 'in_dttm' in self.df.columns else None,
                'latest_in': self.df['in_dttm'].max() if 'in_dttm' in self.df.columns else None,
                'earliest_out': self.df['out_dttm'].min() if 'out_dttm' in self.df.columns else None,
                'latest_out': self.df['out_dttm'].max() if 'out_dttm' in self.df.columns else None
            }
        }
        
        return stats
