from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import json
import os
from ..utils.io import load_data
from ..utils.validator import validate_table


class medication_admin_continuous:
    """Medication Admin Continuous table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []
        
        # Load medication mappings from JSON schema
        self._med_category_to_group = None
        self._load_medication_schema()

        if self.df is not None:
            self.validate()

    def _load_medication_schema(self):
        """Load medication category to group mappings from MedicationAdminContinuousModel.json."""
        try:
            # Get the path to the MedicationAdminContinuousModel.json file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(current_dir, '..', 'mCIDE', 'Medication_admin_continuousModel.json')
            
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            self._med_category_to_group = schema.get('med_category_to_group_mapping', {})
            
        except FileNotFoundError:
            print("Warning: Medication_admin_continuousModel.json not found.")
            self._med_category_to_group = {}
        except json.JSONDecodeError:
            print("Warning: Invalid JSON in Medication_admin_continuousModel.json.")
            self._med_category_to_group = {}

    @property
    def med_category_to_group_mapping(self) -> Dict[str, str]:
        """Get the medication category to group mapping from the schema."""
        return self._med_category_to_group.copy() if self._med_category_to_group else {}

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet"):
        """Load the medication admin continuous table from *table_path* and build a :class:`medication_admin_continuous`."""
        data = load_data("medication_admin_continuous", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *MedicationAdminContinuousModel.json* spec."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "medication_admin_continuous")

        # User-friendly status output
        if not self.errors:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {len(self.errors)} error(s). See `errors` attribute.")

    # ------------------------------------------------------------------
    # Medication Admin Continuous Specific Methods
    # ------------------------------------------------------------------
    def get_med_categories(self) -> List[str]:
        """Return unique medication categories in the dataset."""
        if self.df is None or 'med_category' not in self.df.columns:
            return []
        return self.df['med_category'].dropna().unique().tolist()

    def get_med_groups(self) -> List[str]:
        """Return unique medication groups in the dataset."""
        if self.df is None or 'med_group' not in self.df.columns:
            return []
        return self.df['med_group'].dropna().unique().tolist()
    
    def filter_by_med_group(self, med_group: str) -> pd.DataFrame:
        """Return all records for a specific medication group."""
        if self.df is None or 'med_group' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['med_group'] == med_group].copy()

    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the medication admin continuous data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'med_category_counts': self.df['med_category'].value_counts().to_dict() if 'med_category' in self.df.columns else {},
            'med_group_counts': self.df['med_group'].value_counts().to_dict() if 'med_group' in self.df.columns else {},
            'date_range': {
                'earliest': self.df['admin_dttm'].min() if 'admin_dttm' in self.df.columns else None,
                'latest': self.df['admin_dttm'].max() if 'admin_dttm' in self.df.columns else None
            }
        }
        
        # Add dose statistics by medication group
        if 'med_group' in self.df.columns and 'med_dose' in self.df.columns:
            dose_stats = {}
            for group in self.get_med_groups():
                group_data = self.filter_by_med_group(group)['med_dose'].dropna()
                if not group_data.empty:
                    dose_stats[group] = {
                        'count': len(group_data),
                        'mean_dose': round(group_data.mean(), 3),
                        'min_dose': group_data.min(),
                        'max_dose': group_data.max()
                    }
            stats['dose_stats_by_group'] = dose_stats
        
        return stats