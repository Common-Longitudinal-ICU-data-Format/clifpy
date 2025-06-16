from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import json
import os
from ..utils.io import load_data
from ..utils.validator import validate_table


class patient_assessments:
    """Patient Assessments table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: pd.DataFrame | None = None):
        self.df: pd.DataFrame | None = data
        self.errors: List[dict] = []
        self.range_validation_errors: List[dict] = []
        
        # Load assessment mappings and ranges from JSON schema
        self._assessment_category_to_group = None
        self._assessment_score_ranges = None
        self._load_assessments_schema()

        if self.df is not None:
            self.validate()

    def _load_assessments_schema(self):
        """Load assessment mappings and ranges from Patient_assessmentsModel.json."""
        try:
            # Get the path to the Patient_assessmentsModel.json file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(current_dir, '..', 'mCIDE', 'Patient_assessmentsModel.json')
            
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            self._assessment_category_to_group = schema.get('assessment_category_to_group_mapping', {})
            self._assessment_score_ranges = schema.get('assessment_score_ranges', {})
            
        except FileNotFoundError:
            print("Warning: Patient_assessmentsModel.json not found.")
            self._assessment_category_to_group = {}
            self._assessment_score_ranges = {}
        except json.JSONDecodeError:
            print("Warning: Invalid JSON in Patient_assessmentsModel.json.")
            self._assessment_category_to_group = {}
            self._assessment_score_ranges = {}

    @property
    def assessment_category_to_group_mapping(self) -> Dict[str, str]:
        """Get the assessment category to group mapping from the schema."""
        return self._assessment_category_to_group.copy() if self._assessment_category_to_group else {}

    @property
    def assessment_score_ranges(self) -> Dict[str, Dict[str, float]]:
        """Get the assessment score ranges from the schema."""
        return self._assessment_score_ranges.copy() if self._assessment_score_ranges else {}

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet"):
        """Load the patient assessments table from *table_path* and build a :class:`patient_assessments`."""
        data = load_data("patient_assessments", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors and not self.range_validation_errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *Patient_assessmentsModel.json* spec and score ranges."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "patient_assessments")


        # User-friendly status output
        total_errors = len(self.errors) + len(self.range_validation_errors)
        if total_errors == 0:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {total_errors} error(s).")
            if self.errors:
                print(f"  - {len(self.errors)} schema validation error(s)")
            if self.range_validation_errors:
                print(f"  - {len(self.range_validation_errors)} range validation error(s)")
            print("See `errors` and `range_validation_errors` attributes for details.")

    # ------------------------------------------------------------------
    # Patient Assessments Specific Methods
    # ------------------------------------------------------------------
    def get_assessment_categories(self) -> List[str]:
        """Return unique assessment categories in the dataset."""
        if self.df is None or 'assessment_category' not in self.df.columns:
            return []
        return self.df['assessment_category'].dropna().unique().tolist()

    def filter_by_assessment_category(self, assessment_category: str) -> pd.DataFrame:
        """Return all records for a specific assessment category."""
        if self.df is None or 'assessment_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['assessment_category'] == assessment_category].copy()

    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the patient assessments data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'assessment_category_counts': self.df['assessment_category'].value_counts().to_dict() if 'assessment_category' in self.df.columns else {},
            'assessment_group_counts': self.df['assessment_group'].value_counts().to_dict() if 'assessment_group' in self.df.columns else {},
            'date_range': {
                'earliest': self.df['recorded_dttm'].min() if 'recorded_dttm' in self.df.columns else None,
                'latest': self.df['recorded_dttm'].max() if 'recorded_dttm' in self.df.columns else None
            }
        }
        
        # Add numerical value statistics by assessment category
        if 'assessment_category' in self.df.columns and 'numerical_value' in self.df.columns:
            numerical_stats = {}
            for assessment_cat in self.get_assessment_categories():
                assessment_data = self.filter_by_assessment_category(assessment_cat)['numerical_value'].dropna()
                if not assessment_data.empty:
                    numerical_stats[assessment_cat] = {
                        'count': len(assessment_data),
                        'mean': round(assessment_data.mean(), 2),
                        'min': assessment_data.min(),
                        'max': assessment_data.max(),
                        'std': round(assessment_data.std(), 2)
                    }
            stats['numerical_value_stats'] = numerical_stats
        
        return stats