from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import json
import os
from ..utils.io import load_data
from ..utils.validator import validate_table


class vitals:
    """Vitals table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []
        
        # Load vital ranges and units from JSON schema
        self._vital_units = None
        self._vital_ranges = None
        self._load_vitals_schema()

        if self.df is not None:
            self.validate()

    def _load_vitals_schema(self):
        """Load vital units and ranges from VitalsModel.json."""
        try:
            # Get the path to the VitalsModel.json file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(current_dir, '..', 'mCIDE', 'VitalsModel.json')
            
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            self._vital_units = schema.get('vital_units', {})
            self._vital_ranges = schema.get('vital_ranges', {})
            
        except FileNotFoundError:
            print("Warning: VitalsModel.json not found. Range validation will be skipped.")
            self._vital_units = {}
            self._vital_ranges = {}
        except json.JSONDecodeError:
            print("Warning: Invalid JSON in VitalsModel.json. Range validation will be skipped.")
            self._vital_units = {}
            self._vital_ranges = {}

    @property
    def vital_units(self) -> Dict[str, str]:
        """Get the vital units mapping from the schema."""
        return self._vital_units.copy() if self._vital_units else {}

    @property
    def vital_ranges(self) -> Dict[str, Dict[str, float]]:
        """Get the vital ranges from the schema."""
        return self._vital_ranges.copy() if self._vital_ranges else {}

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet"):
        """Load the vitals table from *table_path* and build a :class:`vitals`."""
        data = load_data("vitals", table_path, table_format_type)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors and not self.range_validation_errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *VitalsModel.json* spec and vital ranges."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "vitals")

        # Run vital range validation
        self.validate_vital_ranges()

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

    def validate_vital_ranges(self):
        """Validate vital values against expected ranges using grouped data for efficiency."""
        self.range_validation_errors = []
        
        if self.df is None or not self._vital_ranges:
            return
        
        required_columns = ['vital_category', 'vital_value']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            self.range_validation_errors.append({
                'error_type': 'missing_columns',
                'message': f"Missing required columns for range validation: {missing_columns}"
            })
            return

        # Group by vital_category to get statistics for each vital type
        vital_stats = (self.df[required_columns]
                      .dropna()
                      .groupby('vital_category')['vital_value']
                      .agg(['count', 'min', 'max', 'mean'])
                      .reset_index())
        
        if vital_stats.empty:
            return
        
        # Check each vital category's ranges
        for _, row in vital_stats.iterrows():
            vital_category = row['vital_category']
            min_val = row['min']
            max_val = row['max']
            count = row['count']
            mean_val = row['mean']
            
            # Check if vital category has defined ranges
            if vital_category not in self._vital_ranges:
                self.range_validation_errors.append({
                    'error_type': 'unknown_vital_category',
                    'vital_category': vital_category,
                    'affected_rows': count,
                    'message': f"Unknown vital category '{vital_category}' affects {count} rows"
                })
                continue
            
            # Check if values are within expected ranges
            expected_range = self._vital_ranges[vital_category]
            expected_min = expected_range.get('min')
            expected_max = expected_range.get('max')
            
            range_issues = []
            if expected_min is not None and min_val < expected_min:
                range_issues.append(f"minimum value {min_val} below expected {expected_min}")
            
            if expected_max is not None and max_val > expected_max:
                range_issues.append(f"maximum value {max_val} above expected {expected_max}")
            
            if range_issues:
                self.range_validation_errors.append({
                    'error_type': 'values_out_of_range',
                    'vital_category': vital_category,
                    'affected_rows': count,
                    'min_value': min_val,
                    'max_value': max_val,
                    'mean_value': round(mean_val, 2),
                    'expected_range': expected_range,
                    'issues': range_issues,
                    'message': f"Vital '{vital_category}' has values out of expected range: {'; '.join(range_issues)}"
                })

    # ------------------------------------------------------------------
    # Vitals Specific Methods
    # ------------------------------------------------------------------
    def get_vital_categories(self) -> List[str]:
        """Return unique vital categories in the dataset."""
        if self.df is None or 'vital_category' not in self.df.columns:
            return []
        return self.df['vital_category'].dropna().unique().tolist()

    def filter_by_hospitalization(self, hospitalization_id: str) -> pd.DataFrame:
        """Return all vital records for a specific hospitalization."""
        if self.df is None:
            return pd.DataFrame()
        
        return self.df[self.df['hospitalization_id'] == hospitalization_id].copy()

    def filter_by_vital_category(self, vital_category: str) -> pd.DataFrame:
        """Return all records for a specific vital category (e.g., 'heart_rate', 'temp_c')."""
        if self.df is None or 'vital_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['vital_category'] == vital_category].copy()

    def filter_by_date_range(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        """Return records within a specific date range."""
        if self.df is None or 'recorded_dttm' not in self.df.columns:
            return pd.DataFrame()
        
        # Convert datetime column to datetime if it's not already
        df_copy = self.df.copy()
        df_copy['recorded_dttm'] = pd.to_datetime(df_copy['recorded_dttm'])
        
        mask = (df_copy['recorded_dttm'] >= start_date) & (df_copy['recorded_dttm'] <= end_date)
        return df_copy[mask]

    def get_summary_stats(self) -> Dict:
        """Return summary statistics for the vitals data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'vital_category_counts': self.df['vital_category'].value_counts().to_dict() if 'vital_category' in self.df.columns else {},
            'date_range': {
                'earliest': self.df['recorded_dttm'].min() if 'recorded_dttm' in self.df.columns else None,
                'latest': self.df['recorded_dttm'].max() if 'recorded_dttm' in self.df.columns else None
            }
        }
        
        # Add vital value statistics by category
        if 'vital_category' in self.df.columns and 'vital_value' in self.df.columns:
            vital_value_stats = {}
            for vital_cat in self.get_vital_categories():
                vital_data = self.filter_by_vital_category(vital_cat)['vital_value']
                vital_value_stats[vital_cat] = {
                    'count': len(vital_data),
                    'mean': round(vital_data.mean(), 2) if not vital_data.empty else None,
                    'min': vital_data.min() if not vital_data.empty else None,
                    'max': vital_data.max() if not vital_data.empty else None,
                    'std': round(vital_data.std(), 2) if not vital_data.empty else None
                }
            stats['vital_value_stats'] = vital_value_stats
        
        return stats

    def get_range_validation_report(self) -> pd.DataFrame:
        """Return a detailed report of vital range validation errors."""
        if not self.range_validation_errors:
            return pd.DataFrame(columns=['error_type', 'vital_category', 'affected_rows', 'message'])
        
        return pd.DataFrame(self.range_validation_errors)
