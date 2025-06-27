from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional, Set
from tqdm import tqdm
import pandas as pd
import json
import os
from ..utils.io import load_data
from ..utils.validator import validate_table


class labs:
    """Labs table wrapper using lightweight JSON-spec validation."""

    def __init__(self, data: Optional[pd.DataFrame] = None):
        self.df: Optional[pd.DataFrame] = data
        self.errors: List[dict] = []
        self.unit_validation_errors: List[dict] = []
        
        # Load reference units from JSON schema
        self._reference_units = None
        self._load_lab_schema()

        if self.df is not None:
            self.validate()

    def _load_lab_schema(self):
        """Load lab reference units from LabsModel.json."""
        try:
            # Get the path to the LabsModel.json file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            schema_path = os.path.join(current_dir, '..', 'mCIDE', 'LabsModel.json')
            
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            self._reference_units = schema.get('lab_reference_units', {})
            
        except FileNotFoundError:
            print("Warning: LabsModel.json not found. Reference unit validation will be skipped.")
            self._reference_units = {}
        except json.JSONDecodeError:
            print("Warning: Invalid JSON in LabsModel.json. Reference unit validation will be skipped.")
            self._reference_units = {}

    @property
    def reference_units(self) -> Dict[str, List[str]]:
        """Get the reference units mapping from the schema."""
        return self._reference_units.copy() if self._reference_units else {}

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------
    @classmethod
    def from_file(cls, table_path: str, table_format_type: str = "parquet", timezone: str = "UTC"):
        """Load the labs table from *table_path* and build a :class:`labs`."""
        data = load_data("labs", table_path, table_format_type, site_tz=timezone)
        return cls(data)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def isvalid(self) -> bool:
        """Return ``True`` if the last :pyfunc:`validate` finished without errors."""
        return not self.errors and not self.unit_validation_errors

    def validate(self):
        """Validate ``self.df`` against the mCIDE *LabsModel.json* spec and reference units."""
        if self.df is None:
            print("No dataframe to validate.")
            return

        # Run shared validation utility
        self.errors = validate_table(self.df, "labs")

        # Run reference unit validation
        self.validate_reference_units()

        # User-friendly status output
        total_errors = len(self.errors) + len(self.unit_validation_errors)
        if total_errors == 0:
            print("Validation completed successfully.")
        else:
            print(f"Validation completed with {total_errors} error(s).")
            if self.errors:
                print(f"  - {len(self.errors)} schema validation error(s)")
            if self.unit_validation_errors:
                print(f"  - {len(self.unit_validation_errors)} reference unit error(s)")
            print("See `errors` and `unit_validation_errors` attributes for details.")

    def validate_reference_units(self):
        """Validate reference units against expected values using grouped data for maximum efficiency."""
        self.unit_validation_errors = []
        
        if self.df is None or not self._reference_units:
            return
        
        required_columns = ['lab_category', 'reference_unit']
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            self.unit_validation_errors.append({
                'error_type': 'missing_columns',
                'message': f"Missing required columns for unit validation: {missing_columns}"
            })
            return

        # Group by lab_category and reference_unit to get unique combinations
        grouped = (self.df[required_columns]
                  .dropna()
                  .groupby(['lab_category', 'reference_unit'])
                  .size()
                  .reset_index(name='count'))
        
        if grouped.empty:
            return
        
        # Check each unique combination
        known_lab_categories = set(self._reference_units.keys())
        
        for _, row in grouped.iterrows():
            lab_category = row['lab_category']
            reference_unit = row['reference_unit']
            count = row['count']
            
            # Check if lab category is known
            if lab_category not in known_lab_categories:
                self.unit_validation_errors.append({
                    'error_type': 'unknown_lab_category',
                    'lab_category': lab_category,
                    'reference_unit': reference_unit,
                    'affected_rows': count,
                    'message': f"Unknown lab category '{lab_category}' with unit '{reference_unit}' affects {count} rows"
                })
                continue
            
            # Check if reference unit is valid for this lab category
            expected_units = self._reference_units[lab_category]
            if reference_unit not in expected_units:
                self.unit_validation_errors.append({
                    'error_type': 'invalid_reference_unit',
                    'lab_category': lab_category,
                    'reference_unit': reference_unit,
                    'expected_units': expected_units,
                    'affected_rows': count,
                    'message': f"Invalid reference unit '{reference_unit}' for lab '{lab_category}' affects {count} rows. Expected: {expected_units}"
                })

    def get_reference_unit_summary(self) -> pd.DataFrame:
        """Return a summary of reference units by lab category in the dataset (super efficient)."""
        if self.df is None or 'lab_category' not in self.df.columns or 'reference_unit' not in self.df.columns:
            return pd.DataFrame()
        
        # Get actual units used in the dataset with counts
        actual_units = (self.df[['lab_category', 'reference_unit']]
                       .dropna()
                       .groupby('lab_category')
                       .agg({
                           'reference_unit': lambda x: list(x.unique())
                       })
                       .reset_index())
        actual_units.columns = ['lab_category', 'actual_units']
        
        # Get expected units from schema
        expected_data = [{'lab_category': lab_cat, 'expected_units': units} 
                        for lab_cat, units in self._reference_units.items()]
        expected_df = pd.DataFrame(expected_data)
        
        # Merge actual and expected
        summary = pd.merge(expected_df, actual_units, on='lab_category', how='outer')
        
        # Add validation status
        def check_units_match(row):
            if pd.isna(row['actual_units']) or not row['actual_units']:
                return 'No data'
            actual = set(row['actual_units'])
            expected = set(row['expected_units'])
            return 'Valid' if actual.issubset(expected) else 'Invalid'
        
        summary['validation_status'] = summary.apply(check_units_match, axis=1)
        
        return summary

    def get_unit_combinations_with_counts(self) -> pd.DataFrame:
        """Get all unique lab_category + reference_unit combinations with their counts."""
        if self.df is None:
            return pd.DataFrame()
        
        required_columns = ['lab_category', 'reference_unit']
        if not all(col in self.df.columns for col in required_columns):
            return pd.DataFrame()
        
        # Group by lab_category and reference_unit to get counts
        combinations = (self.df[required_columns]
                       .dropna()
                       .groupby(['lab_category', 'reference_unit'])
                       .size()
                       .reset_index(name='count')
                       .sort_values(['lab_category', 'count'], ascending=[True, False]))
        
        # Add validation status
        if self._reference_units:
            def is_valid_combination(row):
                lab_cat = row['lab_category']
                ref_unit = row['reference_unit']
                
                if lab_cat not in self._reference_units:
                    return 'Unknown lab category'
                elif ref_unit not in self._reference_units[lab_cat]:
                    return 'Invalid unit'
                else:
                    return 'Valid'
            
            combinations['validation_status'] = combinations.apply(is_valid_combination, axis=1)
        
        return combinations

    def get_validation_summary_stats(self) -> Dict:
        """Get summary statistics for validation in the most efficient way."""
        if self.df is None or not self._reference_units:
            return {}
        
        required_columns = ['lab_category', 'reference_unit']
        if not all(col in self.df.columns for col in required_columns):
            return {'error': 'Missing required columns'}
        
        # Get unique combinations with counts
        combinations = self.get_unit_combinations_with_counts()
        
        if combinations.empty:
            return {'total_combinations': 0, 'total_rows': 0}
        
        # Calculate statistics
        total_combinations = len(combinations)
        total_rows = combinations['count'].sum()
        
        if 'validation_status' in combinations.columns:
            valid_combinations = len(combinations[combinations['validation_status'] == 'Valid'])
            valid_rows = combinations[combinations['validation_status'] == 'Valid']['count'].sum()
            
            invalid_combinations = total_combinations - valid_combinations
            invalid_rows = total_rows - valid_rows
            
            return {
                'total_combinations': total_combinations,
                'valid_combinations': valid_combinations,
                'invalid_combinations': invalid_combinations,
                'total_rows': total_rows,
                'valid_rows': valid_rows,
                'invalid_rows': invalid_rows,
                'combination_validation_rate': round((valid_combinations / total_combinations) * 100, 2),
                'row_validation_rate': round((valid_rows / total_rows) * 100, 2)
            }
        
        return {
            'total_combinations': total_combinations,
            'total_rows': total_rows
        }
    