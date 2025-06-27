from datetime import datetime
from enum import Enum
from typing import List, Dict, Optional
from tqdm import tqdm
import pandas as pd
import numpy as np
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
    def from_file(cls, table_path: str, table_format_type: str = "parquet", timezone: str = "UTC"):
        """Load the medication admin continuous table from *table_path* and build a :class:`medication_admin_continuous`."""
        data = load_data("medication_admin_continuous", table_path, table_format_type, site_tz=timezone)
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

    def convert_vasopressor_units(self, target_unit: str, vitals_table, weight_col: str = "weight") -> pd.DataFrame:
        """
        Convert vasopressor doses to standardized units using first charted weight per hospitalization.
        
        Parameters:
            target_unit: Target unit for conversion (e.g., "mcg/kg/min", "mcg/min", "units/hr")
            vitals_table: Vitals table object containing weight measurements
            weight_col: Column name for weight in vitals table (default: "weight")
            
        Returns:
            pd.DataFrame: Copy of medication data with converted vasopressor doses
            
        Note:
            - Uses first charted weight per hospitalization for all vasopressor doses in that encounter
            - Sets med_dose to NaN if weight required but not available
            - Only converts vasopressors; other medications remain unchanged
        """
        if self.df is None:
            print("No medication data available for conversion.")
            return pd.DataFrame()
        
        # Create a copy to avoid modifying original data
        converted_df = self.df.copy()
        
        # Define vasopressors that require weight-based conversion
        weight_based_vasopressors = [
            'norepinephrine', 'epinephrine', 'dopamine', 'dobutamine', 'phenylephrine'
        ]
        
        # Non-weight-based vasopressors
        non_weight_based_vasopressors = ['vasopressin']
        
        all_vasopressors = weight_based_vasopressors + non_weight_based_vasopressors
        
        # Filter to only vasopressors
        if 'med_category' not in converted_df.columns:
            print("Warning: 'med_category' column not found. Cannot identify vasopressors.")
            return converted_df
        
        vasopressor_mask = converted_df['med_category'].isin(all_vasopressors)
        
        if not vasopressor_mask.any():
            print("No vasopressors found in the dataset.")
            return converted_df
        
        print(f"Found {vasopressor_mask.sum()} vasopressor records to potentially convert.")
        
        # Extract first weight per hospitalization from vitals table
        weight_mapping = self._extract_first_weights(vitals_table, weight_col)
        
        if not weight_mapping:
            print("Warning: No weights found in vitals table.")
            # Set all weight-dependent vasopressor doses to NaN
            weight_dependent_mask = (vasopressor_mask & 
                                   converted_df['med_category'].isin(weight_based_vasopressors) &
                                   target_unit.endswith('/kg/min'))
            converted_df.loc[weight_dependent_mask, 'med_dose'] = np.nan
            return converted_df
        
        # Apply conversions to vasopressors
        for idx, row in converted_df[vasopressor_mask].iterrows():
            med_category = row['med_category']
            current_dose = row['med_dose']
            current_unit = row.get('med_unit', '')
            hospitalization_id = row['hospitalization_id']
            
            # Skip if current dose is already NaN
            if pd.isna(current_dose):
                continue
            
            # Get weight for this hospitalization
            weight_kg = weight_mapping.get(hospitalization_id)
            
            # Convert dose based on medication type and target unit
            converted_dose = self._convert_dose(
                current_dose, current_unit, target_unit, med_category, weight_kg
            )
            
            converted_df.loc[idx, 'med_dose'] = converted_dose
            converted_df.loc[idx, 'med_unit'] = target_unit
        
        # Add conversion tracking
        converted_df['unit_conversion_applied'] = vasopressor_mask
        
        conversions_applied = (vasopressor_mask & converted_df['med_dose'].notna()).sum()
        conversions_failed = (vasopressor_mask & converted_df['med_dose'].isna()).sum()
        
        print(f"Unit conversion completed:")
        print(f"  - Successful conversions: {conversions_applied}")
        print(f"  - Failed conversions (set to NaN): {conversions_failed}")
        
        return converted_df

    def _extract_first_weights(self, vitals_table, weight_col: str) -> Dict[str, float]:
        """Extract first charted weight per hospitalization from vitals table."""
        if vitals_table is None or vitals_table.df is None:
            print("Warning: Vitals table is None or empty.")
            return {}
        
        vitals_df = vitals_table.df
        
        # Check if required columns exist
        if weight_col not in vitals_df.columns:
            print(f"Warning: Weight column '{weight_col}' not found in vitals table.")
            return {}
        
        if 'hospitalization_id' not in vitals_df.columns:
            print("Warning: 'hospitalization_id' column not found in vitals table.")
            return {}
        
        # Get timestamp column for ordering (try common names)
        timestamp_cols = ['recorded_dttm', 'vitals_dttm', 'timestamp', 'admin_dttm']
        timestamp_col = None
        for col in timestamp_cols:
            if col in vitals_df.columns:
                timestamp_col = col
                break
        
        if timestamp_col is None:
            print("Warning: No timestamp column found in vitals table. Using first occurrence.")
            # Just get first non-null weight per hospitalization without time ordering
            first_weights = vitals_df.dropna(subset=[weight_col]).groupby('hospitalization_id')[weight_col].first()
        else:
            # Sort by timestamp and get first weight per hospitalization
            vitals_with_weight = vitals_df.dropna(subset=[weight_col]).copy()
            vitals_with_weight = vitals_with_weight.sort_values(['hospitalization_id', timestamp_col])
            first_weights = vitals_with_weight.groupby('hospitalization_id')[weight_col].first()
        
        weight_mapping = first_weights.to_dict()
        
        print(f"Extracted weights for {len(weight_mapping)} hospitalizations.")
        
        return weight_mapping

    def _convert_dose(self, current_dose: float, current_unit: str, target_unit: str, 
                     med_category: str, weight_kg: Optional[float]) -> float:
        """Convert medication dose from current unit to target unit."""
        
        # Handle vasopressin separately (non-weight-based)
        if med_category == 'vasopressin':
            if target_unit == 'units/hr':
                return self._convert_vasopressin_to_units_per_hour(current_dose, current_unit)
            else:
                print(f"Warning: Vasopressin conversion to {target_unit} not supported. Keeping original dose.")
                return current_dose
        
        # Handle weight-based vasopressors
        if target_unit.endswith('/kg/min'):
            if weight_kg is None or pd.isna(weight_kg):
                print(f"Warning: No weight available for {med_category} conversion. Setting dose to NaN.")
                return np.nan
            
            return self._convert_to_mcg_kg_min(current_dose, current_unit, weight_kg)
        
        elif target_unit == 'mcg/min':
            return self._convert_to_mcg_min(current_dose, current_unit)
        
        else:
            print(f"Warning: Conversion to {target_unit} not implemented. Keeping original dose.")
            return current_dose

    def _convert_to_mcg_kg_min(self, dose: float, current_unit: str, weight_kg: float) -> float:
        """Convert dose to mcg/kg/min."""
        if current_unit == 'mcg/kg/min':
            return dose
        
        # Common conversions
        if current_unit == 'mg/hr':
            # mg/hr to mcg/kg/min: (dose_mg * 1000) / (weight_kg * 60)
            return (dose * 1000) / (weight_kg * 60)
        
        elif current_unit == 'mcg/hr':
            # mcg/hr to mcg/kg/min: dose_mcg / (weight_kg * 60)
            return dose / (weight_kg * 60)
        
        elif current_unit == 'mcg/min':
            # mcg/min to mcg/kg/min: dose_mcg / weight_kg
            return dose / weight_kg
        
        elif current_unit == 'mg/min':
            # mg/min to mcg/kg/min: (dose_mg * 1000) / weight_kg
            return (dose * 1000) / weight_kg
        
        else:
            print(f"Warning: Conversion from {current_unit} to mcg/kg/min not implemented.")
            return np.nan

    def _convert_to_mcg_min(self, dose: float, current_unit: str) -> float:
        """Convert dose to mcg/min."""
        if current_unit == 'mcg/min':
            return dose
        
        if current_unit == 'mcg/hr':
            return dose / 60
        
        elif current_unit == 'mg/hr':
            return (dose * 1000) / 60
        
        elif current_unit == 'mg/min':
            return dose * 1000
        
        else:
            print(f"Warning: Conversion from {current_unit} to mcg/min not implemented.")
            return np.nan

    def _convert_vasopressin_to_units_per_hour(self, dose: float, current_unit: str) -> float:
        """Convert vasopressin dose to units/hr."""
        if current_unit == 'units/hr':
            return dose
        
        elif current_unit == 'units/min':
            return dose * 60
        
        elif current_unit == 'milliunits/min':
            return (dose / 1000) * 60
        
        else:
            print(f"Warning: Vasopressin conversion from {current_unit} to units/hr not implemented.")
            return np.nan