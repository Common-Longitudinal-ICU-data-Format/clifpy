from typing import Optional, List, Dict
import pandas as pd
import json
import os
from .base_table import BaseTable


class Vitals(BaseTable):
    """
    Vitals table wrapper inheriting from BaseTable.
    
    This class handles vitals-specific data and validations including
    range validation for vital signs.
    """
    
    def __init__(
        self,
        data_directory: str = None,
        filetype: str = None,
        timezone: str = "UTC",
        output_directory: Optional[str] = None,
        data: Optional[pd.DataFrame] = None
    ):
        """
        Initialize the vitals table.
        
        Parameters
        ----------
        data_directory : str
            Path to the directory containing data files
        filetype : str
            Type of data file (csv, parquet, etc.)
        timezone : str
            Timezone for datetime columns
        output_directory : str, optional
            Directory for saving output files and logs
        data : pd.DataFrame, optional
            Pre-loaded data to use instead of loading from file
        """
        # Initialize range validation errors list
        self.range_validation_errors: List[dict] = []
        
        # Load vital ranges and units from schema
        self._vital_units = None
        self._vital_ranges = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load vital-specific schema data
        self._load_vitals_schema_data()

    def _load_vitals_schema_data(self):
        """Load vital units and ranges from the YAML schema."""
        if self.schema:
            self._vital_units = self.schema.get('vital_units', {})
            self._vital_ranges = self.schema.get('vital_ranges', {})

    @property
    def vital_units(self) -> Dict[str, str]:
        """Get the vital units mapping from the schema."""
        return self._vital_units.copy() if self._vital_units else {}

    @property
    def vital_ranges(self) -> Dict[str, Dict[str, float]]:
        """Get the vital ranges from the schema."""
        return self._vital_ranges.copy() if self._vital_ranges else {}

    def isvalid(self) -> bool:
        """Return ``True`` if the last validation finished without errors."""
        return not self.errors and not self.range_validation_errors

    def _run_table_specific_validations(self):
        """
        Run vitals-specific validations including range validation.
        
        This overrides the base class method to add vitals-specific validation.
        """
        # Run vital range validation
        self.validate_vital_ranges()

    def validate_vital_ranges(self):
        """Validate vital values against expected ranges using grouped data for efficiency."""
        self.range_validation_errors = []
        
        if self.df is None or not self._vital_ranges:
            return
        
        required_columns = ['vital_category', 'vital_value']
        required_columns_for_df = ['vital_category', 'vital_value']
        if not all(col in self.df.columns for col in required_columns_for_df):
            self.range_validation_errors.append({
                "error_type": "missing_columns_for_range_validation",
                "columns": [col for col in required_columns_for_df if col not in self.df.columns],
                "message": "vital_category or vital_value column missing, cannot perform range validation."
            })
            return

        # Work on a copy to safely convert vital_value to numeric for aggregation
        df_for_stats = self.df[required_columns_for_df].copy()
        df_for_stats['vital_value'] = pd.to_numeric(df_for_stats['vital_value'], errors='coerce')

        # Filter out rows where vital_value could not be converted
        df_for_stats.dropna(subset=['vital_value'], inplace=True)

        if df_for_stats.empty:
            # No numeric vital_value data to perform range validation on
            return

        vital_stats = (df_for_stats
                       .groupby('vital_category')['vital_value']
                       .agg(['min', 'max', 'mean', 'count'])
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
                    'observed_min': min_val,
                    'observed_max': max_val,
                    'message': f"Unknown vital category '{vital_category}' found in data."
                })
                continue
            
            expected_range = self._vital_ranges[vital_category]
            expected_min = expected_range.get('min')
            expected_max = expected_range.get('max')
            
            # Check if any values are outside the expected range
            if expected_min is not None and min_val < expected_min:
                self.range_validation_errors.append({
                    'error_type': 'below_range',
                    'vital_category': vital_category,
                    'observed_min': min_val,
                    'expected_min': expected_min,
                    'message': f"Values below expected minimum for {vital_category}"
                })
            
            if expected_max is not None and max_val > expected_max:
                self.range_validation_errors.append({
                    'error_type': 'above_range',
                    'vital_category': vital_category,
                    'observed_max': max_val,
                    'expected_max': expected_max,
                    'message': f"Values above expected maximum for {vital_category}"
                })
        
        # Add range validation errors to main errors list
        if self.range_validation_errors:
            self.errors.extend(self.range_validation_errors)
            self.logger.warning(f"Found {len(self.range_validation_errors)} range validation errors")

    # ------------------------------------------------------------------
    # Vitals Specific Methods
    # ------------------------------------------------------------------
    def filter_by_vital_category(self, vital_category: str) -> pd.DataFrame:
        """Return all records for a specific vital category (e.g., 'heart_rate', 'temp_c')."""
        if self.df is None or 'vital_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['vital_category'] == vital_category].copy()

    def get_vital_summary_stats(self) -> pd.DataFrame:
        """Return summary statistics for each vital category."""
        if self.df is None or 'vital_value' not in self.df.columns:
            return pd.DataFrame()
        
        # Convert vital_value to numeric
        df_copy = self.df.copy()
        df_copy['vital_value'] = pd.to_numeric(df_copy['vital_value'], errors='coerce')
        
        # Group by vital category and calculate stats
        stats = df_copy.groupby('vital_category')['vital_value'].agg([
            'count', 'mean', 'std', 'min', 'max',
            ('q1', lambda x: x.quantile(0.25)),
            ('median', lambda x: x.quantile(0.5)),
            ('q3', lambda x: x.quantile(0.75))
        ]).round(2)
        
        return stats