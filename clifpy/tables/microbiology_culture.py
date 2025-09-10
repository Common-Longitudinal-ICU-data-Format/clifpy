from typing import Optional, List, Dict
import pandas as pd
import json
import os
from .base_table import BaseTable


class MicrobiologyCulture(BaseTable):
    """
    Microbiology Culture table wrapper inheriting from BaseTable.
    
    This class handles microbiology culture-specific data and validations including
    organism identification validation and culture method validation.
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
        Initialize the microbiology culture table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # Initialize organism validation errors list
        self.organism_validation_errors: List[dict] = []
        
        # Load organism categories and fluid categories from schema
        self._organism_categories = None
        self._fluid_categories = None
        self._method_categories = None
        self._organism_groups = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load microbiology-specific schema data
        self._load_microbiology_schema_data()

    def _load_microbiology_schema_data(self):
        """Load organism categories, fluid categories, and method categories from the YAML schema."""
        if self.schema:
            self._organism_categories = self.schema.get('organism_categories', {})
            self._fluid_categories = self.schema.get('fluid_categories', {})
            self._method_categories = self.schema.get('method_categories', {})
            self._organism_groups = self.schema.get('organism_groups', {})

    @property
    def organism_categories(self) -> Dict[str, str]:
        """Get the organism categories mapping from the schema."""
        return self._organism_categories.copy() if self._organism_categories else {}

    @property
    def fluid_categories(self) -> Dict[str, str]:
        """Get the fluid categories mapping from the schema."""
        return self._fluid_categories.copy() if self._fluid_categories else {}

    @property
    def method_categories(self) -> Dict[str, str]:
        """Get the method categories mapping from the schema."""
        return self._method_categories.copy() if self._method_categories else {}

    @property
    def organism_groups(self) -> Dict[str, str]:
        """Get the organism groups mapping from the schema."""
        return self._organism_groups.copy() if self._organism_groups else {}

    def isvalid(self) -> bool:
        """Return ``True`` if the last validation finished without errors."""
        return not self.errors and not self.organism_validation_errors

    def _run_table_specific_validations(self):
        """
        Run microbiology culture-specific validations including organism validation.
        
        This overrides the base class method to add microbiology-specific validation.
        """
        # Run organism category validation
        self.validate_organism_categories()

    def validate_organism_categories(self):
        """Validate organism categories against known organism categories using grouped data for efficiency."""
        self.organism_validation_errors = []
        
        if self.df is None or not self._organism_categories:
            return
        
        required_columns = ['organism_category', 'organism_group', 'method_category', 'fluid_category']
        required_columns_for_df = [col for col in required_columns if col in self.df.columns]
        
        if not all(col in self.df.columns for col in ['organism_category']):
            self.organism_validation_errors.append({
                "error_type": "missing_columns_for_organism_validation",
                "columns": [col for col in ['organism_category'] if col not in self.df.columns],
                "message": "organism_category column missing, cannot perform organism validation."
            })
            return

        # Work on a copy for analysis
        df_for_validation = self.df[required_columns_for_df].copy()

        # Group by organism_category to get counts and check validity
        organism_stats = (df_for_validation
                         .groupby('organism_category')
                         .agg({
                             'organism_category': 'count',
                             'organism_group': lambda x: x.iloc[0] if 'organism_group' in df_for_validation.columns else None,
                             'method_category': lambda x: x.iloc[0] if 'method_category' in df_for_validation.columns else None,
                             'fluid_category': lambda x: x.iloc[0] if 'fluid_category' in df_for_validation.columns else None
                         })
                         .rename(columns={'organism_category': 'count'})
                         .reset_index())
        
        if organism_stats.empty:
            return
        
        # Check each organism category
        for _, row in organism_stats.iterrows():
            organism_category = row['organism_category']
            count = row['count']
            
            # Check if organism category is recognized
            if organism_category not in self._organism_categories:
                self.organism_validation_errors.append({
                    'error_type': 'unknown_organism_category',
                    'organism_category': organism_category,
                    'affected_rows': count,
                    'message': f"Unknown organism category '{organism_category}' found in data."
                })
                continue
            
            # Check organism group mapping if available
            if 'organism_group' in df_for_validation.columns and self._organism_groups:
                expected_group = self._organism_groups.get(organism_category)
                actual_group = row.get('organism_group')
                if expected_group and actual_group and expected_group != actual_group:
                    self.organism_validation_errors.append({
                        'error_type': 'organism_group_mismatch',
                        'organism_category': organism_category,
                        'expected_group': expected_group,
                        'actual_group': actual_group,
                        'affected_rows': count,
                        'message': f"Organism group mismatch for {organism_category}: expected '{expected_group}', found '{actual_group}'"
                    })
        
        # Validate method categories
        if 'method_category' in df_for_validation.columns and self._method_categories:
            invalid_methods = df_for_validation[
                ~df_for_validation['method_category'].isin(self._method_categories.keys())
            ]['method_category'].unique()
            
            for invalid_method in invalid_methods:
                count = df_for_validation[df_for_validation['method_category'] == invalid_method].shape[0]
                self.organism_validation_errors.append({
                    'error_type': 'unknown_method_category',
                    'method_category': invalid_method,
                    'affected_rows': count,
                    'message': f"Unknown method category '{invalid_method}' found in data."
                })
        
        # Validate fluid categories
        if 'fluid_category' in df_for_validation.columns and self._fluid_categories:
            invalid_fluids = df_for_validation[
                ~df_for_validation['fluid_category'].isin(self._fluid_categories.keys())
            ]['fluid_category'].unique()
            
            for invalid_fluid in invalid_fluids:
                count = df_for_validation[df_for_validation['fluid_category'] == invalid_fluid].shape[0]
                self.organism_validation_errors.append({
                    'error_type': 'unknown_fluid_category',
                    'fluid_category': invalid_fluid,
                    'affected_rows': count,
                    'message': f"Unknown fluid category '{invalid_fluid}' found in data."
                })
        
        # Add organism validation errors to main errors list
        if self.organism_validation_errors:
            self.errors.extend(self.organism_validation_errors)
            self.logger.warning(f"Found {len(self.organism_validation_errors)} organism validation errors")

    # ------------------------------------------------------------------
    # Microbiology Culture Specific Methods
    # ------------------------------------------------------------------
    def filter_by_organism_category(self, organism_category: str) -> pd.DataFrame:
        """Return all records for a specific organism category (e.g., 'acinetobacter_baumanii', 'candida_albicans')."""
        if self.df is None or 'organism_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['organism_category'] == organism_category].copy()

    def filter_by_fluid_category(self, fluid_category: str) -> pd.DataFrame:
        """Return all records for a specific fluid category (e.g., 'Blood/Buffy Coat', 'Brain')."""
        if self.df is None or 'fluid_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['fluid_category'] == fluid_category].copy()

    def filter_by_method_category(self, method_category: str) -> pd.DataFrame:
        """Return all records for a specific method category (e.g., 'culture', 'gram stain')."""
        if self.df is None or 'method_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['method_category'] == method_category].copy()

    def filter_by_organism_group(self, organism_group: str) -> pd.DataFrame:
        """Return all records for a specific organism group."""
        if self.df is None or 'organism_group' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['organism_group'] == organism_group].copy()

    def get_organism_summary_stats(self) -> pd.DataFrame:
        """Return summary statistics for each organism category."""
        if self.df is None or 'organism_category' not in self.df.columns:
            return pd.DataFrame()
        
        # Group by organism category and calculate stats
        stats = self.df.groupby('organism_category').agg({
            'patient_id': 'nunique',
            'hospitalization_id': 'nunique',
            'organism_id': 'count'
        }).rename(columns={
            'patient_id': 'unique_patients',
            'hospitalization_id': 'unique_hospitalizations',
            'organism_id': 'total_cultures'
        })
        
        return stats

    def get_fluid_summary_stats(self) -> pd.DataFrame:
        """Return summary statistics for each fluid category."""
        if self.df is None or 'fluid_category' not in self.df.columns:
            return pd.DataFrame()
        
        # Group by fluid category and calculate stats
        stats = self.df.groupby('fluid_category').agg({
            'patient_id': 'nunique',
            'hospitalization_id': 'nunique',
            'organism_id': 'count',
            'organism_category': 'nunique'
        }).rename(columns={
            'patient_id': 'unique_patients',
            'hospitalization_id': 'unique_hospitalizations',
            'organism_id': 'total_cultures',
            'organism_category': 'unique_organisms'
        })
        
        return stats

    def get_time_to_result_stats(self) -> pd.DataFrame:
        """Calculate time between collection and result for cultures."""
        if self.df is None:
            return pd.DataFrame()
        
        required_cols = ['collect_dttm', 'result_dttm']
        if not all(col in self.df.columns for col in required_cols):
            return pd.DataFrame()
        
        # Calculate time to result
        df_copy = self.df.copy()
        df_copy['collect_dttm'] = pd.to_datetime(df_copy['collect_dttm'])
        df_copy['result_dttm'] = pd.to_datetime(df_copy['result_dttm'])
        df_copy['time_to_result_hours'] = (
            df_copy['result_dttm'] - df_copy['collect_dttm']
        ).dt.total_seconds() / 3600
        
        # Calculate stats by organism category if available
        if 'organism_category' in df_copy.columns:
            stats = df_copy.groupby('organism_category')['time_to_result_hours'].agg([
                'count', 'mean', 'std', 'min', 'max',
                ('q1', lambda x: x.quantile(0.25)),
                ('median', lambda x: x.quantile(0.5)),
                ('q3', lambda x: x.quantile(0.75))
            ]).round(2)
        else:
            stats = df_copy['time_to_result_hours'].agg([
                'count', 'mean', 'std', 'min', 'max',
                ('q1', lambda x: x.quantile(0.25)),
                ('median', lambda x: x.quantile(0.5)),
                ('q3', lambda x: x.quantile(0.75))
            ]).round(2)
        
        return pd.DataFrame(stats).T if isinstance(stats, pd.Series) else stats

    def get_organism_validation_report(self) -> pd.DataFrame:
        """Return a DataFrame containing organism validation errors."""
        if not self.organism_validation_errors:
            return pd.DataFrame(columns=['error_type', 'organism_category', 'affected_rows', 'message'])
        
        return pd.DataFrame(self.organism_validation_errors)


# Create alias for backwards compatibility
microbiology_culture = MicrobiologyCulture
