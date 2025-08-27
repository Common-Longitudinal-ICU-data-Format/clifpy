from typing import Optional, List, Dict
import pandas as pd
from .base_table import BaseTable


class HospitalDiagnosis(BaseTable):
    """
    Hospital Diagnosis table wrapper inheriting from BaseTable.
    
    This class handles hospital diagnosis-specific data and validations while
    leveraging the common functionality provided by BaseTable.
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
        Initialize the hospital diagnosis table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: hospital_diagnosis(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
    
    # ------------------------------------------------------------------
    # Hospital Diagnosis Specific Methods
    # ------------------------------------------------------------------

    def get_diagnosis_types(self) -> List[str]:
        """Return unique diagnosis types in the dataset."""
        if self.df is None or 'diagnosis_type' not in self.df.columns:
            return []
        return self.df['diagnosis_type'].dropna().unique().tolist()

    def get_code_formats(self) -> List[str]:
        """Return unique diagnosis code formats in the dataset."""
        if self.df is None or 'diagnosis_code_format' not in self.df.columns:
            return []
        return self.df['diagnosis_code_format'].dropna().unique().tolist()

    def filter_by_hospitalization(self, hospitalization_id: str) -> pd.DataFrame:
        """Return all diagnosis records for a specific hospitalization."""
        if self.df is None:
            return pd.DataFrame()
        
        return self.df[self.df['hospitalization_id'] == hospitalization_id].copy()

    def filter_by_diagnosis_type(self, diagnosis_type: str) -> pd.DataFrame:
        """Return all records for a specific diagnosis type (e.g., 'Principal', 'Secondary')."""
        if self.df is None or 'diagnosis_type' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['diagnosis_type'] == diagnosis_type].copy()

    def filter_by_code_format(self, code_format: str) -> pd.DataFrame:
        """Return all records for a specific code format (e.g., 'ICD-10-CM', 'ICD-9-CM')."""
        if self.df is None or 'diagnosis_code_format' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['diagnosis_code_format'] == code_format].copy()

    def get_poa_statistics(self) -> Dict:
        """Calculate present on admission statistics."""
        if self.df is None or 'present_on_admission' not in self.df.columns:
            return {}
        
        total_records = len(self.df)
        if total_records == 0:
            return {}
        
        poa_counts = self.df['present_on_admission'].value_counts()
        poa_percentages = (poa_counts / total_records * 100).round(2)
        
        return {
            'total_diagnoses': total_records,
            'poa_counts': poa_counts.to_dict(),
            'poa_percentages': poa_percentages.to_dict()
        }

    def get_diagnosis_counts(self) -> Dict:
        """Count diagnoses by type and code format."""
        if self.df is None:
            return {}
        
        counts = {
            'total_diagnoses': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'unique_diagnosis_codes': self.df['diagnosis_code'].nunique() if 'diagnosis_code' in self.df.columns else 0
        }
        
        if 'diagnosis_type' in self.df.columns:
            counts['diagnosis_type_counts'] = self.df['diagnosis_type'].value_counts().to_dict()
        
        if 'diagnosis_code_format' in self.df.columns:
            counts['code_format_counts'] = self.df['diagnosis_code_format'].value_counts().to_dict()
        
        if 'present_on_admission' in self.df.columns:
            counts['poa_counts'] = self.df['present_on_admission'].value_counts().to_dict()
        
        return counts

    def get_principal_diagnoses(self) -> pd.DataFrame:
        """Return all principal diagnosis records."""
        return self.filter_by_diagnosis_type('Principal')

    def get_secondary_diagnoses(self) -> pd.DataFrame:
        """Return all secondary diagnosis records."""
        return self.filter_by_diagnosis_type('Secondary')

    def get_diagnoses_per_hospitalization(self) -> pd.DataFrame:
        """Return DataFrame with diagnosis counts per hospitalization."""
        if self.df is None or 'hospitalization_id' not in self.df.columns:
            return pd.DataFrame()
        
        diagnosis_counts = (self.df.groupby('hospitalization_id')
                          .agg({
                              'diagnosis_code': 'count',
                              'diagnosis_type': lambda x: (x == 'Principal').sum()
                          })
                          .reset_index())
        
        diagnosis_counts.columns = ['hospitalization_id', 'total_diagnoses', 'principal_diagnoses']
        diagnosis_counts['secondary_diagnoses'] = diagnosis_counts['total_diagnoses'] - diagnosis_counts['principal_diagnoses']
        
        return diagnosis_counts.sort_values('total_diagnoses', ascending=False)

    def get_most_common_diagnoses(self, top_n: int = 10) -> pd.DataFrame:
        """Return the most common diagnosis codes with their descriptions."""
        if self.df is None or 'diagnosis_code' not in self.df.columns:
            return pd.DataFrame()
        
        # Group by diagnosis code and get counts
        common_diagnoses = (self.df.groupby(['diagnosis_code', 'diagnosis_name'])
                          .size()
                          .reset_index(name='count'))
        
        # Handle cases where diagnosis_name might be missing
        if 'diagnosis_name' not in self.df.columns:
            common_diagnoses = (self.df.groupby('diagnosis_code')
                              .size()
                              .reset_index(name='count'))
            common_diagnoses['diagnosis_name'] = None
        
        return common_diagnoses.sort_values('count', ascending=False).head(top_n)

    def get_summary_stats(self) -> Dict:
        """Return comprehensive summary statistics for hospital diagnosis data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_diagnosis_records': len(self.df),
            'unique_hospitalizations': self.df['hospitalization_id'].nunique() if 'hospitalization_id' in self.df.columns else 0,
            'unique_diagnosis_codes': self.df['diagnosis_code'].nunique() if 'diagnosis_code' in self.df.columns else 0,
            'diagnosis_type_counts': self.df['diagnosis_type'].value_counts().to_dict() if 'diagnosis_type' in self.df.columns else {},
            'code_format_counts': self.df['diagnosis_code_format'].value_counts().to_dict() if 'diagnosis_code_format' in self.df.columns else {},
            'present_on_admission_counts': self.df['present_on_admission'].value_counts().to_dict() if 'present_on_admission' in self.df.columns else {}
        }
        
        # Calculate average diagnoses per hospitalization
        if 'hospitalization_id' in self.df.columns:
            diagnoses_per_hosp = self.get_diagnoses_per_hospitalization()
            if not diagnoses_per_hosp.empty:
                stats['avg_diagnoses_per_hospitalization'] = round(diagnoses_per_hosp['total_diagnoses'].mean(), 2)
                stats['max_diagnoses_per_hospitalization'] = diagnoses_per_hosp['total_diagnoses'].max()
                stats['min_diagnoses_per_hospitalization'] = diagnoses_per_hosp['total_diagnoses'].min()
        
        # Add present on admission percentages
        poa_stats = self.get_poa_statistics()
        if poa_stats:
            stats['poa_percentages'] = poa_stats.get('poa_percentages', {})
        
        return stats