from typing import Optional, Dict
import pandas as pd
from .base_table import BaseTable


class Hospitalization(BaseTable):
    """
    Hospitalization table wrapper inheriting from BaseTable.
    
    This class handles hospitalization-specific data and validations while
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
        Initialize the hospitalization table.
        
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
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: hospitalization(data)
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
    # Hospitalization Specific Methods
    # ------------------------------------------------------------------

    def calculate_length_of_stay(self) -> pd.DataFrame:
        """Calculate length of stay for each hospitalization and return DataFrame with LOS column."""
        if self.df is None:
            return pd.DataFrame()
        
        required_cols = ['admission_dttm', 'discharge_dttm']
        if not all(col in self.df.columns for col in required_cols):
            print(f"Missing required columns: {[col for col in required_cols if col not in self.df.columns]}")
            return pd.DataFrame()
        
        df_copy = self.df.copy()
        df_copy['admission_dttm'] = pd.to_datetime(df_copy['admission_dttm'])
        df_copy['discharge_dttm'] = pd.to_datetime(df_copy['discharge_dttm'])
        
        # Calculate LOS in days
        df_copy['length_of_stay_days'] = (df_copy['discharge_dttm'] - df_copy['admission_dttm']).dt.total_seconds() / (24 * 3600)
        
        return df_copy

    def get_mortality_rate(self) -> float:
        """Calculate in-hospital mortality rate."""
        if self.df is None or 'discharge_category' not in self.df.columns:
            return 0.0
        
        total_hospitalizations = len(self.df)
        if total_hospitalizations == 0:
            return 0.0
        
        expired_count = len(self.df[self.df['discharge_category'] == 'Expired'])
        return (expired_count / total_hospitalizations) * 100


    def get_summary_stats(self) -> Dict:
        """Return comprehensive summary statistics for hospitalization data."""
        if self.df is None:
            return {}
        
        stats = {
            'total_hospitalizations': len(self.df),
            'unique_patients': self.df['patient_id'].nunique() if 'patient_id' in self.df.columns else 0,
            'discharge_category_counts': self.df['discharge_category'].value_counts().to_dict() if 'discharge_category' in self.df.columns else {},
            'admission_type_counts': self.df['admission_type_category'].value_counts().to_dict() if 'admission_type_category' in self.df.columns else {},
            'date_range': {
                'earliest_admission': self.df['admission_dttm'].min() if 'admission_dttm' in self.df.columns else None,
                'latest_admission': self.df['admission_dttm'].max() if 'admission_dttm' in self.df.columns else None,
                'earliest_discharge': self.df['discharge_dttm'].min() if 'discharge_dttm' in self.df.columns else None,
                'latest_discharge': self.df['discharge_dttm'].max() if 'discharge_dttm' in self.df.columns else None
            }
        }
        
        # Age statistics
        if 'age_at_admission' in self.df.columns:
            age_data = self.df['age_at_admission'].dropna()
            if not age_data.empty:
                stats['age_stats'] = {
                    'mean': round(age_data.mean(), 1),
                    'median': age_data.median(),
                    'min': age_data.min(),
                    'max': age_data.max(),
                    'std': round(age_data.std(), 1)
                }
        
        # Length of stay statistics
        if all(col in self.df.columns for col in ['admission_dttm', 'discharge_dttm']):
            los_df = self.calculate_length_of_stay()
            if 'length_of_stay_days' in los_df.columns:
                los_data = los_df['length_of_stay_days'].dropna()
                if not los_data.empty:
                    stats['length_of_stay_stats'] = {
                        'mean_days': round(los_data.mean(), 1),
                        'median_days': round(los_data.median(), 1),
                        'min_days': round(los_data.min(), 1),
                        'max_days': round(los_data.max(), 1),
                        'std_days': round(los_data.std(), 1)
                    }
        
        # Mortality rate
        stats['mortality_rate_percent'] = round(self.get_mortality_rate(), 2)
        
        return stats

    def get_patient_hospitalization_counts(self) -> pd.DataFrame:
        """Return DataFrame with hospitalization counts per patient."""
        if self.df is None or 'patient_id' not in self.df.columns:
            return pd.DataFrame()
        
        patient_counts = (self.df.groupby('patient_id')
                         .agg({
                             'hospitalization_id': 'count',
                             'admission_dttm': ['min', 'max']
                         })
                         .reset_index())
        
        # Flatten column names
        patient_counts.columns = ['patient_id', 'hospitalization_count', 'first_admission', 'last_admission']
        
        # Calculate span of care
        patient_counts['first_admission'] = pd.to_datetime(patient_counts['first_admission'])
        patient_counts['last_admission'] = pd.to_datetime(patient_counts['last_admission'])
        patient_counts['care_span_days'] = (patient_counts['last_admission'] - patient_counts['first_admission']).dt.total_seconds() / (24 * 3600)
        
        return patient_counts.sort_values('hospitalization_count', ascending=False)