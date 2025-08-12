from datetime import datetime
from typing import Optional, List, Dict
import pandas as pd
from .base_table import BaseTable


class Adt(BaseTable):
    """
    ADT (Admission/Discharge/Transfer) table wrapper inheriting from BaseTable.
    
    This class handles ADT-specific data and validations while
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
        Initialize the ADT table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: adt(data)
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
    # ADT Specific Methods
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