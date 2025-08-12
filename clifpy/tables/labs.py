from typing import Optional, Dict, List
import pandas as pd
from .base_table import BaseTable


class labs(BaseTable):
    """
    Labs table wrapper inheriting from BaseTable.
    
    This class handles laboratory data and validations including
    reference unit validation while leveraging the common functionality
    provided by BaseTable.
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
        Initialize the labs table.
        
        Parameters:
            data_directory (str): Path to the directory containing data files
            filetype (str): Type of data file (csv, parquet, etc.)
            timezone (str): Timezone for datetime columns
            output_directory (str, optional): Directory for saving output files and logs
            data (pd.DataFrame, optional): Pre-loaded data to use instead of loading from file
        """
        # For backward compatibility, handle the old signature
        if data_directory is None and filetype is None and data is not None:
            # Old signature: labs(data)
            # Use dummy values for required parameters
            data_directory = "."
            filetype = "parquet"
        
        # Initialize lab reference units
        self._lab_reference_units = None
        
        super().__init__(
            data_directory=data_directory,
            filetype=filetype,
            timezone=timezone,
            output_directory=output_directory,
            data=data
        )
        
        # Load lab-specific schema data
        self._load_labs_schema_data()

    def _load_labs_schema_data(self):
        """Load lab reference units from the YAML schema."""
        if self.schema:
            self._lab_reference_units = self.schema.get('lab_reference_units', {})

    @property
    def lab_reference_units(self) -> Dict[str, List[str]]:
        """Get the lab reference units mapping from the schema."""
        return self._lab_reference_units.copy() if self._lab_reference_units else {}
    
    # ------------------------------------------------------------------
    # Labs Specific Methods
    # ------------------------------------------------------------------
    def filter_by_lab_category(self, lab_category: str) -> pd.DataFrame:
        """Return all records for a specific lab category."""
        if self.df is None or 'lab_category' not in self.df.columns:
            return pd.DataFrame()
        
        return self.df[self.df['lab_category'] == lab_category].copy()
    
    def get_lab_summary_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category."""
        if self.df is None or 'lab_value_numeric' not in self.df.columns:
            return pd.DataFrame()
        
        # Group by lab category and calculate stats
        stats = self.df.groupby('lab_category')['lab_value_numeric'].agg([
            'count', 'mean', 'std', 'min', 'max',
            ('q1', lambda x: x.quantile(0.25)),
            ('median', lambda x: x.quantile(0.5)),
            ('q3', lambda x: x.quantile(0.75))
        ]).round(2)
        
        return stats