from typing import Optional, Dict, List
import pandas as pd
from .base_table import BaseTable


class Labs(BaseTable):
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
    def get_lab_category_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.df is None
            or 'lab_value_numeric' not in self.df.columns
            or 'hospitalization_id' not in self.df.columns        # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        stats = (
            self.df
            .groupby('lab_category')
            .agg(
                count=('lab_value_numeric', 'count'),
                unique=('hospitalization_id', 'nunique'),
                missing_pct=('lab_value_numeric', lambda x: 100 * x.isna().mean()),
                mean=('lab_value_numeric', 'mean'),
                std=('lab_value_numeric', 'std'),
                min=('lab_value_numeric', 'min'),
                q1=('lab_value_numeric', lambda x: x.quantile(0.25)),
                median=('lab_value_numeric', 'median'),
                q3=('lab_value_numeric', lambda x: x.quantile(0.75)),
                max=('lab_value_numeric', 'max'),
            )
            .round(2)
        )

        return stats
    
    def get_lab_specimen_stats(self) -> pd.DataFrame:
        """Return summary statistics for each lab category, including missingness and unique hospitalization_id counts."""
        if (
            self.df is None
            or 'lab_value_numeric' not in self.df.columns
            or 'hospitalization_id' not in self.df.columns 
            or 'lab_speciment_category' not in self.df.columns       # remove this line if hosp-id is optional
        ):
            return {"status": "Missing columns"}
        
        stats = (
            self.df
            .groupby('lab_specimen_category')
            .agg(
                count=('lab_value_numeric', 'count'),
                unique=('hospitalization_id', 'nunique'),
                missing_pct=('lab_value_numeric', lambda x: 100 * x.isna().mean()),
                mean=('lab_value_numeric', 'mean'),
                std=('lab_value_numeric', 'std'),
                min=('lab_value_numeric', 'min'),
                q1=('lab_value_numeric', lambda x: x.quantile(0.25)),
                median=('lab_value_numeric', 'median'),
                q3=('lab_value_numeric', lambda x: x.quantile(0.75)),
                max=('lab_value_numeric', 'max'),
            )
            .round(2)
        )

        return stats